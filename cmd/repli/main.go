package main

import (
	"context"
	"fmt"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"

	"github.com/go-redis/redis/v8"
)

var ctx = context.Background()
var config Config
var parser = flags.NewParser(&config, flags.Default)

type Config struct {
	SourceEndpoint    string `short:"f" long:"from" description:"Endpoint of source Redis instance" value-name:"[<IP>]:<PORT>" required:"true"`
	TargetEndpoint    string `short:"t" long:"to" description:"Endpoint of target Redis instance/cluster" value-name:"[<IP>]:<PORT>" required:"true"`
	RedisDatabase     int    `short:"d" long:"database" description:"Redis database to replicate" value-name:"<INT>" default:"0"`
	ClusterMode       bool   `short:"c" long:"cluster" description:"Replicate to Redis cluster" value-name:"<BOOL>"`
	ReplicatorNumber  int    `short:"n" long:"replicator-number" description:"Number of concurrent replicators" value-name:"<INT>" default:"1"`
	WriteBatchSize    int    `short:"b" long:"write-batch-size" description:"Batch size of Redis writing pipeline" value-name:"<INT>" default:"50"`
	WriteBatchLatency int    `short:"l" long:"write-batch-latency" description:"Maximum milliseconds before a batch is written" value-name:"<MILLISECONDS>" default:"100"`
	ReadBatchSize     int    `short:"B" long:"read-batch-size" description:"Batch size of Redis reading pipeline" value-name:"<INT>" default:"30"`
	ReadBatchLatency  int    `short:"L" long:"read-batch-latency" description:"Maximum milliseconds before a batch is read" value-name:"<MILLISECONDS>" default:"50"`
	EventQueueSize    int    `short:"s" long:"event-queue-size" description:"Size of keyspace event queue" value-name:"<INT>" default:"10000"`
	ReadTimeout       int    `long:"read-timeout" description:"Read timeout in seconds" value-name:"<SECONDS>" default:"5"`
	WriteTimeout      int    `long:"write-timeout" description:"Write timeout in seconds" value-name:"<SECONDS>" default:"5"`
	MaxRetries        int    `long:"max-retries" description:"Maximum number of retries before giving up" value-name:"<INT>" default:"10"`
	MinTTL            int    `short:"T" long:"min-ttl" description:"Minimum TTL in seconds, keys with remaining TTL less than this value will be ignored" value-name:"<SECONDS>" default:"3"`
	ReportInterval    int    `short:"i" long:"report-interval" description:"Interval seconds to log status report" value-name:"<SECONDS>" default:"5"`
}

type KeySpaceEvent struct {
	Key    string
	Action string
}

type WriteCommand struct {
	Timestamp int64 // TODO: ignore expired/expiring keys in batch
	Command   string
	Key       string
	TTL       time.Duration
	Bytes     []byte
}

type ReadCommand struct {
	Command string
	Key     string
	TTL     *redis.DurationCmd
	Bytes   *redis.StringCmd
}

type RedisWriter interface {
	Del(ctx context.Context, key ...string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	RestoreReplace(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd
	Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
	Close() error
}

func init() {
	log.SetLevel(log.InfoLevel)
}

func main() {
	_, err := parser.Parse()
	if err != nil {
		return
	}

	l := log.WithFields(log.Fields{
		"source": config.SourceEndpoint,
		"target": config.TargetEndpoint,
	})

	subscriber := redis.NewClient(&redis.Options{
		Addr: config.SourceEndpoint,
		DB:   config.RedisDatabase,
	})
	defer subscriber.Close()

	pattern := fmt.Sprintf("__keyspace@%d__:*", config.RedisDatabase)
	sub := subscriber.PSubscribe(ctx, pattern)
	defer sub.Close()

	// Wait for confirmation
	_, err = sub.Receive(ctx)
	if err != nil {
		panic(err)
	}

	eventCh := make(chan *KeySpaceEvent, config.EventQueueSize)

	var received int64 = 0
	var accepted int64 = 0
	var read int64 = 0
	var written int64 = 0

	go func(eventCh chan *KeySpaceEvent) {
		keyspaceEventCh := sub.Channel(redis.WithChannelSize(config.EventQueueSize))
		for event := range keyspaceEventCh {
			splits := strings.SplitN(event.Channel, ":", 2)
			if len(splits) != 2 {
				l.WithFields(log.Fields{
					"eventPayload": event.Payload,
				}).Error("unknown keyspace event")
				continue
			}

			key := splits[1]
			action := event.Payload
			log.Debug(key, ": ", action)

			eventCh <- &KeySpaceEvent{key, action}
			atomic.AddInt64(&received, 1)
		}
	}(eventCh)

	for i := 0; i < config.ReplicatorNumber; i++ {
		go func() {
			reader := redis.NewClient(&redis.Options{
				Addr:        config.SourceEndpoint,
				DB:          config.RedisDatabase,
				ReadTimeout: time.Second * time.Duration(config.ReadTimeout),
			})
			defer reader.Close()

			var writer RedisWriter
			if config.ClusterMode {
				writer = redis.NewClusterClient(&redis.ClusterOptions{
					Addrs:        []string{config.TargetEndpoint},
					ReadTimeout:  time.Second * time.Duration(config.ReadTimeout),
					WriteTimeout: time.Second * time.Duration(config.WriteTimeout),
					MaxRetries:   config.MaxRetries,
				})

			} else {
				writer = redis.NewClient(&redis.Options{
					Addr:         config.TargetEndpoint,
					DB:           config.RedisDatabase,
					ReadTimeout:  time.Second * time.Duration(config.ReadTimeout),
					WriteTimeout: time.Second * time.Duration(config.WriteTimeout),
					MaxRetries:   config.MaxRetries,
				})
			}
			defer writer.Close()

			writeCh := make(chan *WriteCommand)
			go func(writeCh chan *WriteCommand) {
				var commands []*WriteCommand
				for {
					select {
					case cmd := <-writeCh:
						commands = append(commands, cmd)
						if len(commands) < config.WriteBatchSize {
							continue
						}

					case <-time.After(time.Millisecond * time.Duration(config.WriteBatchLatency)):
						l.Debug("write batch timeout")
					}

					if len(commands) > 0 {
						results, err := writer.Pipelined(ctx, func(batch redis.Pipeliner) error {
							for _, cmd := range commands {
								atomic.AddInt64(&written, 1)

								switch cmd.Command {
								case "DELETE":
									batch.Del(ctx, cmd.Key)
								case "EXPIRE":
									batch.Expire(ctx, cmd.Key, cmd.TTL)
								case "RESTORE":
									batch.RestoreReplace(ctx, cmd.Key, cmd.TTL, string(cmd.Bytes))
								}
							}
							return nil
						})

						if err != nil {
							l.WithFields(log.Fields{
								"error": err,
							}).Warn("failed to write full batch")
						}

						for j, result := range results {
							if result.Err() == nil {
								continue
							}

							l.WithFields(log.Fields{
								"key":     commands[j].Key,
								"command": commands[j].Command,
								"error":   result.Err(),
							}).Error("failed to replicate key")
						}

						commands = nil
					}

					if len(commands) == 0 {
						cmd := <-writeCh
						commands = append(commands, cmd)
					}
				}
			}(writeCh)

			readCh := make(chan *ReadCommand)
			go func(readCh chan *ReadCommand) {
				var commands []*ReadCommand
				for {
					select {
					case cmd := <-readCh:
						commands = append(commands, cmd)
						if len(commands) < config.ReadBatchSize {
							continue
						}

					case <-time.After(time.Millisecond * time.Duration(config.ReadBatchLatency)):
						l.Debug("read batch timeout")
					}

					if len(commands) > 0 {
						_, err := reader.Pipelined(ctx, func(pipe redis.Pipeliner) error {
							for _, cmd := range commands {
								switch cmd.Command {
								case "TTL":
									cmd.TTL = pipe.TTL(ctx, cmd.Key)
								case "DUMP":
									cmd.Bytes = pipe.Dump(ctx, cmd.Key)
									cmd.TTL = pipe.PTTL(ctx, cmd.Key)
								}
							}
							return nil
						})

						if err != nil && err.Error() != "redis: nil" {
							l.WithFields(log.Fields{
								"error": err,
							}).Warn("failed to read full batch")
						}

						for _, cmd := range commands {
							atomic.AddInt64(&read, 1)

							switch cmd.Command {
							case "TTL":
								if cmd.TTL.Err() != nil {
									l.WithFields(log.Fields{
										"key":   cmd.Key,
										"error": cmd.TTL.Err(),
									}).Error("failed to get TTL")
								} else {
									if cmd.TTL.Val().Seconds() < float64(config.MinTTL) {
										// No need to replicate
										l.WithFields(log.Fields{
											"key": cmd.Key,
										}).Debug("expiring key ignored")
										continue
									}

									writeCmd := &WriteCommand{
										Timestamp: time.Now().UnixMilli(),
										Command:   "EXPIRE",
										Key:       cmd.Key,
										TTL:       time.Second * time.Duration(cmd.TTL.Val().Seconds()-(float64(config.MinTTL-1))),
									}
									writeCh <- writeCmd
								}
							case "DUMP":
								dumped, err := cmd.Bytes.Bytes()
								if err != nil && err.Error() == "redis: nil" {
									l.WithFields(log.Fields{
										"key": cmd.Key,
									}).Debug("key already gone")
									continue
								}

								if cmd.Bytes.Err() != nil || cmd.TTL.Err() != nil {
									l.WithFields(log.Fields{
										"key":              cmd.Key,
										"dumpCommandError": cmd.Bytes.Err(),
										"pttlCommandError": cmd.TTL.Err(),
									}).Error("failed to dump key")
								} else {
									writeCmd := &WriteCommand{
										Timestamp: time.Now().UnixMilli(),
										Command:   "RESTORE",
										Key:       cmd.Key,
										TTL:       time.Millisecond * time.Duration(cmd.TTL.Val().Milliseconds()),
										Bytes:     dumped,
									}
									writeCh <- writeCmd
								}
							}
						}

						commands = nil
					}

					if len(commands) == 0 {
						cmd := <-readCh
						commands = append(commands, cmd)
					}
				}
			}(readCh)

			for event := range eventCh {
				atomic.AddInt64(&accepted, 1)

				if event.Action == "expired" {
					l.WithFields(log.Fields{
						"key": event.Key,
					}).Debug("expired key ignored")
					continue
				}

				if event.Action == "del" {
					atomic.AddInt64(&read, 1)
					writeCmd := &WriteCommand{
						Timestamp: time.Now().UnixMilli(),
						Command:   "DELETE",
						Key:       event.Key,
					}
					writeCh <- writeCmd
					continue
				}

				if event.Action == "expire" {
					readCmd := &ReadCommand{
						Command: "TTL",
						Key:     event.Key,
					}
					readCh <- readCmd
					continue
				}

				readCmd := &ReadCommand{
					Command: "DUMP",
					Key:     event.Key,
				}
				readCh <- readCmd
			}
		}()
	}

	var lastReceived int64 = 0
	var lastAccepted int64 = 0
	var lastRead int64 = 0
	var lastWritten int64 = 0
	ticker := time.NewTicker(time.Second * time.Duration(config.ReportInterval))
	for {
		<-ticker.C

		arrived := received - lastReceived
		processed := accepted - lastAccepted
		queried := read - lastRead
		replicated := written - lastWritten

		lastReceived = received
		lastAccepted = accepted
		lastRead = read
		lastWritten = written

		queued := len(eventCh)
		if queued >= config.EventQueueSize {
			l.WithFields(log.Fields{
				"eventsArrived":   arrived,
				"eventsProcessed": processed,
				"keysQueried":     queried,
				"keysReplicated":  replicated,
				"replicators":     config.ReplicatorNumber,
				"queued":          fmt.Sprint(len(eventCh), "/", config.EventQueueSize),
			}).Error("keyspace event queue is full")
		} else {
			l.WithFields(log.Fields{
				"eventsArrived":   arrived,
				"eventsProcessed": processed,
				"keysQueried":     queried,
				"keysReplicated":  replicated,
				"replicators":     config.ReplicatorNumber,
				"queued":          fmt.Sprint(len(eventCh), "/", config.EventQueueSize),
			}).Info("")
		}
	}
}
