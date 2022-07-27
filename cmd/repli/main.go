package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"

	"github.com/go-redis/redis/v8"
)

var mode string
var ctx = context.Background()
var config Config
var parser = flags.NewParser(&config, flags.Default)

type Config struct {
	SourceEndpoint string `short:"f" long:"from" description:"Endpoint of source Redis instance" value-name:"[<IP>]:<PORT>" required:"true"`
	ReadOnly       bool   `long:"read-only" description:"Send READONLY command before replicating" value-name:"<BOOL>"`
	TargetEndpoint string `short:"t" long:"to" description:"Endpoint of target Redis instance/cluster" value-name:"[<IP>]:<PORT>" required:"true"`
	RedisDatabase  int    `short:"d" long:"database" description:"Redis database to replicate" value-name:"<INT>" default:"0"`
	ClusterMode    bool   `short:"c" long:"cluster" description:"Replicate to Redis cluster" value-name:"<BOOL>"`
	ReadTimeout    int    `long:"read-timeout" description:"Read timeout in seconds" value-name:"<SECONDS>" default:"5"`
	WriteTimeout   int    `long:"write-timeout" description:"Write timeout in seconds" value-name:"<SECONDS>" default:"5"`
	MaxRetries     int    `long:"max-retries" description:"Maximum number of retries before giving up" value-name:"<INT>" default:"10"`
}

type KeySpaceEvent struct {
	Key    string
	Action string
}

type WriteCommand struct {
	Timestamp int64 // TODO: ignore expired/expiring keys queued in batch
	Command   string
	Key       string
	TTL       time.Duration
	Bytes     []byte
}

func (w WriteCommand) Delete(key string) *WriteCommand {
	w.Timestamp = time.Now().UnixMilli()
	w.Command = "DELETE"
	w.Key = key
	return &w
}

func (w WriteCommand) Expire(key string, ttl time.Duration) *WriteCommand {
	w.Timestamp = time.Now().UnixMilli()
	w.Command = "EXPIRE"
	w.Key = key
	w.TTL = ttl
	return &w
}

func (w WriteCommand) Restore(key string, ttl time.Duration, value []byte) *WriteCommand {
	w.Timestamp = time.Now().UnixMilli()
	w.Command = "RESTORE"
	w.Key = key
	w.TTL = ttl
	w.Bytes = value
	return &w
}

type ReadCommand struct {
	Command string
	Key     string
	TTL     *redis.DurationCmd
	Bytes   *redis.StringCmd
}

func (r ReadCommand) GetTTL(key string) *ReadCommand {
	r.Command = "TTL"
	r.Key = key
	return &r
}

func (r ReadCommand) Dump(key string) *ReadCommand {
	r.Command = "DUMP"
	r.Key = key
	return &r
}

type RedisWriter interface {
	Del(ctx context.Context, key ...string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	RestoreReplace(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd
	Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
	Close() error
}

type LogItem struct {
	Level   string `json:"level"`
	Message string `json:"msg"`
	Key     string `json:"key,omitempty"`
}

func init() {
	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.JSONFormatter{})
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

	if mode == "REDO" {
		f, err := os.Open(redoConfig.RedoFile)
		if err != nil {
			panic(err)
		}

		scanner := bufio.NewScanner(f)
		scanner.Split(bufio.ScanLines)

		cursor := 0
		cursorFilename := redoConfig.RedoFile + ".cur"
		_, err = os.Stat(cursorFilename)
		var cursorFile *os.File
		if os.IsNotExist(err) {
			cursorFile, err = os.Create(cursorFilename)
			if err != nil {
				panic(err)
			}
		} else {
			cursorFile, err = os.Open(cursorFilename)
			if err != nil {
				panic(err)
			}

			buf := make([]byte, 16)
			n, err := cursorFile.Read(buf)
			if err != nil {
				panic(err)
			}

			cursor, err = strconv.Atoi(string(buf[:n]))
			if err != nil {
				panic(err)
			}
		}
		defer cursorFile.Close()

		reader := redis.NewClient(&redis.Options{
			Addr:        config.SourceEndpoint,
			DB:          config.RedisDatabase,
			ReadTimeout: time.Second * time.Duration(config.ReadTimeout),
			PoolSize:    1,
			OnConnect: func(ctx context.Context, conn *redis.Conn) error {
				if config.ReadOnly {
					ro := conn.ReadOnly(ctx)
					if ro.Err() != nil {
						l.WithFields(log.Fields{
							"error": ro.Err(),
						}).Warn("failed to execute READONLY command")

						return ro.Err()
					}
				}
				return nil
			},
		})
		defer reader.Close()

		var writer RedisWriter
		if config.ClusterMode {
			writer = redis.NewClusterClient(&redis.ClusterOptions{
				Addrs:        []string{config.TargetEndpoint},
				ReadTimeout:  time.Second * time.Duration(config.ReadTimeout),
				WriteTimeout: time.Second * time.Duration(config.WriteTimeout),
				MaxRetries:   config.MaxRetries,
				PoolSize:     1,
			})

		} else {
			writer = redis.NewClient(&redis.Options{
				Addr:         config.TargetEndpoint,
				DB:           config.RedisDatabase,
				ReadTimeout:  time.Second * time.Duration(config.ReadTimeout),
				WriteTimeout: time.Second * time.Duration(config.WriteTimeout),
				MaxRetries:   config.MaxRetries,
				PoolSize:     1,
			})
		}
		defer writer.Close()

		log.SetFormatter(&log.TextFormatter{})

		var item LogItem
		ptr := 0
		for scanner.Scan() {
			ptr += 1
			if ptr <= cursor {
				// Discard lines
				scanner.Text() // :(
				continue
			}

			err = json.Unmarshal(scanner.Bytes(), &item)
			if err != nil {
				l.Warn(err)
				continue
			}

			if item.Key == "" || item.Level != "error" || !strings.HasPrefix(item.Message, "failed") {
				continue
			}

			l.WithFields(log.Fields{
				"key": item.Key,
			}).Info("redo replication")

			dump := reader.Dump(ctx, item.Key)
			if dump.Err() != nil {
				l.Warn(dump.Err())
				continue
			}
			dumped, err := dump.Bytes()
			if err != nil {
				l.Warn(err)
				continue
			}

			pttl := reader.PTTL(ctx, item.Key)
			if pttl.Err() != nil {
				l.Warn(pttl.Err())
				continue
			}

			restore := writer.RestoreReplace(ctx, item.Key, pttl.Val(), string(dumped))
			if restore.Err() != nil {
				l.Error(restore.Err())
				continue
			}
		}

		cursorFile.WriteString(fmt.Sprintf("%v", ptr))

		return
	}

	l.Info(config)

	subscriber := redis.NewClient(&redis.Options{
		Addr:       config.SourceEndpoint,
		DB:         config.RedisDatabase,
		MaxRetries: config.MaxRetries,
		PoolSize:   1,
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

	eventCh := make(chan *KeySpaceEvent, runConfig.EventQueueSize)

	var received int64 = 0
	var accepted int64 = 0
	var read int64 = 0
	var written int64 = 0

	go func(eventCh chan *KeySpaceEvent) {
		keyspaceEventCh := sub.Channel(redis.WithChannelSize(runConfig.EventQueueSize))
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

	for i := 0; i < runConfig.ReplicatorNumber; i++ {
		go func() {
			reader := redis.NewClient(&redis.Options{
				Addr:        config.SourceEndpoint,
				DB:          config.RedisDatabase,
				ReadTimeout: time.Second * time.Duration(config.ReadTimeout),
				PoolSize:    1,
				OnConnect: func(ctx context.Context, conn *redis.Conn) error {
					if config.ReadOnly {
						ro := conn.ReadOnly(ctx)
						if ro.Err() != nil {
							l.WithFields(log.Fields{
								"error": ro.Err(),
							}).Warn("failed to execute READONLY command")

							return ro.Err()
						}
					}
					return nil
				},
			})
			defer reader.Close()

			var writer RedisWriter
			if config.ClusterMode {
				writer = redis.NewClusterClient(&redis.ClusterOptions{
					Addrs:        []string{config.TargetEndpoint},
					ReadTimeout:  time.Second * time.Duration(config.ReadTimeout),
					WriteTimeout: time.Second * time.Duration(config.WriteTimeout),
					MaxRetries:   config.MaxRetries,
					PoolSize:     1,
				})

			} else {
				writer = redis.NewClient(&redis.Options{
					Addr:         config.TargetEndpoint,
					DB:           config.RedisDatabase,
					ReadTimeout:  time.Second * time.Duration(config.ReadTimeout),
					WriteTimeout: time.Second * time.Duration(config.WriteTimeout),
					MaxRetries:   config.MaxRetries,
					PoolSize:     1,
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
						if len(commands) < runConfig.WriteBatchSize {
							continue
						}

					case <-time.After(time.Millisecond * time.Duration(runConfig.WriteBatchLatency)):
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
						if len(commands) < runConfig.ReadBatchSize {
							continue
						}

					case <-time.After(time.Millisecond * time.Duration(runConfig.ReadBatchLatency)):
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
									if cmd.TTL.Val() < 0 {
										// Very unlikely since we have received _expire_ event
										l.WithFields(log.Fields{
											"key": cmd.Key,
										}).Debug("never-expired key ignored")
										continue
									}

									if cmd.TTL.Val().Seconds() < float64(runConfig.MinTTL) {
										// No need to replicate expiring key
										l.WithFields(log.Fields{
											"key": cmd.Key,
										}).Debug("expiring key ignored")
										continue
									}

									ttl := time.Second * time.Duration(cmd.TTL.Val().Seconds()-(float64(runConfig.MinTTL-1)))
									writeCh <- WriteCommand{}.Expire(cmd.Key, ttl)
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
									ttl := time.Millisecond * time.Duration(cmd.TTL.Val().Milliseconds())
									writeCh <- WriteCommand{}.Restore(cmd.Key, ttl, dumped)
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
					writeCh <- WriteCommand{}.Delete(event.Key)
					continue
				}

				if event.Action == "expire" {
					readCh <- ReadCommand{}.GetTTL(event.Key)
					continue
				}

				readCh <- ReadCommand{}.Dump(event.Key)
			}
		}()
	}

	var lastReceived int64 = 0
	var lastAccepted int64 = 0
	var lastRead int64 = 0
	var lastWritten int64 = 0
	ticker := time.NewTicker(time.Second * time.Duration(runConfig.ReportInterval))
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
		if queued >= runConfig.EventQueueSize {
			l.WithFields(log.Fields{
				"eventsArrived":   arrived,
				"eventsProcessed": processed,
				"keysQueried":     queried,
				"keysReplicated":  replicated,
				"replicators":     runConfig.ReplicatorNumber,
				"queued":          fmt.Sprint(len(eventCh), "/", runConfig.EventQueueSize),
			}).Error("keyspace event queue is full")

		} else {
			l.WithFields(log.Fields{
				"eventsArrived":   arrived,
				"eventsProcessed": processed,
				"keysQueried":     queried,
				"keysReplicated":  replicated,
				"replicators":     runConfig.ReplicatorNumber,
				"queued":          fmt.Sprint(len(eventCh), "/", runConfig.EventQueueSize),
			}).Info("status report")
		}
	}
}
