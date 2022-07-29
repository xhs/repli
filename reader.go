package repli

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

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

type Reader struct {
	ReadBatchSize    int
	ReadBatchLatency int
	MinTTL           int
	C                chan *ReadCommand
	reader           *redis.Client
}

func NewReader(config *CommonConfig, readBatchSize, readBatchLatency int, minTTL int) *Reader {
	return &Reader{
		ReadBatchSize:    readBatchSize,
		ReadBatchLatency: readBatchLatency,
		MinTTL:           minTTL,
		C:                make(chan *ReadCommand),
		reader: redis.NewClient(&redis.Options{
			Addr:        config.SourceEndpoint,
			DB:          config.RedisDatabase,
			ReadTimeout: time.Second * time.Duration(config.ReadTimeout),
			PoolSize:    1,
			OnConnect: func(ctx context.Context, conn *redis.Conn) error {
				if config.ReadOnly {
					ro := conn.ReadOnly(ctx)
					if ro.Err() != nil {
						log.WithFields(log.Fields{
							"error": ro.Err(),
						}).Warn("failed to execute READONLY command")

						return ro.Err()
					}
				}
				return nil
			},
		}),
	}
}

func (r *Reader) Close() error {
	return r.reader.Close()
}

func (r *Reader) Run(writeCh chan *WriteCommand, l *log.Entry, metrics *Metrics) {
	ctx := context.Background()

	var commands []*ReadCommand
	for {
		select {
		case cmd := <-r.C:
			commands = append(commands, cmd)
			if len(commands) < r.ReadBatchSize {
				continue
			}

		case <-time.After(time.Millisecond * time.Duration(r.ReadBatchLatency)):
			l.Debug("read batch timeout")
		}

		if len(commands) > 0 {
			_, err := r.reader.Pipelined(ctx, func(pipe redis.Pipeliner) error {
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
				metrics.Queried()

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

						if cmd.TTL.Val().Seconds() < float64(r.MinTTL) {
							// No need to replicate expiring key
							l.WithFields(log.Fields{
								"key": cmd.Key,
							}).Debug("expiring key ignored")
							continue
						}

						ttl := time.Second * time.Duration(cmd.TTL.Val().Seconds()-(float64(r.MinTTL-1)))
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
			cmd := <-r.C
			commands = append(commands, cmd)
		}
	}
}
