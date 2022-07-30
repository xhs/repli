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

type Reader struct {
	readBatchSize    int
	readBatchLatency int
	minTTL           int
	C                chan *ReadCommand
	reader           *redis.Client
}

func NewReader(config *CommonConfig, readBatchSize, readBatchLatency int, minTTL int) *Reader {
	return &Reader{
		readBatchSize:    readBatchSize,
		readBatchLatency: readBatchLatency,
		minTTL:           minTTL,
		C:                make(chan *ReadCommand),
		reader:           config.Reader(),
	}
}

func (r *Reader) Close() {
	close(r.C)
	r.reader.Close()
}

func (r *Reader) TTL(key string) {
	r.C <- &ReadCommand{
		Command: "TTL",
		Key:     key,
	}
}

func (r *Reader) Dump(key string) {
	r.C <- &ReadCommand{
		Command: "DUMP",
		Key:     key,
	}
}

func (r *Reader) Run(l *log.Entry, writer *Writer, metrics *Metrics) {
	ctx := context.Background()

	var commands []*ReadCommand
	for {
		select {
		case cmd := <-r.C:
			commands = append(commands, cmd)
			if len(commands) < r.readBatchSize {
				continue
			}

		case <-time.After(time.Millisecond * time.Duration(r.readBatchLatency)):
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

						if cmd.TTL.Val().Seconds() < float64(r.minTTL) {
							// No need to replicate expiring key
							l.WithFields(log.Fields{
								"key": cmd.Key,
							}).Debug("expiring key ignored")
							continue
						}

						ttl := time.Second * time.Duration(cmd.TTL.Val().Seconds()-(float64(r.minTTL-1)))
						writer.Expire(cmd.Key, ttl)
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
						writer.Restore(cmd.Key, ttl, dumped)
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
