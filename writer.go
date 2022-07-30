package repli

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type WriteCommand struct {
	Command string
	Key     string
	TTL     time.Duration
	Bytes   []byte
}

type Writer struct {
	writeBatchSize    int
	writeBatchLatency int
	C                 chan *WriteCommand
	writer            RedisWriter
}

func NewWriter(config *CommonConfig, writeBatchSize, writeBatchLatency int) *Writer {
	return &Writer{
		writeBatchSize:    writeBatchSize,
		writeBatchLatency: writeBatchLatency,
		C:                 make(chan *WriteCommand),
		writer:            config.Writer(),
	}
}

func (w *Writer) Close() {
	close(w.C)
	w.writer.Close()
}

func (w *Writer) Delete(key string) {
	w.C <- &WriteCommand{
		Command: "DELETE",
		Key:     key,
	}
}

func (w *Writer) Expire(key string, ttl time.Duration) {
	w.C <- &WriteCommand{
		Command: "EXPIRE",
		Key:     key,
		TTL:     ttl,
	}
}

func (w *Writer) Restore(key string, ttl time.Duration, value []byte) {
	w.C <- &WriteCommand{
		Command: "RESTORE",
		Key:     key,
		TTL:     ttl,
		Bytes:   value,
	}
}

func (w *Writer) Run(l *log.Entry, metrics *Metrics) {
	ctx := context.Background()

	var commands []*WriteCommand
	for {
		select {
		case cmd := <-w.C:
			commands = append(commands, cmd)
			if len(commands) < w.writeBatchSize {
				continue
			}

		case <-time.After(time.Millisecond * time.Duration(w.writeBatchLatency)):
			l.Debug("write batch timeout")
		}

		if len(commands) > 0 {
			results, err := w.writer.Pipelined(ctx, func(batch redis.Pipeliner) error {
				for _, cmd := range commands {
					metrics.Replicated()

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
			cmd := <-w.C
			commands = append(commands, cmd)
		}
	}
}
