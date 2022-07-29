package repli

import (
	"context"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

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

type RedisWriter interface {
	Del(ctx context.Context, key ...string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	RestoreReplace(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd
	Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
	Close() error
}

type Writer struct {
	WriteBatchSize    int
	WriteBatchLatency int
	C                 chan *WriteCommand
	writer            RedisWriter
}

func NewWriter(config *CommonConfig, writeBatchSize, writeBatchLatency int) *Writer {
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

	return &Writer{
		WriteBatchSize:    writeBatchSize,
		WriteBatchLatency: writeBatchLatency,
		C:                 make(chan *WriteCommand),
		writer:            writer,
	}
}

func (w *Writer) Close() error {
	return w.writer.Close()
}

func (w *Writer) Run(l *log.Entry, metrics *Metrics) {
	ctx := context.Background()

	var commands []*WriteCommand
	for {
		select {
		case cmd := <-w.C:
			commands = append(commands, cmd)
			if len(commands) < w.WriteBatchSize {
				continue
			}

		case <-time.After(time.Millisecond * time.Duration(w.WriteBatchLatency)):
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
