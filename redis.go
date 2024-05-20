package repli

import (
	"context"
	"time"

	"github.com/redis/go-redis/v9"
	log "github.com/sirupsen/logrus"
)

type RedisWriter interface {
	Del(ctx context.Context, key ...string) *redis.IntCmd
	Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd
	RestoreReplace(ctx context.Context, key string, ttl time.Duration, value string) *redis.StatusCmd
	Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error)
	Close() error
}

func (c *CommonConfig) Reader() *redis.Client {
	reader := redis.NewClient(&redis.Options{
		Addr:        c.SourceEndpoint,
		DB:          c.RedisDatabase,
		ReadTimeout: time.Second * time.Duration(c.ReadTimeout),
		PoolSize:    1,
		OnConnect: func(ctx context.Context, conn *redis.Conn) error {
			if c.ReadOnly {
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
	})

	return reader
}

func (c *CommonConfig) Writer() RedisWriter {
	var writer RedisWriter
	if c.ClusterMode {
		writer = redis.NewClusterClient(&redis.ClusterOptions{
			Addrs:        []string{c.TargetEndpoint},
			ReadTimeout:  time.Second * time.Duration(c.ReadTimeout),
			WriteTimeout: time.Second * time.Duration(c.WriteTimeout),
			MaxRetries:   c.MaxRetries,
			PoolSize:     1,
		})

	} else {
		writer = redis.NewClient(&redis.Options{
			Addr:         c.TargetEndpoint,
			DB:           c.RedisDatabase,
			ReadTimeout:  time.Second * time.Duration(c.ReadTimeout),
			WriteTimeout: time.Second * time.Duration(c.WriteTimeout),
			MaxRetries:   c.MaxRetries,
			PoolSize:     1,
		})
	}

	return writer
}
