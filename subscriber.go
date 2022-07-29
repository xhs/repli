package repli

import (
	"context"
	"fmt"
	"strings"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type KeySpaceEvent struct {
	Key    string
	Action string
}

type Subscriber struct {
	eventQueueSize int
	keyPattern     string
	C              chan *KeySpaceEvent
	subscriber     *redis.Client
}

func NewSubscriber(config *CommonConfig, eventQueueSize int) *Subscriber {
	subscriber := redis.NewClient(&redis.Options{
		Addr:       config.SourceEndpoint,
		DB:         config.RedisDatabase,
		MaxRetries: config.MaxRetries,
		PoolSize:   1,
	})

	return &Subscriber{
		eventQueueSize: eventQueueSize,
		keyPattern:     fmt.Sprintf("__keyspace@%d__:*", config.RedisDatabase),
		C:              make(chan *KeySpaceEvent),
		subscriber:     subscriber,
	}
}

func (s *Subscriber) Close() error {
	return s.subscriber.Close()
}

func (s *Subscriber) Run(l *log.Entry, metrics *Metrics) {
	ctx := context.Background()
	sub := s.subscriber.PSubscribe(ctx, s.keyPattern)
	defer sub.Close()

	// Wait for confirmation
	_, err := sub.Receive(ctx)
	if err != nil {
		l.Fatal(err)
	}

	keyspaceEventCh := sub.Channel(redis.WithChannelSize(s.eventQueueSize))
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

		s.C <- &KeySpaceEvent{key, action}
		metrics.Received()
	}
}
