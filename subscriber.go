package repli

import (
	"context"
	"fmt"
	"regexp"
	"strings"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type KeyspaceEvent struct {
	Key    string
	Action string
}

type Subscriber struct {
	eventQueueSize int
	keyPattern     string
	skipPatterns   []*regexp.Regexp
	subscriber     *redis.Client
	pubsub         *redis.PubSub
	C              chan *KeyspaceEvent
}

func NewSubscriber(config *CommonConfig, eventQueueSize int) *Subscriber {
	subscriber := redis.NewClient(&redis.Options{
		Addr:       config.SourceEndpoint,
		DB:         config.RedisDatabase,
		MaxRetries: config.MaxRetries,
		PoolSize:   1,
	})

	var skipPatterns []*regexp.Regexp
	for _, pattern := range config.SkipKeyPatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			panic(err)
		}

		skipPatterns = append(skipPatterns, re)
	}

	return &Subscriber{
		eventQueueSize: eventQueueSize,
		keyPattern:     fmt.Sprintf("__keyspace@%d__:%s", config.RedisDatabase, config.KeyspacePattern),
		skipPatterns:   skipPatterns,
		subscriber:     subscriber,
		C:              make(chan *KeyspaceEvent),
	}
}

func (s *Subscriber) Close() {
	close(s.C)
	if s.pubsub != nil {
		s.pubsub.Close()
	}
	s.subscriber.Close()
}

func (s *Subscriber) Run(l *log.Entry, metrics *Metrics) {
	ctx := context.Background()
	s.pubsub = s.subscriber.PSubscribe(ctx, s.keyPattern)

	// Wait for PSUBSCRIBE confirmation
	_, err := s.pubsub.Receive(ctx)
	if err != nil {
		l.Fatal(err)
	}

	keyspaceEventCh := s.pubsub.Channel(redis.WithChannelSize(s.eventQueueSize))
loop:
	for event := range keyspaceEventCh {
		splits := strings.SplitN(event.Channel, ":", 2)
		if len(splits) != 2 {
			l.WithFields(log.Fields{
				"eventPayload": event.Payload,
			}).Error("unknown keyspace event")
			continue
		}

		key := splits[1]
		for _, re := range s.skipPatterns {
			if re.Match([]byte(key)) {
				l.WithFields(log.Fields{
					"key": key,
				}).Debug("skip pattern matched")

				continue loop
			}
		}
		action := event.Payload

		metrics.Modify(key, action)
		s.C <- &KeyspaceEvent{key, action}
		metrics.Received()
	}
}
