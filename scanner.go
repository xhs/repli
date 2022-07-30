package repli

import (
	"context"
	"regexp"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type Scanner struct {
	keyspacePattern string
	skipPatterns    []*regexp.Regexp
	readBatchSize   int
	scanner         *redis.Client
	reader          *Reader
	writer          *Writer
}

func NewScanner(config *CommonConfig, readBatchSize, readBatchLatency int, writeBatchSize, writeBatchLatency int, minTTL int) *Scanner {
	var skipPatterns []*regexp.Regexp
	for _, pattern := range config.SkipKeyPatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			panic(err)
		}

		skipPatterns = append(skipPatterns, re)
	}

	return &Scanner{
		keyspacePattern: config.KeyspacePattern,
		skipPatterns:    skipPatterns,
		readBatchSize:   readBatchSize,
		scanner:         config.Reader(),
		reader:          NewReader(config, readBatchSize, readBatchLatency, minTTL),
		writer:          NewWriter(config, writeBatchSize, writeBatchLatency),
	}
}

func (s *Scanner) Close() {
	s.scanner.Close()
	s.reader.Close()
	s.writer.Close()
}

func (s *Scanner) Run(l *log.Entry, metrics *Metrics) {
	ctx := context.Background()
	var cursor uint64 = 0

	go s.writer.Run(l, metrics)
	go s.reader.Run(l, s.writer, metrics)

	for {
		keys, cursor, err := s.scanner.Scan(ctx, cursor, s.keyspacePattern, int64(s.readBatchSize)).Result()
		if err != nil {
			l.WithFields(log.Fields{
				"error":  err,
				"cursor": cursor,
			}).Error("failed to scan keys")
		}

		for _, key := range keys {
			metrics.Scanned()

			s.reader.Dump(key)
		}

		if cursor == 0 {
			break
		}
	}
}
