package repli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type RedoLog struct {
	Level string `json:"level"`
	Key   string `json:"key,omitempty"`
}

type Redoer struct {
	reader *redis.Client
	writer RedisWriter
	cursor int
}

func NewRedoer(config *CommonConfig) *Redoer {
	reader := redis.NewClient(&redis.Options{
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
	})

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

	return &Redoer{
		reader: reader,
		writer: writer,
	}
}

func (r *Redoer) Close() {
	r.reader.Close()
	r.writer.Close()
}

func (r *Redoer) Redo(entry *log.Entry, redoFile string) {
	f, err := os.Open(redoFile)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)

	cursorFilename := redoFile + ".cur"
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

		buf := make([]byte, 32)
		n, err := cursorFile.Read(buf)
		if err != nil {
			panic(err)
		}

		r.cursor, err = strconv.Atoi(string(buf[:n]))
		if err != nil {
			panic(err)
		}
	}
	defer cursorFile.Close()

	var item RedoLog
	ctx := context.Background()
	count := 0
	for scanner.Scan() {
		count += 1
		if count <= r.cursor {
			// Discard lines
			scanner.Text()
			continue
		}

		err = json.Unmarshal(scanner.Bytes(), &item)
		if err != nil {
			log.Warn(err)
			continue
		}

		if item.Key == "" || strings.HasPrefix(item.Key, "ip_") || item.Level != "error" {
			continue
		}

		l := entry.WithFields(log.Fields{
			"key": item.Key,
		})
		l.Info("redo replication")

		dump := r.reader.Dump(ctx, item.Key)
		if dump.Err() != nil {
			l.Error(dump.Err())
			continue
		}
		dumped, err := dump.Bytes()
		if err != nil {
			l.Error(err)
			continue
		}

		pttl := r.reader.PTTL(ctx, item.Key)
		if pttl.Err() != nil {
			l.Error(pttl.Err())
			continue
		}

		restore := r.writer.RestoreReplace(ctx, item.Key, pttl.Val(), string(dumped))
		if restore.Err() != nil {
			l.Error(restore.Err())
			continue
		}
	}

	cursorFile.WriteString(fmt.Sprint(r.cursor))
}
