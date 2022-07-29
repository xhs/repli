package repli

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"regexp"
	"strconv"

	"github.com/go-redis/redis/v8"
	log "github.com/sirupsen/logrus"
)

type RedoLog struct {
	Level string `json:"level"`
	Key   string `json:"key,omitempty"`
}

type Redoer struct {
	cursor            int
	skipPatterns      []*regexp.Regexp
	deleteMissingKeys bool
	reader            *redis.Client
	writer            RedisWriter
}

func NewRedoer(config *CommonConfig, deleteMissingKeys bool) *Redoer {
	var skipPatterns []*regexp.Regexp
	for _, pattern := range config.SkipKeyPatterns {
		re, err := regexp.Compile(pattern)
		if err != nil {
			panic(err)
		}

		skipPatterns = append(skipPatterns, re)
	}

	return &Redoer{
		skipPatterns:      skipPatterns,
		deleteMissingKeys: deleteMissingKeys,
		reader:            config.Reader(),
		writer:            config.Writer(),
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
	if !os.IsNotExist(err) {
		content, err := ioutil.ReadFile(cursorFilename)
		if err != nil {
			panic(err)
		}
		r.cursor, err = strconv.Atoi(string(content))
		if err != nil {
			panic(err)
		}
	}

	var item RedoLog
	ctx := context.Background()
	offset := 0
loop:
	for scanner.Scan() {
		offset += 1
		if offset <= r.cursor {
			// Discard processed lines
			scanner.Text()
			continue
		}
		r.cursor = offset

		err = json.Unmarshal(scanner.Bytes(), &item)
		if err != nil {
			log.Warn(err)
			continue
		}

		if item.Key == "" || item.Level != "error" {
			continue
		}

		l := entry.WithFields(log.Fields{
			"key": item.Key,
		})
		for _, re := range r.skipPatterns {
			if re.Match([]byte(item.Key)) {
				l.Debug("skip pattern matched")

				continue loop
			}
		}

		l.Info("redo replication")
		dump := r.reader.Dump(ctx, item.Key)
		if dump.Err() != nil {
			if r.deleteMissingKeys && dump.Err().Error() == "redis: nil" {
				del := r.writer.Del(ctx, item.Key)
				if del.Err() != nil {
					l.Error(del.Err())
				}
			} else {
				l.Error(dump.Err())
			}
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

	os.WriteFile(cursorFilename, []byte(fmt.Sprint(r.cursor)), 0666)
}
