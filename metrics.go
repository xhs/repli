package repli

import (
	"fmt"
	"sync/atomic"
	"time"

	log "github.com/sirupsen/logrus"
)

type Metrics struct {
	mode            string
	EventsReceived  int64
	EventsProcessed int64
	KeysScanned     int64
	KeysQueried     int64
	KeysReplicated  int64
}

func NewMetrics(mode string) *Metrics {
	return &Metrics{
		mode: mode,
	}
}

func (m *Metrics) Received() {
	atomic.AddInt64(&m.EventsReceived, 1)
}

func (m *Metrics) Processed() {
	atomic.AddInt64(&m.EventsProcessed, 1)
}

func (m *Metrics) Scanned() {
	atomic.AddInt64(&m.KeysScanned, 1)
}

func (m *Metrics) Queried() {
	atomic.AddInt64(&m.KeysQueried, 1)
}

func (m *Metrics) Replicated() {
	atomic.AddInt64(&m.KeysReplicated, 1)
}

func (m *Metrics) Report(eventCh chan *KeyspaceEvent, reportInterval int, eventQueueSize int) {
	var lastReceived int64 = 0
	var lastProcessed int64 = 0
	var lastScanned int64 = 0
	var lastQueried int64 = 0
	var lastReplicated int64 = 0

	ticker := time.NewTicker(time.Second * time.Duration(reportInterval))
	for {
		<-ticker.C
		received := m.EventsReceived - lastReceived
		processed := m.EventsProcessed - lastProcessed
		scanned := m.KeysScanned - lastScanned
		queried := m.KeysQueried - lastQueried
		replicated := m.KeysReplicated - lastReplicated

		if m.mode == "snapshot" && scanned == 0 && m.KeysScanned == m.KeysReplicated {
			log.Info("snapshot completed")
			return
		}

		lastReceived = m.EventsReceived
		lastProcessed = m.EventsProcessed
		lastScanned = m.KeysScanned
		lastQueried = m.KeysQueried
		lastReplicated = m.KeysReplicated

		queued := len(eventCh)
		if queued >= eventQueueSize {
			log.WithFields(log.Fields{
				"eventsReceived":  received,
				"eventsProcessed": processed,
				"keysScanned":     scanned,
				"keysQueried":     queried,
				"keysReplicated":  replicated,
				"eventsQueued":    fmt.Sprint(len(eventCh), "/", eventQueueSize),
			}).Error("keyspace event queue is full")

		} else {
			log.WithFields(log.Fields{
				"eventsReceived":  received,
				"eventsProcessed": processed,
				"keysScanned":     scanned,
				"keysQueried":     queried,
				"keysReplicated":  replicated,
				"eventsQueued":    fmt.Sprint(len(eventCh), "/", eventQueueSize),
			}).Info("status report")
		}
	}
}
