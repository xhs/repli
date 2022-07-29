package main

import (
	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"

	"github.com/xhs/repli"
)

var mode string

var config repli.CommonConfig
var parser = flags.NewParser(&config, flags.Default)

func init() {
	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.JSONFormatter{})
}

func main() {
	_, err := parser.Parse()
	if err != nil {
		return
	}

	l := log.WithFields(log.Fields{
		"source": config.SourceEndpoint,
		"target": config.TargetEndpoint,
	})

	if mode == "REDO" {
		redoer := repli.NewRedoer(&config)
		defer redoer.Close()

		redoer.Redo(l, redoConfig.RedoFile)

		return
	}

	l.Info(config)

	metrics := repli.NewMetrics()
	subscriber := repli.NewSubscriber(&config, runConfig.EventQueueSize)
	defer subscriber.Close()

	go subscriber.Run(l, metrics)

	for i := 0; i < runConfig.ReplicatorNumber; i++ {
		go func() {
			writer := repli.NewWriter(&config, runConfig.WriteBatchSize, runConfig.WriteBatchLatency)
			defer writer.Close()

			go writer.Run(l, metrics)

			reader := repli.NewReader(&config, runConfig.ReadBatchSize, runConfig.ReadBatchLatency, runConfig.MinTTL)
			defer reader.Close()

			go reader.Run(writer.C, l, metrics)

			for event := range subscriber.C {
				metrics.Processed()

				if event.Action == "expired" {
					l.WithFields(log.Fields{
						"key": event.Key,
					}).Debug("expired key ignored")
					continue
				}

				if event.Action == "del" {
					metrics.Queried()
					writer.C <- repli.WriteCommand{}.Delete(event.Key)
					continue
				}

				if event.Action == "expire" {
					reader.C <- repli.ReadCommand{}.GetTTL(event.Key)
					continue
				}

				reader.C <- repli.ReadCommand{}.Dump(event.Key)
			}
		}()
	}

	metrics.Report(subscriber.C, runConfig.ReportInterval, runConfig.EventQueueSize)
}
