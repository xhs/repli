package main

import (
	"github.com/jessevdk/go-flags"
	log "github.com/sirupsen/logrus"

	"github.com/xhs/repli"
)

var command string

var config repli.CommonConfig
var parser = flags.NewParser(&config, flags.Default)

func init() {
	log.SetLevel(log.InfoLevel)
	log.SetFormatter(&log.JSONFormatter{
		DisableHTMLEscape: true,
	})
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
	l.Debug(config)

	if command == "REDO" {
		redoer := repli.NewRedoer(&config, redoCommand.DeleteMissingKeys)
		defer redoer.Close()

		redoer.Redo(l, redoCommand.RedoFile)
		return
	}

	metrics := repli.NewMetrics(runCommand.Mode)

	subscriber := repli.NewSubscriber(&config, runCommand.EventQueueSize)
	defer subscriber.Close()

	if runCommand.Mode != "live" {
		scanner := repli.NewScanner(&config, runCommand.ReadBatchSize, runCommand.ReadBatchLatency, runCommand.WriteBatchSize, runCommand.WriteBatchLatency, runCommand.MinTTL)
		defer scanner.Close()

		go scanner.Run(l, metrics)
	}

	if runCommand.Mode != "snapshot" {
		go subscriber.Run(l, metrics)

		for i := 0; i < runCommand.ReplicatorNumber; i++ {
			go func() {
				writer := repli.NewWriter(&config, runCommand.WriteBatchSize, runCommand.WriteBatchLatency)
				defer writer.Close()

				go writer.Run(l, metrics)

				reader := repli.NewReader(&config, runCommand.ReadBatchSize, runCommand.ReadBatchLatency, runCommand.MinTTL)
				defer reader.Close()

				go reader.Run(l, writer, metrics)

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
						writer.Delete(event.Key)
						continue
					}

					if event.Action == "expire" {
						reader.TTL(event.Key)
						continue
					}

					reader.Dump(event.Key)
				}
			}()
		}
	}

	metrics.Report(subscriber.C, runCommand.ReportInterval, runCommand.EventQueueSize)
}
