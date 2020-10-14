package beater

import (
	"fmt"
	"time"

	"github.com/elastic/beats/v7/libbeat/beat"
	"github.com/elastic/beats/v7/libbeat/common"
	"github.com/elastic/beats/v7/libbeat/logp"

	"github.com/mmguero-dev/protologbeat/config"
	"github.com/mmguero-dev/protologbeat/protolog"
)

// protologbeat configuration.
type protologbeat struct {
	done   chan struct{}
	config config.Config
	client beat.Client
	logListener *protolog.LogListener
}

// New creates an instance of protologbeat.
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	c := config.DefaultConfig
	if err := cfg.Unpack(&c); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &protologbeat{
		done:   make(chan struct{}),
		config: c,
		logListener: protolog.NewLogListener(c),
	}
	return bt, nil
}

// Run starts protologbeat.
func (bt *protologbeat) Run(b *beat.Beat) error {
	logp.Info("protologbeat is running! Hit CTRL-C to stop it.")

	var err error
	bt.client, err = b.Publisher.Connect()
	if err != nil {
		return err
	}

	logEntriesRecieved := make(chan common.MapStr, 100000)
	logEntriesErrors := make(chan bool, 1)

	go func(logs chan common.MapStr, errs chan bool) {
		bt.logListener.Start(logs, errs)
	}(logEntriesRecieved, logEntriesErrors)

	var logEntry common.MapStr

	for {
		select {
		case <-bt.done:
			return nil
		case <-logEntriesErrors:
			return nil
		case logEntry = <-logEntriesRecieved:
			if logEntry == nil {
				return nil
			}
			if _, ok := logEntry["type"]; !ok {
				logEntry["type"] = bt.config.DefaultEsLogType
			}
      event := beat.Event{
        Timestamp: time.Now(),
        Fields: logEntry,
      }
			bt.client.Publish(event)
			// logp.Info("Event sent")
		}
	}

}

// Stop stops protologbeat.
func (bt *protologbeat) Stop() {
	bt.client.Close()
	close(bt.done)
	bt.logListener.Shutdown()
}
