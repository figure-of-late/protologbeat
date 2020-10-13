package beater

import (
	"fmt"
  "time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"

	"github.com/mmguero-dev/protologbeat/config"
	"github.com/mmguero-dev/protologbeat/protolog"
)

type Protologbeat struct {
	done        chan struct{}
	config      config.Config
	client      beat.Client
	logListener *protolog.LogListener
}

// Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Protologbeat{
		done:        make(chan struct{}),
		config:      config,
		logListener: protolog.NewLogListener(config),
	}

	return bt, nil
}

func (bt *Protologbeat) Run(b *beat.Beat) error {
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

func (bt *Protologbeat) Stop() {
	bt.client.Close()
	close(bt.done)
	bt.logListener.Shutdown()
}
