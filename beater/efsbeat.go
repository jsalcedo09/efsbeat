package beater

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"time"

	"github.com/elastic/beats/libbeat/beat"
	"github.com/elastic/beats/libbeat/common"
	"github.com/elastic/beats/libbeat/logp"
	"github.com/elastic/beats/libbeat/publisher"

	"github.com/jsalcedo09/efsbeat/config"
)

//Efsbeat defines the struct info for the Beat
type Efsbeat struct {
	done   chan struct{}
	config config.Config
	client publisher.Client
	b      *beat.Beat
}

//EfsRoundValue AWS EFS size in bytes that files and folders will be rounded to 4Kb
const EfsRoundValue int64 = 4 * 1024

//EfsMetadata AWS EFS Size in bytes of the files metadata 2Kb
const EfsMetadata int64 = 2 * 1024

//New Creates beater
func New(b *beat.Beat, cfg *common.Config) (beat.Beater, error) {
	config := config.DefaultConfig
	if err := cfg.Unpack(&config); err != nil {
		return nil, fmt.Errorf("Error reading config file: %v", err)
	}

	bt := &Efsbeat{
		done:   make(chan struct{}),
		config: config,
	}
	return bt, nil
}

//Run Contains the main application loop that captures data and sends it to the defined output using the publisher
func (bt *Efsbeat) Run(b *beat.Beat) error {
	logp.Info("efsbeat is running! Hit CTRL-C to stop it.")
	bt.b = b
	bt.client = b.Publisher.Connect()
	ticker := time.NewTicker(bt.config.Period)
	for {
		select {
		case <-bt.done:
			return nil
		case <-ticker.C:
		}
		currentTime := time.Now()
		for _, path := range bt.config.Paths {
			files, err := filepath.Glob(path)
			if err != nil {
				logp.Err("Error resolving paths %v", err)
				return err
			}
			for _, file := range files {
				fi, err := os.Stat(file)
				if err != nil {
					logp.Err("Error getting stats %v", err)
					return err
				}
				switch mode := fi.Mode(); {
				case mode.IsDir():
					err := bt.walkAndPublishDir(file, currentTime)
					if err != nil {
						logp.Err("Error while walking directories %v", err)
						return err
					}
				case mode.IsRegular():
					if !bt.config.DirOnly {
						err := bt.walkAndPublishDir(file, currentTime)
						if err != nil {
							logp.Err("Error while walking directories %v", err)
							return err
						}
					}
				}
			}
		}

	}
}

func (bt *Efsbeat) walkAndPublishDir(path string, tickTime time.Time) error {
	var realSize int64
	var efsSize int64
	logp.Info("Calculating path %s size...", path)
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		var s = info.Size()
		realSize += s
		if s <= 0 {
			s = EfsMetadata
		}
		if info.IsDir() {
			efsSize += ((int64(math.Ceil(float64(s) / float64(EfsRoundValue)))) * EfsRoundValue)
		} else {
			efsSize += ((int64(math.Ceil(float64(s) / float64(EfsRoundValue)))) * EfsRoundValue) + EfsMetadata
		}

		return err
	})

	if err != nil {
		logp.Err("Error calculating path size %v", err)
	}

	event := common.MapStr{
		"@timestamp":      common.Time(tickTime),
		"type":            bt.b.Name,
		"path":            path,
		"size.real":       realSize,
		"size.efsmetered": efsSize,
	}

	bt.client.PublishEvent(event)
	return err
}

//Stop Contains logic that is called when the Beat is signaled to stop
func (bt *Efsbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}
