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

type Efsbeat struct {
	done   chan struct{}
	config config.Config
	client publisher.Client
	b      *beat.Beat
}

const EFS_ROUND_VALUE int64 = 4 * 1024 //4Kb
const EFS_METADATA int64 = 2 * 1024    //2Kb

// Creates beater
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
					err := bt.walkAndPublishDir(file)
					if err != nil {
						logp.Err("Error while walking directories %v", err)
						return err
					}
				case mode.IsRegular():
					if !bt.config.DirOnly {
						err := bt.walkAndPublishDir(file)
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

func (bt *Efsbeat) walkAndPublishDir(path string) error {
	var realSize int64
	var efsSize int64
	logp.Info("Calculating path %s size...", path)
	err := filepath.Walk(path, func(_ string, info os.FileInfo, err error) error {
		var s = info.Size()
		realSize += s
		if s <= 0 {
			s = EFS_METADATA
		}
		if info.IsDir() {
			efsSize += ((int64(math.Ceil(float64(s) / float64(EFS_ROUND_VALUE)))) * EFS_ROUND_VALUE)
		} else {
			efsSize += ((int64(math.Ceil(float64(s) / float64(EFS_ROUND_VALUE)))) * EFS_ROUND_VALUE) + EFS_METADATA
		}

		return err
	})
	logp.Info("%v %v %s", realSize, efsSize, path)
	if err != nil {
		logp.Err("Error calculating path size %v", err)
	}

	event := common.MapStr{
		"@timestamp":      common.Time(time.Now()),
		"type":            bt.b.Name,
		"path":            path,
		"size.real":       realSize,
		"size.efsmetered": efsSize,
	}

	bt.client.PublishEvent(event)
	return err
}

func (bt *Efsbeat) Stop() {
	bt.client.Close()
	close(bt.done)
}
