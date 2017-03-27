package main

import (
	"os"

	"github.com/elastic/beats/libbeat/beat"

	"github.com/jsalcedo09/efsbeat/beater"
)

func main() {
	err := beat.Run("efsbeat", "", beater.New)
	if err != nil {
		os.Exit(1)
	}
}
