package main

import (
	"os"

	"github.com/mmguero-dev/protologbeat/cmd"

	_ "github.com/mmguero-dev/protologbeat/include"
)

func main() {
	if err := cmd.RootCmd.Execute(); err != nil {
		os.Exit(1)
	}
}
