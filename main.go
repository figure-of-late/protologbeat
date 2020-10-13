package main

import (
	"os"

  "github.com/mmguero-dev/protologbeat/cmd"
)

func main() {
  if err := cmd.RootCmd.Execute(); err != nil {
    os.Exit(1)
  }
}
