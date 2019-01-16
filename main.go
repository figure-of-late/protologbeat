package main

import (
	"os"

  "github.com/figure-of-late/protologbeat/cmd"
)

func main() {
  if err := cmd.RootCmd.Execute(); err != nil {
    os.Exit(1)
  }
}
