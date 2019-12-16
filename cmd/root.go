package cmd

import (
  "github.com/elastic/beats/libbeat/cmd"
  "github.com/elastic/beats/libbeat/cmd/instance"

	"github.com/mmguero/protologbeat/beater"
)

// RootCmd to handle beats cli
var RootCmd = cmd.GenRootCmdWithSettings(beater.New, instance.Settings{Name: "protologbeat"})
