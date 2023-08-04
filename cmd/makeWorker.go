package cmd

import (
	"fmt"
	"os"
	"plugin"

	"github.com/spf13/cobra"

	mr "mapReduce/helper"

	log "github.com/sirupsen/logrus"
)

var makeWorkerCmd = &cobra.Command{
	Use:   "mrworkers",
	Short: "Create a worker",
	Run: func(_ *cobra.Command, args []string) {
		if len(args) != 1 {
			fmt.Fprintf(os.Stderr, "Usage: mrworker xyz.so\n")
			os.Exit(1)
		}
		mapf, reducef := loadPlugin(args[0])
		mr.StartWorkers(mapf, reducef)
	},
}

func init() {
	rootCmd.AddCommand(makeWorkerCmd)

	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}

func loadPlugin(filename string) (func(string, string) []mr.KeyValue, func(string, []string) string) {
	p, err := plugin.Open(filename)
	if err != nil {

		fmt.Printf("cannot load plugin %v", filename)
		log.WithFields(log.Fields{
			"actorType":  "worker",
			"methodName": "loadPlugin()",
		}).Fatal(err)
	}

	xmapf, err := p.Lookup("Map")
	if err != nil {
		log.WithFields(log.Fields{
			"actorType":  "worker",
			"methodName": "loadPlugin()",
		}).Fatalf("cannot find Map in %v", filename)
	}
	mapf := xmapf.(func(string, string) []mr.KeyValue)

	xreducef, err := p.Lookup("Reduce")
	if err != nil {
		log.WithFields(log.Fields{
			"actorType":  "worker",
			"methodName": "loadPlugin()",
		}).Fatalf("cannot find Reduce in %v", filename)
	}
	reducef := xreducef.(func(string, []string) string)

	return mapf, reducef
}
