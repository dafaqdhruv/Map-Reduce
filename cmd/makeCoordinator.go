package cmd

import (
	"fmt"
	mr "mapReduce/helper"
	"os"
	"time"

	log "github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

var makeCoordinatorCmd = &cobra.Command{
	Use:   "mrcoordinator",
	Short: "A brief description of your command",
	Run: func(_ *cobra.Command, args []string) {
		if len(args) < 1 {
			fmt.Fprintf(os.Stderr, "Usage: mrcoordinator inputfiles...\n")
			os.Exit(1)
		}

		coordinator := mr.MakeCoordinator(args[:], 6)
		for !coordinator.JobsFinished() {
			time.Sleep(time.Second * 2)
		}

		time.Sleep(time.Second * 2)
	},
}

func init() {
	rootCmd.AddCommand(makeCoordinatorCmd)

	log.SetFormatter(&log.TextFormatter{})
	log.SetOutput(os.Stdout)
	log.SetLevel(log.InfoLevel)
}
