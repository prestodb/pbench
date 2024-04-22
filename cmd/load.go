package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"pbench/load"
	"pbench/stage"
	"time"
)

// loadCmd represents the load command
var loadCmd = &cobra.Command{
	Use:                   `load [flags] [list of files or directories to process]`,
	Short:                 "Load query JSON files into event listener database and run recorders",
	Long:                  `Load query JSON files into event listener database and run recorders`,
	DisableFlagsInUseLine: true,
	Args:                  cobra.MinimumNArgs(1),
	Run:                   load.Run,
}

func init() {
	rootCmd.AddCommand(loadCmd)
	wd, _ := os.Getwd()
	loadCmd.Flags().StringVarP(&load.Name, "name", "n", fmt.Sprintf("load_%s", time.Now().Format(stage.RunNameTimeFormat)), `Assign a name to this run. (default: "load_<current time>")`)
	loadCmd.Flags().StringVarP(&load.Comment, "comment", "c", "", `Add a comment to this run (optional)`)
	loadCmd.Flags().StringVarP(&load.OutputPath, "output-path", "o", wd, "Output directory path")
	loadCmd.Flags().StringVar(&load.InfluxCfgPath, "influx", "", "InfluxDB connection config for run recorder (optional)")
	loadCmd.Flags().StringVar(&load.MySQLCfgPath, "mysql", "", "MySQL connection config for event listener and  run recorder (optional)")
}
