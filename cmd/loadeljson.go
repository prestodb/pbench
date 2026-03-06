package cmd

import (
	"fmt"
	"os"
	"pbench/cmd/loadeljson"
	"pbench/utils"
	"runtime"
	"time"

	"github.com/spf13/cobra"
)

// loadElJsonCmd represents the loadeljson command
var loadElJsonCmd = &cobra.Command{
	Use:                   `loadeljson [flags] [list of files or directories to process]`,
	Short:                 "Load event listener JSON files into database and run recorders",
	Long:                  `Load event listener JSON files (QueryCompletedEvent) into database and run recorders`,
	DisableFlagsInUseLine: true,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("requires at least 1 arg, only received %d", len(args))
		}
		if loadeljson.Parallelism < 1 || loadeljson.Parallelism > runtime.NumCPU() {
			return fmt.Errorf("invalid parallelism: %d, it should be >= 1 and <= %d", loadeljson.Parallelism, runtime.NumCPU())
		}
		utils.ExpandHomeDirectory(&loadeljson.MySQLCfgPath)
		utils.ExpandHomeDirectory(&loadeljson.InfluxCfgPath)
		utils.ExpandHomeDirectory(&loadeljson.OutputPath)
		return nil
	},
	Run: loadeljson.Run,
}

func init() {
	RootCmd.AddCommand(loadElJsonCmd)
	wd, _ := os.Getwd()
	loadElJsonCmd.Flags().StringVarP(&loadeljson.RunName, "name", "n", fmt.Sprintf("load_el_%s", time.Now().Format(utils.DirectoryNameTimeFormat)), `Assign a name to this run. (default: "load_el_<current time>")`)
	loadElJsonCmd.Flags().StringVarP(&loadeljson.Comment, "comment", "c", "", `Add a comment to this run (optional)`)
	loadElJsonCmd.Flags().BoolVarP(&loadeljson.RecordRun, "record-run", "r", false, "Record all the loaded JSON as a run")
	loadElJsonCmd.Flags().StringVarP(&loadeljson.OutputPath, "output-path", "o", wd, "Output directory path")
	loadElJsonCmd.Flags().IntVarP(&loadeljson.Parallelism, "parallel", "P", runtime.NumCPU(), "Number of parallel threads to load json files")
	loadElJsonCmd.Flags().StringVar(&loadeljson.InfluxCfgPath, "influx", "", "InfluxDB connection config for run recorder (optional)")
	loadElJsonCmd.Flags().StringVar(&loadeljson.MySQLCfgPath, "mysql", "", "MySQL connection config for event listener and run recorder (optional)")
	loadElJsonCmd.Flags().BoolVar(&loadeljson.IsNDJSON, "ndjson", false, "Process files as NDJSON (newline-delimited JSON) format")
}
