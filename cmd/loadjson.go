package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"pbench/cmd/loadjson"
	"pbench/utils"
	"runtime"
	"time"
)

// loadJsonCmd represents the loadjson command
var loadJsonCmd = &cobra.Command{
	Use:                   `loadjson [flags] [list of files or directories to process]`,
	Short:                 "Load query JSON files into event listener database and run recorders",
	Long:                  `Load query JSON files into event listener database and run recorders`,
	DisableFlagsInUseLine: true,
	Args: func(cmd *cobra.Command, args []string) error {
		if err := cobra.MinimumNArgs(1)(cmd, args); err != nil {
			return err
		}
		if loadjson.Parallelism < 1 || loadjson.Parallelism > runtime.NumCPU() {
			return fmt.Errorf("invalid parallelism: %d, it should be >= 1 and <= %d", loadjson.Parallelism, runtime.NumCPU())
		}
		return nil
	},
	Run: loadjson.Run,
}

func init() {
	rootCmd.AddCommand(loadJsonCmd)
	wd, _ := os.Getwd()
	loadJsonCmd.Flags().StringVarP(&loadjson.RunName, "name", "n", fmt.Sprintf("load_%s", time.Now().Format(utils.DirectoryNameTimeFormat)), `Assign a name to this run. (default: "load_<current time>")`)
	loadJsonCmd.Flags().StringVarP(&loadjson.Comment, "comment", "c", "", `Add a comment to this run (optional)`)
	loadJsonCmd.Flags().BoolVarP(&loadjson.RecordRun, "record-run", "r", false, "Record all the loaded JSON as a run")
	loadJsonCmd.Flags().StringVarP(&loadjson.OutputPath, "output-path", "o", wd, "Output directory path")
	loadJsonCmd.Flags().IntVarP(&loadjson.Parallelism, "parallelism", "p", runtime.NumCPU(), "Number of parallel threads to load json files")
	loadJsonCmd.Flags().StringVar(&loadjson.InfluxCfgPath, "influx", "", "InfluxDB connection config for run recorder (optional)")
	loadJsonCmd.Flags().StringVar(&loadjson.MySQLCfgPath, "mysql", "", "MySQL connection config for event listener and run recorder (optional)")
}
