package loadjson

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"pbench/cmd"
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
		if len(args) < 1 {
			return fmt.Errorf("requires at least 1 arg, only received %d", len(args))
		}
		if Parallelism < 1 || Parallelism > runtime.NumCPU() {
			return fmt.Errorf("invalid parallelism: %d, it should be >= 1 and <= %d", Parallelism, runtime.NumCPU())
		}
		utils.ExpandHomeDirectory(&OutputPath)
		return nil
	},
	Run: Run,
}

func init() {
	cmd.RootCmd.AddCommand(loadJsonCmd)
	wd, _ := os.Getwd()
	loadJsonCmd.Flags().StringVarP(&RunName, "name", "n", fmt.Sprintf("load_%s", time.Now().Format(utils.DirectoryNameTimeFormat)), `Assign a name to this run. (default: "load_<current time>")`)
	loadJsonCmd.Flags().StringVarP(&Comment, "comment", "c", "", `Add a comment to this run (optional)`)
	loadJsonCmd.Flags().BoolVarP(&RecordRun, "record-run", "r", false, "Record all the loaded JSON as a run")
	loadJsonCmd.Flags().StringVarP(&OutputPath, "output-path", "o", wd, "Output directory path")
	loadJsonCmd.Flags().BoolVarP(&ExtractPlanJson, "extract-plan", "x", false, "Extract the plan JSON from query JSON then save them to the output path")
	loadJsonCmd.Flags().IntVarP(&Parallelism, "parallel", "P", runtime.NumCPU(), "Number of parallel threads to load json files")
	loadJsonCmd.Flags().StringVar(&InfluxCfgPath, "influx", "", "InfluxDB connection config for run recorder (optional)")
	loadJsonCmd.Flags().StringVar(&MySQLCfgPath, "mysql", "", "MySQL connection config for event listener and run recorder (optional)")
}
