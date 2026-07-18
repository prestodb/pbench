package cmd

import (
	"os"
	"pbench/cmd/run"
	"time"

	"github.com/spf13/cobra"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:                   `run [flags] [list of root-level benchmark stage JSON files]`,
	Short:                 "Run a benchmark",
	Long:                  `Run a benchmark that is defined by a sequence of JSON configuration files.`,
	DisableFlagsInUseLine: true,
	Args:                  cobra.MinimumNArgs(1),
	Run:                   run.Run,
}

func init() {
	RootCmd.AddCommand(runCmd)
	runCmd.Flags().StringVarP(&run.Name, "name", "n", "", `Assign a name to this run. (default: "<main stage name>_<current time>")`)
	runCmd.Flags().StringVarP(&run.Comment, "comment", "c", "", `Add a comment to this run (optional)`)
	run.PrestoFlags.Install(runCmd)
	wd, _ := os.Getwd()
	runCmd.Flags().StringVarP(&run.OutputPath, "output-path", "o", wd, "Output directory path")
	runCmd.Flags().Int64VarP(&run.RandSeed, "seed", "e", time.Now().UnixMicro(), "Random seed for randomized execution")
	runCmd.Flags().IntVarP(&run.RandSkip, "rand-skip", "k", 0, "Skip the first N random selections from the sequence (optional)")
	runCmd.Flags().StringVar(&run.InfluxCfgPath, "influx", "", "InfluxDB connection config for run recorder (optional)")
	runCmd.Flags().StringVar(&run.MySQLCfgPath, "mysql", "", "MySQL connection config for run recorder (optional)")
	runCmd.Flags().StringVar(&run.PulumiCfgPath, "pulumi", "", "(only works when a MySQL run recorder is specified) Pulumi API config for storing deployment details with MySQL (optional)")
	runCmd.Flags().BoolVar(&run.Snapshots, "snapshots", false,
		"Periodically save /v1/query snapshots while queries run (DIAGNOSTIC MODE: adds coordinator load, not for timed baseline runs; default false)")
	runCmd.Flags().DurationVar(&run.SnapshotInterval, "snapshot-interval", 30*time.Second,
		"Polling interval for query json snapshots when --snapshots is on (floor 5s)")
	runCmd.Flags().IntVar(&run.SnapshotMax, "snapshot-max", 20,
		"Maximum snapshots retained per query (0 = unlimited; must be set explicitly)")
	runCmd.Flags().DurationVar(&run.SnapshotFetchTimeout, "snapshot-fetch-timeout", 0,
		"Per-fetch timeout for query json snapshots (default: same as --snapshot-interval)")
}
