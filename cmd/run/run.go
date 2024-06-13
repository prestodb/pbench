package run

import (
	"github.com/spf13/cobra"
	"pbench/cmd"
	"time"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:                   `run [flags] [list of root-level benchmark stage JSON files]`,
	Short:                 "Run a benchmark",
	Long:                  `Run a benchmark that is defined by a sequence of JSON configuration files.`,
	DisableFlagsInUseLine: true,
	Args:                  cobra.MinimumNArgs(1),
	Run:                   Run,
}

func init() {
	cmd.RootCmd.AddCommand(runCmd)
	runCmd.Flags().StringVarP(&Name, "name", "n", "", `Assign a name to this run. (default: "<main stage name>-<current time>")`)
	runCmd.Flags().StringVarP(&Comment, "comment", "c", "", `Add a comment to this run (optional)`)
	PrestoFlags.InstallPrestoFlags(runCmd)
	runCmd.Flags().Int64VarP(&RandSeed, "seed", "e", time.Now().UnixMicro(), "Random seed for randomized execution")
	runCmd.Flags().IntVarP(&RandSkip, "rand-skip", "k", 0, "Skip the first N random selections from the sequence (optional)")
	runCmd.Flags().StringVar(&InfluxCfgPath, "influx", "", "InfluxDB connection config for run recorder (optional)")
	runCmd.Flags().StringVar(&MySQLCfgPath, "mysql", "", "MySQL connection config for run recorder (optional)")
	runCmd.Flags().StringVar(&PulumiCfgPath, "pulumi", "", "(only works when a MySQL run recorder is specified) Pulumi API config for storing deployment details with MySQL (optional)")
}
