package cmd

import (
	"github.com/spf13/cobra"
	"os"
	"pbench/presto"
	"pbench/run"
	"time"
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
	rootCmd.AddCommand(runCmd)
	wd, _ := os.Getwd()
	runCmd.Flags().StringVarP(&run.Name, "name", "n", "", `Assign a name to this run. (default: "<main stage name>-<current time>")`)
	runCmd.Flags().StringVarP(&run.Comment, "comment", "c", "", `Add a comment to this run (optional)`)
	runCmd.Flags().StringVarP(&run.ServerUrl, "server", "s", run.ServerUrl, "Presto server address")
	runCmd.Flags().StringVarP(&run.OutputPath, "output-path", "o", wd, "Output directory path")
	runCmd.Flags().BoolVarP(&run.IsTrino, "trino", "", false, "Use Trino protocol")
	runCmd.Flags().BoolVarP(&run.ForceHttps, "force-https", "", false, "Force all API requests to use HTTPS")
	runCmd.Flags().StringVarP(&run.UserName, "user", "u", presto.DefaultUser, "Presto user name")
	runCmd.Flags().StringVarP(&run.Password, "password", "p", "", "Presto user password (optional)")
	runCmd.Flags().Int64VarP(&run.RandSeed, "seed", "e", time.Now().UnixMicro(), "Random seed for randomized execution")
	runCmd.Flags().IntVarP(&run.RandSkip, "rand-skip", "k", 0, "Skip the first N random selections from the sequence (optional)")
	runCmd.Flags().StringVar(&run.InfluxCfgPath, "influx", "", "InfluxDB connection config for run recorder (optional)")
	runCmd.Flags().StringVar(&run.MySQLCfgPath, "mysql", "", "MySQL connection config for run recorder (optional)")
	runCmd.Flags().StringVar(&run.PulumiCfgPath, "pulumi", "", "(only works when a MySQL run recorder is specified) Pulumi API config for storing deployment details with MySQL (optional)")
}
