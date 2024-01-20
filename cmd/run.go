package cmd

import (
	"github.com/spf13/cobra"
	"os"
	"presto-benchmark/run"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use: `run 
	[-n | --name <run name>]
	[-s | --server <server address>] [-o | --output-path <output_path>]
	[-u | --user <username>] [-p | --password <password>]
	[-d | --database <InfluxDB connection config>]
	[<root-level benchmark stage JSON files>...]`,
	Short:                 "Run a benchmark",
	Long:                  `Run a benchmark that is defined by a sequence of JSON configuration files.`,
	DisableFlagsInUseLine: true,
	Args:                  cobra.MinimumNArgs(1),
	Run:                   run.Run,
}

func init() {
	rootCmd.AddCommand(runCmd)
	wd, _ := os.Getwd()
	runCmd.Flags().StringVarP(&run.Name, "name", "n", "", "Assign a name to this run. Default: <main stage name>-<current time>")
	runCmd.Flags().StringVarP(&run.ServerUrl, "server", "s", run.ServerUrl, "Presto server address")
	runCmd.Flags().StringVarP(&run.OutputPath, "output-path", "o", wd, "Output directory path")
	runCmd.Flags().StringVarP(&run.UserName, "user", "u", "", "Presto user name")
	runCmd.Flags().StringVarP(&run.Password, "password", "p", "", "Presto user password")
	runCmd.Flags().StringVarP(&run.InfluxCfgPath, "database", "d", run.InfluxCfgPath, "InfluxDB connection configuration file path")
}
