package cmd

import (
	"github.com/spf13/cobra"
	"os"
	"presto-benchmark/run"
	"presto-benchmark/stage"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use: `run 
	[-s | --server <server address>] [-o | --output-path <output_path>]
	[-m | --save-metadata] [-u | --user <username>] [-p | --password <password>]
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
	runCmd.Flags().StringVarP(&stage.DefaultServerUrl, "server", "s", stage.DefaultServerUrl, "Presto server address")
	runCmd.Flags().StringVarP(&run.OutputPath, "output-path", "o", wd, "Output directory path")
	runCmd.Flags().StringVarP(&run.UserName, "user", "u", "", "Presto user name")
	runCmd.Flags().StringVarP(&run.Password, "password", "p", "", "Presto user password")
	runCmd.Flags().BoolVarP(&stage.SaveColMetadata, "save-metadata", "m", false, "Save column metadata when query output is saved.")
}
