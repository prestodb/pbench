package cmd

import (
	"github.com/spf13/cobra"
	"os"
	"presto-benchmark/run"
	"presto-benchmark/stage"
)

// runCmd represents the run command
var runCmd = &cobra.Command{
	Use:   "run",
	Short: "Run a benchmark",
	Long:  `Run a benchmark that is defined by a sequence of JSON configuration files.`,
	Args:  run.Args,
	Run:   run.Run,
}

func init() {
	rootCmd.AddCommand(runCmd)
	wd, _ := os.Getwd()
	runCmd.Flags().StringVarP(&stage.DefaultServerUrl, "server", "s", stage.DefaultServerUrl, "Presto server address")
	runCmd.Flags().StringVarP(&run.OutputPath, "output-path", "o", wd, "Output directory path")
}
