package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"pbench/cmd/replay"
	"pbench/utils"
	"runtime"
	"time"
)

// replayCmd represents the replay command
var replayCmd = &cobra.Command{
	Use:                   `replay [flags] [list of files or directories to process]`,
	Short:                 "Produce a replay workload from query log",
	DisableFlagsInUseLine: true,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) < 1 {
			return fmt.Errorf("requires at least 1 arg, only received %d", len(args))
		}
		if replay.Parallelism < 1 || replay.Parallelism > runtime.NumCPU() {
			return fmt.Errorf("invalid parallelism: %d, it should be >= 1 and <= %d", replay.Parallelism, runtime.NumCPU())
		}
		utils.ExpandHomeDirectory(&replay.OutputPath)
		return nil
	},
	Run: replay.Run,
}

func init() {
	rootCmd.AddCommand(replayCmd)
	wd, _ := os.Getwd()
	replayCmd.Flags().StringVarP(&replay.RunName, "name", "n", fmt.Sprintf("replay_%s", time.Now().Format(utils.DirectoryNameTimeFormat)), `Assign a name to this run. (default: "replay_<current time>")`)
	replayCmd.Flags().StringVarP(&replay.OutputPath, "output-path", "o", wd, "Output directory path")
	replayCmd.Flags().IntVarP(&replay.Parallelism, "parallelism", "p", runtime.NumCPU(), "Number of parallel threads to load json files")
}
