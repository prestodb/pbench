package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"pbench/cmd/replay"
	"pbench/utils"
	"time"
)

// replayCmd represents the replay command
var replayCmd = &cobra.Command{
	Use:   `replay [flags] [workload csv file]`,
	Short: "Replay workload from a CSV file",
	Long: `Replay workload from a CSV file
The fields in the CSV file are:
"query_id","create_time","wall_time_millis","output_rows","written_output_rows","catalog","schema","session_properties","query"
We also expect the queries in this CSV file are sorted by "create_time" in ascending order.`,
	DisableFlagsInUseLine: true,
	Args: func(cmd *cobra.Command, args []string) error {
		if len(args) != 1 {
			return fmt.Errorf("requires 1 arg, only received %d", len(args))
		}
		utils.ExpandHomeDirectory(&replay.OutputPath)
		utils.ExpandHomeDirectory(&args[0])
		if replay.PrestoFlags.IsTrino {
			return fmt.Errorf("replay command does not support Trino yet")
		}
		return nil
	},
	Run: replay.Run,
}

func init() {
	RootCmd.AddCommand(replayCmd)
	wd, _ := os.Getwd()
	replay.PrestoFlags.Install(replayCmd)
	_ = replayCmd.Flags().MarkHidden("trino")
	replayCmd.Flags().StringVarP(&replay.OutputPath, "output-path", "o", wd, "Output directory path")
	replayCmd.Flags().StringVarP(&replay.RunName, "name", "n", fmt.Sprintf("replay_%s", time.Now().Format(utils.DirectoryNameTimeFormat)), `Assign a name to this run. (default: "replay_<current time>")`)
	replayCmd.Flags().IntVarP(&replay.Parallelism, "parallel", "P", 150, "Maximum number of concurrent queries to replay. Adjust this if the recorded workload has a higher server concurrency level.")
}
