//go:build experimental

package cmd

import (
	"github.com/spf13/cobra"
	"pbench/cmd/round"
)

// roundCmd represents the round command
var roundCmd = &cobra.Command{
	Use:   `round [flags] [list of files or directories to process]`,
	Short: "Round the decimal values in the benchmark query output files for easier comparison.",
	Long: `The program will try to match every column in the first row to see which column has matching decimal.
After processing the first row, it will only look at the matched columns. So if the overly long decimal only appears from the second row, this might not work properly.
A PR was opened to fix the native/Java decimal precision discrepancy but so far it does not work quite well:
https://github.com/facebookincubator/velox/pull/7944`,
	DisableFlagsInUseLine: true,
	Args:                  round.Args,
	Run:                   round.Run,
}

func init() {
	RootCmd.AddCommand(roundCmd)
	roundCmd.Flags().IntVarP(&round.DecimalPrecision, "precision", "p", 12, "Decimal precision to preserve.")
	roundCmd.Flags().StringArrayVarP(&round.FileExtensions, "file-extension", "e", []string{".output"},
		"Specifies the file extensions ton include for processing (including the dot). You can specify multiple file extensions.")
	roundCmd.Flags().StringVarP(&round.FileFormat, "format", "f", "json",
		`Specifies the format of the files. Accepted values are: "csv"" or "json" which is the output file from the "run"" command`)
	roundCmd.Flags().BoolVarP(&round.InPlaceRewrite, "rewrite-in-place", "i", false,
		"When turned on, we will rewrite the file in-place. Otherwise, we save the rewritten file separately.")
	roundCmd.Flags().BoolVarP(&round.Recursive, "recursive", "r", false,
		`Recursively walk a path if a directory is provided in the arguments.`)
}
