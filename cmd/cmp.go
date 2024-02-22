package cmd

import (
	"github.com/spf13/cobra"
	"presto-benchmark/cmp"
)

var cmpCmd = &cobra.Command{
	Use:                   `cmp [flags] [directory 1] [directory 2]`,
	DisableFlagsInUseLine: true,
	Run:                   cmp.Run,
	Args:                  cobra.ExactArgs(2),
	Short:                 "Compare two query result directories",
}

func init() {
	rootCmd.AddCommand(cmpCmd)
	cmpCmd.Flags().StringVarP(&cmp.IdRegexStr, "id-regex", "r", `.*(query_\d{2}).*\.output`, "regex to extract result id from file names in two directories to find matching files to compare")
	cmpCmd.Flags().StringVarP(&cmp.OutputPath, "output-path", "o", "./diff", "diff output path")
}
