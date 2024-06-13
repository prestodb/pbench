//go:build experimental

package cmd

import (
	"github.com/spf13/cobra"
	"pbench/cmd/cmp"
)

var cmpCmd = &cobra.Command{
	Use:                   `cmp [flags] [directory 1] [directory 2]`,
	DisableFlagsInUseLine: true,
	Run:                   cmp.Run,
	Args:                  cobra.ExactArgs(2),
	Short:                 "Compare two query result directories",
}

func init() {
	RootCmd.AddCommand(cmpCmd)
	cmpCmd.Flags().StringVarP(&cmp.FileIdRegexStr, "file-id-regex", "r", `.*(query_\d{2}).*\.output`, "regex to extract file id from file names in two directories to find matching files to compare")
	cmpCmd.Flags().StringVarP(&cmp.OutputPath, "output-path", "o", "./diff", "diff output path")
}
