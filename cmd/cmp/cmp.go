//go:build experimental

package cmp

import (
	"github.com/spf13/cobra"
	"pbench/cmd"
)

var cmpCmd = &cobra.Command{
	Use:                   `cmp [flags] [directory 1] [directory 2]`,
	DisableFlagsInUseLine: true,
	Run:                   Run,
	Args:                  cobra.ExactArgs(2),
	Short:                 "Compare two query result directories",
}

func init() {
	cmd.RootCmd.AddCommand(cmpCmd)
	cmpCmd.Flags().StringVarP(&FileIdRegexStr, "file-id-regex", "r", `.*(query_\d{2}).*\.output`, "regex to extract file id from file names in two directories to find matching files to compare")
	cmpCmd.Flags().StringVarP(&OutputPath, "output-path", "o", "./diff", "diff output path")
}
