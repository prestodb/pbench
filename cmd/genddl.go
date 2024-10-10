package cmd

import (
	"github.com/spf13/cobra"
	"path/filepath"
	"pbench/cmd/genddl"
)

var genddlCmd = &cobra.Command{
	Use:                   `genddl [config file]`,
	DisableFlagsInUseLine: true,
	Run:                   genddl.Run,
	Args:                  cobra.ExactArgs(1),
	ValidArgsFunction:     fileCompletion,
	Short:                 "Generate DDL scripts based on a config file",
}

func init() {
	RootCmd.AddCommand(genddlCmd)
}

func fileCompletion(_ *cobra.Command, _ []string, toComplete string) ([]string, cobra.ShellCompDirective) {
	matches, err := filepath.Glob(toComplete + "*")
	if err != nil {
		return nil, cobra.ShellCompDirectiveDefault
	}

	return matches, cobra.ShellCompDirectiveDefault
}
