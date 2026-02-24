package cmd

import (
	"github.com/spf13/cobra"
	"pbench/cmd/genconfig"
)

var genConfigCmd = &cobra.Command{
	Use:                   `genconfig [flags] [directory to search recursively for genconfig.json]`,
	DisableFlagsInUseLine: true,
	Run:                   genconfig.Run,
	Args:                  cobra.ExactArgs(1),
	Short:                 "Generate benchmark cluster configurations",
}

func init() {
	RootCmd.AddCommand(genConfigCmd)
	genConfigCmd.Flags().StringVarP(&genconfig.TemplatePath, "template-dir",
		"t", "", "Specifies the template directory. Use built-in template if not specified.")
	genConfigCmd.Flags().StringArrayVarP(&genconfig.ParameterPaths, "parameter-file",
		"p", nil, "Specifies a parameter file. Can be repeated; later files override earlier ones. Use built-in defaults if not specified.")
	genConfigCmd.AddCommand(&cobra.Command{
		Use:                   "default",
		Short:                 "Print the built-in default generator parameter file.",
		DisableFlagsInUseLine: true,
		Run:                   genconfig.PrintDefaultParams,
	})
}
