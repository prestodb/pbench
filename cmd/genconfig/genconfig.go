package genconfig

import (
	"github.com/spf13/cobra"
	"pbench/cmd"
)

var genConfigCmd = &cobra.Command{
	Use:                   `genconfig [flags] [directory to search recursively for config.json]`,
	DisableFlagsInUseLine: true,
	Run:                   Run,
	Args:                  cobra.ExactArgs(1),
	Short:                 "Generate benchmark cluster configurations",
}

func init() {
	cmd.RootCmd.AddCommand(genConfigCmd)
	genConfigCmd.Flags().StringVarP(&TemplatePath, "template-dir",
		"t", "", "Specifies the template directory. Use built-in template if not specified.")
	genConfigCmd.Flags().StringVarP(&ParameterPath, "parameter-file",
		"p", "", "Specifies the parameter file. Use built-in defaults if not specified.")
	genConfigCmd.AddCommand(&cobra.Command{
		Use:                   "default",
		Short:                 "Print the built-in default generator parameter file.",
		DisableFlagsInUseLine: true,
		Run:                   PrintDefaultParams,
	})
}
