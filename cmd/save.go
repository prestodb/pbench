package cmd

import (
	"github.com/spf13/cobra"
	"pbench/cmd/save"
)

// saveCmd represents the save command
var saveCmd = &cobra.Command{
	Use:                   `save [flags] [list of table names]`,
	Short:                 "Save table information for recreating the schema and data",
	Long:                  `Save table information for recreating the schema and data`,
	DisableFlagsInUseLine: true,
	Args:                  cobra.MinimumNArgs(1),
	Run:                   save.Run,
}

func init() {
	rootCmd.AddCommand(saveCmd)
	save.PrestoFlags.InstallPrestoFlags(saveCmd)
	saveCmd.Flags().StringVarP(&save.Catalog, "catalog", "", "", "Catalog name")
	saveCmd.Flags().StringVarP(&save.Schema, "schema", "", "", "Schema name")
	saveCmd.Flags().StringArrayVarP(&save.Session, "session", "", nil,
		"Session property (property can be used multiple times; format is\nkey=value; use 'SHOW SESSION' in Presto CLI to see available properties)")
}
