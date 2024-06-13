package cmd

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"pbench/cmd/save"
	"pbench/utils"
	"runtime"
)

// saveCmd represents the save command
var saveCmd = &cobra.Command{
	Use:                   `save [flags] [list of table names]`,
	Short:                 "Save table information for recreating the schema and data",
	Long:                  `Save table information for recreating the schema and data`,
	DisableFlagsInUseLine: true,
	Args: func(cmd *cobra.Command, args []string) error {
		utils.ExpandHomeDirectory(&save.OutputPath)
		if save.InputFilePath != "" {
			utils.ExpandHomeDirectory(&save.InputFilePath)
		} else if len(args) < 1 {
			return fmt.Errorf("requires at least 1 arg when -f is not used")
		}
		return nil
	},
	Run: save.Run,
}

func init() {
	RootCmd.AddCommand(saveCmd)
	save.PrestoFlags.Install(saveCmd)
	wd, _ := os.Getwd()
	saveCmd.Flags().StringVarP(&save.OutputPath, "output-path", "o", wd, "Output directory path")
	saveCmd.Flags().StringVarP(&save.Catalog, "catalog", "", "", "Catalog name")
	saveCmd.Flags().StringVarP(&save.Schema, "schema", "", "", "Schema name")
	saveCmd.Flags().StringArrayVarP(&save.Session, "session", "", nil,
		"Session property (property can be used multiple times; format is\nkey=value; use 'SHOW SESSION' in Presto CLI to see available properties)")
	saveCmd.Flags().StringVarP(&save.InputFilePath, "file", "f", "", "CSV file to read catalog,schema,table.")
	saveCmd.Flags().IntVarP(&save.Parallelism, "parallel", "P", runtime.NumCPU(), "Number of parallel threads to save table summaries.")
}
