package save

import (
	"fmt"
	"github.com/spf13/cobra"
	"pbench/cmd"
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
		utils.ExpandHomeDirectory(&PrestoFlags.OutputPath)
		if InputFilePath != "" {
			utils.ExpandHomeDirectory(&InputFilePath)
		} else if len(args) < 1 {
			return fmt.Errorf("requires at least 1 arg when -f is not used")
		}
		return nil
	},
	Run: Run,
}

func init() {
	cmd.RootCmd.AddCommand(saveCmd)
	PrestoFlags.InstallPrestoFlags(saveCmd)
	saveCmd.Flags().StringVarP(&Catalog, "catalog", "", "", "Catalog name")
	saveCmd.Flags().StringVarP(&Schema, "schema", "", "", "Schema name")
	saveCmd.Flags().StringArrayVarP(&Session, "session", "", nil,
		"Session property (property can be used multiple times; format is\nkey=value; use 'SHOW SESSION' in Presto CLI to see available properties)")
	saveCmd.Flags().StringVarP(&InputFilePath, "file", "f", "", "CSV file to read catalog,schema,table.")
	saveCmd.Flags().IntVarP(&Parallelism, "parallel", "P", runtime.NumCPU(), "Number of parallel threads to save table summaries.")
}
