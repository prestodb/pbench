package load

import (
	"database/sql"
	"encoding/json"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"pbench/log"
	"pbench/presto"
	"pbench/utils"
)

var (
	Name          string
	Comment       string
	OutputPath    string
	InfluxCfgPath string
	MySQLCfgPath  string

	fileLoaded int
	mysqlDb    *sql.DB
)

func Run(_ *cobra.Command, args []string) {
	utils.PrepareOutputDirectory(OutputPath)
	mysqlDb = utils.InitMySQLConnFromCfg(MySQLCfgPath)
	for _, path := range args {
		if err := processPath(path); err != nil {
			log.Error().Str("path", path).Err(err).Msg("failed to process path")
		}
	}
	log.Info().Int("file_loaded", fileLoaded).Send()
}

func processPath(path string) error {
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return processFile(path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		fullPath := filepath.Join(path, entry.Name())
		if entry.IsDir() {
			continue
		} else {
			if err = processFile(fullPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func processFile(path string) (err error) {
	bytes, ioErr := os.ReadFile(path)
	if ioErr != nil {
		return ioErr
	}
	queryInfo := new(presto.QueryInfo)
	if unmarshalErr := json.Unmarshal(bytes, queryInfo); unmarshalErr != nil {
		return unmarshalErr
	}
	if mysqlDb != nil {
		e := utils.SqlInsertObject(mysqlDb, queryInfo, "presto_query_operator_stats")
		if e != nil {
			log.Error().Err(e).Msg("failed to insert")
			return e
		}
	}
	return nil
}
