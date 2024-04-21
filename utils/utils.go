package utils

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"pbench/log"
)

func PrepareOutputDirectory(path string) {
	if stat, statErr := os.Stat(path); statErr != nil {
		if errors.Is(statErr, unix.ENOENT) {
			if mkdirErr := os.MkdirAll(path, 0755); mkdirErr != nil {
				log.Fatal().Err(mkdirErr).Msg("failed to create output directory")
			} else {
				log.Info().Str("output_path", path).Msg("output directory created")
			}
		} else {
			log.Fatal().Err(statErr).Msg("output path not valid")
		}
	} else if !stat.IsDir() {
		log.Fatal().Str("output_path", path).Msg("output path is not a directory")
	} else {
		log.Info().Str("output_path", path).Msg("output directory")
	}
}

func InitMySQLConnFromCfg(cfgPath string) *sql.DB {
	if cfgPath == "" {
		return nil
	}
	if bytes, ioErr := os.ReadFile(cfgPath); ioErr != nil {
		log.Error().Err(ioErr).Msg("failed to read MySQL connection config")
		return nil
	} else {
		mySQLCfg := &struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Server   string `json:"server"`
			Database string `json:"database"`
		}{}
		if err := json.Unmarshal(bytes, mySQLCfg); err != nil {
			log.Error().Err(err).Msg("failed to unmarshal MySQL connection config for the run recorder")
			return nil
		}
		if db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
			mySQLCfg.Username, mySQLCfg.Password, mySQLCfg.Server, mySQLCfg.Database)); err != nil {
			log.Error().Err(err).Msg("failed to initialize MySQL connection for the run recorder")
			return nil
		} else {
			return db
		}
	}
}
