package utils

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"golang.org/x/sys/unix"
	"io"
	"os"
	"path/filepath"
	"pbench/log"
	"reflect"
	"strings"
	"time"
)

const (
	OpenNewFileFlags        = os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	DirectoryNameTimeFormat = "060102-150405"
)

func GetCtxWithTimeout(timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(context.Background(), timeout)
}

func ExpandHomeDirectory(path *string) {
	if path != nil && (*path == "~" || strings.HasPrefix(*path, "~/")) {
		if home, err := os.UserHomeDir(); err == nil {
			*path = filepath.Join(home, strings.TrimPrefix(*path, "~"))
		}
	}
}

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

func InitLogFile(logPath string) (finalizer func()) {
	if logFile, err := os.OpenFile(logPath, OpenNewFileFlags, 0644); err != nil {
		log.Error().Str("log_path", logPath).Err(err).Msg("failed to create the log file")
		// In this case, the global logger is not changed. Log messages are still printed to stderr.
		return func() {}
	} else {
		bufWriter := bufio.NewWriter(logFile)
		syncWriter := zerolog.SyncWriter(io.MultiWriter(os.Stderr, bufWriter))
		log.SetGlobalLogger(zerolog.New(syncWriter).With().Timestamp().Stack().Logger())
		log.Info().Str("log_path", logPath).Msg("log file will be saved to this path")
		return func() {
			_ = bufWriter.Flush()
			_ = logFile.Close()
		}
	}
}

func InitMySQLConnFromCfg(cfgPath string) *sql.DB {
	if cfgPath == "" {
		return nil
	}
	if cfgBytes, ioErr := os.ReadFile(cfgPath); ioErr != nil {
		log.Error().Err(ioErr).Msg("failed to read MySQL connection config")
		return nil
	} else {
		mySQLCfg := &struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Server   string `json:"server"`
			Database string `json:"database"`
		}{}
		if err := json.Unmarshal(cfgBytes, mySQLCfg); err != nil {
			log.Error().Err(err).Msg("failed to unmarshal MySQL connection config for the run recorder")
			return nil
		}
		if db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
			mySQLCfg.Username, mySQLCfg.Password, mySQLCfg.Server, mySQLCfg.Database)); err != nil {
			log.Error().Err(err).Msg("failed to initialize MySQL connection for the run recorder")
			return nil
		} else if err = db.Ping(); err != nil {
			_ = db.Close()
			log.Error().Err(err).Msg("failed to connect to MySQL for the run recorder")
			return nil
		} else {
			return db
		}
	}
}

func DerefValue(v *reflect.Value) reflect.Kind {
	k := v.Kind()
	for k == reflect.Pointer || k == reflect.Interface {
		*v = v.Elem()
		k = v.Kind()
	}
	return k
}
