package save

import (
	"context"
	"encoding/csv"
	"fmt"
	"github.com/rs/zerolog"
	"github.com/spf13/cobra"
	"io"
	"os"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/presto"
	"pbench/utils"
	"strings"
	"time"
)

var (
	PrestoFlags   utils.PrestoFlags
	Schema        string
	Catalog       string
	Session       []string
	InputFilePath string
)

func Run(_ *cobra.Command, args []string) {
	PrestoFlags.OutputPath = filepath.Join(PrestoFlags.OutputPath,
		"save_table_"+time.Now().Format(utils.DirectoryNameTimeFormat))
	utils.PrepareOutputDirectory(PrestoFlags.OutputPath)

	// also start to write logs to the output directory from this point on.
	logPath := filepath.Join(PrestoFlags.OutputPath, "save_table.log")
	flushLog := utils.InitLogFile(logPath)
	defer flushLog()

	ctx, cancel := context.WithCancel(context.Background())
	timeToExit := make(chan os.Signal, 1)
	signal.Notify(timeToExit, os.Interrupt, os.Kill)
	// Handle SIGKILL and SIGINT. When ctx is canceled, in-progress MySQL transactions and InfluxDB operations will roll back.
	go func() {
		sig := <-timeToExit
		if sig != nil {
			log.Info().Msg("abort loading")
			cancel()
		}
	}()

	client := PrestoFlags.NewPrestoClient().Catalog(Catalog).Schema(Schema)

	for _, param := range Session {
		kv := strings.SplitN(param, "=", 2)
		if len(kv) < 2 {
			log.Error().Msgf("invalid session parameter: %s", param)
			continue
		}
		client.SessionParam(kv[0], kv[1])
	}

	if InputFilePath == "" {
		for _, table := range args {
			saveTable(ctx, client, Catalog, Schema, table)
		}
	} else {
		inputFile, oErr := os.Open(InputFilePath)
		if oErr != nil {
			log.Fatal().Str("file_path", InputFilePath).Err(oErr).Msg("failed to open file")
		}
		r := csv.NewReader(inputFile)
		for {
			record, err := r.Read()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatal().Str("file_path", InputFilePath).Err(err).Msg("failed to parse file")
			}
			if l := len(record); l != 3 {
				log.Error().Str("file_path", InputFilePath).Array("record", log.NewMarshaller(record)).Msgf("expected 3 columns, got %d", l)
				continue
			}
			saveTable(ctx, client, record[0], record[1], record[2])
		}
		_ = inputFile.Close()
	}
}

func saveTable(ctx context.Context, client *presto.Client, catalog, schema, table string) {
	logTableInfo := func(e *zerolog.Event) *zerolog.Event {
		e.Str("catalog", catalog).Str("schema", schema).Str("table_name", table)
		return e
	}
	if ctx.Err() != nil {
		return
	}
	ts := &TableSummary{Catalog: catalog, Schema: schema, Name: table}
	if err := ts.QueryTableSummary(ctx, client); err != nil {
		logTableInfo(log.Error()).Err(err).Msg("failed to query table summary")
		return
	}
	filePath := filepath.Join(PrestoFlags.OutputPath,
		fmt.Sprintf("%s_%s_%s.json", catalog, schema, table))
	if err := ts.SaveToFile(filePath); err != nil {
		logTableInfo(log.Error()).Str("file_path", filePath).Err(err).Msg("failed to save table summary")
	} else {
		logTableInfo(log.Info()).Str("file_path", filePath).Msg("table summary saved")
	}
}
