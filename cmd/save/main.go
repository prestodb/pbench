package save

import (
	"context"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/utils"
	"strings"
	"time"
)

var (
	PrestoFlags utils.PrestoFlags
	Schema      string
	Catalog     string
	Session     []string
)

func Run(_ *cobra.Command, args []string) {
	utils.ExpandHomeDirectory(&PrestoFlags.OutputPath)
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

	for _, table := range args {
		saveTable(ctx, client, table)
	}
}
