package replay

import (
	"context"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/utils"
)

var (
	RunName     string
	Parallelism int
	OutputPath  string

	parallelismGuard chan struct{}
)

func Run(_ *cobra.Command, args []string) {
	OutputPath = filepath.Join(OutputPath, RunName)
	utils.PrepareOutputDirectory(OutputPath)

	// also start to write logs to the output directory from this point on.
	logPath := filepath.Join(OutputPath, "replay.log")
	flushLog := utils.InitLogFile(logPath)
	defer flushLog()

	log.Info().Int("parallelism", Parallelism).Send()
	_, cancel := context.WithCancel(context.Background())
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

	// Use this to make sure there will be no more than Parallelism goroutines.
	parallelismGuard = make(chan struct{}, Parallelism)

}
