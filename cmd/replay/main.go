package replay

import (
	"context"
	"encoding/csv"
	"github.com/spf13/cobra"
	"io"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/utils"

	presto "github.com/ethanyzhang/presto-go"
	"sync"
	"syscall"
	"time"
)

var (
	RunName     string
	OutputPath  string
	PrestoFlags utils.PrestoFlags

	queryFrameChan = make(chan *QueryFrame, 128)
	done           = make(chan any)
	runningTasks   sync.WaitGroup
)

func Run(_ *cobra.Command, args []string) {
	OutputPath = filepath.Join(OutputPath, RunName)
	utils.PrepareOutputDirectory(OutputPath)

	// also start to write logs to the output directory from this point on.
	logPath := filepath.Join(OutputPath, "replay.log")
	flushLog := utils.InitLogFile(logPath)
	defer flushLog()

	csvFile, ioErr := os.Open(args[0])
	if ioErr != nil {
		log.Fatal().Err(ioErr).Msg("failed to open csv file")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	timeToExit := make(chan os.Signal, 1)
	signal.Notify(timeToExit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	// Handle SIGINT, SIGTERM, and SIGQUIT. When ctx is canceled, in-progress MySQL transactions and InfluxDB operations will roll back.
	go func() {
		sig := <-timeToExit
		if sig != nil {
			log.Info().Msg("abort replaying")
			cancel()
		}
	}()
	go QueryFrameScheduler(ctx)

	reader := csv.NewReader(csvFile)
	reader.FieldsPerRecord = 9
	_, _ = reader.Read() // skip header
	for lineNumber := 1; ; lineNumber++ {
		fields, err := reader.Read()
		if ctx.Err() != nil || err == io.EOF {
			break
		}
		if err != nil {
			log.Error().Str("file_path", args[0]).Err(err).
				Int("line_num", lineNumber).Msg("failed to parse line")
			continue
		}
		frame, err := NewQueryFrame(fields)
		if err != nil {
			log.Error().Err(err).Int("line_num", lineNumber).Msg("failed to parse frame")
			continue
		}
		queryFrameChan <- frame
	}

	_ = csvFile.Close()
	close(queryFrameChan)
	<-done
	signal.Stop(timeToExit)
	close(timeToExit)
}

func QueryFrameScheduler(ctx context.Context) {
	firstFrame := <-queryFrameChan
	if firstFrame == nil { // No query in the CSV file at all, or context canceled before the first frame was sent.
		close(done)
		return
	}
	client := PrestoFlags.NewPrestoClient()
	sessionParamHeader := client.GenerateSessionParamsHeaderValue(firstFrame.ParseSessionParams())
	sessionParamCache := map[string]string{
		firstFrame.SessionProperties: sessionParamHeader,
	}
	lastFiredTime := firstFrame.CreateTime
	runningTasks.Add(1)
	go RunQueryFrame(ctx, client, firstFrame, sessionParamHeader)

	for frame := range queryFrameChan {
		if frame.CreateTime.After(lastFiredTime) {
			waitTime := frame.CreateTime.Sub(lastFiredTime)
			lastFiredTime = frame.CreateTime
			log.Info().Str("query_id", frame.QueryId).Time("create_time", frame.CreateTime).
				Dur("wait_for_ms", waitTime).Str("wait_for", waitTime.String()).Send()
			timer := time.NewTimer(waitTime)
			select {
			case <-ctx.Done():
				timer.Stop()
			case <-timer.C:
			}
		}
		if ctx.Err() != nil {
			for range queryFrameChan { // drain the backlog
			}
			break
		}
		sessionParamHeader, ok := sessionParamCache[frame.SessionProperties]
		if !ok {
			sessionParamHeader = client.GenerateSessionParamsHeaderValue(frame.ParseSessionParams())
			sessionParamCache[frame.SessionProperties] = sessionParamHeader
		}
		runningTasks.Add(1)
		go RunQueryFrame(ctx, client, frame, sessionParamHeader)
	}

	runningTasks.Wait()
	close(done)
}

func RunQueryFrame(ctx context.Context, client *presto.Client, frame *QueryFrame, sessionParams string) {
	defer runningTasks.Done()
	clientResult, _, err := client.Query(ctx, frame.Query, func(req *http.Request) {
		req.Header.Set(presto.CatalogHeader, frame.Catalog)
		req.Header.Set(presto.SchemaHeader, frame.Schema)
		req.Header.Set(presto.SessionHeader, sessionParams)
		req.Header.Set(presto.SourceHeader, frame.QueryId)
	})
	if err != nil {
		log.Error().Str("query_id", frame.QueryId).Err(err).Msg("failed to execute query")
		return
	}
	rowCount := 0
	err = clientResult.Drain(ctx, func(qr *presto.QueryResults) error {
		rowCount += len(qr.Data)
		return nil
	})
	if err != nil {
		log.Error().Str("query_id", frame.QueryId).Err(err).Msg("failed to fetch query result")
		return
	}
	log.Info().Str("query_id", frame.QueryId).Int("row_count", rowCount).Msg("query executed successfully")
}
