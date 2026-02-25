package loadeljson

import (
	"bufio"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/stage"
	"pbench/utils"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var (
	RunName       string
	Comment       string
	OutputPath    string
	RecordRun     bool
	MySQLCfgPath  string
	InfluxCfgPath string
	Parallelism   int
	IsNDJSON      bool

	runRecorders             = make([]stage.RunRecorder, 0, 3)
	queryResults             = make([]*stage.QueryResult, 0, 8)
	runStartTime, runEndTime = newSyncedTime(time.Now()), newSyncedTime(time.UnixMilli(0))
	mysqlDb                  *sql.DB
	pseudoStage              *stage.Stage

	parallelismGuard chan struct{}
	resultChan       = make(chan *stage.QueryResult)
	runningTasks     sync.WaitGroup
)

func Run(_ *cobra.Command, args []string) {
	OutputPath = filepath.Join(OutputPath, RunName)
	utils.PrepareOutputDirectory(OutputPath)

	// also start to write logs to the output directory from this point on.
	logPath := filepath.Join(OutputPath, "loadeljson.log")
	flushLog := utils.InitLogFile(logPath)
	defer flushLog()

	// Any error run recorder initialization will make the run recorder a noop.
	// The program will continue with corresponding error logs.
	mysqlDb = utils.InitMySQLConnFromCfg(MySQLCfgPath)
	if RecordRun {
		registerRunRecorder(stage.NewFileBasedRunRecorder())
		registerRunRecorder(stage.NewInfluxRunRecorder(InfluxCfgPath))
		registerRunRecorder(stage.NewMySQLRunRecorderWithDb(mysqlDb))
	}

	log.Info().Int("parallelism", Parallelism).Send()
	ctx, cancel := context.WithCancel(context.Background())
	timeToExit := make(chan os.Signal, 1)
	signal.Notify(timeToExit, syscall.SIGINT, syscall.SIGTERM, syscall.SIGQUIT)
	// Handle SIGINT, SIGTERM, and SIGQUIT. When ctx is canceled, in-progress MySQL transactions and InfluxDB operations will roll back.
	go func() {
		sig := <-timeToExit
		if sig != nil {
			log.Info().Msg("abort loading")
			cancel()
		}
	}()

	// To reuse the `pbench run` code, especially run recorders, we create a pseudo main stage.
	pseudoStage = &stage.Stage{
		Id:       "load_el_json",
		ColdRuns: &stage.RunsValueOne,
		States: &stage.SharedStageStates{
			RunName:      RunName,
			Comment:      Comment,
			OutputPath:   OutputPath,
			RunStartTime: time.Now(),
		},
	}

	// Kick off preparation work for all the run recorders
	for _, recorder := range runRecorders {
		if err := recorder.Start(ctx, pseudoStage); err != nil {
			log.Fatal().Err(err).Msgf("failed to prepare %s", reflect.TypeOf(recorder).Name())
		}
	}

	// Use this to make sure there will be no more than Parallelism goroutines.
	parallelismGuard = make(chan struct{}, Parallelism)

	// This is the task scheduler go routine. It feeds files to task runners with back pressure.
	go func() {
		for _, path := range args {
			if ctx.Err() != nil {
				break
			}
			if err := processPath(ctx, path); err != nil {
				// This whole command is not abort-on-error. We only log errors.
				log.Error().Str("path", path).Err(err).Msg("failed to process path")
			}
		}
		// Keep the main thread waiting for queryResults until all task runner finishes.
		runningTasks.Wait()
		close(resultChan)
	}()

	for qr := range resultChan {
		queryResults = append(queryResults, qr)
	}

	pseudoStage.States.RunStartTime = runStartTime.GetTime()
	pseudoStage.States.RunFinishTime = runEndTime.GetTime()
	for _, r := range runRecorders {
		rCtx, rCancel := utils.GetCtxWithTimeout(time.Second * 5)
		r.RecordRun(rCtx, pseudoStage, queryResults)
		rCancel()
	}

	log.Info().Int("file_loaded", len(queryResults)).Send()
	// This causes the signal handler to exit.
	close(timeToExit)
}

func scheduleFile(ctx context.Context, path string) {
	parallelismGuard <- struct{}{}
	runningTasks.Add(1)
	if IsNDJSON {
		go processNDJSONFile(ctx, path)
	} else {
		go processFile(ctx, path)
	}
}

func processFile(ctx context.Context, path string) {
	defer func() {
		// Allow another task runner to start.
		<-parallelismGuard
		runningTasks.Done()
	}()

	bytes, ioErr := os.ReadFile(path)
	if ioErr != nil {
		log.Error().Err(ioErr).Str("path", path).Msg("failed to read file")
		return
	}

	processJSONBytes(ctx, path, bytes, 0)
}

func processNDJSONFile(ctx context.Context, path string) {
	defer func() {
		// Allow another task runner to start.
		<-parallelismGuard
		runningTasks.Done()
	}()

	file, err := os.Open(path)
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("failed to open NDJSON file")
		return
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	// Increase buffer size to handle large JSON lines (default is 64KB, we set to 10MB)
	const maxCapacity = 10 * 1024 * 1024 // 10MB
	buf := make([]byte, maxCapacity)
	scanner.Buffer(buf, maxCapacity)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		if ctx.Err() != nil {
			log.Info().Str("path", path).Msg("abort processing NDJSON file")
			break
		}
		line := scanner.Bytes()
		if len(line) == 0 {
			continue
		}
		processJSONBytes(ctx, path, line, lineNum)
	}

	if err := scanner.Err(); err != nil {
		log.Error().Err(err).Str("path", path).Msg("error reading NDJSON file")
	}
}

func processJSONBytes(ctx context.Context, path string, jsonBytes []byte, lineNum int) {
	queryEvent := new(QueryEvent)
	// Note that this step can succeed with any valid JSON file. But we need to do some additional validation to skip
	// invalid event listener JSON files.
	if unmarshalErr := json.Unmarshal(jsonBytes, queryEvent); unmarshalErr != nil {
		if lineNum > 0 {
			log.Error().Err(unmarshalErr).Str("path", path).Int("line", lineNum).Msg("failed to unmarshal JSON")
		} else {
			log.Error().Err(unmarshalErr).Str("path", path).Msg("failed to unmarshal JSON")
		}
		return
	}

	// Validate that this is a QueryCompletedEvent
	if queryEvent.QueryCompletedEvent == nil {
		if lineNum > 0 {
			log.Error().Str("path", path).Int("line", lineNum).Msg("no QueryCompletedEvent found")
		} else {
			log.Error().Str("path", path).Msg("no QueryCompletedEvent found in file")
		}
		return
	}

	qce := queryEvent.QueryCompletedEvent
	if qce.Metadata.QueryId == "" || qce.CreateTime.Time.IsZero() {
		if lineNum > 0 {
			log.Error().Str("path", path).Int("line", lineNum).Msg("invalid QueryCompletedEvent: missing queryId or createTime")
		} else {
			log.Error().Str("path", path).Msg("invalid QueryCompletedEvent: missing queryId or createTime")
		}
		return
	}

	// Copy the Plan from QueryEvent to QueryMetadata
	qce.Metadata.Plan = &queryEvent.Plan

	if lineNum > 0 {
		log.Info().Str("path", path).Int("line", lineNum).Msg("start to process event listener line")
	} else {
		log.Info().Str("path", path).Msg("start to process event listener file")
	}

	queryId := qce.Metadata.QueryId
	if RecordRun {
		queryId = RunName + "_" + queryId
	}

	fileName := filepath.Base(path)
	queryResult := &stage.QueryResult{
		StageId: pseudoStage.Id,
		Query: &stage.Query{
			Text:             qce.Metadata.Query,
			File:             &fileName,
			ColdRun:          true,
			ExpectedRowCount: -1, // means disabled
		},
		QueryId:   queryId,
		InfoUrl:   qce.Metadata.Uri,
		RowCount:  int(qce.Statistics.OutputPositions),
		StartTime: qce.CreateTime.Time,
		EndTime:   &qce.EndTime.Time,
	}

	if qce.FailureInfo != nil {
		// Need to set this so the run recorders will mark this query as failed.
		queryResult.QueryError = fmt.Errorf("%s", qce.FailureInfo.ErrorCode.Name)
	}

	// Unlike benchmarks run by pbench, we do not know when did the run start and finish when loading them from files.
	// We infer that the whole run starts at min(queryStartTime) and ends at max(queryEndTime).
	runStartTime.Synchronized(func(st *syncedTime) {
		if queryResult.StartTime.Before(st.t) {
			st.t = queryResult.StartTime
			// Changes to the pseudoStage will be synced to the database by the run recorder.
			pseudoStage.States.RunStartTime = queryResult.StartTime
		}
	})

	if queryResult.EndTime != nil {
		dur := queryResult.EndTime.Sub(queryResult.StartTime)
		queryResult.Duration = &dur
		runEndTime.Synchronized(func(st *syncedTime) {
			if queryResult.EndTime.After(st.t) {
				st.t = *queryResult.EndTime
			}
		})
	}

	// Insert into MySQL if configured
	if mysqlDb != nil {
		if err := insertEventListenerData(ctx, mysqlDb, qce, queryId); err != nil {
			if lineNum > 0 {
				log.Error().Err(err).Str("path", path).Int("line", lineNum).Msg("failed to insert event listener record")
			} else {
				log.Error().Err(err).Str("path", path).Msg("failed to insert event listener record")
			}
			return
		}
	}

	for _, r := range runRecorders {
		rCtx, rCancel := utils.GetCtxWithTimeout(time.Second * 5)
		r.RecordQuery(rCtx, pseudoStage, queryResult)
		rCancel()
	}

	if lineNum > 0 {
		log.Info().Str("path", path).Int("line", lineNum).Str("query_id", queryId).Msg("success")
	} else {
		log.Info().Str("path", path).Str("query_id", queryId).Msg("success")
	}
	resultChan <- queryResult
}

func processPath(ctx context.Context, path string) error {
	utils.ExpandHomeDirectory(&path)
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		// Skip dot files
		if filepath.Base(path)[0] == '.' {
			log.Debug().Str("path", path).Msg("skipping dot file")
			return nil
		}
		scheduleFile(ctx, path)
		return nil
	}

	// Process directory recursively
	return filepath.WalkDir(path, func(filePath string, d os.DirEntry, err error) error {
		if err != nil {
			log.Error().Err(err).Str("path", filePath).Msg("error walking directory")
			return nil // Continue walking despite errors
		}

		if ctx.Err() != nil {
			log.Info().Msg("abort task scheduling")
			return filepath.SkipAll
		}

		// Skip dot files and dot directories
		name := d.Name()
		if name[0] == '.' {
			if d.IsDir() {
				log.Debug().Str("path", filePath).Msg("skipping dot directory")
				return filepath.SkipDir
			}
			log.Debug().Str("path", filePath).Msg("skipping dot file")
			return nil
		}

		// Only schedule regular files
		if !d.IsDir() {
			scheduleFile(ctx, filePath)
		}

		return nil
	})
}

func registerRunRecorder(r stage.RunRecorder) {
	if r == nil || reflect.ValueOf(r).IsNil() {
		return
	}
	runRecorders = append(runRecorders, r)
}
