package loadjson

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/presto/query_json"
	"pbench/stage"
	"pbench/utils"
	"reflect"
	"sync"
	"syscall"
	"time"

	"github.com/spf13/cobra"
)

var (
	RunName         string
	Comment         string
	OutputPath      string
	RecordRun       bool
	MySQLCfgPath    string
	InfluxCfgPath   string
	ExtractPlanJson bool
	Parallelism     int

	runRecorders             = make([]stage.RunRecorder, 0, 3)
	queryResults             = make([]*stage.QueryResult, 0, 8)
	runStartTime, runEndTime = newSyncedTime(time.Now()), newSyncedTime(time.UnixMilli(0))
	mysqlDb                  *sql.DB
	pseudoStage              *stage.Stage

	parallelismGuard     chan struct{}
	resultChan           = make(chan *stage.QueryResult)
	runningTasks         sync.WaitGroup
	DefaultColdRunsValue = 1
)

func Run(_ *cobra.Command, args []string) {
	OutputPath = filepath.Join(OutputPath, RunName)
	utils.PrepareOutputDirectory(OutputPath)

	// also start to write logs to the output directory from this point on.
	logPath := filepath.Join(OutputPath, "loadjson.log")
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
		Id:       "load_json",
		ColdRuns: &DefaultColdRunsValue,
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
		r.RecordRun(utils.GetCtxWithTimeout(time.Second*5), pseudoStage, queryResults)
	}

	log.Info().Int("file_loaded", len(queryResults)).Send()
	// This causes the signal handler to exit.
	close(timeToExit)
}

func scheduleFile(ctx context.Context, path string) {
	parallelismGuard <- struct{}{}
	runningTasks.Add(1)
	go processFile(ctx, path)
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
	queryInfo := new(query_json.QueryInfo)
	// Note that this step can succeed with any valid JSON file. But we need to do some additional validation to skip
	// invalid query JSON files.
	if unmarshalErr := json.Unmarshal(bytes, queryInfo); unmarshalErr != nil {
		return
	}
	if queryInfo.QueryId == "" || queryInfo.QueryStats == nil || queryInfo.QueryStats.CreateTime == nil {
		return
	}
	log.Info().Str("path", path).Msg("start to process file")
	if RecordRun {
		queryInfo.QueryId = RunName + "_" + queryInfo.QueryId
	}
	fileName := filepath.Base(path)
	queryResult := &stage.QueryResult{
		StageId: pseudoStage.Id,
		Query: &stage.Query{
			Text:             queryInfo.Query,
			File:             &fileName,
			ColdRun:          true,
			ExpectedRowCount: -1, // means disabled
		},
		QueryId:   queryInfo.QueryId,
		InfoUrl:   queryInfo.Self,
		RowCount:  int(queryInfo.QueryStats.OutputPositions),
		StartTime: *queryInfo.QueryStats.CreateTime,
		EndTime:   queryInfo.QueryStats.EndTime,
	}
	if queryInfo.ErrorCode != nil {
		// Need to set this so the run recorders will mark this query as failed.
		queryResult.QueryError = fmt.Errorf(*queryInfo.ErrorCode.Name)
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

	if mysqlDb != nil || ExtractPlanJson {
		// OutputStage is in a tree structure, and we need to flatten it for its ORM to be correctly parsed.
		// There are many other derived metrics, so we need to do soe preprocessing before sending it to the database.
		if err := queryInfo.PrepareForInsert(); err != nil {
			log.Error().Err(err).Str("path", path).Msg("failed to pre-process query info JSON")
			return
		}
	}
	if ExtractPlanJson {
		planJsonFilePath := filepath.Join(OutputPath, fileName[0:len(fileName)-len(filepath.Ext(fileName))]+".plan.json")
		if jsonFile, createErr := os.Create(planJsonFilePath); createErr == nil {
			if _, writeErr := jsonFile.WriteString(queryInfo.AssembledQueryPlanJson); writeErr != nil {
				log.Error().Err(writeErr).Str("path", planJsonFilePath).Msg("failed to write plan json file")
			} else {
				log.Info().Str("path", planJsonFilePath).Msg("wrote plan json file")
			}
			_ = jsonFile.Close()
		}
	}
	if mysqlDb != nil {
		if err := utils.SqlInsertObject(ctx, mysqlDb, queryInfo,
			"presto_query_creation_info",
			"presto_query_operator_stats",
			"presto_query_plans",
			"presto_query_stage_stats",
			"presto_query_statistics",
		); err != nil {
			log.Error().Err(err).Str("path", path).Msg("failed to insert event listener record")
			return
		}
	}
	for _, r := range runRecorders {
		r.RecordQuery(utils.GetCtxWithTimeout(time.Second*5), pseudoStage, queryResult)
	}
	log.Info().Str("path", path).Str("query_id", queryInfo.QueryId).Msg("success")
	resultChan <- queryResult
}

func processPath(ctx context.Context, path string) error {
	utils.ExpandHomeDirectory(&path)
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		scheduleFile(ctx, path)
		return nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if ctx.Err() != nil {
			log.Info().Msg("abort task scheduling")
			break
		}
		if entry.IsDir() {
			continue
		}
		fullPath := filepath.Join(path, entry.Name())
		scheduleFile(ctx, fullPath)
	}
	return nil
}

func registerRunRecorder(r stage.RunRecorder) {
	if r == nil || reflect.ValueOf(r).IsNil() {
		return
	}
	runRecorders = append(runRecorders, r)
}
