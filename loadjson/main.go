package loadjson

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"os/signal"
	"path/filepath"
	"pbench/log"
	"pbench/presto/query_json"
	"pbench/stage"
	"pbench/utils"
	"reflect"
	"sync"
	"time"
)

var (
	RunName       string
	Comment       string
	OutputPath    string
	RecordRun     bool
	MySQLCfgPath  string
	InfluxCfgPath string
	GoRoutineCap  int

	runRecorders             = make([]stage.RunRecorder, 0, 3)
	queryResults             = make([]*stage.QueryResult, 0, 8)
	runStartTime, runEndTime = newSyncedTime(time.Now()), newSyncedTime(time.UnixMilli(0))
	mysqlDb                  *sql.DB
	pseudoStage              *stage.Stage

	goRoutineCapGuard chan struct{}
	resultChan        = make(chan *stage.QueryResult)
	fileToProcessChan = make(chan string)
	runningTasks      sync.WaitGroup
)

func Run(_ *cobra.Command, args []string) {
	utils.PrepareOutputDirectory(OutputPath)
	mysqlDb = utils.InitMySQLConnFromCfg(MySQLCfgPath)
	if RecordRun {
		registerRunRecorder(stage.NewFileBasedRunRecorder())
		registerRunRecorder(stage.NewMySQLRunRecorderWithDb(mysqlDb))
		registerRunRecorder(stage.NewInfluxRunRecorder(InfluxCfgPath))
	}

	log.Info().Int("parallel", GoRoutineCap).Send()
	ctx, cancel := context.WithCancel(context.Background())
	timeToExit := make(chan os.Signal, 1)
	signal.Notify(timeToExit, os.Interrupt, os.Kill)
	go func() {
		sig := <-timeToExit
		if sig != nil {
			log.Info().Msg("abort loading")
			cancel()
		}
	}()

	pseudoStage = &stage.Stage{
		Id:       "load_json",
		ColdRuns: 1,
		States: &stage.SharedStageStates{
			RunName:      RunName,
			Comment:      Comment,
			OutputPath:   OutputPath,
			RunStartTime: time.Now(),
		},
	}

	for _, recorder := range runRecorders {
		if err := recorder.Start(ctx, pseudoStage); err != nil {
			log.Fatal().Err(err).Msgf("failed to prepare %s", reflect.TypeOf(recorder).Name())
		}
	}

	runningTasks.Add(1)
	go func() {
		for _, path := range args {
			select {
			case <-ctx.Done():
				break
			default:
				if err := processPath(path); err != nil {
					log.Error().Str("path", path).Err(err).Msg("failed to process path")
				}
			}
		}
		close(fileToProcessChan)
		runningTasks.Done()
	}()

	goRoutineCapGuard = make(chan struct{}, GoRoutineCap)
	go func() {
		defer func() {
			runningTasks.Wait()
			close(resultChan)
		}()
		for {
			select {
			case <-ctx.Done():
				for _ = range fileToProcessChan {
				}
				log.Info().Msg("file queue drained")
				return
			case fileToProcess, ok := <-fileToProcessChan:
				if !ok {
					return
				}
				goRoutineCapGuard <- struct{}{}
				go processFile(ctx, fileToProcess)
			}
		}
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
	close(timeToExit)
}

func processPath(path string) error {
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		fileToProcessChan <- path
		return nil
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		fullPath := filepath.Join(path, entry.Name())
		fileToProcessChan <- fullPath
	}
	return nil
}

func processFile(ctx context.Context, path string) {
	defer func() {
		<-goRoutineCapGuard
		runningTasks.Done()
	}()
	runningTasks.Add(1)

	bytes, ioErr := os.ReadFile(path)
	if ioErr != nil {
		log.Error().Err(ioErr).Str("path", path).Msg("failed to read file")
		return
	}
	queryInfo := new(query_json.QueryInfo)
	if unmarshalErr := json.Unmarshal(bytes, queryInfo); unmarshalErr != nil || queryInfo.QueryId == "" {
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
			ExpectedRowCount: -1,
		},
		QueryId:   queryInfo.QueryId,
		InfoUrl:   queryInfo.Self,
		RowCount:  int(queryInfo.QueryStats.OutputPositions),
		StartTime: *queryInfo.QueryStats.CreateTime,
		EndTime:   queryInfo.QueryStats.EndTime,
	}
	if queryInfo.ErrorCode != nil {
		queryResult.QueryError = fmt.Errorf(*queryInfo.ErrorCode.Name)
	}
	runStartTime.Synchronized(func(st *syncedTime) {
		if queryResult.StartTime.Before(st.t) {
			st.t = queryResult.StartTime
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

	var err error
	if mysqlDb != nil {
		// OutputStage is in a tree structure, and we need to flatten it for its ORM to be correctly parsed.
		err = queryInfo.PrepareForInsert()
		if err == nil {
			err = utils.SqlInsertObject(ctx, mysqlDb, queryInfo,
				"presto_query_creation_info",
				"presto_query_operator_stats",
				"presto_query_plans",
				"presto_query_stage_stats",
				"presto_query_statistics",
			)
		}
	}
	if err != nil {
		log.Error().Err(err).Str("path", path).Msg("failed to insert event listener record")
		return
	}
	for _, r := range runRecorders {
		r.RecordQuery(utils.GetCtxWithTimeout(time.Second*5), pseudoStage, queryResult)
	}
	log.Info().Str("path", path).Str("query_id", queryInfo.QueryId).Msg("success")
	resultChan <- queryResult
}

func registerRunRecorder(r stage.RunRecorder) {
	if r == nil || reflect.ValueOf(r).IsNil() {
		return
	}
	runRecorders = append(runRecorders, r)
}

type syncedTime struct {
	t time.Time
	m sync.Mutex
}

func newSyncedTime(t time.Time) *syncedTime {
	return &syncedTime{
		t: t,
	}
}

func (st *syncedTime) Synchronized(f func(st *syncedTime)) {
	st.m.Lock()
	defer st.m.Unlock()
	f(st)
}

func (st *syncedTime) GetTime() time.Time {
	st.m.Lock()
	defer st.m.Unlock()
	return st.t
}
