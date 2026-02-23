package stage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	presto "github.com/ethanyzhang/presto-go"
	"os"
	"path/filepath"
	"pbench/log"
	"pbench/utils"
	"strconv"
	"time"

	"github.com/rs/zerolog"
)

const (
	DefaultStageFileExt = ".json"
)

var (
	RunsValueOne  = 1
	RunsValueZero = 0
)

type OnQueryCompletionFn func(result *QueryResult)

var DefaultNewClientFn = func() *presto.Client {
	client, _ := presto.NewClient(utils.DefaultServerUrl)
	return client
}

func (s *Stage) MergeWith(other *Stage) *Stage {
	s.Id = other.Id
	if other.Catalog != nil {
		s.Catalog = other.Catalog
	}
	if other.Schema != nil {
		s.Schema = other.Schema
	}
	if s.SessionParams == nil {
		s.SessionParams = make(map[string]any)
	}
	for k, v := range other.SessionParams {
		if v != nil {
			s.SessionParams[k] = v
		} else {
			delete(s.SessionParams, k)
		}
	}
	if other.TimeZone != nil {
		s.TimeZone = other.TimeZone
	}
	s.Queries = append(s.Queries, other.Queries...)
	s.QueryFiles = append(s.QueryFiles, other.QueryFiles...)
	if s.ExpectedRowCounts == nil {
		s.ExpectedRowCounts = make(map[string][]int)
	}
	for k, v := range other.ExpectedRowCounts {
		if v != nil {
			s.ExpectedRowCounts[k] = v
		} else {
			delete(s.ExpectedRowCounts, k)
		}
	}
	if other.RandomExecution != nil {
		s.RandomExecution = other.RandomExecution
	}
	if other.RandomlyExecuteUntil != nil {
		s.RandomlyExecuteUntil = other.RandomlyExecuteUntil
	}
	if other.ColdRuns != nil {
		s.ColdRuns = other.ColdRuns
	}
	if other.WarmRuns != nil {
		s.WarmRuns = other.WarmRuns
	}
	s.StartOnNewClient = other.StartOnNewClient
	if other.AbortOnError != nil {
		s.AbortOnError = other.AbortOnError
	}
	if other.SaveOutput != nil {
		s.SaveOutput = other.SaveOutput
	}
	if other.SaveColumnMetadata != nil {
		s.SaveColumnMetadata = other.SaveColumnMetadata
	}
	if other.SaveJson != nil {
		s.SaveJson = other.SaveJson
	}
	s.NextStagePaths = append(s.NextStagePaths, other.NextStagePaths...)
	s.BaseDir = other.BaseDir

	s.PreStageShellScripts = append(s.PreStageShellScripts, other.PreStageShellScripts...)
	s.PreQueryShellScripts = append(s.PreQueryShellScripts, other.PreQueryShellScripts...)
	s.PostQueryShellScripts = append(s.PostQueryShellScripts, other.PostQueryShellScripts...)
	s.PostStageShellScripts = append(s.PostStageShellScripts, other.PostStageShellScripts...)
	s.PreQueryCycleShellScripts = append(s.PreQueryCycleShellScripts, other.PreQueryCycleShellScripts...)
	s.PostQueryCycleShellScripts = append(s.PostQueryCycleShellScripts, other.PostQueryCycleShellScripts...)

	return s
}

func (s *Stage) MarshalZerologObject(e *zerolog.Event) {
	e.Str("benchmark_stage_id", s.Id)
}

func (s *Stage) String() string {
	return s.Id
}

func (s *Stage) InitStates() *Stage {
	s.States = &SharedStageStates{}
	return s
}

func (s *Stage) waitForPrerequisites() <-chan struct{} {
	ch := make(chan struct{}, 1)
	go func() {
		s.wgPrerequisites.Wait()
		close(ch)
	}()
	return ch
}

func (s *Stage) logErr(ctx context.Context, err error) {
	var queryResult *QueryResult
	logEvent := log.Error()
	if ctx.Err() != nil {
		logEvent.EmbedObject(s)
		if cause := context.Cause(ctx); cause != nil && errors.As(cause, &queryResult) {
			logEvent.Str("caused_by_stage", queryResult.StageId).
				Str("caused_by_query", queryResult.QueryId).
				Str("info_url", queryResult.InfoUrl)
		} else {
			logEvent.AnErr("caused_by_error", err)
		}
		logEvent.Msg("stage aborted")
		return
	}
	if errors.As(err, &queryResult) {
		logEvent.EmbedObject(queryResult)
	} else {
		logEvent.EmbedObject(s).EmbedObject(log.NewMarshaller(err))
	}
	logEvent.Msg("execution failed")
}

func (s *Stage) prepareClient() {
	if s.Client != nil && !s.StartOnNewClient {
		needsNewClient := false
		if s.Catalog != nil && *s.Catalog != s.Client.GetCatalog() {
			needsNewClient = true
		}
		if s.Schema != nil && *s.Schema != s.Client.GetSchema() {
			needsNewClient = true
		}
		if s.TimeZone != nil && *s.TimeZone != s.Client.GetTimeZone() {
			needsNewClient = true
		}
		if !needsNewClient {
			s.currentCatalog = s.Client.GetCatalog()
			s.currentSchema = s.Client.GetSchema()
			s.currentTimeZone = s.Client.GetTimeZone()
			return
		}
		log.Info().EmbedObject(s).Msg("auto-creating new client because catalog/schema/timezone changed")
	}
	if s.States.NewClient == nil {
		s.States.NewClient = DefaultNewClientFn
		log.Debug().Msg("using DefaultNewClientFn")
	}
	s.Client = s.States.NewClient()
	log.Info().EmbedObject(s).Msg("created new client")
	if s.Catalog != nil {
		s.currentCatalog = *s.Catalog
		s.Client.Catalog(s.currentCatalog)
		log.Info().EmbedObject(s).Str("catalog", s.currentCatalog).Msg("set catalog")
	} else {
		s.currentCatalog = s.Client.GetCatalog()
	}
	if s.Schema != nil {
		s.currentSchema = *s.Schema
		s.Client.Schema(s.currentSchema)
		log.Info().EmbedObject(s).Str("schema", s.currentSchema).Msg("set schema")
	} else {
		s.currentSchema = s.Client.GetSchema()
	}
	for k, v := range s.SessionParams {
		s.Client.SessionParam(k, v)
	}
	if len(s.SessionParams) > 0 {
		log.Info().EmbedObject(s).
			Str("values", s.Client.GetSessionParams()).
			Msg("set session params")
	}
	if s.TimeZone != nil {
		s.currentTimeZone = *s.TimeZone
		s.Client.TimeZone(s.currentTimeZone)
		log.Info().EmbedObject(s).Str("timezone", s.currentTimeZone).Msg("set timezone")
	} else {
		s.currentTimeZone = s.Client.GetTimeZone()
	}
	s.Client.AppendClientTag(s.Id)
}

func (s *Stage) setDefaults() {
	falseValue := false
	if s.RandomExecution == nil {
		s.RandomExecution = &falseValue
	}
	if s.AbortOnError == nil {
		s.AbortOnError = &falseValue
	}
	if s.SaveOutput == nil {
		s.SaveOutput = &falseValue
	}
	if s.SaveColumnMetadata == nil {
		s.SaveColumnMetadata = &falseValue
	}
	if s.SaveJson == nil {
		s.SaveJson = &falseValue
	}
	if s.ColdRuns == nil {
		s.ColdRuns = &RunsValueZero
	}
	if s.WarmRuns == nil {
		s.WarmRuns = &RunsValueZero
	}
	if *s.ColdRuns+*s.WarmRuns <= 0 {
		s.ColdRuns = &RunsValueOne
		s.WarmRuns = &RunsValueZero
	}
}

func (s *Stage) propagateStates() {
	for _, nextStage := range s.NextStages {
		nextStage.propagateOnce.Do(func() {
			if nextStage.Catalog == nil {
				nextStage.Catalog = s.Catalog
			}
			if nextStage.Schema == nil {
				nextStage.Schema = s.Schema
			}
			if nextStage.SessionParams == nil {
				nextStage.SessionParams = make(map[string]any)
			}
			if nextStage.TimeZone == nil {
				nextStage.TimeZone = s.TimeZone
			}
			if nextStage.RandomExecution == nil {
				nextStage.RandomExecution = s.RandomExecution
			}
			if nextStage.RandomlyExecuteUntil == nil {
				nextStage.RandomlyExecuteUntil = s.RandomlyExecuteUntil
			}
			for k, v := range s.SessionParams {
				if v == nil {
					continue
				}
				if _, ok := nextStage.SessionParams[k]; !ok {
					nextStage.SessionParams[k] = v
				}
			}
			if nextStage.ColdRuns == nil && nextStage.WarmRuns == nil {
				nextStage.ColdRuns = s.ColdRuns
				nextStage.WarmRuns = s.WarmRuns
			} else if nextStage.ColdRuns == nil {
				nextStage.ColdRuns = &RunsValueZero
			} else if nextStage.WarmRuns == nil {
				nextStage.WarmRuns = &RunsValueZero
			}
			if *nextStage.ColdRuns+*nextStage.WarmRuns <= 0 {
				nextStage.ColdRuns = &RunsValueOne
				nextStage.WarmRuns = &RunsValueZero
			}
			if nextStage.AbortOnError == nil {
				nextStage.AbortOnError = s.AbortOnError
			}
			if nextStage.SaveOutput == nil {
				nextStage.SaveOutput = s.SaveOutput
			}
			if nextStage.SaveColumnMetadata == nil {
				nextStage.SaveColumnMetadata = s.SaveColumnMetadata
			}
			if nextStage.SaveJson == nil {
				nextStage.SaveJson = s.SaveJson
			}
			nextStage.States = s.States
			nextStage.Client = s.Client
		})
	}
}

func (s *Stage) saveQueryJsonFile(result *QueryResult) {
	// We do not save json file when saveJson is false. But when there is an error, we always save the json file.
	if !*s.SaveJson && result.QueryError == nil {
		return
	}
	s.States.wgExitMainStage.Add(1)
	go func() {
		checkErr := func(err error) {
			if err != nil {
				log.Error().Err(err).EmbedObject(result.SimpleLogging()).Msg("error when saving query json file")
			}
		}
		querySourceStr := s.querySourceString(result)
		{
			queryJsonFile, err := os.OpenFile(
				filepath.Join(s.States.OutputPath, querySourceStr)+".json",
				utils.OpenNewFileFlags, 0644)
			checkErr(err)
			if err == nil {
				// We need to save the query json file even if the stage context is canceled.
				qCtx, qCancel := utils.GetCtxWithTimeout(time.Second * 5)
				_, err = s.Client.GetQueryInfo(qCtx, result.QueryId, queryJsonFile)
				qCancel()
				checkErr(err)
				checkErr(queryJsonFile.Close())
			}
		}
		if result.QueryError != nil {
			queryErrorFile, err := os.OpenFile(
				filepath.Join(s.States.OutputPath, querySourceStr)+".error.json",
				utils.OpenNewFileFlags, 0644)
			checkErr(err)
			if err == nil {
				bytes, e := json.MarshalIndent(result.QueryError, "", "  ")
				// If marshaling produced "{}" or failed, fall back to the error message string.
				if e != nil || string(bytes) == "{}" {
					bytes, e = json.MarshalIndent(map[string]string{
						"error": result.QueryError.Error(),
					}, "", "  ")
				}
				if e == nil {
					_, e = queryErrorFile.Write(bytes)
				}
				checkErr(e)
				checkErr(queryErrorFile.Close())
			}
		}
		s.States.wgExitMainStage.Done()
	}()
}

func (s *Stage) saveColumnMetadataFile(qr *presto.QueryResults, result *QueryResult, querySourceStr string) (returnErr error) {
	if !*s.SaveColumnMetadata || len(qr.Columns) == 0 {
		return
	}
	defer func() {
		if returnErr != nil {
			log.Error().Err(returnErr).EmbedObject(result.SimpleLogging()).
				Msg("failed to write query column metadata")
		}
	}()
	columnMetadataFile, ioErr := os.OpenFile(
		filepath.Join(s.States.OutputPath, querySourceStr)+".cols.json",
		utils.OpenNewFileFlags, 0644)
	if ioErr != nil {
		return ioErr
	}
	defer columnMetadataFile.Close()
	bytes, marshalErr := json.MarshalIndent(qr.Columns, "", "  ")
	if marshalErr != nil {
		return marshalErr
	}
	_, returnErr = columnMetadataFile.Write(bytes)
	return
}

func getNow() *time.Time {
	now := time.Now()
	return &now
}

func (s *Stage) querySourceString(result *QueryResult) (sourceStr string) {
	if result.Query.File != nil {
		sourceStr = fileNameWithoutPathAndExt(*result.Query.File)
	} else {
		sourceStr = "inline"
	}
	if result.Query.BatchSize > 1 {
		sourceStr = fmt.Sprintf("%s_%s_q%d", s.Id, sourceStr, result.Query.Index)
	} else {
		sourceStr = fmt.Sprintf("%s_%s", s.Id, sourceStr)
	}
	if *s.ColdRuns+*s.WarmRuns > 1 {
		if result.Query.ColdRun {
			sourceStr += "_c"
		} else {
			sourceStr += "_w"
		}
		sourceStr += strconv.Itoa(result.Query.SequenceNo)
	}
	return
}
