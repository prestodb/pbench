package stage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/rs/zerolog"
	"os"
	"path/filepath"
	"presto-benchmark/log"
	"presto-benchmark/presto"
	"strconv"
	"time"
)

var SaveColMetadata bool

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
	if s.ColdRuns+s.WarmRuns > 1 {
		if result.Query.ColdRun {
			sourceStr += "_c"
		} else {
			sourceStr += "_w"
		}
		sourceStr += strconv.Itoa(result.Query.RunIndex)
	}
	return
}

func (s *Stage) MarshalZerologObject(e *zerolog.Event) {
	e.Str("benchmark_stage_id", s.Id)
}

func (s *Stage) String() string {
	return s.Id
}

func (s *Stage) logErr(ctx context.Context, err error) {
	var queryResult *QueryResult
	logEvent := log.Error()
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
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
	logEvent.Msg("query failed")
}

func (s *Stage) prepareClient() {
	if s.Client == nil || s.StartOnNewClient {
		if s.GetClient == nil {
			s.GetClient = DefaultGetClientFn
			log.Debug().Msg("using DefaultGetClientFn")
		}
		s.Client = s.GetClient()
		log.Info().EmbedObject(s).Msg("created new client")
	}
	if s.Catalog != nil {
		s.Client.Catalog(*s.Catalog)
		log.Info().EmbedObject(s).Str("catalog", *s.Catalog).Msg("set catalog")
	}
	if s.Schema != nil {
		s.Client.Schema(*s.Schema)
		log.Info().EmbedObject(s).Str("schema", *s.Schema).Msg("set schema")
	}
	for k, v := range s.SessionParams {
		s.Client.SessionParam(k, v)
	}
	if len(s.SessionParams) > 0 {
		log.Info().EmbedObject(s).
			Object("delta", log.NewMarshaller(s.SessionParams)).
			Str("final", s.Client.GetSessionParams()).
			Msg("added session params")
	}
	s.Client.AppendClientTag(s.Id)
}

func (s *Stage) setDefaults() {
	falseValue := false
	if s.SaveJson == nil {
		s.SaveJson = &falseValue
	}
	if s.SaveOutput == nil {
		s.SaveOutput = &falseValue
	}
	if s.SaveColumnMetadata == nil {
		s.SaveColumnMetadata = &falseValue
	}
	if s.AbortOnError == nil {
		s.AbortOnError = &falseValue
	}
	if s.WarmRuns == 0 {
		s.WarmRuns = 1
	}
}

func (s *Stage) propagateStates() {
	for _, nextStage := range s.NextStages {
		if nextStage.ColdRuns == 0 {
			nextStage.ColdRuns = s.ColdRuns
		}
		if nextStage.WarmRuns == 0 {
			nextStage.WarmRuns = s.WarmRuns
		}
		if nextStage.AbortOnError == nil {
			nextStage.AbortOnError = s.AbortOnError
		}
		if nextStage.SaveOutput == nil {
			nextStage.SaveOutput = s.SaveOutput
		}
		if nextStage.SaveJson == nil {
			nextStage.SaveJson = s.SaveJson
		}
		if nextStage.SaveColumnMetadata == nil {
			nextStage.SaveColumnMetadata = s.SaveColumnMetadata
		}
		nextStage.OutputPath = s.OutputPath
		if nextStage.GetClient == nil {
			nextStage.GetClient = s.GetClient
			if nextStage.Client == nil {
				// If the next stage didn't have a getClient function and a client before,
				// then we just let it use ours.
				nextStage.Client = s.Client
			}
		}
		nextStage.AbortAll = s.AbortAll
		nextStage.OnQueryCompletion = s.OnQueryCompletion
		nextStage.wgExitMainStage = s.wgExitMainStage
		nextStage.resultChan = s.resultChan
	}
}

func (s *Stage) MergeWith(other *Stage) *Stage {
	if s.Id == "" {
		s.Id = other.Id
	} else {
		s.Id += "_" + other.Id
	}
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
		s.SessionParams[k] = v
	}
	s.Queries = append(s.Queries, other.Queries...)
	s.QueryFiles = append(s.QueryFiles, other.QueryFiles...)
	if other.ColdRuns > 0 {
		s.ColdRuns = other.ColdRuns
	}
	if other.WarmRuns > 0 {
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
	if s.OutputPath == "" {
		s.OutputPath = other.OutputPath
	}
	s.NextStagePaths = append(s.NextStagePaths, other.NextStagePaths...)
	s.BaseDir = other.BaseDir
	return s
}

func (s *Stage) saveQueryJsonFile(ctx context.Context, result *QueryResult) {
	// We do not save json file for the cold run or when saveJson is false. But when there is an error, we always save the json file.
	if (result.Query.ColdRun || !*s.SaveJson) && result.QueryError == nil {
		return
	}
	s.wgExitMainStage.Add(1)
	go func() {
		checkErr := func(err error) {
			if err != nil {
				log.Error().Err(err).EmbedObject(result.SimpleLogging()).Msg("error when saving query json file")
			}
		}
		querySourceStr := s.querySourceString(result)
		{
			queryJsonFile, err := os.OpenFile(
				filepath.Join(s.OutputPath, querySourceStr)+".json",
				OpenNewFileFlags, 0644)
			checkErr(err)
			if err == nil {
				_, err = s.Client.GetQueryInfo(ctx, result.QueryId, false, queryJsonFile)
				checkErr(err)
				checkErr(queryJsonFile.Close())
			}
		}
		if result.QueryError != nil {
			queryErrorFile, err := os.OpenFile(
				filepath.Join(s.OutputPath, querySourceStr)+".error.json",
				OpenNewFileFlags, 0644)
			checkErr(err)
			if err == nil {
				bytes, e := json.MarshalIndent(result.QueryError, "", "  ")
				if e == nil {
					_, e = queryErrorFile.Write(bytes)
				} else {
					checkErr(e)
					_, e = queryErrorFile.WriteString(e.Error())
				}
				checkErr(e)
				checkErr(queryErrorFile.Close())
			}
		}
		s.wgExitMainStage.Done()
	}()
}

func (s *Stage) saveColumnMetadataFile(qr *presto.QueryResults, result *QueryResult, querySourceStr string) (returnErr error) {
	if result.Query.ColdRun || !(SaveColMetadata || *s.SaveColumnMetadata) || len(qr.Columns) == 0 {
		return
	}
	defer func() {
		if returnErr != nil {
			log.Error().Err(returnErr).EmbedObject(result.SimpleLogging()).
				Msg("failed to write query column metadata")
		}
	}()
	columnMetadataFile, ioErr := os.OpenFile(
		filepath.Join(s.OutputPath, querySourceStr)+".cols.json",
		OpenNewFileFlags, 0644)
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
