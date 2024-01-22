package stage

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"github.com/rs/zerolog"
	"os"
	"path/filepath"
	"presto-benchmark/log"
	"presto-benchmark/presto"
	"strconv"
	"strings"
	"time"
)

const (
	OpenNewFileFlags    = os.O_CREATE | os.O_TRUNC | os.O_WRONLY
	DefaultStageFileExt = ".json"
	RunNameTimeFormat   = "060102-150405"
)

type GetClientFn func() *presto.Client
type OnQueryCompletionFn func(result *QueryResult)

var (
	DefaultServerUrl   = "http://127.0.0.1:8080"
	DefaultGetClientFn = func() *presto.Client {
		client, _ := presto.NewClient(DefaultServerUrl)
		return client
	}
)

func (s *Stage) InitInfluxDB(InfluxCfgPath string) {
	if bytes, err := os.ReadFile(InfluxCfgPath); err != nil {
		log.Info().Err(err).Msg("InfluxDB client was not initialized, results won't be written to the database.")
		return
	} else {
		influxCfg := &struct {
			Url    string `json:"url"`
			Org    string `json:"org"`
			Bucket string `json:"bucket"`
			Token  string `json:"token"`
		}{}
		err = json.Unmarshal(bytes, influxCfg)
		if err != nil {
			log.Info().Err(err).Msg("InfluxDB client was not initialized, results won't be written to the database.")
			return
		}
		s.States.influxClient = influxdb2.NewClient(influxCfg.Url, influxCfg.Token)
		s.States.influxWriter = s.States.influxClient.WriteAPIBlocking(influxCfg.Org, influxCfg.Bucket)
		log.Info().Str("url", influxCfg.Url).Str("org", influxCfg.Org).Str("bucket", influxCfg.Bucket).
			Msg("InfluxDB client initialized, benchmark result summary will be sent to this database.")
	}
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
	s.NextStagePaths = append(s.NextStagePaths, other.NextStagePaths...)
	s.BaseDir = other.BaseDir
	return s
}

func (s *Stage) MarshalZerologObject(e *zerolog.Event) {
	e.Str("benchmark_stage_id", s.Id)
}

func (s *Stage) String() string {
	return s.Id
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
	if s.Client != nil && !s.StartOnNewClient {
		return
	}
	if s.States.GetClient == nil {
		s.States.GetClient = DefaultGetClientFn
		log.Debug().Msg("using DefaultGetClientFn")
	}
	s.Client = s.States.GetClient()
	log.Info().EmbedObject(s).Msg("created new client")
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
			Str("values", s.Client.GetSessionParams()).
			Msg("set session params")
	}
	s.Client.AppendClientTag(s.Id)
}

func (s *Stage) setDefaults() {
	falseValue := false
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
	if s.WarmRuns == 0 {
		s.WarmRuns = 1
	}
}

func (s *Stage) propagateStates() {
	for _, nextStage := range s.NextStages {
		if nextStage.Catalog == nil {
			nextStage.Catalog = s.Catalog
		}
		if nextStage.Schema == nil {
			nextStage.Schema = s.Schema
		}
		if nextStage.SessionParams == nil {
			nextStage.SessionParams = make(map[string]any)
		}
		for k, v := range s.SessionParams {
			if _, ok := nextStage.SessionParams[k]; !ok {
				nextStage.SessionParams[k] = v
			}
		}
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
		if nextStage.SaveColumnMetadata == nil {
			nextStage.SaveColumnMetadata = s.SaveColumnMetadata
		}
		if nextStage.SaveJson == nil {
			nextStage.SaveJson = s.SaveJson
		}
		nextStage.States = s.States
		nextStage.Client = s.Client
	}
}

func (s *Stage) saveQueryJsonFile(ctx context.Context, result *QueryResult) {
	// We do not save json file for the cold run or when saveJson is false. But when there is an error, we always save the json file.
	if (result.Query.ColdRun || !*s.SaveJson) && result.QueryError == nil {
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
				filepath.Join(s.States.OutputPath, querySourceStr)+".error.json",
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
		s.States.wgExitMainStage.Done()
	}()
}

func (s *Stage) saveColumnMetadataFile(qr *presto.QueryResults, result *QueryResult, querySourceStr string) (returnErr error) {
	if result.Query.ColdRun || !*s.SaveColumnMetadata || len(qr.Columns) == 0 {
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

func (s *Stage) appendQuerySummary(summaryBuilder *strings.Builder, result *QueryResult) {
	summaryBuilder.WriteString(result.StageId + ",")
	if result.Query.File != nil {
		summaryBuilder.WriteString(*result.Query.File)
	} else {
		summaryBuilder.WriteString("inline")
	}
	summaryBuilder.WriteString(fmt.Sprintf(",%d,%t,%d,%s,%t,%d,%s,%s,%f\n",
		result.Query.Index, result.Query.ColdRun, result.Query.RunIndex, result.InfoUrl,
		result.QueryError == nil, result.RowCount, result.StartTime.Format(time.RFC3339),
		result.EndTime.Format(time.RFC3339), result.Duration.Seconds()))
}

func (s *Stage) sendQuerySummaryToInfluxDB(ctx context.Context, result *QueryResult) {
	if s.States.influxWriter == nil {
		return
	}
	tags := map[string]string{
		"run_name": s.States.RunName,
		"stage_id": result.StageId,
		"query_id": result.QueryId,
	}
	fields := map[string]interface{}{
		"query_index": result.Query.Index,
		"cold_run":    result.Query.ColdRun,
		"run_index":   result.Query.RunIndex,
		"info_url":    result.InfoUrl,
		"succeeded":   result.QueryError == nil,
		"row_count":   result.RowCount,
		"start_time":  result.StartTime.UnixNano(),
		"duration_ms": result.Duration.Milliseconds(),
	}
	if result.Query.File != nil {
		fields["query_file"] = *result.Query.File
	} else {
		fields["query_file"] = "inline"
	}
	point := write.NewPoint("queries", tags, fields, *result.EndTime)
	if err := s.States.influxWriter.WritePoint(ctx, point); err != nil {
		log.Error().EmbedObject(result).Err(err).Msg("failed to send query summary to influxdb")
	}
}

func (s *Stage) sendRunSummaryToInfluxDB(ctx context.Context, results []*QueryResult) {
	if s.States.influxWriter == nil {
		return
	}
	tags := map[string]string{
		"run_name": s.States.RunName,
	}
	fields := map[string]interface{}{
		"start_time":  s.States.RunStartTime.UnixNano(),
		"queries_ran": len(results),
		"duration_ms": s.States.RunFinishTime.Sub(s.States.RunStartTime).Milliseconds(),
	}
	point := write.NewPoint("runs", tags, fields, s.States.RunFinishTime)
	if err := s.States.influxWriter.WritePoint(ctx, point); err != nil {
		log.Error().Str("run_name", s.States.RunName).Err(err).Msg("failed to send run summary to influxdb")
	}
}
