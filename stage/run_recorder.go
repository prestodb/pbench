package stage

import (
	"context"
	"encoding/json"
	"fmt"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxapi "github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"os"
	"path/filepath"
	"presto-benchmark/log"
	"strings"
	"time"
)

type RunRecorder interface {
	RecordQuery(ctx context.Context, s *Stage, result *QueryResult)
	RecordRun(ctx context.Context, s *Stage, results []*QueryResult)
}

type InfluxRunRecorder struct {
	influxClient influxdb2.Client
	influxWriter influxapi.WriteAPIBlocking
}

type FileBasedRunRecorder struct {
	summaryBuilder strings.Builder
}

func NewInfluxRunRecorder(cfgPath string) *InfluxRunRecorder {
	if bytes, err := os.ReadFile(cfgPath); err != nil {
		return nil
	} else {
		influxCfg := &struct {
			Url    string `json:"url"`
			Org    string `json:"org"`
			Bucket string `json:"bucket"`
			Token  string `json:"token"`
		}{}
		err = json.Unmarshal(bytes, influxCfg)
		if err != nil {
			log.Info().Err(err).Msg("failed to initialize InfluxDB connection as the run recorder")
			return nil
		}
		influxClient := influxdb2.NewClient(influxCfg.Url, influxCfg.Token)
		r := &InfluxRunRecorder{
			influxClient: influxClient,
			influxWriter: influxClient.WriteAPIBlocking(influxCfg.Org, influxCfg.Bucket),
		}
		log.Info().Str("url", influxCfg.Url).Str("org", influxCfg.Org).Str("bucket", influxCfg.Bucket).
			Msg("InfluxDB connection initialized, benchmark result summary will be sent to this database.")
		return r
	}
}

func (i *InfluxRunRecorder) RecordQuery(ctx context.Context, s *Stage, result *QueryResult) {
	if i.influxWriter == nil {
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
	if err := i.influxWriter.WritePoint(ctx, point); err != nil {
		log.Error().EmbedObject(result).Err(err).Msg("failed to send query summary to influxdb")
	}
}

func (i *InfluxRunRecorder) RecordRun(ctx context.Context, s *Stage, results []*QueryResult) {
	if i.influxWriter == nil {
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
	if err := i.influxWriter.WritePoint(ctx, point); err != nil {
		log.Error().Str("run_name", s.States.RunName).Err(err).Msg("failed to send run summary to influxdb")
	}
}

func (f *FileBasedRunRecorder) RecordQuery(_ context.Context, _ *Stage, result *QueryResult) {
	f.summaryBuilder.WriteString(result.StageId + ",")
	if result.Query.File != nil {
		f.summaryBuilder.WriteString(*result.Query.File)
	} else {
		f.summaryBuilder.WriteString("inline")
	}
	f.summaryBuilder.WriteString(fmt.Sprintf(",%d,%t,%d,%s,%t,%d,%s,%s,%f\n",
		result.Query.Index, result.Query.ColdRun, result.Query.RunIndex, result.InfoUrl,
		result.QueryError == nil, result.RowCount, result.StartTime.Format(time.RFC3339),
		result.EndTime.Format(time.RFC3339), result.Duration.Seconds()))
}

func (f *FileBasedRunRecorder) RecordRun(_ context.Context, s *Stage, _ []*QueryResult) {
	_ = os.WriteFile(filepath.Join(s.States.OutputPath, s.Id+"_summary.csv"), []byte(f.summaryBuilder.String()), 0644)
}
