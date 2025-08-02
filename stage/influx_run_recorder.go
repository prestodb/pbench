//go:build influx

package stage

import (
	"context"
	"encoding/json"
	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	influxapi "github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"os"
	"pbench/log"
)

type InfluxRunRecorder struct {
	influxClient influxdb2.Client
	influxWriter influxapi.WriteAPIBlocking
	failed       int
	mismatch     int
}

func NewInfluxRunRecorder(cfgPath string) RunRecorder {
	if cfgPath == "" {
		return nil
	}
	if bytes, err := os.ReadFile(cfgPath); err != nil {
		log.Error().Err(err).Msg("failed to read InfluxDB connection config")
		return nil
	} else {
		influxCfg := &struct {
			Url    string `json:"url"`
			Org    string `json:"org"`
			Bucket string `json:"bucket"`
			Token  string `json:"token"`
		}{}
		if err = json.Unmarshal(bytes, influxCfg); err != nil {
			log.Error().Err(err).Msg("failed to initialize InfluxDB connection for the run recorder")
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

func (i *InfluxRunRecorder) Start(_ context.Context, _ *Stage) error {
	return nil
}

func (i *InfluxRunRecorder) RecordQuery(ctx context.Context, s *Stage, result *QueryResult) {
	tags := map[string]string{
		"run_name": s.States.RunName,
		"stage_id": result.StageId,
		"query_id": result.QueryId,
		"workload": s.States.Workload,
	}
	fields := map[string]interface{}{
		"query_index":        result.Query.Index,
		"cold_run":           result.Query.ColdRun,
		"sequence_no":        result.Query.SequenceNo,
		"info_url":           result.InfoUrl,
		"succeeded":          result.QueryError == nil,
		"row_count":          result.RowCount,
		"expected_row_count": result.Query.ExpectedRowCount,
		"start_time":         result.StartTime.UnixNano(),
		"duration_ms":        result.Duration.Milliseconds(),
	}
	if result.Query.ExpectedRowCount < 0 {
		delete(fields, "expected_row_count")
	} else if result.Query.ExpectedRowCount != result.RowCount {
		i.mismatch++
	}
	if result.QueryError != nil {
		i.failed++
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
	tags := map[string]string{
		"run_name": s.States.RunName,
	}
	fields := map[string]interface{}{
		"start_time":  s.States.RunStartTime.UnixNano(),
		"queries_ran": len(results),
		"failed":      i.failed,
		"mismatch":    i.mismatch,
		"duration_ms": s.States.RunFinishTime.Sub(s.States.RunStartTime).Milliseconds(),
		"comment":     s.States.Comment,
	}
	if s.States.RandSeedUsed {
		fields["rand_seed"] = s.States.RandSeed
	}
	point := write.NewPoint("runs", tags, fields, s.States.RunFinishTime)
	if err := i.influxWriter.WritePoint(ctx, point); err != nil {
		log.Error().Str("run_name", s.States.RunName).Err(err).Msg("failed to send run summary to influxdb")
	}
}
