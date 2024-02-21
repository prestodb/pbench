package stage

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
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

type MySQLRunRecorder struct {
	db    *sql.DB
	runId int64
}

type InfluxRunRecorder struct {
	influxClient influxdb2.Client
	influxWriter influxapi.WriteAPIBlocking
}

type FileBasedRunRecorder struct {
	summaryBuilder strings.Builder
}

var (
	//go:embed pbench_runs_ddl.sql
	pbenchRunsDDL string
	//go:embed pbench_queries_ddl.sql
	pbenchQueriesDDL string
)

func NewMySQLRunRecorder(cfgPath string) *MySQLRunRecorder {
	if cfgPath == "" {
		return nil
	}
	if bytes, err := os.ReadFile(cfgPath); err != nil {
		log.Error().Err(err).Msg("failed to read MySQL connection config")
		return nil
	} else {
		mySQLCfg := &struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Server   string `json:"server"`
			Database string `json:"database"`
		}{}
		if err = json.Unmarshal(bytes, mySQLCfg); err != nil {
			log.Error().Err(err).Msg("failed to unmarshal MySQL connection config for the run recorder")
			return nil
		}
		if db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
			mySQLCfg.Username, mySQLCfg.Password, mySQLCfg.Server, mySQLCfg.Database)); err != nil {
			log.Error().Err(err).Msg("failed to initialize MySQL connection for the run recorder")
			return nil
		} else {
			log.Info().Msg("MySQL connection initialized, benchmark result summary will be sent to this database.")
			_, err = db.Exec(pbenchRunsDDL)
			if err == nil {
				_, err = db.Exec(pbenchQueriesDDL)
			}
			if err != nil {
				log.Error().Err(err).Msg("failed to create MySQL table")
			}
			return &MySQLRunRecorder{
				db:    db,
				runId: -1,
			}
		}
	}
}

func NewInfluxRunRecorder(cfgPath string) *InfluxRunRecorder {
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

func (m *MySQLRunRecorder) RecordQuery(ctx context.Context, s *Stage, result *QueryResult) {
	if m.runId < 0 {
		recordNewRun := `INSERT INTO pbench_runs (run_name, start_time) VALUES (?, ?)`
		res, err := m.db.Exec(recordNewRun, s.States.RunName, s.States.RunStartTime)
		if err != nil {
			log.Error().Err(err).Str("run_name", s.States.RunName).Time("start_time", s.States.RunStartTime).
				Msg("failed to add a new run to the MySQL database")
		} else {
			m.runId, _ = res.LastInsertId()
			log.Info().Int64("run_id", m.runId).Str("run_name", s.States.RunName).
				Msg("added a new run to the MySQL database")
		}
	}
	recordNewQuery := `INSERT INTO pbench_queries (run_id, stage_id, query_file, query_index, query_id, run_index,
cold_run, succeeded, start_time, end_time, row_count, duration_ms, info_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	var queryFile string
	if result.Query.File != nil {
		queryFile = *result.Query.File
	} else {
		queryFile = "inline"
	}
	_, err := m.db.Exec(recordNewQuery, m.runId, result.StageId, queryFile, result.Query.Index, result.QueryId,
		result.Query.RunIndex, result.Query.ColdRun, result.QueryError == nil, result.StartTime, *result.EndTime,
		result.RowCount, result.Duration.Milliseconds(), result.InfoUrl)
	if err != nil {
		log.Error().EmbedObject(result).Err(err).Msg("failed to send query summary to MySQL")
	}
}

func (m *MySQLRunRecorder) RecordRun(ctx context.Context, s *Stage, results []*QueryResult) {
	completeRunInfo := `UPDATE pbench_runs SET queries_ran = ?, duration_ms = ? WHERE run_id = ?`
	res, err := m.db.Exec(completeRunInfo, len(results), s.States.RunFinishTime.Sub(s.States.RunStartTime).Milliseconds(), m.runId)
	if err != nil {
		log.Error().Err(err).Str("run_name", s.States.RunName).Int64("run_id", m.runId).
			Msg("failed to complete the run information in the MySQL database")
	}
	if rowsAffected, _ := res.RowsAffected(); rowsAffected > 1 {
		log.Error().Err(err).Str("run_name", s.States.RunName).Int64("run_id", m.runId).Int64("rows_affected", rowsAffected).
			Msg("more than 1 row was affected when trying to complete the run information in the MySQL database")
	}
}

func (i *InfluxRunRecorder) RecordQuery(ctx context.Context, s *Stage, result *QueryResult) {
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
