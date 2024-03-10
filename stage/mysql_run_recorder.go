package stage

import (
	"context"
	"database/sql"
	_ "embed"
	"encoding/json"
	"fmt"
	_ "github.com/go-sql-driver/mysql"
	"os"
	"pbench/log"
)

var (
	//go:embed pbench_runs_ddl.sql
	pbenchRunsDDL string
	//go:embed pbench_queries_ddl.sql
	pbenchQueriesDDL string
)

type MySQLRunRecorder struct {
	db       *sql.DB
	runId    int64
	failed   int
	mismatch int
}

func initMySQLConn(cfgPath string) *sql.DB {
	if cfgPath == "" {
		return nil
	}
	if bytes, ioErr := os.ReadFile(cfgPath); ioErr != nil {
		log.Error().Err(ioErr).Msg("failed to read MySQL connection config")
		return nil
	} else {
		mySQLCfg := &struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Server   string `json:"server"`
			Database string `json:"database"`
		}{}
		if err := json.Unmarshal(bytes, mySQLCfg); err != nil {
			log.Error().Err(err).Msg("failed to unmarshal MySQL connection config for the run recorder")
			return nil
		}
		if db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
			mySQLCfg.Username, mySQLCfg.Password, mySQLCfg.Server, mySQLCfg.Database)); err != nil {
			log.Error().Err(err).Msg("failed to initialize MySQL connection for the run recorder")
			return nil
		} else {
			return db
		}
	}
}

func NewMySQLRunRecorder(cfgPath string) *MySQLRunRecorder {
	db := initMySQLConn(cfgPath)
	if db == nil {
		return nil
	}
	_, err := db.Exec(pbenchRunsDDL)
	if err == nil {
		_, err = db.Exec(pbenchQueriesDDL)
	}
	if err != nil {
		log.Error().Err(err).Msg("failed to create MySQL table")
		return nil
	}
	log.Info().Msg("MySQL connection initialized, benchmark result summary will be sent to this database.")
	return &MySQLRunRecorder{
		db:    db,
		runId: -1,
	}
}

func (m *MySQLRunRecorder) RecordQuery(ctx context.Context, s *Stage, result *QueryResult) {
	if m.runId < 0 {
		recordNewRun := `INSERT INTO pbench_runs (run_name, cluster_fqdn, start_time) VALUES (?, ?, ?)`
		res, err := m.db.Exec(recordNewRun, s.States.RunName, s.States.ServerFQDN, s.States.RunStartTime)
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
cold_run, succeeded, start_time, end_time, row_count, expected_row_count, duration_ms, info_url) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
	var queryFile string
	if result.Query.File != nil {
		queryFile = *result.Query.File
	} else {
		queryFile = "inline"
	}
	if result.QueryError != nil {
		m.failed++
	}
	if result.Query.ExpectedRowCount >= 0 && result.Query.ExpectedRowCount != result.RowCount {
		m.mismatch++
	}
	_, err := m.db.Exec(recordNewQuery, m.runId, result.StageId, queryFile, result.Query.Index, result.QueryId,
		result.Query.RunIndex, result.Query.ColdRun, result.QueryError == nil, result.StartTime, *result.EndTime,
		result.RowCount, sql.NullInt32{
			Int32: int32(result.Query.ExpectedRowCount),
			Valid: result.Query.ExpectedRowCount >= 0,
		}, result.Duration.Milliseconds(), result.InfoUrl)
	if err != nil {
		log.Error().EmbedObject(result).Err(err).Msg("failed to send query summary to MySQL")
	}
}

func (m *MySQLRunRecorder) RecordRun(ctx context.Context, s *Stage, results []*QueryResult) {
	completeRunInfo := `UPDATE pbench_runs SET queries_ran = ?, failed = ?, mismatch = ?, duration_ms = ? WHERE run_id = ?`
	res, err := m.db.Exec(completeRunInfo, len(results), m.failed, m.mismatch,
		s.States.RunFinishTime.Sub(s.States.RunStartTime).Milliseconds(), m.runId)
	if err != nil {
		log.Error().Err(err).Str("run_name", s.States.RunName).Int64("run_id", m.runId).
			Msg("failed to complete the run information in the MySQL database")
	}
	if rowsAffected, _ := res.RowsAffected(); rowsAffected > 1 {
		log.Error().Err(err).Str("run_name", s.States.RunName).Int64("run_id", m.runId).Int64("rows_affected", rowsAffected).
			Msg("more than 1 row was affected when trying to complete the run information in the MySQL database")
	}
}
