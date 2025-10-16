package stage

import (
	"context"
	"database/sql"
	_ "embed"
	"pbench/log"
	"pbench/utils"

	_ "github.com/go-sql-driver/mysql"
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

func NewMySQLRunRecorder(cfgPath string) *MySQLRunRecorder {
	db := utils.InitMySQLConnFromCfg(cfgPath)
	return NewMySQLRunRecorderWithDb(db)
}

func NewMySQLRunRecorderWithDb(db *sql.DB) *MySQLRunRecorder {
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

func (m *MySQLRunRecorder) Start(_ context.Context, s *Stage) error {
	recordNewRun := `INSERT INTO pbench_runs (run_name, cluster_fqdn, start_time, queries_ran, failed, mismatch, comment)
VALUES (?, ?, ?, 0, 0, 0, ?)`
	res, err := m.db.Exec(recordNewRun, s.States.RunName, s.States.ServerFQDN, s.States.RunStartTime, s.States.Comment)
	if err != nil {
		log.Error().Err(err).Str("run_name", s.States.RunName).Time("start_time", s.States.RunStartTime).
			Msg("failed to add a new run to the MySQL database")
		return err
	} else {
		m.runId, _ = res.LastInsertId()
		log.Info().Int64("run_id", m.runId).Str("run_name", s.States.RunName).
			Msg("added a new run to the MySQL database")
	}
	return nil
}

func (m *MySQLRunRecorder) RecordQuery(_ context.Context, s *Stage, result *QueryResult) {
	recordNewQuery := `INSERT INTO pbench_queries (run_id, stage_id, query_file, query_index, query_id, sequence_no,
cold_run, succeeded, start_time, end_time, row_count, expected_row_count, duration_ms, info_url, seed) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`
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
		result.Query.SequenceNo, result.Query.ColdRun, result.QueryError == nil, result.StartTime, *result.EndTime,
		result.RowCount, sql.NullInt32{
			Int32: int32(result.Query.ExpectedRowCount),
			Valid: result.Query.ExpectedRowCount >= 0,
		}, result.Duration.Milliseconds(), result.InfoUrl, result.Seed)
	log.Info().Str("stage_id", result.StageId).Stringer("start_time", result.StartTime).Stringer("end_time", result.EndTime).
		Str("info_url", result.InfoUrl).Int64("seed", result.Seed).Msg("recorded query result to MySQL")
	if err != nil {
		log.Error().EmbedObject(result).Err(err).Msg("failed to send query summary to MySQL")
	}
	updateRunInfo := `UPDATE pbench_runs SET start_time = ?, queries_ran = queries_ran + 1, failed = ? , mismatch = ? WHERE run_id = ?`
	res, err := m.db.Exec(updateRunInfo, s.States.RunStartTime, m.failed, m.mismatch, m.runId)
	if err != nil {
		log.Error().Err(err).Str("run_name", s.States.RunName).Int64("run_id", m.runId).
			Msg("failed to update the run information in the MySQL database")
	}
	if rowsAffected, _ := res.RowsAffected(); rowsAffected > 1 {
		log.Error().Err(err).Str("run_name", s.States.RunName).Int64("run_id", m.runId).Int64("rows_affected", rowsAffected).
			Msg("more than 1 row was affected when trying to complete the run information in the MySQL database")
	}
}

func (m *MySQLRunRecorder) RecordRun(ctx context.Context, s *Stage, results []*QueryResult) {
	completeRunInfo := `UPDATE pbench_runs SET start_time = ?, duration_ms = ?, rand_seed = ? WHERE run_id = ?`
	randSeed := sql.NullInt64{
		Int64: s.States.RandSeed,
		Valid: s.States.RandSeedUsed,
	}
	res, err := m.db.Exec(completeRunInfo, s.States.RunStartTime,
		s.States.RunFinishTime.Sub(s.States.RunStartTime).Milliseconds(), randSeed, m.runId)
	if err != nil {
		log.Error().Err(err).Str("run_name", s.States.RunName).Int64("run_id", m.runId).
			Msg("failed to complete the run information in the MySQL database")
	}
	if rowsAffected, _ := res.RowsAffected(); rowsAffected > 1 {
		log.Error().Err(err).Str("run_name", s.States.RunName).Int64("run_id", m.runId).Int64("rows_affected", rowsAffected).
			Msg("more than 1 row was affected when trying to complete the run information in the MySQL database")
	}
}
