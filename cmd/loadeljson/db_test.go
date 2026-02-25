package loadeljson

import (
	"context"
	"database/sql"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInsertQueryCreationInfo(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	queryId := "test_query_id"

	qce := &QueryCompletedEvent{
		Metadata: QueryMetadata{
			QueryId:    "20250616_085426_00030_ux3a6",
			Query:      "SELECT * FROM test",
			QueryState: "FINISHED",
			Uri:        "http://example.com",
		},
		Context: QueryContext{
			User:              "presto",
			ServerVersion:     "0.282",
			Environment:       "test",
			ClientTags:        []string{},
			SessionProperties: map[string]string{},
			ResourceEstimates: ResourceEstimates{},
		},
		CreateTime: PrestoTime{Time: time.Date(2025, 6, 16, 8, 48, 8, 0, time.UTC)},
	}

	mock.ExpectExec("REPLACE INTO presto_query_creation_info").
		WithArgs(
			queryId,
			qce.Metadata.Query,
			sqlmock.AnyArg(), // create_time
			"", "",           // schema, catalog
			qce.Context.Environment,
			qce.Context.User,
			"", "", "", // remote_client_address, source, user_agent
			qce.Metadata.Uri,
			sqlmock.AnyArg(), // session_properties_json
			qce.Context.ServerVersion,
			"", "", // client_info, resource_group_name
			"", "", // principal, transaction_id
			sqlmock.AnyArg(), // client_tags
			sqlmock.AnyArg(), // resource_estimates
			sqlmock.AnyArg(), // dt
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = insertQueryCreationInfo(ctx, db, qce, queryId)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertQueryPlans(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	queryId := "test_query_id"
	plan := "test plan"
	jsonPlan := "{}"

	qce := &QueryCompletedEvent{
		Metadata: QueryMetadata{
			QueryId:  "test_id",
			Query:    "SELECT 1",
			Plan:     &plan,
			JsonPlan: &jsonPlan,
		},
		Context: QueryContext{
			Environment: "test",
		},
		CreateTime: PrestoTime{Time: time.Date(2025, 6, 16, 8, 48, 8, 0, time.UTC)},
	}

	mock.ExpectExec("REPLACE INTO presto_query_plans").
		WithArgs(
			queryId,
			qce.Metadata.Query,
			plan,
			jsonPlan,
			qce.Context.Environment,
			sqlmock.AnyArg(), // dt
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = insertQueryPlans(ctx, db, qce, queryId)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertQueryStageStats(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	queryId := "test_query_id"

	qce := &QueryCompletedEvent{
		StageStatistics: []StageStatistics{
			{
				StageId:                 0,
				StageExecutionId:        0,
				Tasks:                   1,
				TotalScheduledTime:      Duration{Duration: 209 * time.Millisecond},
				TotalCpuTime:            Duration{Duration: 19 * time.Millisecond},
				RetriedCpuTime:          Duration{Duration: 0},
				TotalBlockedTime:        Duration{Duration: 36*time.Minute + 46*time.Second},
				RawInputDataSize:        DataSize{Bytes: 40500},
				ProcessedInputDataSize:  DataSize{Bytes: 23840},
				PhysicalWrittenDataSize: DataSize{Bytes: 0},
			},
		},
		CreateTime: PrestoTime{Time: time.Date(2025, 6, 16, 8, 48, 8, 0, time.UTC)},
	}

	mock.ExpectExec("REPLACE INTO presto_query_stage_stats").
		WithArgs(
			queryId,
			0, 0, 1, // stage_id, stage_execution_id, tasks
			int64(209), int64(19), int64(0), // scheduled, cpu, retried cpu
			int64(2206000),                       // blocked time in ms
			int64(40500), int64(23840), int64(0), // data sizes
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), // gc, cpu dist, mem dist
			sqlmock.AnyArg(), // dt
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = insertQueryStageStats(ctx, db, qce, queryId)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertQueryStageStats_Empty(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	queryId := "test_query_id"

	qce := &QueryCompletedEvent{
		StageStatistics: []StageStatistics{},
		CreateTime:      PrestoTime{Time: time.Now()},
	}

	// Should not execute any queries when there are no stage statistics
	err = insertQueryStageStats(ctx, db, qce, queryId)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertQueryOperatorStats(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	queryId := "test_query_id"
	info := "test info"

	qce := &QueryCompletedEvent{
		OperatorStatistics: []OperatorStatistics{
			{
				StageId:          0,
				StageExecutionId: 0,
				PipelineId:       0,
				OperatorId:       0,
				PlanNodeId:       "637",
				OperatorType:     "ExchangeOperator",
				TotalDrivers:     32,
				AddInputCalls:    0,
				AddInputWall:     Duration{Duration: 0},
				AddInputCpu:      Duration{Duration: 0},
				Info:             &info,
			},
		},
		CreateTime: PrestoTime{Time: time.Date(2025, 6, 16, 8, 48, 8, 0, time.UTC)},
	}

	mock.ExpectExec("REPLACE INTO presto_query_operator_stats").
		WithArgs(
			queryId,
			0, 0, 0, 0, // stage_id, stage_execution_id, pipeline_id, operator_id
			"637", "ExchangeOperator", int64(32), // plan_node_id, operator_type, total_drivers
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), // add_input_calls, add_input_wall_ms, add_input_cpu_ms, add_input_allocation_bytes
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), // raw_input_data_size_bytes, raw_input_positions, input_data_size_bytes, input_positions
			sqlmock.AnyArg(),                                                       // sum_squared_input_positions
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), // get_output_calls, get_output_wall_ms, get_output_cpu_ms, get_output_allocation_bytes
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), // output_data_size_bytes, output_positions, physical_written_data_size_bytes
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), // blocked_wall_ms, finish_calls, finish_wall_ms
			sqlmock.AnyArg(), sqlmock.AnyArg(), // finish_cpu_ms, finish_allocation_bytes
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), // user_memory_reservation_bytes, revocable_memory_reservation_bytes, system_memory_reservation_bytes
			sqlmock.AnyArg(), sqlmock.AnyArg(), sqlmock.AnyArg(), // peak_user_memory_reservation_bytes, peak_system_memory_reservation_bytes, peak_total_memory_reservation_bytes
			sqlmock.AnyArg(), info, sqlmock.AnyArg(), // spilled_data_size_bytes, info, dt
		).
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = insertQueryOperatorStats(ctx, db, qce, queryId)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertQueryStatistics(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	queryId := "test_query_id"
	queryType := "SELECT"

	qce := &QueryCompletedEvent{
		Metadata: QueryMetadata{
			QueryId:    "test_id",
			Query:      "SELECT 1",
			QueryState: "FINISHED",
			Uri:        "http://example.com",
		},
		Statistics: QueryStatistics{
			CpuTime:                          Duration{Duration: 371792 * time.Millisecond},
			WallTime:                         Duration{Duration: 67795 * time.Millisecond},
			QueuedTime:                       Duration{Duration: 0},
			PeakUserMemoryBytes:              98161684,
			PeakTotalNonRevocableMemoryBytes: 1098226989,
			PeakTaskUserMemory:               10433657,
			PeakTaskTotalMemory:              202274048,
			PeakNodeTotalMemory:              222073173,
			TotalBytes:                       26975446031,
			TotalRows:                        2880361048,
			OutputPositions:                  100,
			OutputBytes:                      4801,
			WrittenOutputRows:                0,
			WrittenOutputBytes:               0,
			CumulativeMemory:                 5.359635941788949e12,
			CumulativeTotalMemory:            8.514280417681293e12,
			CompletedSplits:                  7776,
		},
		Context: QueryContext{
			User:              "presto",
			ServerVersion:     "0.282",
			Environment:       "test",
			ClientTags:        []string{},
			SessionProperties: map[string]string{},
			ResourceEstimates: ResourceEstimates{},
		},
		QueryType:          &queryType,
		Warnings:           []interface{}{},
		CreateTime:         PrestoTime{Time: time.Date(2025, 6, 16, 8, 48, 8, 759000000, time.UTC)},
		ExecutionStartTime: PrestoTime{Time: time.Date(2025, 6, 16, 8, 48, 9, 69000000, time.UTC)},
		EndTime:            PrestoTime{Time: time.Date(2025, 6, 16, 8, 49, 16, 554000000, time.UTC)},
		StageStatistics:    []StageStatistics{},
	}

	mock.ExpectExec("REPLACE INTO presto_query_statistics").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = insertQueryStatistics(ctx, db, qce, queryId)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertQueryStatistics_WithFailure(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	queryId := "test_query_id"
	failureMsg := "Syntax error"
	failureType := "com.facebook.presto.sql.parser.ParsingException"

	qce := &QueryCompletedEvent{
		Metadata: QueryMetadata{
			QueryId:    "test_id",
			Query:      "SELECT * FROM nonexistent",
			QueryState: "FAILED",
			Uri:        "http://example.com",
		},
		Statistics: QueryStatistics{
			CpuTime:                          Duration{Duration: 0},
			WallTime:                         Duration{Duration: 0},
			QueuedTime:                       Duration{Duration: 0},
			PeakUserMemoryBytes:              0,
			PeakTotalNonRevocableMemoryBytes: 0,
			PeakTaskUserMemory:               0,
			PeakTaskTotalMemory:              0,
			PeakNodeTotalMemory:              0,
			TotalBytes:                       0,
			TotalRows:                        0,
			OutputPositions:                  0,
			OutputBytes:                      0,
			WrittenOutputRows:                0,
			WrittenOutputBytes:               0,
			CumulativeMemory:                 0,
			CumulativeTotalMemory:            0,
			CompletedSplits:                  0,
		},
		Context: QueryContext{
			User:              "test",
			ServerVersion:     "0.282",
			Environment:       "test",
			ClientTags:        []string{},
			SessionProperties: map[string]string{},
			ResourceEstimates: ResourceEstimates{},
		},
		FailureInfo: &QueryFailureInfo{
			ErrorCode: ErrorCode{
				Code: 1,
				Name: "SYNTAX_ERROR",
				Type: "USER_ERROR",
			},
			FailureMessage: &failureMsg,
			FailureType:    &failureType,
			FailuresJson:   "{}",
		},
		Warnings:           []interface{}{},
		CreateTime:         PrestoTime{Time: time.Now()},
		ExecutionStartTime: PrestoTime{Time: time.Now()},
		EndTime:            PrestoTime{Time: time.Now()},
		StageStatistics:    []StageStatistics{},
	}

	mock.ExpectExec("REPLACE INTO presto_query_statistics").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = insertQueryStatistics(ctx, db, qce, queryId)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertEventListenerData(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	queryId := "test_query_id"

	qce := &QueryCompletedEvent{
		Metadata: QueryMetadata{
			QueryId:    "test_id",
			Query:      "SELECT 1",
			QueryState: "FINISHED",
			Uri:        "http://example.com",
		},
		Statistics: QueryStatistics{
			CpuTime:                          Duration{Duration: 0},
			WallTime:                         Duration{Duration: 0},
			QueuedTime:                       Duration{Duration: 0},
			PeakUserMemoryBytes:              0,
			PeakTotalNonRevocableMemoryBytes: 0,
			PeakTaskUserMemory:               0,
			PeakTaskTotalMemory:              0,
			PeakNodeTotalMemory:              0,
			TotalBytes:                       0,
			TotalRows:                        0,
			OutputPositions:                  0,
			OutputBytes:                      0,
			WrittenOutputRows:                0,
			WrittenOutputBytes:               0,
			CumulativeMemory:                 0,
			CumulativeTotalMemory:            0,
			CompletedSplits:                  0,
		},
		Context: QueryContext{
			User:              "test",
			ServerVersion:     "0.282",
			Environment:       "test",
			ClientTags:        []string{},
			SessionProperties: map[string]string{},
			ResourceEstimates: ResourceEstimates{},
		},
		Warnings:           []interface{}{},
		CreateTime:         PrestoTime{Time: time.Now()},
		ExecutionStartTime: PrestoTime{Time: time.Now()},
		EndTime:            PrestoTime{Time: time.Now()},
		StageStatistics:    []StageStatistics{},
		OperatorStatistics: []OperatorStatistics{},
	}

	// Expect all 5 insert operations
	mock.ExpectExec("REPLACE INTO presto_query_creation_info").
		WillReturnResult(sqlmock.NewResult(1, 1))
	mock.ExpectExec("REPLACE INTO presto_query_plans").
		WillReturnResult(sqlmock.NewResult(1, 1))
	// Stage stats and operator stats are empty, so no expectations for them
	mock.ExpectExec("REPLACE INTO presto_query_statistics").
		WillReturnResult(sqlmock.NewResult(1, 1))

	err = insertEventListenerData(ctx, db, qce, queryId)
	assert.NoError(t, err)
	assert.NoError(t, mock.ExpectationsWereMet())
}

func TestInsertEventListenerData_DatabaseError(t *testing.T) {
	db, mock, err := sqlmock.New()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	queryId := "test_query_id"

	qce := &QueryCompletedEvent{
		Metadata: QueryMetadata{
			QueryId:    "test_id",
			Query:      "SELECT 1",
			QueryState: "FINISHED",
			Uri:        "http://example.com",
		},
		Statistics:         QueryStatistics{},
		Context:            QueryContext{User: "test", ServerVersion: "0.282", Environment: "test", ClientTags: []string{}, SessionProperties: map[string]string{}, ResourceEstimates: ResourceEstimates{}},
		Warnings:           []interface{}{},
		CreateTime:         PrestoTime{Time: time.Now()},
		ExecutionStartTime: PrestoTime{Time: time.Now()},
		EndTime:            PrestoTime{Time: time.Now()},
		StageStatistics:    []StageStatistics{},
		OperatorStatistics: []OperatorStatistics{},
	}

	// Simulate a database error
	mock.ExpectExec("REPLACE INTO presto_query_creation_info").
		WillReturnError(sql.ErrConnDone)

	err = insertEventListenerData(ctx, db, qce, queryId)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "failed to insert query creation info")
}
