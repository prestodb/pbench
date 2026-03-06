package loadeljson

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"pbench/log"
)

// insertEventListenerData inserts data into multiple tables to mirror Java implementation in MySQLWriter.post()
func insertEventListenerData(ctx context.Context, db *sql.DB, qce *QueryCompletedEvent, queryId string) error {
	// 1. Insert into presto_query_creation_info
	if err := insertQueryCreationInfo(ctx, db, qce, queryId); err != nil {
		return fmt.Errorf("failed to insert query creation info: %w", err)
	}

	// 2. Insert into presto_query_plans
	if err := insertQueryPlans(ctx, db, qce, queryId); err != nil {
		return fmt.Errorf("failed to insert query plans: %w", err)
	}

	// 3. Insert into presto_query_stage_stats
	if err := insertQueryStageStats(ctx, db, qce, queryId); err != nil {
		return fmt.Errorf("failed to insert query stage stats: %w", err)
	}

	// 4. Insert into presto_query_operator_stats
	if err := insertQueryOperatorStats(ctx, db, qce, queryId); err != nil {
		return fmt.Errorf("failed to insert query operator stats: %w", err)
	}

	// 5. Insert into presto_query_statistics
	if err := insertQueryStatistics(ctx, db, qce, queryId); err != nil {
		return fmt.Errorf("failed to insert query statistics: %w", err)
	}

	return nil
}

func insertQueryCreationInfo(ctx context.Context, db *sql.DB, qce *QueryCompletedEvent, queryId string) error {
	query := `REPLACE INTO presto_query_creation_info(
		query_id, query, create_time, schema_name, catalog_name, environment,
		user, remote_client_address, source, user_agent, uri,
		session_properties_json, server_version, client_info, resource_group_name,
		principal, transaction_id, client_tags, resource_estimates, dt
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	var schema, catalog, remoteAddr, source, userAgent, clientInfo, principal, resourceGroupId string
	if qce.Context.Schema != nil {
		schema = *qce.Context.Schema
	}
	if qce.Context.Catalog != nil {
		catalog = *qce.Context.Catalog
	}
	if qce.Context.RemoteClientAddress != nil {
		remoteAddr = *qce.Context.RemoteClientAddress
	}
	if qce.Context.Source != nil {
		source = *qce.Context.Source
	}
	if qce.Context.UserAgent != nil {
		userAgent = *qce.Context.UserAgent
	}
	if qce.Context.ClientInfo != nil {
		clientInfo = *qce.Context.ClientInfo
	}
	if qce.Context.Principal != nil {
		principal = *qce.Context.Principal
	}
	if qce.Context.ResourceGroupId != nil {
		resourceGroupId = qce.Context.ResourceGroupId.Value
	}

	// Handle transaction_id
	var transactionId string
	if qce.Metadata.TransactionId != nil {
		transactionId = *qce.Metadata.TransactionId
	}

	// Marshal client_tags
	clientTagsJson, err := json.Marshal(qce.Context.ClientTags)
	if err != nil {
		log.Error().Err(err).Str("query_id", queryId).Msg("failed to marshal client tags")
		clientTagsJson = []byte("[]")
	}

	// Marshal resource_estimates
	resourceEstimatesJson, err := json.Marshal(qce.Context.ResourceEstimates)
	if err != nil {
		log.Error().Err(err).Str("query_id", queryId).Msg("failed to marshal resource estimates")
		resourceEstimatesJson = []byte("{}")
	}

	sessionPropsJson, err := json.Marshal(qce.Context.SessionProperties)
	if err != nil {
		log.Error().Err(err).Str("query_id", queryId).Msg("failed to marshal session properties")
		sessionPropsJson = []byte("{}")
	}

	dt := qce.CreateTime.Time.Format("2006-01-02 15:04:05")

	_, err = db.ExecContext(ctx, query,
		queryId, qce.Metadata.Query,
		qce.CreateTime.Time.Format("2006-01-02 15:04:05"),
		schema, catalog, qce.Context.Environment,
		qce.Context.User, remoteAddr, source, userAgent, qce.Metadata.Uri,
		string(sessionPropsJson), qce.Context.ServerVersion, clientInfo, resourceGroupId,
		principal, transactionId, string(clientTagsJson), string(resourceEstimatesJson), dt,
	)

	return err
}

func insertQueryPlans(ctx context.Context, db *sql.DB, qce *QueryCompletedEvent, queryId string) error {
	query := `REPLACE INTO presto_query_plans(
		query_id, query, plan, json_plan, environment, dt
	) VALUES (?, ?, ?, ?, ?, ?)`

	var plan, jsonPlan string
	if qce.Metadata.Plan != nil {
		plan = *qce.Metadata.Plan
	}
	if qce.Metadata.JsonPlan != nil {
		jsonPlan = *qce.Metadata.JsonPlan
	}

	dt := qce.CreateTime.Time.Format("2006-01-02 15:04:05")

	_, err := db.ExecContext(ctx, query,
		queryId, qce.Metadata.Query, plan, jsonPlan, qce.Context.Environment, dt,
	)

	return err
}

func insertQueryStageStats(ctx context.Context, db *sql.DB, qce *QueryCompletedEvent, queryId string) error {
	if len(qce.StageStatistics) == 0 {
		return nil
	}

	query := `REPLACE INTO presto_query_stage_stats(
		query_id, stage_id, stage_execution_id, tasks,
		total_scheduled_time_ms, total_cpu_time_ms, retried_cpu_time_ms, total_blocked_time_ms,
		raw_input_data_size_bytes, processed_input_data_size_bytes, physical_written_data_size_bytes,
		gc_statistics, cpu_distribution, memory_distribution, dt
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	dt := qce.CreateTime.Time.Format("2006-01-02 15:04:05")

	for _, stage := range qce.StageStatistics {
		// These fields are not available in the event listener JSON, so we use empty JSON objects
		gcStatsJson := []byte("{}")
		cpuDistJson := []byte("{}")
		memDistJson := []byte("{}")

		_, err := db.ExecContext(ctx, query,
			queryId, stage.StageId, stage.StageExecutionId, stage.Tasks,
			stage.TotalScheduledTime.Milliseconds(),
			stage.TotalCpuTime.Milliseconds(),
			stage.RetriedCpuTime.Milliseconds(),
			stage.TotalBlockedTime.Milliseconds(),
			stage.RawInputDataSize.Bytes,
			stage.ProcessedInputDataSize.Bytes,
			stage.PhysicalWrittenDataSize.Bytes,
			string(gcStatsJson), string(cpuDistJson), string(memDistJson), dt,
		)

		if err != nil {
			return fmt.Errorf("failed to insert stage %d: %w", stage.StageId, err)
		}
	}

	return nil
}

func insertQueryOperatorStats(ctx context.Context, db *sql.DB, qce *QueryCompletedEvent, queryId string) error {
	if len(qce.OperatorStatistics) == 0 {
		return nil
	}

	query := `REPLACE INTO presto_query_operator_stats(
		query_id, stage_id, stage_execution_id, pipeline_id, operator_id,
		plan_node_id, operator_type, total_drivers,
		add_input_calls, add_input_wall_ms, add_input_cpu_ms, add_input_allocation_bytes,
		raw_input_data_size_bytes, raw_input_positions, input_data_size_bytes, input_positions,
		sum_squared_input_positions, get_output_calls, get_output_wall_ms, get_output_cpu_ms,
		get_output_allocation_bytes, output_data_size_bytes, output_positions,
		physical_written_data_size_bytes, blocked_wall_ms, finish_calls, finish_wall_ms,
		finish_cpu_ms, finish_allocation_bytes, user_memory_reservation_bytes,
		revocable_memory_reservation_bytes, system_memory_reservation_bytes,
		peak_user_memory_reservation_bytes, peak_system_memory_reservation_bytes,
		peak_total_memory_reservation_bytes, spilled_data_size_bytes, info, dt
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	dt := qce.CreateTime.Time.Format("2006-01-02 15:04:05")

	for _, op := range qce.OperatorStatistics {
		var info string
		if op.Info != nil {
			info = *op.Info
		}

		_, err := db.ExecContext(ctx, query,
			queryId, op.StageId, op.StageExecutionId, op.PipelineId, op.OperatorId,
			op.PlanNodeId, op.OperatorType, op.TotalDrivers,
			op.AddInputCalls, op.AddInputWall.Milliseconds(), op.AddInputCpu.Milliseconds(),
			op.AddInputAllocation.Bytes, op.RawInputDataSize.Bytes, op.RawInputPositions,
			op.InputDataSize.Bytes, op.InputPositions, 0.0, // sumSquaredInputPositions not available
			op.GetOutputCalls, op.GetOutputWall.Milliseconds(), op.GetOutputCpu.Milliseconds(),
			op.GetOutputAllocation.Bytes, op.OutputDataSize.Bytes, op.OutputPositions,
			op.PhysicalWrittenDataSize.Bytes, op.BlockedWall.Milliseconds(), op.FinishCalls,
			op.FinishWall.Milliseconds(), op.FinishCpu.Milliseconds(), op.FinishAllocation.Bytes,
			op.UserMemoryReservation.Bytes, op.RevocableMemoryReservation.Bytes,
			op.SystemMemoryReservation.Bytes, op.PeakUserMemoryReservation.Bytes,
			op.PeakSystemMemoryReservation.Bytes, op.PeakTotalMemoryReservation.Bytes,
			op.SpilledDataSize.Bytes, info, dt,
		)

		if err != nil {
			return fmt.Errorf("failed to insert operator %d: %w", op.OperatorId, err)
		}
	}

	return nil
}

func insertQueryStatistics(ctx context.Context, db *sql.DB, qce *QueryCompletedEvent, queryId string) error {
	// Insert into presto_query_statistics table
	// This mirrors the Java implementation in PrestoQueryStatsDao.insertQueryStatistics

	query := `REPLACE INTO presto_query_statistics(
		query_id, query, query_type, schema_name, catalog_name, environment,
		user, remote_client_address, source, user_agent, uri,
		session_properties_json, server_version, client_info, resource_group_name,
		principal, transaction_id, client_tags, resource_estimates,
		create_time, end_time, execution_start_time, query_state,
		failure_message, failure_type, failures_json, failure_task, failure_host,
		error_code, error_code_name, error_category, warnings_json,
		splits, analysis_time_ms, queued_time_ms, query_wall_time_ms,
		query_execution_time_ms, bytes_per_cpu_sec, bytes_per_sec, rows_per_cpu_sec,
		total_bytes, total_rows, output_rows, output_bytes,
		written_rows, written_bytes, cumulative_memory, peak_user_memory_bytes,
		peak_total_memory_bytes, peak_task_total_memory, peak_task_user_memory,
		written_intermediate_bytes, peak_node_total_memory, total_split_cpu_time_ms,
		stage_count, cumulative_total_memory, dt
	) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`

	var queryType string
	if qce.QueryType != nil {
		queryType = *qce.QueryType
	}

	var schema, catalog, remoteAddr, source, userAgent, clientInfo, principal, resourceGroupId string
	if qce.Context.Schema != nil {
		schema = *qce.Context.Schema
	}
	if qce.Context.Catalog != nil {
		catalog = *qce.Context.Catalog
	}
	if qce.Context.RemoteClientAddress != nil {
		remoteAddr = *qce.Context.RemoteClientAddress
	}
	if qce.Context.Source != nil {
		source = *qce.Context.Source
	}
	if qce.Context.UserAgent != nil {
		userAgent = *qce.Context.UserAgent
	}
	if qce.Context.ClientInfo != nil {
		clientInfo = *qce.Context.ClientInfo
	}
	if qce.Context.Principal != nil {
		principal = *qce.Context.Principal
	}
	if qce.Context.ResourceGroupId != nil {
		resourceGroupId = qce.Context.ResourceGroupId.Value
	}

	var failureMsg, failureType, failureTask, failureHost, failuresJson string
	var errorCode int
	var errorCodeName, errorCategory string

	if qce.FailureInfo != nil {
		if qce.FailureInfo.FailureMessage != nil {
			failureMsg = *qce.FailureInfo.FailureMessage
		}
		if qce.FailureInfo.FailureType != nil {
			failureType = *qce.FailureInfo.FailureType
		}
		if qce.FailureInfo.FailureTask != nil {
			failureTask = *qce.FailureInfo.FailureTask
		}
		if qce.FailureInfo.FailureHost != nil {
			failureHost = *qce.FailureInfo.FailureHost
		}
		failuresJson = qce.FailureInfo.FailuresJson
		errorCode = qce.FailureInfo.ErrorCode.Code
		errorCodeName = qce.FailureInfo.ErrorCode.Name
		errorCategory = qce.FailureInfo.ErrorCode.Type
	}

	// Handle transaction_id
	var transactionId string
	if qce.Metadata.TransactionId != nil {
		transactionId = *qce.Metadata.TransactionId
	}

	// Marshal client_tags
	clientTagsJson, err := json.Marshal(qce.Context.ClientTags)
	if err != nil {
		log.Error().Err(err).Str("query_id", queryId).Msg("failed to marshal client tags")
		clientTagsJson = []byte("[]")
	}

	// Marshal resource_estimates
	resourceEstimatesJson, err := json.Marshal(qce.Context.ResourceEstimates)
	if err != nil {
		log.Error().Err(err).Str("query_id", queryId).Msg("failed to marshal resource estimates")
		resourceEstimatesJson = []byte("{}")
	}

	sessionPropsJson, err := json.Marshal(qce.Context.SessionProperties)
	if err != nil {
		log.Error().Err(err).Str("query_id", queryId).Msg("failed to marshal session properties")
		sessionPropsJson = []byte("{}")
	}
	warningsJson, err := json.Marshal(qce.Warnings)
	if err != nil {
		log.Error().Err(err).Str("query_id", queryId).Msg("failed to marshal warnings")
		warningsJson = []byte("[]")
	}

	var analysisTimeMs int64
	if qce.Statistics.AnalysisTime != nil {
		analysisTimeMs = qce.Statistics.AnalysisTime.Milliseconds()
	}

	// Calculate query_execution_time_ms from ResourceEstimates
	var queryExecutionTimeMs int64
	if qce.Context.ResourceEstimates.ExecutionTime != nil {
		queryExecutionTimeMs = qce.Context.ResourceEstimates.ExecutionTime.Milliseconds()
	}

	cpuTimeMs := qce.Statistics.CpuTime.Milliseconds()
	var bytesPerCpuSec, bytesPerSec, rowsPerCpuSec int64
	if cpuTimeMs > 0 {
		bytesPerCpuSec = qce.Statistics.TotalBytes / cpuTimeMs
		rowsPerCpuSec = qce.Statistics.TotalRows / cpuTimeMs
	}
	// Calculate bytes_per_sec from ResourceEstimates.ExecutionTime
	if queryExecutionTimeMs > 0 {
		bytesPerSec = qce.Statistics.TotalBytes / queryExecutionTimeMs
	}

	dt := qce.CreateTime.Time.Format("2006-01-02 15:04:05")

	_, err = db.ExecContext(ctx, query,
		queryId, qce.Metadata.Query, queryType, schema, catalog, qce.Context.Environment,
		qce.Context.User, remoteAddr, source, userAgent, qce.Metadata.Uri,
		string(sessionPropsJson), qce.Context.ServerVersion, clientInfo, resourceGroupId,
		principal, transactionId, string(clientTagsJson), string(resourceEstimatesJson),
		qce.CreateTime.Time.Format("2006-01-02 15:04:05"),
		qce.EndTime.Time.Format("2006-01-02 15:04:05"),
		qce.ExecutionStartTime.Time.Format("2006-01-02 15:04:05"),
		qce.Metadata.QueryState,
		failureMsg, failureType, failuresJson, failureTask, failureHost,
		errorCode, errorCodeName, errorCategory, string(warningsJson),
		qce.Statistics.CompletedSplits, analysisTimeMs,
		qce.Statistics.QueuedTime.Milliseconds(),
		qce.Statistics.WallTime.Milliseconds(),
		queryExecutionTimeMs,
		bytesPerCpuSec, bytesPerSec, rowsPerCpuSec,
		qce.Statistics.TotalBytes, qce.Statistics.TotalRows,
		qce.Statistics.OutputPositions, qce.Statistics.OutputBytes,
		qce.Statistics.WrittenOutputRows, qce.Statistics.WrittenOutputBytes, qce.Statistics.CumulativeMemory,
		qce.Statistics.PeakUserMemoryBytes,
		qce.Statistics.PeakTotalNonRevocableMemoryBytes, qce.Statistics.PeakTaskTotalMemory, qce.Statistics.PeakTaskUserMemory,
		qce.Statistics.WrittenOutputBytes, qce.Statistics.PeakNodeTotalMemory,
		cpuTimeMs,
		len(qce.StageStatistics), qce.Statistics.CumulativeTotalMemory, dt,
	)

	return err
}
