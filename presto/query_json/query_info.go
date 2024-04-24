package query_json

import (
	"encoding/json"
	"time"
)

type QueryInfo struct {
	QueryId         string           `json:"queryId" presto_query_creation_info:"query_id" presto_query_operator_stats:"query_id" presto_query_plans:"query_id" presto_query_stage_stats:"query_id" presto_query_statistics:"query_id"`
	Self            string           `json:"self" presto_query_creation_info:"uri" presto_query_statistics:"uri"`
	Query           string           `json:"query" presto_query_creation_info:"query" presto_query_plans:"query" presto_query_statistics:"query"`
	QueryType       string           `json:"queryType" presto_query_statistics:"query_type"`
	State           string           `json:"state" presto_query_statistics:"query_state"`
	FailureInfo     *json.RawMessage `json:"failureInfo" presto_query_statistics:"failures_json"`
	ErrorCode       *ErrorCode       `json:"errorCode"`
	Warnings        *json.RawMessage `json:"warnings" presto_query_statistics:"warnings_json"`
	ResourceGroupId *json.RawMessage `json:"resourceGroupId" presto_query_creation_info:"resource_group_name" presto_query_statistics:"resource_group_name"`
	Session         *Session         `json:"session"`
	QueryStats      *QueryStats      `json:"queryStats"`
	OutputStage     *StageInfo       `json:"outputStage"`

	// Populated by PrepareForInsert
	FlattenedStageList     []*StageInfo
	ParsedFailureInfo      *FailureInfo
	AssembledQueryPlanJson string `presto_query_plans:"json_plan"`
}

type QueryStats struct {
	CreateTime                          *time.Time         `json:"createTime" presto_query_creation_info:"create_time" presto_query_statistics:"create_time"`
	EndTime                             *time.Time         `json:"endTime" presto_query_statistics:"end_time"`
	ExecutionStartTime                  *time.Time         `json:"executionStartTime" presto_query_statistics:"execution_start_time"`
	AnalysisTime                        Duration           `json:"analysisTime" presto_query_statistics:"analysis_time_ms"`
	QueuedTime                          Duration           `json:"queuedTime" presto_query_statistics:"queued_time_ms"`
	TotalPlanningTime                   Duration           `json:"totalPlanningTime" presto_query_statistics:"planning_time_ms"`
	ElapsedTime                         Duration           `json:"elapsedTime" presto_query_statistics:"query_wall_time_ms"`
	ExecutionTime                       Duration           `json:"executionTime" presto_query_statistics:"query_execution_time_ms"`
	TotalCpuTime                        Duration           `json:"totalCpuTime" presto_query_statistics:"total_split_cpu_time_ms"`
	RawInputPositions                   int64              `json:"rawInputPositions" presto_query_statistics:"total_rows"`
	RawInputDataSize                    SISize             `json:"rawInputDataSize" presto_query_statistics:"total_bytes"`
	OutputPositions                     int64              `json:"outputPositions" presto_query_statistics:"output_rows"`
	OutputDataSize                      SISize             `json:"outputDataSize" presto_query_statistics:"output_bytes"`
	WrittenOutputPositions              int64              `json:"writtenOutputPositions" presto_query_statistics:"written_rows"`
	WrittenOutputDataSize               SISize             `json:"writtenOutputDataSize" presto_query_statistics:"written_bytes"`
	CumulativeUserMemory                float64            `json:"cumulativeUserMemory" presto_query_statistics:"cumulative_memory"`
	CumulativeTotalMemory               float64            `json:"cumulativeTotalMemory" presto_query_statistics:"cumulative_total_memory"`
	PeakUserMemoryReservation           SISize             `json:"peakUserMemoryReservation" presto_query_statistics:"peak_user_memory_bytes"`
	PeakTotalMemoryReservation          SISize             `json:"peakTotalMemoryReservation" presto_query_statistics:"peak_total_memory_bytes"`
	PeakTaskUserMemory                  SISize             `json:"peakTaskUserMemory" presto_query_statistics:"peak_task_user_memory"`
	PeakTaskTotalMemory                 SISize             `json:"peakTaskTotalMemory" presto_query_statistics:"peak_task_total_memory"`
	WrittenIntermediatePhysicalDataSize SISize             `json:"writtenIntermediatePhysicalDataSize" presto_query_statistics:"written_intermediate_bytes"`
	PeakNodeTotalMemory                 SISize             `json:"peakNodeTotalMemory" presto_query_statistics:"peak_node_total_memory"`
	TotalDrivers                        int                `json:"totalDrivers" presto_query_statistics:"splits"`
	StageGcStatistics                   []*json.RawMessage `json:"stageGcStatistics"`
	OperatorSummaries                   []*OperatorSummary `json:"operatorSummaries"`

	// Calculated by PrepareForInsert
	BytesPerCPUSec int64 `presto_query_statistics:"bytes_per_cpu_sec"`
	RowsPerCPUSec  int64 `presto_query_statistics:"rows_per_cpu_sec"`
	BytesPerSec    int64 `presto_query_statistics:"bytes_per_sec"`
	StageCount     int   `presto_query_statistics:"stage_count"`
}

func (q *QueryInfo) PrepareForInsert() error {
	if q.FailureInfo != nil {
		q.ParsedFailureInfo = new(FailureInfo)
		if err := json.Unmarshal(*q.FailureInfo, q.ParsedFailureInfo); err != nil {
			return err
		}
	}

	q.QueryStats.BytesPerSec = int64(q.QueryStats.RawInputDataSize) / q.QueryStats.ExecutionTime.Milliseconds()
	if c := q.QueryStats.TotalCpuTime.Milliseconds(); c != 0 {
		q.QueryStats.BytesPerCPUSec = int64(q.QueryStats.RawInputDataSize) / c
		q.QueryStats.RowsPerCPUSec = q.QueryStats.RawInputPositions / c
	}
	q.QueryStats.StageCount = len(q.QueryStats.StageGcStatistics)

	q.FlattenedStageList = make([]*StageInfo, 0, 8)
	s := q.OutputStage
	// To avoid this root output stage to be visited more than once during reflection,
	// we set to nil after the tree is flattened.
	q.OutputStage = nil
	AssembledQueryPlan := make(map[string]WrappedPlan)
	if err := s.PrepareForInsert(&q.FlattenedStageList, AssembledQueryPlan); err != nil {
		return err
	}
	if planJson, err := json.Marshal(AssembledQueryPlan); err != nil {
		return err
	} else {
		q.AssembledQueryPlanJson = string(planJson)
	}
	return nil
}
