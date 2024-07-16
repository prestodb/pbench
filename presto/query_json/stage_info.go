package query_json

import (
	"bytes"
	"encoding/json"
	"strconv"
)

type StageInfo struct {
	StageId                    string               `json:"stageId" presto_query_stage_stats:"stage_id"`
	LatestAttemptExecutionInfo *StageExecutionInfo  `json:"latestAttemptExecutionInfo"`
	Plan                       *StagePlan           `json:"plan"`
	TrinoStats                 *StageExecutionStats `json:"stageStats"`

	SubStages []*StageInfo `json:"subStages"`

	// Stats will be written by PrepareForInsert
	StageExecutionId int `json:"-" presto_query_stage_stats:"stage_execution_id"`
}

type StageExecutionInfo struct {
	State string               `json:"state"`
	Stats *StageExecutionStats `json:"stats"`
}

type StageExecutionStats struct {
	TotalTasks              int              `json:"totalTasks" presto_query_stage_stats:"tasks"`
	TotalScheduledTime      Duration         `json:"totalScheduledTime" presto_query_stage_stats:"total_scheduled_time_ms"`
	TotalCpuTime            Duration         `json:"totalCpuTime" presto_query_stage_stats:"total_cpu_time_ms"`
	RetriedCpuTime          Duration         `json:"retriedCpuTime" presto_query_stage_stats:"retried_cpu_time_ms"`
	TotalBlockedTime        Duration         `json:"totalBlockedTime" presto_query_stage_stats:"total_blocked_time_ms"`
	RawInputDataSize        SISize           `json:"rawInputDataSize" presto_query_stage_stats:"raw_input_data_size_bytes"`
	ProcessedInputDataSize  SISize           `json:"processedInputDataSize" presto_query_stage_stats:"processed_input_data_size_bytes"`
	PhysicalWrittenDataSize SISize           `json:"physicalWrittenDataSize" presto_query_stage_stats:"physical_written_data_size_bytes"`
	GcInfoJson              *json.RawMessage `json:"gcInfo" presto_query_stage_stats:"gc_statistics"`
	// We need to insert the JSON text of GcInfo as well as the stage_execution_id from GcInfo, so we maintain a parsed copy.
	GcInfo *StageGcInfo `json:"-"`
}

type StagePlan struct {
	JsonRepresentation string `json:"jsonRepresentation"`
}

type StageGcInfo struct {
	StageExecutionId int `json:"stageExecutionId"`
}

type RawPlanWrapper struct {
	Plan json.RawMessage `json:"plan"`
}

func selectStats(stats *StageExecutionStats, stageExecutionInfo *StageExecutionInfo) *StageExecutionStats {
	if stats != nil {
		return stats
	}
	if stageExecutionInfo != nil {
		return stageExecutionInfo.Stats
	}
	return nil
}

func (s *StageInfo) PrepareForInsert(flattened *[]*StageInfo, queryPlan map[string]RawPlanWrapper) error {
	if s == nil {
		return nil
	}
	if index := bytes.IndexByte([]byte(s.StageId), '.'); index > 0 && index+1 < len(s.StageId) {
		// The stage IDs are in the format of 'query_id.[index]', we only keep the index in the database.
		s.StageId = s.StageId[index+1:]
	}
	// Trino plan does not have a last attempt execution info, unlike Presto
	// https://github.com/prestodb/presto/commit/009a234eac113194396d858df69c23a4c578e3f0#diff-d1065b7bf35e2a6b74d251e3d7c2a439e3a029057f87c3a166b89074dd58c4ee
	stats := selectStats(s.TrinoStats, s.LatestAttemptExecutionInfo)
	stats.GcInfo = new(StageGcInfo)
	if err := json.Unmarshal(*stats.GcInfoJson, stats.GcInfo); err != nil {
		return err
	}
	s.StageExecutionId = stats.GcInfo.StageExecutionId
	*flattened = append(*flattened, s)

	// RawPlanWrapper is only needed for formatting the json.
	queryPlan[strconv.Itoa(len(queryPlan))] = RawPlanWrapper{
		Plan: json.RawMessage(s.Plan.JsonRepresentation),
	}

	for _, child := range s.SubStages {
		if err := child.PrepareForInsert(flattened, queryPlan); err != nil {
			return err
		}
	}
	// Avoid SubStages to be visited again during reflection.
	s.SubStages = nil
	return nil
}
