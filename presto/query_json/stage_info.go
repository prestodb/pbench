package query_json

import (
	"bytes"
	"encoding/json"
	"strconv"
)

type StageInfo struct {
	StageId                    string              `json:"stageId" presto_query_stage_stats:"stage_id"`
	LatestAttemptExecutionInfo *StageExecutionInfo `json:"latestAttemptExecutionInfo"`
	Plan                       *StagePlan          `json:"plan"`

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

type WrappedPlan struct {
	Plan json.RawMessage `json:"plan"`
}

func (s *StageInfo) PrepareForInsert(flattened *[]*StageInfo, queryPlan map[string]WrappedPlan) error {
	if s == nil {
		return nil
	}
	if index := bytes.IndexByte([]byte(s.StageId), '.'); index > 0 && index+1 < len(s.StageId) {
		// The stage IDs are in the format of 'query_id.[index]', we only keep the index in the database.
		s.StageId = s.StageId[index+1:]
	}
	stats := s.LatestAttemptExecutionInfo.Stats
	stats.GcInfo = new(StageGcInfo)
	if err := json.Unmarshal(*stats.GcInfoJson, stats.GcInfo); err != nil {
		return err
	}
	s.StageExecutionId = stats.GcInfo.StageExecutionId
	*flattened = append(*flattened, s)

	// WrappedPlan is only needed for formatting the json.
	queryPlan[strconv.Itoa(len(queryPlan))] = WrappedPlan{
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
