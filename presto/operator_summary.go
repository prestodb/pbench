package presto

import (
	"encoding/json"
	"fmt"
	"time"
)

type OperatorSummary struct {
	StageId          int      `json:"stageId" presto_query_operator_stats:"stage_id"`
	StageExecutionId int      `json:"stageExecutionId" presto_query_operator_stats:"stage_execution_id"`
	PipelineId       int      `json:"pipelineId" presto_query_operator_stats:"pipeline_id"`
	OperatorId       int      `json:"operatorId" presto_query_operator_stats:"operator_id"`
	PlanNodeId       string   `json:"planNodeId" presto_query_operator_stats:"plan_node_id"`
	OperatorType     string   `json:"operatorType" presto_query_operator_stats:"operator_type"`
	TotalDrivers     int      `json:"totalDrivers" presto_query_operator_stats:"total_drivers"`
	AddInputCalls    int      `json:"addInputCalls" presto_query_operator_stats:"add_input_calls"`
	AddInputWall     Duration `json:"addInputWall" presto_query_operator_stats:"add_input_wall_ms"`
	AddInputCpu      Duration `json:"addInputCpu" presto_query_operator_stats:"add_input_cpu_ms"`
}

type Duration struct {
	time.Duration
}

func (d *Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v any
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value)
		return nil
	case string:
		var err error
		d.Duration, err = time.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("invalid duration")
	}
}
