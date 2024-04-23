package presto

import "encoding/json"

type QueryInfo struct {
	QueryId         string           `json:"queryId" presto_query_creation_info:"query_id" presto_query_operator_stats:"query_id" presto_query_stage_stats:"query_id" presto_query_statistics:"query_id"`
	Session         *Session         `json:"session"`
	Self            string           `json:"self" presto_query_creation_info:"uri" presto_query_statistics:"uri"`
	Query           string           `json:"query" presto_query_creation_info:"query" presto_query_statistics:"query"`
	QueryStats      *QueryStats      `json:"queryStats"`
	OutputStage     *StageInfo       `json:"outputStage"`
	ResourceGroupId *json.RawMessage `json:"resourceGroupId" presto_query_creation_info:"resource_group_name" presto_query_statistics:"resource_group_name"`

	FlattenedStageList []*StageInfo
}

func (q *QueryInfo) FlattenAndPrepareForInsert() error {
	q.FlattenedStageList = make([]*StageInfo, 0, 8)
	s := q.OutputStage
	// To avoid this root output stage to be visited more than once during reflection,
	// we set to nil after the tree is flattened.
	q.OutputStage = nil
	return s.FlattenAndPrepareForInsert(&q.FlattenedStageList)
}
