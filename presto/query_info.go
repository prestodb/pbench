package presto

import "encoding/json"

type QueryInfo struct {
	QueryId         string          `json:"queryId" presto_query_creation_info:"query_id" presto_query_operator_stats:"query_id" presto_query_plans:"query_id" presto_query_stage_stats:"query_id" presto_query_statistics:"query_id"`
	Session         Session         `json:"session"`
	Self            string          `json:"self" presto_query_creation_info:"uri" presto_query_statistics:"uri"`
	Query           string          `json:"query" presto_query_creation_info:"query" presto_query_plans:"query" presto_query_statistics:"query"`
	QueryStats      QueryStats      `json:"queryStats"`
	ResourceGroupId json.RawMessage `json:"resourceGroupId" presto_query_creation_info:"resource_group_name" presto_query_statistics:"resource_group_name"`
}
