package presto

import "time"

type QueryStats struct {
	CreateTime        time.Time         `json:"createTime" presto_query_creation_info:"create_time" presto_query_statistics:"create_time"`
	OperatorSummaries []OperatorSummary `json:"operatorSummaries"`
}
