package query_json

type FailureInfo struct {
	Type    *string `json:"type,omitempty" presto_query_statistics:"failure_type"`
	Message *string `json:"message,omitempty" presto_query_statistics:"failure_message"`
}

type ErrorCode struct {
	Code *int    `json:"code,omitempty" presto_query_statistics:"error_code"`
	Name *string `json:"name,omitempty" presto_query_statistics:"error_code_name"`
	Type *string `json:"type,omitempty" presto_query_statistics:"error_category"`
}
