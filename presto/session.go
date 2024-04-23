package presto

import "encoding/json"

type Session struct {
	TransactionId     string           `json:"transactionId" presto_query_creation_info:"transaction_id" presto_query_statistics:"transaction_id"`
	Schema            *string          `json:"schema,omitempty" presto_query_creation_info:"schema_name" presto_query_statistics:"schema_name"`
	Catalog           *string          `json:"catalog,omitempty" presto_query_creation_info:"catalog_name" presto_query_statistics:"catalog_name"`
	SystemProperties  *json.RawMessage `json:"systemProperties" presto_query_creation_info:"session_properties_json" presto_query_statistics:"session_properties_json"`
	User              string           `json:"user" presto_query_creation_info:"user" presto_query_statistics:"user"`
	RemoteUserAddress string           `json:"remoteUserAddress" presto_query_creation_info:"remote_client_address" presto_query_statistics:"remote_client_address"`
	Source            *string          `json:"source,omitempty" presto_query_creation_info:"source" presto_query_statistics:"source"`
	UserAgent         string           `json:"userAgent" presto_query_creation_info:"user_agent" presto_query_statistics:"user_agent"`
	ClientTags        *json.RawMessage `json:"clientTags" presto_query_creation_info:"client_tags" presto_query_statistics:"client_tags"`
}
