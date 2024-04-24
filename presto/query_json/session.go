package query_json

import "encoding/json"

type Session struct {
	TransactionId     *string          `json:"transactionId,omitempty" presto_query_creation_info:"transaction_id" presto_query_statistics:"transaction_id"`
	Schema            *string          `json:"schema,omitempty" presto_query_creation_info:"schema_name" presto_query_statistics:"schema_name"`
	Catalog           *string          `json:"catalog,omitempty" presto_query_creation_info:"catalog_name" presto_query_statistics:"catalog_name"`
	SystemProperties  *json.RawMessage `json:"systemProperties" presto_query_creation_info:"session_properties_json" presto_query_statistics:"session_properties_json"`
	User              *string          `json:"user,omitempty" presto_query_creation_info:"user" presto_query_statistics:"user"`
	Principal         *string          `json:"principal,omitempty" presto_query_creation_info:"principal" presto_query_statistics:"principal"`
	RemoteUserAddress *string          `json:"remoteUserAddress,omitempty" presto_query_creation_info:"remote_client_address" presto_query_statistics:"remote_client_address"`
	Source            *string          `json:"source,omitempty" presto_query_creation_info:"source" presto_query_statistics:"source"`
	ResourceEstimates *json.RawMessage `json:"resourceEstimates,omitempty" presto_query_creation_info:"resource_estimates" presto_query_statistics:"resource_estimates"`
	UserAgent         *string          `json:"userAgent,omitempty" presto_query_creation_info:"user_agent" presto_query_statistics:"user_agent"`
	ClientTags        *json.RawMessage `json:"clientTags,omitempty" presto_query_creation_info:"client_tags" presto_query_statistics:"client_tags"`
}
