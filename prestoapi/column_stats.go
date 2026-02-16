package prestoapi

type ColumnStats struct {
	ColumnName string `presto:"column_name" json:"column_name,omitempty"`
	// From SHOW STATS FOR
	DataSize            *float64 `presto:"data_size" json:"data_size,omitempty"`
	DistinctValuesCount *float64 `presto:"distinct_values_count" json:"distinct_values_count,omitempty"`
	NullsFraction       *float64 `presto:"nulls_fraction" json:"nulls_fraction,omitempty"`
	RowCount            *float64 `presto:"row_count" json:"row_count,omitempty"`
	LowValue            *string  `presto:"low_value" json:"low_value,omitempty"`
	HighValue           *string  `presto:"high_value" json:"high_value,omitempty"`
	// From running queries manually
	MaxDataSize        *float64 `presto:"max_data_size" json:"max_data_size,omitempty"`
	TrueValuesCount    *float64 `presto:"true_values_count" json:"true_values_count,omitempty"`
	NonNullValuesCount *float64 `presto:"non_null_values_count" json:"non_null_values_count,omitempty"`
	// From DESCRIBE
	DataType  *string  `presto:"Type" json:"data_type,omitempty"`
	Extra     *string  `presto:"Extra" json:"extra,omitempty"`
	Precision *float64 `presto:"Precision" json:"precision,omitempty"`
	Scale     *float64 `presto:"Scale" json:"scale,omitempty"`
	Length    *float64 `presto:"Length" json:"length,omitempty"`
}
