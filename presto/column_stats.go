package presto

type ColumnStats struct {
	ColumnName          string   `presto:"column_name" json:"column_name,omitempty"`
	DataSize            *float64 `presto:"data_size" json:"data_size,omitempty"`
	DistinctValuesCount *float64 `presto:"distinct_values_count" json:"distinct_values_count,omitempty"`
	NullsFraction       *float64 `presto:"nulls_fraction" json:"nulls_fraction,omitempty"`
	RowCount            *float64 `presto:"row_count" json:"row_count,omitempty"`
	LowValue            *string  `presto:"low_value" json:"low_value,omitempty"`
	HighValue           *string  `presto:"high_value" json:"high_value,omitempty"`
	Histogram           *string  `presto:"histogram" json:"histogram,omitempty"`
	DataType            *string  `presto:"Type" json:"data_type,omitempty"`
	Extra               *string  `presto:"Extra" json:"extra,omitempty"`
}
