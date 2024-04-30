package presto

type TableStats struct {
	Columns  []ColumnStats
	RowCount int64
}

type ColumnStats struct {
	ColumnName          string   `presto:"column_name"`
	DataSize            *float64 `presto:"data_size"`
	DistinctValuesCount *float64 `presto:"distinct_values_count"`
	NullsFraction       *float64 `presto:"nulls_fraction"`
	RowCount            *float64 `presto:"row_count"`
	LowValue            *string  `presto:"low_value"`
	HighValue           *string  `presto:"high_value"`
	Histogram           *string  `presto:"histogram"`
}
