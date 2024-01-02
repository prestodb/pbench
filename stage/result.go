package stage

import "presto-benchmark/presto"

type BenchMarkQueryResult struct {
	StageContext *presto.QueryMetadata
	RowCount     int
}
