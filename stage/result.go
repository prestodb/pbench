package stage

import (
	"github.com/rs/zerolog"
	"presto-benchmark/log"
	"time"
)

type QueryResult struct {
	StageId       string
	Query         string
	QueryFile     *string
	QueryIndex    int
	QueryId       string
	InfoUrl       string
	QueryError    error
	RowCount      int
	StartTime     time.Time
	EndTime       *time.Time
	Duration      *time.Duration
	simpleLogging bool
}

func (q *QueryResult) SimpleLogging() *QueryResult {
	q.simpleLogging = true
	return q
}

func (q *QueryResult) MarshalZerologObject(e *zerolog.Event) {
	e.Str("benchmark_stage_id", q.StageId)
	if q.QueryFile != nil {
		e.Str("query_file", *q.QueryFile)
	} else if !q.simpleLogging {
		e.Str("query", q.Query)
	}
	e.Int("query_index", q.QueryIndex)
	e.Str("info_url", q.InfoUrl)
	if q.simpleLogging {
		q.simpleLogging = false
		return
	}
	e.Str("query_id", q.QueryId)
	if q.QueryError != nil {
		e.Object("query_error", log.NewMarshaller(q.QueryError))
	} else {
		e.Int("row_count", q.RowCount)
	}
	e.Time("start_time", q.StartTime)
	if q.EndTime != nil {
		e.Time("finish_time", *q.EndTime)
	}
	if q.Duration != nil {
		e.Float64("duration_in_seconds", q.Duration.Seconds())
	}
}

func (q *QueryResult) Error() string {
	if q.QueryError != nil {
		return q.QueryError.Error()
	}
	return ""
}

func (q *QueryResult) Unwrap() error {
	return q.QueryError
}

// ConcludeExecution sets the query end time and calculate the query duration
func (q *QueryResult) ConcludeExecution() {
	q.EndTime = getNow()
	dur := q.EndTime.Sub(q.StartTime)
	q.Duration = &dur
}
