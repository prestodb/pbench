package stage

import (
	"github.com/rs/zerolog"
	"presto-benchmark/log"
	"time"
)

type QueryResult struct {
	StageId       string
	Query         *Query
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
	if q.Query.File != nil {
		e.Str("query_file", *q.Query.File)
	} else if !q.simpleLogging {
		e.Str("query", q.Query.Text)
	}
	e.Int("query_index", q.Query.Index)
	e.Bool("cold_run", q.Query.ColdRun)
	e.Int("run_index", q.Query.RunIndex)
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
	if q.Query.ExpectedRowCount >= 0 {
		e.Int("expected_row_count", q.Query.ExpectedRowCount)
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
