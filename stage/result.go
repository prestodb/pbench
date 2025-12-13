package stage

import (
	"pbench/log"
	"time"

	"github.com/rs/zerolog"
)

type QueryResult struct {
	StageId    string
	Query      *Query
	QueryId    string
	InfoUrl    string
	QueryError error
	RowCount   int
	StartTime  time.Time
	EndTime    *time.Time
	Duration   *time.Duration
}

// simpleQueryResult is a wrapper for logging QueryResult with reduced output
type simpleQueryResult struct {
	*QueryResult
}

func (q *QueryResult) SimpleLogging() zerolog.LogObjectMarshaler {
	return simpleQueryResult{q}
}

func (q *QueryResult) MarshalZerologObject(e *zerolog.Event) {
	q.marshalZerologObject(e, false)
}

func (s simpleQueryResult) MarshalZerologObject(e *zerolog.Event) {
	s.QueryResult.marshalZerologObject(e, true)
}

func (q *QueryResult) marshalZerologObject(e *zerolog.Event, simpleLogging bool) {
	e.Str("benchmark_stage_id", q.StageId)
	if q.Query.File != nil {
		e.Str("query_file", *q.Query.File)
	} else if !simpleLogging {
		e.Str("query", q.Query.Text)
	}
	e.Int("query_index", q.Query.Index)
	e.Bool("cold_run", q.Query.ColdRun)
	e.Int("sequence_no", q.Query.SequenceNo)
	e.Str("info_url", q.InfoUrl)
	if simpleLogging {
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
