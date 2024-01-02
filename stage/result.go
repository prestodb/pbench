package stage

import (
	"github.com/rs/zerolog"
	"presto-benchmark/log"
	"presto-benchmark/presto"
	"time"
)

type QueryResult struct {
	StageId        string
	Query          string
	QueryFile      *string
	QueryIndex     int
	QueryId        string
	InfoUrl        string
	QueryError     error
	RowCount       int
	QueryRows      []presto.QueryRow
	StartTime      time.Time
	EndTime        *time.Time
	Duration       *time.Duration
	noLoggingQuery bool
}

func (q *QueryResult) NoLoggingQuery() *QueryResult {
	q.noLoggingQuery = true
	return q
}

func (q *QueryResult) MarshalZerologObject(e *zerolog.Event) {
	e.Str("benchmark_stage_id", q.StageId)
	if q.QueryFile != nil {
		e.Str("query_file", *q.QueryFile)
	} else if q.noLoggingQuery {
		q.noLoggingQuery = false
	} else {
		e.Str("query", q.Query)
	}
	e.Int("query_index", q.QueryIndex).
		Str("query_id", q.QueryId).
		Str("info_url", q.InfoUrl)
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
		e.Dur("duration", *q.Duration)
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
