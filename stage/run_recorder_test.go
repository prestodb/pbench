package stage

import (
	"context"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFileBasedRunRecorder_StartWritesHeader(t *testing.T) {
	recorder := NewFileBasedRunRecorder()
	err := recorder.Start(context.Background(), nil)
	require.NoError(t, err)

	// csv.Writer buffers internally; flush to see the output
	recorder.csvWriter.Flush()
	assert.Contains(t, recorder.buf.String(), "stage_id")
	assert.Contains(t, recorder.buf.String(), "duration_in_seconds")
}

func TestFileBasedRunRecorder_RecordQuery(t *testing.T) {
	recorder := NewFileBasedRunRecorder()
	require.NoError(t, recorder.Start(context.Background(), nil))

	queryFile := "test.sql"
	endTime := time.Now()
	dur := 1500 * time.Millisecond
	result := &QueryResult{
		StageId:   "stage_1",
		Query:     &Query{File: &queryFile, Index: 0, ColdRun: true, SequenceNo: 0, ExpectedRowCount: 10},
		QueryId:   "q1",
		RowCount:  10,
		StartTime: endTime.Add(-dur),
		EndTime:   &endTime,
		Duration:  &dur,
	}

	recorder.RecordQuery(context.Background(), nil, result)

	recorder.csvWriter.Flush()
	output := recorder.buf.String()
	assert.Contains(t, output, "stage_1")
	assert.Contains(t, output, "test.sql")
}

func TestFileBasedRunRecorder_RecordQueryInline(t *testing.T) {
	recorder := NewFileBasedRunRecorder()
	require.NoError(t, recorder.Start(context.Background(), nil))

	result := &QueryResult{
		StageId:   "stage_1",
		Query:     &Query{Index: 0, ColdRun: false, SequenceNo: 1, ExpectedRowCount: -1},
		StartTime: time.Now(),
	}

	recorder.RecordQuery(context.Background(), nil, result)

	recorder.csvWriter.Flush()
	output := recorder.buf.String()
	assert.Contains(t, output, "inline")
}
