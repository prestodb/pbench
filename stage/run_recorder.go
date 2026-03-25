package stage

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"os"
	"path/filepath"
	"pbench/log"
	"strconv"
	"time"
)

type RunRecorder interface {
	Start(ctx context.Context, s *Stage) error
	RecordQuery(ctx context.Context, s *Stage, result *QueryResult)
	RecordRun(ctx context.Context, s *Stage, results []*QueryResult)
}

type FileBasedRunRecorder struct {
	buf       bytes.Buffer
	csvWriter *csv.Writer
}

func NewFileBasedRunRecorder() *FileBasedRunRecorder {
	return &FileBasedRunRecorder{}
}

func (f *FileBasedRunRecorder) Start(_ context.Context, _ *Stage) error {
	f.csvWriter = csv.NewWriter(&f.buf)
	f.csvWriter.UseCRLF = false
	if err := f.csvWriter.Write([]string{
		"stage_id", "query_file", "query_index", "cold_run", "sequence_no",
		"info_url", "succeeded", "row_count", "expected_row_count",
		"start_time", "end_time", "duration_in_seconds",
	}); err != nil {
		return fmt.Errorf("failed to write CSV header: %w", err)
	}
	return nil
}

func (f *FileBasedRunRecorder) RecordQuery(_ context.Context, _ *Stage, result *QueryResult) {
	queryFile := "inline"
	if result.Query.File != nil {
		queryFile = *result.Query.File
	}
	endTimeStr := ""
	if result.EndTime != nil {
		endTimeStr = result.EndTime.Format(time.RFC3339)
	}
	durationSecs := 0.0
	if result.Duration != nil {
		durationSecs = result.Duration.Seconds()
	}
	if err := f.csvWriter.Write([]string{
		result.StageId,
		queryFile,
		strconv.Itoa(result.Query.Index),
		strconv.FormatBool(result.Query.ColdRun),
		strconv.Itoa(result.Query.SequenceNo),
		result.InfoUrl,
		strconv.FormatBool(result.QueryError == nil),
		strconv.Itoa(result.RowCount),
		strconv.Itoa(result.Query.ExpectedRowCount),
		result.StartTime.Format(time.RFC3339),
		endTimeStr,
		fmt.Sprintf("%f", durationSecs),
	}); err != nil {
		log.Error().Err(err).Msg("failed to write CSV row")
	}
}

func (f *FileBasedRunRecorder) RecordRun(_ context.Context, s *Stage, _ []*QueryResult) {
	f.csvWriter.Flush()
	if err := os.WriteFile(filepath.Join(s.States.OutputPath, s.Id+"_summary.csv"), f.buf.Bytes(), 0644); err != nil {
		log.Error().Err(err).Msg("failed to write run summary CSV")
	}
}
