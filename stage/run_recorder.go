package stage

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"
)

type RunRecorder interface {
	RecordQuery(ctx context.Context, s *Stage, result *QueryResult)
	RecordRun(ctx context.Context, s *Stage, results []*QueryResult)
}

type FileBasedRunRecorder struct {
	summaryBuilder strings.Builder
}

func NewFileBasedRunRecorder() *FileBasedRunRecorder {
	r := &FileBasedRunRecorder{}
	r.summaryBuilder.WriteString("stage_id,query_file,query_index,cold_run,sequence_no,info_url,succeeded,row_count,expected_row_count,start_time,end_time,duration_in_seconds\n")
	return r
}

func (f *FileBasedRunRecorder) RecordQuery(_ context.Context, _ *Stage, result *QueryResult) {
	f.summaryBuilder.WriteString(result.StageId + ",")
	if result.Query.File != nil {
		f.summaryBuilder.WriteString(*result.Query.File)
	} else {
		f.summaryBuilder.WriteString("inline")
	}
	f.summaryBuilder.WriteString(fmt.Sprintf(",%d,%t,%d,%s,%t,%d,%d,%s,%s,%f\n",
		result.Query.Index, result.Query.ColdRun, result.Query.SequenceNo, result.InfoUrl,
		result.QueryError == nil, result.RowCount, result.Query.ExpectedRowCount, result.StartTime.Format(time.RFC3339),
		result.EndTime.Format(time.RFC3339), result.Duration.Seconds()))
}

func (f *FileBasedRunRecorder) RecordRun(_ context.Context, s *Stage, _ []*QueryResult) {
	_ = os.WriteFile(filepath.Join(s.States.OutputPath, s.Id+"_summary.csv"), []byte(f.summaryBuilder.String()), 0644)
}
