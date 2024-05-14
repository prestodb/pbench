//go:build !influx

package stage

import (
	"context"
	"errors"
)

func NewInfluxRunRecorder(_ string) RunRecorder {
	return &NotSupportedRecorder{}
}

type NotSupportedRecorder struct{}

func (*NotSupportedRecorder) Start(ctx context.Context, s *Stage) error {
	return errors.New("InfluxDB support not available with this build. Please rebuild with `make TAGS=influx`")
}

func (*NotSupportedRecorder) RecordQuery(ctx context.Context, s *Stage, result *QueryResult) {
}

func (*NotSupportedRecorder) RecordRun(ctx context.Context, s *Stage, results []*QueryResult) {
}
