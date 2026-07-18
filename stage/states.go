package stage

import (
	"context"
	"pbench/utils"
	"reflect"
	"sync"
	"sync/atomic"
	"time"
)

type SharedStageStates struct {
	RunName       string
	Comment       string
	ServerFQDN    string
	RandSeed      int64
	RandSkip      int
	RandSeedUsed  atomic.Bool
	RunStartTime  time.Time
	RunFinishTime time.Time
	// OutputPath is where we store the logs, query results, query json files, query column metadata files, etc.
	// It should be set by the --output/-o command-line argument. Once set there, its value gets propagated to all the stages.
	OutputPath string
	// SnapshotsEnabled is the run-wide default for periodic query JSON snapshot collection,
	// set via the --snapshots command-line flag. Stages can override it with SaveJsonSnapshots.
	// See stage/json_snapshot.go.
	SnapshotsEnabled bool
	// SnapshotInterval is the run-wide default snapshot polling interval, set via --snapshot-interval.
	SnapshotInterval time.Duration
	// SnapshotMax is the run-wide default snapshot retention cap per query, set via --snapshot-max.
	// 0 means unlimited (only meaningful when explicitly set, since the flag's own default is non-zero).
	SnapshotMax int
	// SnapshotFetchTimeout is the run-wide default per-fetch timeout for snapshot requests, set via
	// --snapshot-fetch-timeout. <= 0 means "use the polling interval".
	SnapshotFetchTimeout time.Duration
	// NewClient is called when the stage needs to create a new Presto client. This function is passed down to descendant stages by default.
	NewClient utils.NewPrestoClientFn
	// AbortAll is passed down to descendant stages by default and will be used to cancel the current context.
	AbortAll context.CancelCauseFunc `json:"-"`
	// OnQueryCompletion is called after a query's result is drained. You cannot access query result in this function.
	// If you need to access the query result, pass in a ResultBatchHandler when calling Drain() on a query result object.
	OnQueryCompletion OnQueryCompletionFn `json:"-"`
	// wgExitMainStage blocks the main stage from exiting before it counts down to zero, so we can wait for other
	// goroutines to finish.
	// All descendant stages of the main stage and the goroutines that are responsible for persisting the benchmark result
	// will increase the value of this latch.
	wgExitMainStage *sync.WaitGroup
	// Stages use resultChan to send the query result back to the main stage.
	resultChan   chan *QueryResult
	runRecorders []RunRecorder
	exitCode     atomic.Int32
}

func (states *SharedStageStates) RegisterRunRecorder(r RunRecorder) {
	if r == nil || reflect.ValueOf(r).IsNil() {
		return
	}
	states.runRecorders = append(states.runRecorders, r)
}
