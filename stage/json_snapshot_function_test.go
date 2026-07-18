package stage

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/http/httputil"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	presto "github.com/prestodb/presto-go-client/v2"
	"github.com/prestodb/presto-go-client/v2/prestotest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Phase-1 function tests (FT-1..FT-7), end-to-end oriented per design.md §8.2. They drive
// the real Stage.Run() lifecycle (setDefaults/propagateStates/runQuery/saveQueryJsonFile)
// against a mock Presto coordinator, rather than calling jsonSnapshotPoller methods
// directly (see json_snapshot_test.go for those narrower unit tests).
//
// Deviation from design.md's illustrative timings: §4.1 fixes a 5s floor on the polling
// interval (protects a real coordinator from excessive polling), but several FT-* examples
// in §8.2 illustrate sub-floor intervals (e.g. "--snapshot-interval 3s", "interval 1s") for
// brevity. To keep this suite's total runtime reasonable while still exercising the real,
// floor-respecting production path end-to-end, these tests use interval=5s (the floor) or
// larger wherever multiple ticks must be observed, and scale query durations accordingly.
// FT-6's backoff/resume behavior is exercised via the same direct-poller technique used for
// UT-6 (see TestSnapshotPoller_NeverPermanentlyDisabled_ResumesAfterRecovery and
// TestSnapshotBackoffTicks_Sequence in json_snapshot_test.go), since it is already covered
// end-to-end (real ticker, real HTTP, real backoff state machine) there, just without the
// 5s floor, which would otherwise require ~30s of failing polls to observe recovery.
//
// M1 follow-up: stop() now cancels the context shared with every fetch (see
// stage/json_snapshot.go), so a fetch already in flight when the query drains is aborted
// deterministically instead of being left to run to completion. TestFT7_* below exercises
// that hard guarantee end-to-end (superseding the previous "let the in-flight fetch finish"
// behavior/test).

// registerLatencyQuery registers a query template on mock sized so that the *poller's*
// running window - i.e. the wall-clock time from when s.Client.Query() returns (and the
// poller can start) to when the query fully drains - is approximately pollerWindow.
//
// MockPrestoServer.sendQueryResponse spreads Latency evenly across every request in the
// statement lifecycle, INCLUDING the very first one (the initial POST /v1/statement that
// s.Client.Query() itself blocks on before returning). With dataBatches data-fetch requests
// plus 1 implicit queue request, that's dataBatches+1 total requests/sleeps; the first sleep
// happens before Query() returns (and thus before the poller can start), so only
// dataBatches of the dataBatches+1 sleeps occur during the poller's window. Using a large
// dataBatches keeps that "lost" first slot a small, predictable fraction of the total.
func registerLatencyQuery(mock *prestotest.MockPrestoServer, sql string, pollerWindow time.Duration) {
	const dataBatches = 20
	data := make([][]any, dataBatches)
	for i := range data {
		data[i] = []any{"row"}
	}
	totalRequests := dataBatches + 1 // +1 for the implicit QueueBatches=1 default
	latency := pollerWindow * time.Duration(totalRequests) / time.Duration(dataBatches)
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:         sql,
		DataBatches: dataBatches,
		Columns:     []presto.Column{{Name: "_col0", Type: "varchar"}},
		Data:        data,
		Latency:     latency,
	})
}

func newFunctionTestStage(id string, states *SharedStageStates) *Stage {
	return &Stage{
		Id:      id,
		States:  states,
		Catalog: strPtr("tpch"),
		Schema:  strPtr("sf1"),
	}
}

func strPtr(v string) *string { return &v }

func snapshotDirFor(t *testing.T, s *Stage, sql string) string {
	t.Helper()
	result := &QueryResult{Query: &Query{Text: sql}}
	return filepath.Join(s.States.OutputPath, s.querySourceString(result)+".snapshots")
}

// FT-1 (FP-1): a run-wide default applies to a plain stage; a sibling stage opts out via
// save_json_snapshots=false; another sibling overrides json_snapshot_interval. Precedence
// (per-stage JSON > CLI flag > built-in default) is proven both by inspecting the resolved
// config and by the resulting file-system side effects.
func TestFT1_ConfigPrecedenceEndToEnd(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()
	const q1, q2, q3 = "select 1", "select 2", "select 3"
	registerLatencyQuery(mock, q1, 8*time.Second)
	registerLatencyQuery(mock, q2, 8*time.Second)
	registerLatencyQuery(mock, q3, 8*time.Second)

	tmpDir := t.TempDir()
	states := &SharedStageStates{
		OutputPath:       tmpDir,
		SnapshotsEnabled: true,
		SnapshotInterval: 5 * time.Second, // the floor; the run-wide default
		SnapshotMax:      0,
		NewClient: func() *presto.Client {
			c, _ := presto.NewClient(mock.URL())
			return c
		},
	}

	parent := newFunctionTestStage("parent", states)
	parent.Queries = []string{}
	parent.ColdRuns, parent.WarmRuns = intPtr(1), intPtr(0)

	plainChild := newFunctionTestStage("plain_child", nil)
	plainChild.Queries = []string{q1}

	optOutChild := newFunctionTestStage("opt_out_child", nil)
	optOutChild.Queries = []string{q2}
	falseVal := false
	optOutChild.SaveJsonSnapshots = &falseVal

	overrideChild := newFunctionTestStage("override_child", nil)
	overrideChild.Queries = []string{q3}
	overrideInterval := "6s"
	overrideChild.JsonSnapshotInterval = &overrideInterval

	parent.NextStages = []*Stage{plainChild, optOutChild, overrideChild}
	plainChild.wgPrerequisites.Add(1)
	optOutChild.wgPrerequisites.Add(1)
	overrideChild.wgPrerequisites.Add(1)

	parent.Run(context.Background())

	// Precedence proof via resolved config state.
	assert.True(t, plainChild.snapshotsEnabled)
	assert.Equal(t, 5*time.Second, plainChild.snapshotInterval, "plain stage should use the run-wide default")
	assert.False(t, optOutChild.snapshotsEnabled, "stage explicitly opted out despite the run-wide default")
	assert.True(t, overrideChild.snapshotsEnabled)
	assert.Equal(t, 6*time.Second, overrideChild.snapshotInterval, "stage override must win over the run-wide default")

	// End-to-end file-system proof.
	plainDir := snapshotDirFor(t, plainChild, q1)
	entries, err := os.ReadDir(plainDir)
	require.NoError(t, err, "the plain (run-wide-default) stage should have produced a .snapshots dir")
	assert.GreaterOrEqual(t, len(entries), 1)

	optOutDir := snapshotDirFor(t, optOutChild, q2)
	_, statErr := os.Stat(optOutDir)
	assert.True(t, os.IsNotExist(statErr), "the opted-out stage must not produce a .snapshots dir")

	overrideDir := snapshotDirFor(t, overrideChild, q3)
	_, err = os.ReadDir(overrideDir)
	require.NoError(t, err, "the override stage should also have produced a .snapshots dir")
}

// FT-2 (FP-2): snapshot files exist with verbatim payloads and correctly-formatted names.
func TestFT2_SnapshotFilesVerbatimAndCorrectlyNamed(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()
	const q = "select 'ft2'"
	registerLatencyQuery(mock, q, 12*time.Second)

	tmpDir := t.TempDir()
	s := newFunctionTestStage("ft2", &SharedStageStates{
		OutputPath:       tmpDir,
		SnapshotsEnabled: true,
		SnapshotInterval: 5 * time.Second,
		NewClient: func() *presto.Client {
			c, _ := presto.NewClient(mock.URL())
			return c
		},
	})
	s.Queries = []string{q}
	s.ColdRuns, s.WarmRuns = intPtr(1), intPtr(0)

	s.Run(context.Background())

	dir := snapshotDirFor(t, s, q)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.GreaterOrEqual(t, len(entries), 2, "an 11s query polled every 5s should yield at least 2 snapshots")

	for i, e := range entries {
		assert.True(t, strings.HasPrefix(e.Name(), "snapshot_"), "unexpected file name %s", e.Name())
		assert.True(t, strings.HasSuffix(e.Name(), ".json"), "unexpected file name %s", e.Name())
		content, readErr := os.ReadFile(filepath.Join(dir, e.Name()))
		require.NoError(t, readErr)
		// The mock's /v1/query/{id} handler always returns a JSON object with a queryId field.
		assert.Contains(t, string(content), `"queryId"`, "entry %d: %s", i, e.Name())
	}
}

// FT-3 (FP-3): polling starts after submission and stops promptly on context cancellation
// (the abort path); no snapshots are written after the poller stops. Cancellation is
// triggered off observed poller activity (rather than a fixed sleep) so the test is not
// sensitive to exact mock-server timing.
func TestFT3_StartAfterSubmitAndStopOnContextCancel(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()
	const q = "select 'ft3'"
	// Long poller window: the test cancels well before this query would finish naturally,
	// so its exact length only needs to be "long enough", not tightly tuned.
	registerLatencyQuery(mock, q, 30*time.Second)

	tmpDir := t.TempDir()
	// Set RunName explicitly (rather than relying on Run()'s default-to-s.Id, which it
	// assigns concurrently at the start of Run()) so the test goroutine can compute the
	// final output path itself without racing on a field Run() also mutates.
	const runName = "ft3_run"
	s := newFunctionTestStage("ft3", &SharedStageStates{
		RunName:          runName,
		OutputPath:       tmpDir,
		SnapshotsEnabled: true,
		SnapshotInterval: 5 * time.Second,
		NewClient: func() *presto.Client {
			c, _ := presto.NewClient(mock.URL())
			return c
		},
	})
	s.Queries = []string{q}
	s.ColdRuns, s.WarmRuns = intPtr(1), intPtr(0)
	trueVal := true
	s.AbortOnError = &trueVal

	ctx, cancel := context.WithCancel(context.Background())
	runDone := make(chan struct{})
	go func() {
		s.Run(ctx)
		close(runDone)
	}()

	result := &QueryResult{Query: &Query{Text: q}}
	dir := filepath.Join(tmpDir, runName, s.querySourceString(result)+".snapshots")
	require.Eventually(t, func() bool {
		entries, _ := os.ReadDir(dir)
		return len(entries) >= 1
	}, 15*time.Second, 50*time.Millisecond, "polling should start once the query is submitted (FP-3)")

	cancel()

	select {
	case <-runDone:
	case <-time.After(5 * time.Second):
		t.Fatal("Stage.Run did not return promptly after context cancellation")
	}

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	countAfterRun := len(entries)

	// No further snapshots should appear after Run() has returned (poller fully stopped).
	time.Sleep(300 * time.Millisecond)
	entries, err = os.ReadDir(dir)
	require.NoError(t, err)
	assert.Equal(t, countAfterRun, len(entries), "no snapshots should be written after the poller stops")
}

// FT-4 (FP-4): with snapshots off, output is byte-identical to a snapshots-on run of the
// same query (same deterministic query id, since each test uses a fresh mock server whose
// query id counter starts at 0), and no .snapshots directory is created.
func TestFT4_ByteIdenticalOutputWhenDisabled(t *testing.T) {
	const q = "select 'ft4'"
	runOnce := func(snapshotsOn bool) (terminalJSON []byte, colsJSON []byte, snapshotDirExists bool) {
		mock := prestotest.NewMockPrestoServer()
		defer mock.Close()
		mock.AddQuery(&prestotest.MockQueryTemplate{
			SQL:     q,
			Columns: []presto.Column{{Name: "_col0", Type: "varchar"}},
			Data:    [][]any{{"ft4"}},
		})

		tmpDir := t.TempDir()
		s := newFunctionTestStage("ft4", &SharedStageStates{
			OutputPath:       tmpDir,
			SnapshotsEnabled: snapshotsOn,
			SnapshotInterval: 5 * time.Second,
			NewClient: func() *presto.Client {
				c, _ := presto.NewClient(mock.URL())
				return c
			},
		})
		s.Queries = []string{q}
		s.ColdRuns, s.WarmRuns = intPtr(1), intPtr(0)
		trueVal := true
		s.SaveJson = &trueVal
		s.SaveColumnMetadata = &trueVal

		s.Run(context.Background())

		result := &QueryResult{Query: &Query{Text: q}}
		base := s.querySourceString(result)
		terminalJSON, err := os.ReadFile(filepath.Join(s.States.OutputPath, base+".json"))
		require.NoError(t, err)
		colsJSON, err = os.ReadFile(filepath.Join(s.States.OutputPath, base+".cols.json"))
		require.NoError(t, err)
		_, statErr := os.Stat(filepath.Join(s.States.OutputPath, base+".snapshots"))
		return terminalJSON, colsJSON, statErr == nil
	}

	offTerminal, offCols, offDirExists := runOnce(false)
	onTerminal, onCols, onDirExists := runOnce(true)

	assert.False(t, offDirExists, "snapshots-off run must not produce a .snapshots directory")
	assert.Equal(t, offTerminal, onTerminal, "terminal query json must be byte-identical regardless of snapshotting")
	assert.Equal(t, offCols, onCols, "column metadata file must be byte-identical regardless of snapshotting")
	// The snapshots-on run may or may not have captured a frame for this near-instant query,
	// but if it exists, it must be the only difference in artifacts produced.
	_ = onDirExists
}

// FT-5 (FP-5): retention keeps snapshot #1 plus the most recent (max-1) frames, exercised
// end-to-end through Stage.Run() at the production floor interval.
func TestFT5_RetentionEndToEnd(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()
	const q = "select 'ft5'"
	registerLatencyQuery(mock, q, 17*time.Second) // ticks at ~5s, 10s, 15s => 3 captures

	tmpDir := t.TempDir()
	s := newFunctionTestStage("ft5", &SharedStageStates{
		OutputPath:       tmpDir,
		SnapshotsEnabled: true,
		SnapshotInterval: 5 * time.Second,
		SnapshotMax:      2,
		NewClient: func() *presto.Client {
			c, _ := presto.NewClient(mock.URL())
			return c
		},
	})
	s.Queries = []string{q}
	s.ColdRuns, s.WarmRuns = intPtr(1), intPtr(0)

	s.Run(context.Background())

	dir := snapshotDirFor(t, s, q)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.LessOrEqual(t, len(entries), 2, "retention must cap the number of files at snapshot-max")
	found1 := false
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), "snapshot_000001_") {
			found1 = true
		}
	}
	assert.True(t, found1, "snapshot #1 must always be retained")
}

// FT-7 (FP-7 / M1): a snapshot fetch that is still in flight when the query completes is
// aborted deterministically by stop() (M1's hard guarantee: the fetch's context is derived
// from - and canceled together with - the poller's own context), rather than being left to
// run to completion. wgExitMainStage still blocks Stage.Run() from returning until that
// abort and its cleanup (removing the now-incomplete file) have finished - so there is no
// truncated/partial file left on disk and no premature process exit - but Run() does not
// wait for the full (stalled) response to arrive.
func TestFT7_StopAbortsInFlightFetchOnQueryCompletion(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()
	const q = "select 'ft7'"
	// Poller window ~6s: one tick fires at t~5s, comfortably before the query itself drains.
	registerLatencyQuery(mock, q, 6*time.Second)

	backend, err := url.Parse(mock.URL())
	require.NoError(t, err)
	proxy := httputil.NewSingleHostReverseProxy(backend)

	var fetchStarted, fetchAborted atomic.Bool
	mux := http.NewServeMux()
	mux.HandleFunc("GET /v1/query/{queryId}", func(w http.ResponseWriter, r *http.Request) {
		fetchStarted.Store(true)
		// Stall far longer than the query itself takes to drain, but respond to client-side
		// cancellation (mirroring how a real coordinator's connection would be torn down)
		// rather than the fixed 5s sleep the pre-M1 version of this test used.
		select {
		case <-time.After(5 * time.Second):
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{"queryId":"stalled-response-payload","state":"RUNNING"}`))
		case <-r.Context().Done():
			fetchAborted.Store(true)
		}
	})
	mux.Handle("/", proxy)
	srv := httptest.NewServer(mux)
	defer srv.Close()

	tmpDir := t.TempDir()
	// Generous on purpose: proves the abort is caused by stop() (M1), not by the per-fetch
	// timeout racing it.
	fetchTimeout := "20s"
	s := newFunctionTestStage("ft7", &SharedStageStates{
		OutputPath:       tmpDir,
		SnapshotsEnabled: true,
		SnapshotInterval: 5 * time.Second,
		NewClient: func() *presto.Client {
			c, _ := presto.NewClient(srv.URL)
			return c
		},
	})
	s.Queries = []string{q}
	s.ColdRuns, s.WarmRuns = intPtr(1), intPtr(0)
	s.JsonSnapshotFetchTimeout = &fetchTimeout

	start := time.Now()
	s.Run(context.Background())
	elapsed := time.Since(start)

	require.True(t, fetchStarted.Load(), "the tick at t~5s should have started a fetch before the ~6.3s-long query drained")
	assert.True(t, fetchAborted.Load(), "stop() should have canceled the in-flight fetch's context, observable server-side")
	// The query itself drains at ~6.3s (registerLatencyQuery's poller-window formula). Run()
	// must not wait for the full 5s stall (which would put elapsed at ~11.3s); a 9s
	// threshold sits well below that but comfortably above "query-only" (~6.3s) plus normal
	// scheduling slack, so a regression that goes back to waiting for the stalled fetch would
	// fail this assertion.
	assert.Less(t, elapsed, 9*time.Second, "Stage.Run() must not wait for the stalled fetch to complete")

	dir := snapshotDirFor(t, s, q)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "the aborted in-flight fetch must not leave a partial (or complete) snapshot file behind")
}
