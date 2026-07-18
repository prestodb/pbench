package stage

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	presto "github.com/prestodb/presto-go-client/v2"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newPollerTestStage builds a minimal Stage wired to a Presto client pointed at srvURL,
// with wgExitMainStage ready for poller registration, for direct jsonSnapshotPoller unit
// tests (UT-2..UT-7). It intentionally bypasses cmd/run.go and setDefaults()'s 5s interval
// floor so tests can use short intervals and stay fast.
func newPollerTestStage(t *testing.T, srvURL string) *Stage {
	t.Helper()
	client, err := presto.NewClient(srvURL)
	require.NoError(t, err)
	return &Stage{
		Id:     "poller_test",
		Client: client,
		States: &SharedStageStates{
			OutputPath:      t.TempDir(),
			wgExitMainStage: &sync.WaitGroup{},
		},
	}
}

// UT-2: poller writes snapshot_<seq>_<epochms>.json files; content byte-equal to the mock response.
func TestSnapshotCapture_WritesVerbatimContent(t *testing.T) {
	payload := []byte(`{"queryId":"20260101_000000_00001_abcde","state":"RUNNING","weird":"NaN"}`)
	var reqCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "base.snapshots")
	require.NoError(t, os.MkdirAll(dir, 0755))
	p := &jsonSnapshotPoller{stage: s, queryId: "q1", dir: dir, fetchTimeout: time.Second, max: 0}

	require.NoError(t, p.captureOnce(context.Background()))
	assert.Equal(t, 1, p.seq)
	require.Len(t, p.saved, 1)

	name := filepath.Base(p.saved[0])
	assert.True(t, strings.HasPrefix(name, "snapshot_000001_"), "unexpected file name %s", name)
	assert.True(t, strings.HasSuffix(name, ".json"), "unexpected file name %s", name)

	content, err := os.ReadFile(p.saved[0])
	require.NoError(t, err)
	assert.Equal(t, payload, content, "snapshot content must be byte-identical to the raw response")
	assert.Equal(t, int32(1), reqCount.Load())

	// A second capture advances seq and adds a second file.
	require.NoError(t, p.captureOnce(context.Background()))
	assert.Equal(t, 2, p.seq)
	require.Len(t, p.saved, 2)
	assert.True(t, strings.HasPrefix(filepath.Base(p.saved[1]), "snapshot_000002_"))
}

// UT-3 (part 1): poller start-after-submit / stop-on-drain via the Stage-level API, and
// idempotent stop().
func TestSnapshotPoller_StartStopLifecycle(t *testing.T) {
	var reqCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	s.States.SnapshotsEnabled = true
	s.States.SnapshotInterval = 30 * time.Second // will be resolved/floored; overridden below
	s.setDefaults()
	// Bypass the 5s production floor for a fast test.
	s.snapshotInterval = 15 * time.Millisecond
	s.snapshotFetchTimeout = 15 * time.Millisecond

	result := &QueryResult{QueryId: "q1", Query: &Query{}}
	poller := s.startJsonSnapshotPoller(context.Background(), result)
	require.NotNil(t, poller, "poller must start once snapshotting is enabled and the query id is known")

	// Let a few ticks happen.
	require.Eventually(t, func() bool { return reqCount.Load() >= 2 }, 2*time.Second, 10*time.Millisecond)

	poller.stop()
	waitForWaitGroup(t, s.States.wgExitMainStage, 2*time.Second)

	countAfterStop := reqCount.Load()
	time.Sleep(80 * time.Millisecond)
	assert.Equal(t, countAfterStop, reqCount.Load(), "no more polling should happen after stop()")

	// stop() must be idempotent.
	poller.stop()
	poller.stop()

	// stop() on a nil poller (disabled case) must be a safe no-op.
	var nilPoller *jsonSnapshotPoller
	nilPoller.stop()
}

// UT-3 (part 2): the poller stops when the stage context is canceled (abort path).
func TestSnapshotPoller_StopsOnContextCancel(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	s.setDefaults()
	s.snapshotsEnabled = true
	s.snapshotInterval = 15 * time.Millisecond
	s.snapshotFetchTimeout = 15 * time.Millisecond

	ctx, cancel := context.WithCancel(context.Background())
	result := &QueryResult{QueryId: "q1", Query: &Query{}}
	poller := s.startJsonSnapshotPoller(ctx, result)
	require.NotNil(t, poller)

	time.Sleep(40 * time.Millisecond)
	cancel()

	waitForWaitGroup(t, s.States.wgExitMainStage, 2*time.Second)
}

// UT-3 (part 3): a fetch slower than the interval never overlaps with the next tick - the
// ticker naturally coalesces/drops ticks while captureOnce is in flight (design.md §4.2).
func TestSnapshotPoller_SlowFetchNeverOverlaps(t *testing.T) {
	const fetchDelay = 120 * time.Millisecond
	var inFlight atomic.Int32
	var maxInFlight atomic.Int32
	var reqCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := inFlight.Add(1)
		for {
			cur := maxInFlight.Load()
			if n <= cur || maxInFlight.CompareAndSwap(cur, n) {
				break
			}
		}
		reqCount.Add(1)
		time.Sleep(fetchDelay)
		inFlight.Add(-1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "slow.snapshots")
	ctx, cancel := context.WithCancel(context.Background())
	p := &jsonSnapshotPoller{
		stage: s, queryId: "q1", dir: dir,
		interval: 20 * time.Millisecond, fetchTimeout: time.Second, max: 0,
		cancel: cancel,
	}
	s.States.wgExitMainStage.Add(1)
	go p.run(ctx)

	// Run for enough wall-clock time that, without tick-coalescing, dozens of ticks
	// (interval=20ms) would have fired; with coalescing, only a handful of fetches (limited
	// by fetchDelay=120ms) should actually happen.
	time.Sleep(500 * time.Millisecond)
	p.stop()
	waitForWaitGroup(t, s.States.wgExitMainStage, 2*time.Second)

	assert.LessOrEqual(t, int32(1), maxInFlight.Load())
	assert.Equal(t, int32(1), maxInFlight.Load(), "fetches must never overlap")
	assert.Less(t, int(reqCount.Load()), 10, "slow fetches should cause ticks to be dropped, not queued")
	assert.GreaterOrEqual(t, int(reqCount.Load()), 2, "at least a couple of fetches should have completed")
}

// M1: captureOnce must abort deterministically - and clean up any partial file - when its
// context is canceled mid-fetch, regardless of scheduler timing. This is the mechanism
// stop() now relies on (see startJsonSnapshotPoller/stop()) to make "no capture proceeds
// after stop()" a hard guarantee rather than a best-effort race.
func TestSnapshotCapture_CanceledContextAbortsAndCleansUpPartialFile(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-release:
			w.WriteHeader(http.StatusOK)
			_, _ = w.Write([]byte(`{}`))
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "cancel.snapshots")
	ctx, cancel := context.WithCancel(context.Background())
	p := &jsonSnapshotPoller{stage: s, queryId: "q1", dir: dir, fetchTimeout: 10 * time.Second, max: 0, cancel: cancel}

	// Cancel while the fetch is in flight, simulating stop() racing an in-flight capture.
	go func() {
		time.Sleep(50 * time.Millisecond)
		cancel()
	}()

	err := p.captureOnce(ctx)
	assert.Error(t, err, "a fetch whose context is canceled mid-flight must fail (capture returns due to the canceled ctx)")
	assert.Equal(t, 0, p.seq, "seq must not advance when the fetch is canceled")
	assert.Empty(t, p.saved)

	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	assert.Empty(t, entries, "a canceled fetch must not leave a partial snapshot file behind")
}

// M1: deterministically forces the exact interleaving the fix targets - a tick already
// buffered in ticker.C (interval=10ms) while a first capture is blocked in flight, then
// stop() called while that first capture is still in flight - and asserts no second
// capture is ever started, regardless of which case Go's select happens to pick once the
// first capture unblocks. Before the fix, this could let one extra frame through (see
// review finding M1); the hard guarantee comes from tying every fetch's context to the same
// cancelable context stop() cancels, not from the (still present, best-effort) priority
// check in run().
func TestSnapshotPoller_StopDeterministicallyPreventsFurtherCaptures(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	var reqCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reqCount.Add(1) == 1 {
			// The first fetch blocks well past one more tick interval, so a second tick has
			// time to buffer in ticker.C before stop() is called and this unblocks.
			select {
			case <-release:
			case <-r.Context().Done():
			}
			return
		}
		// A second request must never arrive; if it did (the bug this test targets), let it
		// succeed instantly so the failure is about reqCount, not a hang.
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "poststop.snapshots")
	ctx, cancel := context.WithCancel(context.Background())
	p := &jsonSnapshotPoller{
		stage: s, queryId: "q1", dir: dir,
		interval: 10 * time.Millisecond, fetchTimeout: 10 * time.Second, max: 0,
		cancel: cancel,
	}
	s.States.wgExitMainStage.Add(1)
	go p.run(ctx)

	// Wait for the first fetch to actually start.
	require.Eventually(t, func() bool { return reqCount.Load() >= 1 }, 2*time.Second, 5*time.Millisecond)
	// Let a second (and likely third) tick buffer/coalesce in ticker.C while the first fetch
	// is still blocked (interval=10ms << this 50ms wait).
	time.Sleep(50 * time.Millisecond)

	p.stop() // cancels ctx: aborts the in-flight first fetch AND must prevent any buffered tick's capture.
	waitForWaitGroup(t, s.States.wgExitMainStage, 2*time.Second)

	// Give a wrongly-started second request a moment to arrive, if the bug were present.
	time.Sleep(100 * time.Millisecond)
	assert.Equal(t, int32(1), reqCount.Load(), "no capture must be started after stop(), even with a tick already buffered")

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries, "the aborted first capture must not leave a partial file, and no second capture must have run")
}

// UT-4: save_json_snapshots=false (disabled) -> no .snapshots dir, no extra HTTP calls;
// startJsonSnapshotPoller must be a pure no-op.
func TestSnapshotDisabled_NoDirNoRequests(t *testing.T) {
	var reqCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	s.setDefaults() // States is zero-value -> snapshotsEnabled stays false

	result := &QueryResult{QueryId: "q1", Query: &Query{}}
	poller := s.startJsonSnapshotPoller(context.Background(), result)
	assert.Nil(t, poller)

	time.Sleep(50 * time.Millisecond)
	assert.Equal(t, int32(0), reqCount.Load(), "no /v1/query polls should happen when snapshotting is disabled")

	dir := filepath.Join(s.States.OutputPath, s.querySourceString(result)+".snapshots")
	_, statErr := os.Stat(dir)
	assert.True(t, os.IsNotExist(statErr), "no .snapshots directory should be created")

	// A nil poller's stop() must be a safe no-op (mirrors the `defer poller.stop()` call
	// site in runQuery(), which does not nil-check).
	poller.stop()
}

// UT-4 (part 2): also disabled when the query id is not yet known (submit error path).
func TestSnapshotDisabled_NoQueryId(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	s.setDefaults()
	s.snapshotsEnabled = true

	result := &QueryResult{QueryId: "", Query: &Query{}}
	poller := s.startJsonSnapshotPoller(context.Background(), result)
	assert.Nil(t, poller)
}

// N1: enabled but a query that drains before its first tick fires (or one that is stopped
// before any tick fires) must leave no .snapshots directory behind - the directory is now
// created lazily on the first attempted capture (captureOnce), not eagerly when the poller
// starts.
func TestSnapshotEnabled_NoDirWhenNoTickEverFires(t *testing.T) {
	var reqCount atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		reqCount.Add(1)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	s.setDefaults()
	s.snapshotsEnabled = true
	s.snapshotInterval = time.Hour // will certainly not tick during this test
	s.snapshotFetchTimeout = time.Hour

	result := &QueryResult{QueryId: "q1", Query: &Query{}}
	poller := s.startJsonSnapshotPoller(context.Background(), result)
	require.NotNil(t, poller)

	time.Sleep(30 * time.Millisecond)
	poller.stop()
	waitForWaitGroup(t, s.States.wgExitMainStage, 2*time.Second)

	assert.Equal(t, int32(0), reqCount.Load(), "no fetch should have been attempted")
	dir := filepath.Join(s.States.OutputPath, s.querySourceString(result)+".snapshots")
	_, statErr := os.Stat(dir)
	assert.True(t, os.IsNotExist(statErr), "no .snapshots directory should be created when no tick ever fires (N1)")
}

// UT-5: retention keeps snapshot #1 plus the most recent (max-1) frames.
// max=3 over 6 successful captures keeps snapshots {1,5,6}.
func TestSnapshotRetention_KeepsFirstAndMostRecent(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "retention.snapshots")
	require.NoError(t, os.MkdirAll(dir, 0755))
	p := &jsonSnapshotPoller{stage: s, queryId: "q1", dir: dir, fetchTimeout: time.Second, max: 3}

	for i := 0; i < 6; i++ {
		require.NoError(t, p.captureOnce(context.Background()))
		time.Sleep(2 * time.Millisecond) // ensure distinct epoch-millis in file names
	}

	assert.Len(t, p.saved, 3)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 3, "exactly max=3 files should remain on disk")

	var seqs []int
	for _, e := range entries {
		var seq int
		var ts int64
		_, scanErr := fmt.Sscanf(e.Name(), "snapshot_%06d_%d.json", &seq, &ts)
		require.NoError(t, scanErr)
		seqs = append(seqs, seq)
	}
	sort.Ints(seqs)
	assert.Equal(t, []int{1, 5, 6}, seqs)
}

// UT-5 (part 2): max=0 (unlimited) never deletes anything.
func TestSnapshotRetention_UnlimitedKeepsAll(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "unlimited.snapshots")
	require.NoError(t, os.MkdirAll(dir, 0755))
	p := &jsonSnapshotPoller{stage: s, queryId: "q1", dir: dir, fetchTimeout: time.Second, max: 0}

	for i := 0; i < 5; i++ {
		require.NoError(t, p.captureOnce(context.Background()))
		time.Sleep(2 * time.Millisecond)
	}
	assert.Len(t, p.saved, 5)
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 5)
}

// UT-5 (part 3, N2): max=1 is special-cased to keep the most recent frame, not the oldest.
// Literally applying "keep #1 + most-recent (max-1=0)" would keep only the very first
// snapshot forever and evict every later one - useless for a user who asked to retain
// exactly one frame, who almost certainly wants the latest state. See applyRetention().
func TestSnapshotRetention_MaxOneKeepsLatestNotOldest(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "max_one.snapshots")
	require.NoError(t, os.MkdirAll(dir, 0755))
	p := &jsonSnapshotPoller{stage: s, queryId: "q1", dir: dir, fetchTimeout: time.Second, max: 1}

	for i := 0; i < 4; i++ {
		require.NoError(t, p.captureOnce(context.Background()))
		time.Sleep(2 * time.Millisecond)
	}

	require.Len(t, p.saved, 1)
	assert.True(t, strings.HasPrefix(filepath.Base(p.saved[0]), "snapshot_000004_"),
		"max=1 should retain the most recent (4th) capture, not the first")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	require.Len(t, entries, 1)
	assert.True(t, strings.HasPrefix(entries[0].Name(), "snapshot_000004_"))
}

// UT-6: fetch errors (500) are logged and never fail the query; exponential backoff skips
// 1,2,4,8,8... ticks; success resets the streak; the poller is never permanently disabled -
// it resumes capturing once the mock server recovers.
func TestSnapshotBackoffTicks_Sequence(t *testing.T) {
	assert.Equal(t, 0, snapshotBackoffTicks(0))
	assert.Equal(t, 1, snapshotBackoffTicks(1))
	assert.Equal(t, 2, snapshotBackoffTicks(2))
	assert.Equal(t, 4, snapshotBackoffTicks(3))
	assert.Equal(t, 8, snapshotBackoffTicks(4))
	assert.Equal(t, 8, snapshotBackoffTicks(5))
	assert.Equal(t, 8, snapshotBackoffTicks(10))
}

func TestSnapshotPoller_NeverPermanentlyDisabled_ResumesAfterRecovery(t *testing.T) {
	var reqCount atomic.Int32
	const failCount = 6
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := reqCount.Add(1)
		if n <= failCount {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "resume.snapshots")
	ctx, cancel := context.WithCancel(context.Background())
	p := &jsonSnapshotPoller{
		stage: s, queryId: "q1", dir: dir,
		interval: 15 * time.Millisecond, fetchTimeout: time.Second, max: 0,
		cancel: cancel,
	}
	s.States.wgExitMainStage.Add(1)
	go p.run(ctx)

	// Wait on the atomic request counter (not p.seq/p.failStreak, which are only touched by
	// the poller goroutine and unsafe to read concurrently without synchronization - they
	// are read further below, but only after waitForWaitGroup establishes a happens-before
	// relationship via the poller's exit) until the mock has served at least one successful
	// response past the failure run.
	require.Eventually(t, func() bool {
		return int(reqCount.Load()) > failCount
	}, 10*time.Second, 5*time.Millisecond,
		"poller must resume issuing requests after the mock coordinator recovers (never permanently disabled)")

	// Give the corresponding successful capture a brief moment to land on disk. (Checking
	// the directory during the failure run itself would be racy: a failed capture creates
	// the file before removing it, so it can transiently appear in a directory listing.)
	require.Eventually(t, func() bool {
		entries, _ := os.ReadDir(dir)
		return len(entries) > 0
	}, 2*time.Second, 10*time.Millisecond, "the successful capture should have been written to disk")

	p.stop()
	waitForWaitGroup(t, s.States.wgExitMainStage, 2*time.Second)

	assert.GreaterOrEqual(t, int(reqCount.Load()), failCount+1)
	assert.Equal(t, 0, p.failStreak, "a successful capture must reset the failure streak")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.GreaterOrEqual(t, len(entries), 1)
}

// UT-6 (M3): a 404 - the query aged out of coordinator memory, design.md §4.5 - flows
// through the same non-200 error path as any other fetch failure: the partial file is
// removed, no seq/saved advance, and the poller backs off exactly like a 500 or timeout
// (never permanently disabled).
func TestSnapshotFetchError_404AgedOutOfCoordinatorMemory(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		http.NotFound(w, r)
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "notfound.snapshots")
	require.NoError(t, os.MkdirAll(dir, 0755))
	p := &jsonSnapshotPoller{stage: s, queryId: "q1", dir: dir, fetchTimeout: time.Second, max: 0}

	err := p.captureOnce(context.Background())
	assert.Error(t, err, "a 404 (query aged out of coordinator memory) must be treated as a fetch failure")
	assert.Equal(t, 0, p.seq, "seq must not advance on a 404")
	assert.Empty(t, p.saved)

	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	assert.Empty(t, entries, "the partial file for a 404 response must be removed")

	// Exercised through the full run() loop too, to prove backoff (FP-6) applies identically
	// to a 404 as it does to a 500/timeout, and that the poller keeps retrying afterwards.
	var reqCount atomic.Int32
	const failCount = 3
	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if reqCount.Add(1) <= failCount {
			http.NotFound(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv2.Close()

	s2 := newPollerTestStage(t, srv2.URL)
	dir2 := filepath.Join(s2.States.OutputPath, "notfound_resume.snapshots")
	ctx, cancel := context.WithCancel(context.Background())
	p2 := &jsonSnapshotPoller{
		stage: s2, queryId: "q1", dir: dir2,
		interval: 10 * time.Millisecond, fetchTimeout: time.Second, max: 0,
		cancel: cancel,
	}
	s2.States.wgExitMainStage.Add(1)
	go p2.run(ctx)

	require.Eventually(t, func() bool {
		return int(reqCount.Load()) > failCount
	}, 10*time.Second, 5*time.Millisecond, "poller must keep retrying (never permanently disabled) after repeated 404s")
	require.Eventually(t, func() bool {
		entries, _ := os.ReadDir(dir2)
		return len(entries) > 0
	}, 2*time.Second, 10*time.Millisecond, "the successful capture after recovery should have been written to disk")

	p2.stop()
	waitForWaitGroup(t, s2.States.wgExitMainStage, 2*time.Second)
}

func TestSnapshotCapture_ErrorRemovesPartialFile(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "err.snapshots")
	require.NoError(t, os.MkdirAll(dir, 0755))
	p := &jsonSnapshotPoller{stage: s, queryId: "q1", dir: dir, fetchTimeout: time.Second, max: 0}

	err := p.captureOnce(context.Background())
	assert.Error(t, err)
	assert.Equal(t, 0, p.seq, "seq must not advance on failure")
	assert.Empty(t, p.saved)

	entries, readErr := os.ReadDir(dir)
	require.NoError(t, readErr)
	assert.Empty(t, entries, "the partially-written file must be removed on error")
}

// UT-7: per-fetch timeout is honored (a stalled fetch is aborted) and is overridable
// (raising it lets an otherwise-too-slow fetch succeed).
func TestSnapshotFetchTimeout_Honored(t *testing.T) {
	unblock := make(chan struct{})
	defer close(unblock)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		select {
		case <-unblock:
		case <-r.Context().Done():
		}
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "timeout.snapshots")
	require.NoError(t, os.MkdirAll(dir, 0755))
	p := &jsonSnapshotPoller{stage: s, queryId: "q1", dir: dir, fetchTimeout: 50 * time.Millisecond, max: 0}

	start := time.Now()
	err := p.captureOnce(context.Background())
	elapsed := time.Since(start)
	assert.Error(t, err, "a fetch stalled past the timeout must fail")
	assert.Less(t, elapsed, time.Second, "the fetch should have been aborted by the per-fetch timeout, not run forever")
}

func TestSnapshotFetchTimeout_OverridableForSlowQueries(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(80 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`{}`))
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	dir := filepath.Join(s.States.OutputPath, "timeout_override.snapshots")
	require.NoError(t, os.MkdirAll(dir, 0755))
	// A 500ms timeout comfortably accommodates an 80ms-slow coordinator response.
	p := &jsonSnapshotPoller{stage: s, queryId: "q1", dir: dir, fetchTimeout: 500 * time.Millisecond, max: 0}

	assert.NoError(t, p.captureOnce(context.Background()))
	assert.Equal(t, 1, p.seq)
}

// UT-7 (wgExitMainStage accounting): the poller's goroutine must be registered on
// wgExitMainStage for as long as run() is executing - including through an in-flight
// fetch's abort and cleanup when stop() is called (M1) - and Wait() must not return before
// run() actually exits. The mock handler respects client-side cancellation (r.Context()),
// mirroring how a real coordinator's connection would be torn down once stop() cancels the
// fetch's context; it deliberately does NOT unblock on its own, so if run()/captureOnce
// regressed to waiting for the response instead of aborting, this test would hang/time out
// rather than pass.
func TestSnapshotPoller_RegistersOnWgExitMainStage(t *testing.T) {
	handlerEntered := make(chan struct{})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(handlerEntered)
		<-r.Context().Done()
	}))
	defer srv.Close()

	s := newPollerTestStage(t, srv.URL)
	s.setDefaults()
	s.snapshotsEnabled = true
	s.snapshotInterval = 10 * time.Millisecond
	s.snapshotFetchTimeout = 5 * time.Second

	result := &QueryResult{QueryId: "q1", Query: &Query{}}
	poller := s.startJsonSnapshotPoller(context.Background(), result)
	require.NotNil(t, poller)

	select {
	case <-handlerEntered:
	case <-time.After(2 * time.Second):
		t.Fatal("the poller never started a fetch")
	}

	poller.stop()
	waitForWaitGroup(t, s.States.wgExitMainStage, 2*time.Second)

	// The aborted fetch must not have left a partial (or complete) snapshot file behind.
	dir := filepath.Join(s.States.OutputPath, s.querySourceString(result)+".snapshots")
	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Empty(t, entries)
}

// waitForWaitGroup waits for wg to reach zero, failing the test if it takes longer than d.
func waitForWaitGroup(t *testing.T, wg *sync.WaitGroup, d time.Duration) {
	t.Helper()
	done := make(chan struct{})
	go func() {
		wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(d):
		t.Fatal("timed out waiting for wgExitMainStage to reach zero")
	}
}

// B-1: BenchmarkSnapshotCapture - one captureOnce against a 10MB payload. max=1 bounds disk
// usage across b.N iterations (each new capture immediately evicts the prior one, since
// only snapshot #1 is ever retained when max=1); this does not affect the measured cost,
// which is dominated by the io.Copy of the response body.
func BenchmarkSnapshotCapture(b *testing.B) {
	payload := make([]byte, 10*1024*1024)
	for i := range payload {
		payload[i] = byte('a' + i%26)
	}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(payload)
	}))
	defer srv.Close()

	client, err := presto.NewClient(srv.URL)
	if err != nil {
		b.Fatal(err)
	}
	s := &Stage{
		Id:     "bench",
		Client: client,
		States: &SharedStageStates{OutputPath: b.TempDir(), wgExitMainStage: &sync.WaitGroup{}},
	}
	dir := filepath.Join(s.States.OutputPath, "bench.snapshots")
	if err := os.MkdirAll(dir, 0755); err != nil {
		b.Fatal(err)
	}
	p := &jsonSnapshotPoller{stage: s, queryId: "q1", dir: dir, fetchTimeout: 30 * time.Second, max: 1}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := p.captureOnce(context.Background()); err != nil {
			b.Fatal(err)
		}
	}
}
