package stage

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"pbench/log"
	"pbench/utils"
	"sync"
	"time"
)

const (
	// DefaultSnapshotInterval is the built-in default snapshot polling interval, used
	// when neither a per-stage json_snapshot_interval override nor the run-wide
	// --snapshot-interval CLI flag resolves to a usable (positive) value.
	DefaultSnapshotInterval = 30 * time.Second
	// DefaultSnapshotMax is the built-in default snapshot retention cap per query
	// (~10 minutes of coverage at the default 30s interval). 0 means unlimited.
	DefaultSnapshotMax = 20
	// MinSnapshotInterval is the floor below which a configured snapshot interval is
	// clamped (with a warning), to protect the coordinator from excessive polling.
	MinSnapshotInterval = 5 * time.Second
	// maxSnapshotBackoffTicks caps the exponential backoff applied after consecutive
	// snapshot fetch failures (FP-6): skip 1, 2, 4, 8, 8, 8, ... ticks.
	maxSnapshotBackoffTicks = 8
)

// resolveSnapshotSettings resolves the effective, fully-materialized snapshot settings
// for this stage into the private snapshotsEnabled/snapshotInterval/snapshotMax/
// snapshotFetchTimeout fields. Called once per stage from setDefaults(), per the fixed
// precedence: per-stage JSON value > CLI flag (run-wide default via SharedStageStates) >
// built-in default (design.md §4.1).
func (s *Stage) resolveSnapshotSettings() {
	if s.SaveJsonSnapshots != nil {
		s.snapshotsEnabled = *s.SaveJsonSnapshots
	} else {
		s.snapshotsEnabled = s.States.SnapshotsEnabled
	}

	s.snapshotInterval = s.States.SnapshotInterval
	if s.snapshotInterval <= 0 {
		s.snapshotInterval = DefaultSnapshotInterval
	}
	if s.JsonSnapshotInterval != nil {
		if d, err := time.ParseDuration(*s.JsonSnapshotInterval); err != nil {
			log.Error().Err(err).EmbedObject(s).Str("json_snapshot_interval", *s.JsonSnapshotInterval).
				Msg("failed to parse json_snapshot_interval; disabling json snapshots for this stage")
			s.snapshotsEnabled = false
		} else {
			s.snapshotInterval = d
		}
	}
	if s.snapshotInterval < MinSnapshotInterval {
		log.Warn().EmbedObject(s).Dur("requested_interval", s.snapshotInterval).
			Dur("floor", MinSnapshotInterval).Msg("json snapshot interval below floor; clamping")
		s.snapshotInterval = MinSnapshotInterval
	}

	if s.JsonSnapshotMax != nil {
		s.snapshotMax = *s.JsonSnapshotMax
	} else {
		s.snapshotMax = s.States.SnapshotMax
	}
	if s.snapshotMax < 0 {
		// Should already be rejected by struct validation (validate:"omitempty,gte=0")
		// when parsed from JSON, but guard against direct/programmatic construction too.
		s.snapshotMax = DefaultSnapshotMax
	}

	s.snapshotFetchTimeout = s.States.SnapshotFetchTimeout
	if s.JsonSnapshotFetchTimeout != nil {
		if d, err := time.ParseDuration(*s.JsonSnapshotFetchTimeout); err != nil {
			log.Error().Err(err).EmbedObject(s).Str("json_snapshot_fetch_timeout", *s.JsonSnapshotFetchTimeout).
				Msg("failed to parse json_snapshot_fetch_timeout; falling back to the polling interval")
			s.snapshotFetchTimeout = 0
		} else {
			s.snapshotFetchTimeout = d
		}
	}
	if s.snapshotFetchTimeout <= 0 {
		// Default: the (already-resolved) polling interval (FP-7).
		s.snapshotFetchTimeout = s.snapshotInterval
	}
}

// jsonSnapshotPoller periodically fetches GET /v1/query/{queryId} while a query runs and
// saves each raw response, verbatim, to a snapshot file. See design.md §4.2. A single
// poller goroutine is created per polled query.
type jsonSnapshotPoller struct {
	stage        *Stage
	queryId      string
	dir          string // <OutputPath>/<querySourceStr>.snapshots
	interval     time.Duration
	fetchTimeout time.Duration // default == interval
	max          int           // <= 0 means unlimited

	// cancel cancels the context passed to run()/captureOnce(). stop() calls this instead
	// of signalling a separate "stop" channel: deriving every fetch's context from the same
	// cancelable context means stop() deterministically aborts a fetch that is already in
	// flight (not just future ones) - see stop() and captureOnce() for the hard guarantee
	// this gives (review finding M1). stopOnce makes calling it more than once, or calling
	// it concurrently with run() exiting on its own (e.g. via the caller's ctx), safe.
	cancel   context.CancelFunc
	stopOnce sync.Once

	seq        int
	saved      []string // retained snapshot file paths, in capture order, for FP-5 retention
	failStreak int      // consecutive failures, drives exponential backoff (FP-6)
	dirReady   bool     // true once the .snapshots dir has been created; single-goroutine field
}

// startJsonSnapshotPoller starts a background poller for the query identified by
// result.QueryId, registering its goroutine on s.States.wgExitMainStage exactly like the
// async writer in saveQueryJsonFile. It returns nil when snapshotting is disabled for this
// stage or the query id is not yet known (e.g. the query failed to submit); stop() on a
// nil *jsonSnapshotPoller is a safe no-op, so callers do not need to nil-check first.
func (s *Stage) startJsonSnapshotPoller(ctx context.Context, result *QueryResult) *jsonSnapshotPoller {
	if !s.snapshotsEnabled || result.QueryId == "" {
		return nil
	}
	querySourceStr := s.querySourceString(result)
	// pollCtx is derived from the caller's ctx and is also the context every fetch's
	// context.WithTimeout is built from; canceling it (via stop(), or transitively via the
	// caller's ctx being canceled) aborts an in-flight fetch immediately rather than merely
	// preventing the *next* one - see captureOnce().
	pollCtx, cancel := context.WithCancel(ctx)
	p := &jsonSnapshotPoller{
		stage:        s,
		queryId:      result.QueryId,
		dir:          filepath.Join(s.States.OutputPath, querySourceStr+".snapshots"),
		interval:     s.snapshotInterval,
		fetchTimeout: s.snapshotFetchTimeout,
		max:          s.snapshotMax,
		cancel:       cancel,
	}
	s.States.wgExitMainStage.Add(1)
	go p.run(pollCtx)
	return p
}

// run is the poller goroutine body. It exits when ctx is canceled - either because stop()
// was called (normal completion) or because the caller's ctx was canceled (abort); the
// terminal save in saveQueryJsonFile still runs on context.Background() and captures final
// state independently either way.
func (p *jsonSnapshotPoller) run(ctx context.Context) {
	defer p.stage.States.wgExitMainStage.Done()
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()
	// skipTicks implements the exponential backoff window from FP-6: while positive, ticks
	// are consumed without triggering a fetch.
	skipTicks := 0
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			// Because this loop is single-threaded and captureOnce is synchronous, a slow
			// fetch naturally causes any tick(s) that occur while it is running to be
			// coalesced/dropped by time.Ticker - there is no overlap and no backpressure
			// queue is needed (FP-3).
			//
			// No extra "is ctx already done?" pre-check is needed here (an earlier version
			// of this fix had one): even if select's pseudo-random choice between ready
			// cases picks this branch despite ctx already being canceled (e.g. stop() raced
			// a tick that was already buffered while a prior, slow captureOnce was in
			// flight), captureOnce's own context is derived from - and canceled together
			// with - ctx (see captureOnce/startJsonSnapshotPoller), so it fails
			// deterministically instead of writing a frame. The ctx.Err() check right below
			// turns that into a clean exit rather than a logged failure/backoff. This is the
			// hard guarantee M1 asks for: it does not depend on winning any select race.
			if skipTicks > 0 {
				skipTicks--
				continue
			}
			if err := p.captureOnce(ctx); err != nil {
				if ctx.Err() != nil {
					// stop() (or the caller's ctx) was canceled during, or immediately
					// before, this capture; the fetch's own context is derived from ctx
					// (see captureOnce), so it aborted deterministically instead of writing
					// a frame (M1's hard guarantee). This is a shutdown, not a fetch
					// failure: exit without logging a warning or applying backoff.
					return
				}
				p.failStreak++
				log.Warn().Err(err).Str("query_id", p.queryId).Int("seq", p.seq).
					Int("fail_streak", p.failStreak).Msg("json snapshot fetch failed")
				skipTicks = snapshotBackoffTicks(p.failStreak)
			} else {
				p.failStreak = 0
			}
		}
	}
}

// snapshotBackoffTicks returns the number of ticks to skip after failStreak consecutive
// failures: 1, 2, 4, 8, 8, 8, ... (never disables the poller permanently, FP-6).
func snapshotBackoffTicks(failStreak int) int {
	if failStreak <= 0 {
		return 0
	}
	if failStreak > 4 {
		return maxSnapshotBackoffTicks
	}
	return 1 << (failStreak - 1)
}

// captureOnce fetches the query's current /v1/query/{queryId} JSON and streams it,
// verbatim, to the next snapshot file. On success it advances seq, records the saved path
// for retention accounting, and applies retention (FP-5). On any error - including ctx
// being canceled mid-fetch (M1: stop() cancels ctx, so an in-flight fetch is aborted
// deterministically rather than left to complete) - the partially written file is removed
// and the error is returned for the caller to log/back off on (unless the cause was
// cancellation, handled by the caller without backoff).
func (p *jsonSnapshotPoller) captureOnce(ctx context.Context) error {
	// The .snapshots directory is created lazily, on the first attempted capture, rather
	// than eagerly when the poller starts (N1): a query that drains before its first tick
	// fires (or one where snapshotting never ticks at all) leaves no empty directory behind.
	if !p.dirReady {
		if err := os.MkdirAll(p.dir, 0755); err != nil {
			return fmt.Errorf("create snapshot dir: %w", err)
		}
		p.dirReady = true
	}
	nextSeq := p.seq + 1
	// Zero-padded to 6 digits (N3): 4 digits would break lexical/glob ordering past 9999
	// frames. Phase-2 frame discovery parses the numeric seq out of the name rather than
	// relying on lexical order, so widening the padding does not affect that parsing, and
	// the snapshot_<seq>_<epochMillis>.json shape is unchanged.
	path := filepath.Join(p.dir, fmt.Sprintf("snapshot_%06d_%d.json", nextSeq, time.Now().UnixMilli()))
	file, err := os.OpenFile(path, utils.OpenNewFileFlags, 0644)
	if err != nil {
		return err
	}
	qCtx, qCancel := context.WithTimeout(ctx, p.fetchTimeout)
	// Raw stream to disk, identical mechanism to saveQueryJsonFile: the file carries
	// Presto's JSON schema verbatim (FP-2). qCtx is derived from ctx, so canceling ctx (via
	// stop()) aborts this fetch immediately, even if it is already in flight.
	_, fetchErr := p.stage.Client.GetQueryInfo(qCtx, p.queryId, file)
	qCancel()
	closeErr := file.Close()
	if fetchErr != nil || closeErr != nil {
		// A canceled fetch must not leave a partial snapshot file behind, exactly like any
		// other fetch/write error.
		if rmErr := os.Remove(path); rmErr != nil && !errors.Is(rmErr, os.ErrNotExist) {
			log.Debug().Err(rmErr).Str("path", path).Msg("failed to remove incomplete json snapshot file")
		}
		if fetchErr != nil {
			return fetchErr
		}
		return closeErr
	}
	p.seq = nextSeq
	p.saved = append(p.saved, path)
	p.applyRetention()
	return nil
}

// applyRetention enforces the FP-5 retention policy. p.max <= 0 means unlimited (keep all).
//
// For p.max == 1 (N2): a single retained frame is far more useful as the latest observed
// state than as the very first one, so this is special-cased to keep only the most recent
// capture (rather than literally applying "keep #1 + the most recent max-1 = 0", which would
// keep only the oldest and evict every later one - counter-intuitive for a user who asked
// to retain exactly one snapshot).
//
// For p.max >= 2, the design's rule applies as specified: keep snapshot #1 (the earliest
// frame preserves initial plan/queue state) plus the most recent (max-1) frames, deleting
// the oldest-except-first snapshot as new ones arrive.
func (p *jsonSnapshotPoller) applyRetention() {
	if p.max <= 0 {
		return
	}
	if p.max == 1 {
		for len(p.saved) > 1 {
			p.removeSnapshotFile(p.saved[0])
			p.saved = p.saved[1:]
		}
		return
	}
	for len(p.saved) > p.max {
		victim := p.saved[1]
		p.removeSnapshotFile(victim)
		p.saved = append(p.saved[:1], p.saved[2:]...)
	}
}

// removeSnapshotFile deletes path, logging (but not failing on) any error other than the
// file already being gone.
func (p *jsonSnapshotPoller) removeSnapshotFile(path string) {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		log.Warn().Err(err).Str("path", path).Msg("failed to delete old json snapshot during retention")
	}
}

// stop halts the poller; it is idempotent and safe to call on a nil receiver (the value
// returned by startJsonSnapshotPoller when snapshotting is disabled for a query). It
// cancels the context shared with every fetch (see startJsonSnapshotPoller/captureOnce),
// so a fetch that is already in flight when stop() is called is aborted immediately rather
// than left to complete (M1): wgExitMainStage still blocks the process from exiting until
// run() actually returns (after that abort and its cleanup, i.e. the removal of the
// now-incomplete snapshot file, have finished), so there is no truncated file left behind
// and no premature process exit.
func (p *jsonSnapshotPoller) stop() {
	if p == nil {
		return
	}
	p.stopOnce.Do(func() {
		p.cancel()
	})
}
