package run

import (
	"net/url"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// M2: asserts the run-level CLI flag vars (Snapshots, SnapshotInterval, SnapshotMax,
// SnapshotFetchTimeout - populated by cmd/run.go's flag registration) are correctly copied
// into SharedStageStates by newSharedStageStates(), for both the documented defaults
// (30s / 20 / 0=use-interval) and explicit overrides. Run() itself calls os.Exit() at the
// end and so cannot be invoked directly from a test; newSharedStageStates() was extracted
// specifically to make this wiring testable in isolation (see main.go).
func TestNewSharedStageStates_SnapshotFlagDefaults(t *testing.T) {
	restoreSnapshotVars(t)
	// Mirrors the defaults cmd/run.go registers for these flags.
	Snapshots = false
	SnapshotInterval = 30 * time.Second
	SnapshotMax = 20
	SnapshotFetchTimeout = 0

	states := newSharedStageStates(mustParseURL(t, "http://localhost:8080"))

	assert.False(t, states.SnapshotsEnabled)
	assert.Equal(t, 30*time.Second, states.SnapshotInterval)
	assert.Equal(t, 20, states.SnapshotMax)
	assert.Equal(t, time.Duration(0), states.SnapshotFetchTimeout, "0 means \"use the interval\", resolved later in stage.resolveSnapshotSettings")
}

func TestNewSharedStageStates_SnapshotFlagOverrides(t *testing.T) {
	restoreSnapshotVars(t)
	Snapshots = true
	SnapshotInterval = 45 * time.Second
	SnapshotMax = 0 // explicit unlimited
	SnapshotFetchTimeout = 90 * time.Second

	states := newSharedStageStates(mustParseURL(t, "http://localhost:8080"))

	assert.True(t, states.SnapshotsEnabled)
	assert.Equal(t, 45*time.Second, states.SnapshotInterval)
	assert.Equal(t, 0, states.SnapshotMax)
	assert.Equal(t, 90*time.Second, states.SnapshotFetchTimeout)
}

// TestNewSharedStageStates_OtherFieldsUnaffected is a light sanity check that the
// pre-existing (non-snapshot) wiring survived the refactor that extracted
// newSharedStageStates() out of Run().
func TestNewSharedStageStates_OtherFieldsUnaffected(t *testing.T) {
	restoreSnapshotVars(t)
	origName, origComment, origSeed, origSkip, origOutput := Name, Comment, RandSeed, RandSkip, OutputPath
	t.Cleanup(func() {
		Name, Comment, RandSeed, RandSkip, OutputPath = origName, origComment, origSeed, origSkip, origOutput
	})
	Name = "my_run"
	Comment = "a comment"
	RandSeed = 42
	RandSkip = 3
	OutputPath = "/tmp/somewhere"

	states := newSharedStageStates(mustParseURL(t, "http://coordinator.example.com:8889"))

	assert.Equal(t, "my_run", states.RunName)
	assert.Equal(t, "a comment", states.Comment)
	assert.Equal(t, int64(42), states.RandSeed)
	assert.Equal(t, 3, states.RandSkip)
	assert.Equal(t, "/tmp/somewhere", states.OutputPath)
	assert.Equal(t, "coordinator.example.com:8889", states.ServerFQDN)
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	u, err := url.Parse(raw)
	require.NoError(t, err)
	return u
}

// restoreSnapshotVars snapshots and restores the four package-level snapshot flag vars so
// tests don't leak mutations into each other.
func restoreSnapshotVars(t *testing.T) {
	t.Helper()
	origSnapshots := Snapshots
	origInterval := SnapshotInterval
	origMax := SnapshotMax
	origTimeout := SnapshotFetchTimeout
	t.Cleanup(func() {
		Snapshots = origSnapshots
		SnapshotInterval = origInterval
		SnapshotMax = origMax
		SnapshotFetchTimeout = origTimeout
	})
}
