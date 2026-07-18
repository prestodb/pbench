package cmd

import (
	"pbench/cmd/run"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// M2: verifies the four snapshot flags are actually registered on runCmd (cmd/run.go's
// init()) with the documented defaults (design.md §4.1: --snapshots default false,
// --snapshot-interval default 30s, --snapshot-max default 20, --snapshot-fetch-timeout
// default 0 = "use the interval"), and that setting them updates the run package vars they
// are bound to. This is the flag-registration half of the wiring; cmd/run/main_test.go
// covers the SharedStageStates-construction half (newSharedStageStates()).
func TestRunCmdSnapshotFlags_RegisteredWithDocumentedDefaults(t *testing.T) {
	flags := runCmd.Flags()

	snapshotsFlag := flags.Lookup("snapshots")
	require.NotNil(t, snapshotsFlag, "--snapshots must be registered on `pbench run`")
	assert.Equal(t, "false", snapshotsFlag.DefValue)

	intervalFlag := flags.Lookup("snapshot-interval")
	require.NotNil(t, intervalFlag, "--snapshot-interval must be registered on `pbench run`")
	assert.Equal(t, "30s", intervalFlag.DefValue)

	maxFlag := flags.Lookup("snapshot-max")
	require.NotNil(t, maxFlag, "--snapshot-max must be registered on `pbench run`")
	assert.Equal(t, "20", maxFlag.DefValue)

	fetchTimeoutFlag := flags.Lookup("snapshot-fetch-timeout")
	require.NotNil(t, fetchTimeoutFlag, "--snapshot-fetch-timeout must be registered on `pbench run`")
	assert.Equal(t, "0s", fetchTimeoutFlag.DefValue, `0 means "use the polling interval", resolved later in stage.resolveSnapshotSettings`)
}

// TestRunCmdSnapshotFlags_ParsingUpdatesBoundVars proves the flags are bound to the exact
// run package vars that newSharedStageStates() reads from (rather than merely registered),
// by parsing a flag set and checking the bound vars afterwards.
func TestRunCmdSnapshotFlags_ParsingUpdatesBoundVars(t *testing.T) {
	restoreCmdSnapshotVars(t)

	err := runCmd.Flags().Parse([]string{
		"--snapshots",
		"--snapshot-interval=45s",
		"--snapshot-max=7",
		"--snapshot-fetch-timeout=90s",
	})
	require.NoError(t, err)

	assert.True(t, run.Snapshots)
	assert.Equal(t, 45*time.Second, run.SnapshotInterval)
	assert.Equal(t, 7, run.SnapshotMax)
	assert.Equal(t, 90*time.Second, run.SnapshotFetchTimeout)
}

// restoreCmdSnapshotVars snapshots and restores the run package's snapshot vars (and resets
// the flags to their defaults) so this test doesn't leak state into others.
func restoreCmdSnapshotVars(t *testing.T) {
	t.Helper()
	origSnapshots := run.Snapshots
	origInterval := run.SnapshotInterval
	origMax := run.SnapshotMax
	origTimeout := run.SnapshotFetchTimeout
	t.Cleanup(func() {
		run.Snapshots = origSnapshots
		run.SnapshotInterval = origInterval
		run.SnapshotMax = origMax
		run.SnapshotFetchTimeout = origTimeout
	})
}
