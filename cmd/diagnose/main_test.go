// Copyright 2026.

// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at

//     http://www.apache.org/licenses/LICENSE-2.0

// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package diagnose

import (
	"crypto/sha256"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"pbench/prestoapi/diagnosis"
)

// minimalQueryJSON returns a small, generic, valid query JSON document with
// no elevated categories, optionally with a distinguishing queryId.
func minimalQueryJSON(queryId string) []byte {
	doc := map[string]any{
		"queryId": queryId,
		"state":   "FINISHED",
		"queryStats": map[string]any{
			"elapsedTime":               100000.0,
			"totalCpuTime":              1000.0,
			"totalScheduledTime":        2000.0,
			"rawInputPositions":         1000000,
			"totalSplits":               10,
			"operatorSummaries":         []any{},
			"stageGcStatistics":         []any{},
			"runtimeStats":              map[string]any{},
			"peakUserMemoryReservation": float64(1000),
		},
		"planStatsAndCosts":    map[string]any{"stats": map[string]any{}},
		"optimizerInformation": []any{},
	}
	b, err := json.Marshal(doc)
	if err != nil {
		panic(err)
	}
	return b
}

func resetFlags() {
	Format = "md"
	Sidecar = false
	OutputPath = ""
	NoSnapshots = false
	QueryIdFilter = ""
	Top = 5
	ThresholdsPath = ""
}

// TestModeOneTerminalFileWithSiblingSnapshots covers FP-8/FP-13 Mode 1: the
// terminal file's sibling ".snapshots/" directory is auto-discovered by the
// exact, mechanical rule (strip ".json", append ".snapshots").
func TestModeOneTerminalFileWithSiblingSnapshots(t *testing.T) {
	resetFlags()
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "q1.json")
	require.NoError(t, os.WriteFile(jsonPath, minimalQueryJSON("q1"), 0644))

	// No sibling snapshots dir yet -> full static diagnosis, no progression.
	n, err := processInput(jsonPath, diagnosis.DefaultThresholds())
	require.NoError(t, err)
	assert.Equal(t, 1, n)

	// Now add a sibling snapshots dir with one frame.
	snapDir := filepath.Join(dir, "q1.snapshots")
	require.NoError(t, os.MkdirAll(snapDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "snapshot_000001_1000.json"), minimalQueryJSON("q1"), 0644))

	frames := discoverSiblingFrames(jsonPath)
	assert.Len(t, frames, 1, "sibling .snapshots/ dir must be auto-discovered")

	// --no-snapshots must suppress auto-discovery.
	NoSnapshots = true
	assert.Empty(t, discoverSiblingFrames(jsonPath))
	NoSnapshots = false
}

// TestModeTwoSnapshotsDirDirect covers Mode 2: diagnosing a .snapshots/
// directory directly (no terminal file at all).
func TestModeTwoSnapshotsDirDirect(t *testing.T) {
	resetFlags()
	dir := t.TempDir()
	snapDir := filepath.Join(dir, "q2.snapshots")
	require.NoError(t, os.MkdirAll(snapDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "snapshot_000001_1000.json"), minimalQueryJSON("q2"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "snapshot_000002_2000.json"), minimalQueryJSON("q2"), 0644))

	assert.True(t, isSnapshotsDirPath(snapDir))
	assert.True(t, isSnapshotsDirPath(snapDir+string(filepath.Separator)))
	assert.False(t, isSnapshotsDirPath(dir))

	n, err := processInput(snapDir, diagnosis.DefaultThresholds())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
}

// TestModeThreeBatchDirAutoPairingAndFilters covers FP-14 Mode 3: batch over a
// run-output directory, non-recursive, auto-pairing sibling snapshots dirs,
// skipping sidecar/aux files and invalid JSON, and --query-id filtering.
func TestModeThreeBatchDirAutoPairingAndFilters(t *testing.T) {
	resetFlags()
	dir := t.TempDir()
	require.NoError(t, os.WriteFile(filepath.Join(dir, "q1.json"), minimalQueryJSON("q1"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "q2.json"), minimalQueryJSON("q2"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "q3.json"), []byte(`not valid json`), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "q1.error.json"), []byte(`{}`), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "q1.analysis.json"), []byte(`{}`), 0644))
	snapDir := filepath.Join(dir, "q1.snapshots")
	require.NoError(t, os.MkdirAll(snapDir, 0755))
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "snapshot_000001_1000.json"), minimalQueryJSON("q1"), 0644))

	n, err := processBatchDir(dir, diagnosis.DefaultThresholds())
	require.NoError(t, err)
	assert.Equal(t, 2, n, "q3.json is invalid and must be skipped with a warning; error/analysis sidecars excluded; .snapshots/ not descended into as a separate file")

	QueryIdFilter = "q2"
	n2, err := processBatchDir(dir, diagnosis.DefaultThresholds())
	require.NoError(t, err)
	assert.Equal(t, 1, n2, "--query-id must narrow the batch to the single matching file")
	QueryIdFilter = ""
}

// TestSidecarAndNeverModifyGuarantee covers FP-12/UT-15: sidecar naming,
// --output-path placement, stdout always non-empty, and the input file's
// SHA-256 unchanged before/after (never-modify guarantee).
func TestSidecarAndNeverModifyGuarantee(t *testing.T) {
	resetFlags()
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "q1.json")
	content := minimalQueryJSON("q1")
	require.NoError(t, os.WriteFile(jsonPath, content, 0644))
	before := sha256.Sum256(content)

	Sidecar = true
	for _, format := range []string{"md", "json"} {
		Format = format
		n, err := processSingleFile(jsonPath, diagnosis.DefaultThresholds())
		require.NoError(t, err)
		assert.Equal(t, 1, n)
		sidecarPath := filepath.Join(dir, "q1.analysis."+format)
		body, readErr := os.ReadFile(sidecarPath)
		require.NoError(t, readErr)
		assert.NotEmpty(t, body)
	}

	after, err := os.ReadFile(jsonPath)
	require.NoError(t, err)
	afterSum := sha256.Sum256(after)
	assert.Equal(t, before, afterSum, "the input query JSON must never be modified")

	// --output-path places the sidecar elsewhere.
	outDir := t.TempDir()
	OutputPath = outDir
	Format = "md"
	_, err = processSingleFile(jsonPath, diagnosis.DefaultThresholds())
	require.NoError(t, err)
	_, statErr := os.Stat(filepath.Join(outDir, "q1.analysis.md"))
	assert.NoError(t, statErr)
}

// TestInvalidInputPath covers the "no analyzable input" error path.
func TestInvalidInputPath(t *testing.T) {
	resetFlags()
	_, err := processInput(filepath.Join(t.TempDir(), "does-not-exist.json"), diagnosis.DefaultThresholds())
	assert.Error(t, err)
}

// TestProcessSingleSnapshotsDirEmpty covers the error path when a
// ".snapshots" directory has no parsable frames at all.
func TestProcessSingleSnapshotsDirEmpty(t *testing.T) {
	resetFlags()
	dir := t.TempDir()
	snapDir := filepath.Join(dir, "empty.snapshots")
	require.NoError(t, os.MkdirAll(snapDir, 0755))
	_, err := processSingleSnapshotsDir(snapDir, diagnosis.DefaultThresholds())
	assert.Error(t, err)

	// With a sidecar requested and a real frame, the sidecar must be written
	// using the ".snapshots"-stripped basename.
	require.NoError(t, os.WriteFile(filepath.Join(snapDir, "snapshot_000001_1000.json"), minimalQueryJSON("q1"), 0644))
	os.Remove(filepath.Join(dir, "empty.analysis.md"))
	Sidecar = true
	n, err := processSingleSnapshotsDir(snapDir, diagnosis.DefaultThresholds())
	require.NoError(t, err)
	assert.Equal(t, 1, n)
	_, statErr := os.Stat(filepath.Join(dir, "empty.analysis.md"))
	assert.NoError(t, statErr)
	Sidecar = false
}

// TestAnalyzeFileReadError covers analyzeFile's error path for an unreadable
// file.
func TestAnalyzeFileReadError(t *testing.T) {
	resetFlags()
	_, err := analyzeFile(filepath.Join(t.TempDir(), "missing.json"), diagnosis.DefaultThresholds())
	assert.Error(t, err)
}

// TestSiblingSnapshotsDirNonJsonSuffix covers the fallback branch when the
// input path does not end in ".json".
func TestSiblingSnapshotsDirNonJsonSuffix(t *testing.T) {
	assert.Equal(t, "foo.snapshots", siblingSnapshotsDir("foo"))
	assert.Equal(t, "foo.snapshots", siblingSnapshotsDir("foo.json"))
}

// TestPrintSummaryTableEmpty covers the "no queries analyzed" message.
func TestPrintSummaryTableEmpty(t *testing.T) {
	printSummaryTable(nil)
}

// TestWriteSidecarError covers a write failure (nonexistent output directory).
func TestWriteSidecarError(t *testing.T) {
	resetFlags()
	OutputPath = filepath.Join(t.TempDir(), "does", "not", "exist")
	report, err := diagnosis.Analyze(minimalQueryJSON("q1"), diagnosis.DefaultThresholds())
	require.NoError(t, err)
	err = writeSidecar(report, "q1.json")
	assert.Error(t, err)
}

// TestRunHappyPathAndPartialFailure exercises the cobra Run entrypoint
// end-to-end: one valid file (analyzed) plus one bad path (logged, does not
// abort the run since at least one input was analyzable, FP-8/exit-code
// design.md §10 OQ-5).
func TestRunHappyPathAndPartialFailure(t *testing.T) {
	resetFlags()
	dir := t.TempDir()
	jsonPath := filepath.Join(dir, "q1.json")
	require.NoError(t, os.WriteFile(jsonPath, minimalQueryJSON("q1"), 0644))
	badPath := filepath.Join(dir, "missing.json")

	// Must not panic or os.Exit(1) when at least one input is analyzable.
	Run(nil, []string{jsonPath, badPath})
}
