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

package diagnosis

import (
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseQueryValidation covers the loadjson-style validation: queryId and
// queryStats must be present.
func TestParseQueryValidation(t *testing.T) {
	_, err := ParseQuery([]byte(`{"queryId": "", "queryStats": {}}`))
	assert.Error(t, err)

	_, err = ParseQuery([]byte(`{"queryId": "q1"}`))
	assert.Error(t, err)

	_, err = ParseQuery([]byte(`not json`))
	assert.Error(t, err)

	q, err := ParseQuery(marshal(t, baseQuery("q1")))
	require.NoError(t, err)
	assert.Equal(t, "q1", q.Info.QueryId)
}

// TestAnalyzeGenericCardinalityExplosion is an end-to-end, fully synthetic
// (non-real-sample) exercise of the whole pipeline: a join node with a
// LOW-confidence, all-NaN-join-stats estimate and a large measured fan-out,
// with CPU/shuffle/wait symptoms elevated downstream. Every number is
// synthetic and chosen only to cross the default thresholds -- this proves the
// generic engine reaches PLAN PATH / HIGH confidence on ITS OWN, the same shape
// of proof FT-15 makes against the real sample.
func TestAnalyzeGenericCardinalityExplosion(t *testing.T) {
	q := baseQuery("synthetic-explosion")
	qs := q["queryStats"].(map[string]any)
	qs["elapsedTime"] = 600000.0  // 600s
	qs["totalCpuTime"] = 480000.0 // 480s -> cpu/scheduled will be high
	qs["totalScheduledTime"] = 500000.0
	qs["rawInputDataSize"] = float64(1_000_000)
	qs["shuffledDataSize"] = float64(100_000) // small, B5 should stay quiet via wall-share gate
	qs["operatorSummaries"] = []any{
		operatorSummary(1, "join1", "LookupJoinOperator", 1000, 5_000_000), // fan-out 5000x
		operatorSummary(1, "join1", "HashBuilderOperator", 200, 0),
	}
	q["planStatsAndCosts"].(map[string]any)["stats"] = map[string]any{
		"join1": nodeEstimate(2.79e22, "LOW", lowConfNoNdv()),
	}
	// Give the join's stage (1) essentially all of the query's CPU, so B1 is
	// attributed to it deterministically.
	q["outputStage"] = map[string]any{
		"stageId": "synthetic-explosion.1",
		"plan":    map[string]any{"jsonRepresentation": `{"id":"join1","name":"InnerJoin","identifier":"[(\"a\" = \"k1\")]","children":[]}`},
		"latestAttemptExecutionInfo": map[string]any{
			"state": "FINISHED",
			"tasks": []any{},
		},
		"subStages": []any{},
	}

	report, err := Analyze(marshal(t, q), DefaultThresholds())
	require.NoError(t, err)
	require.NotNil(t, report.Verdict)
	assert.Equal(t, CatPlanPath, report.Verdict.Category)
	assert.Equal(t, "join1", report.Verdict.NodeId)
	assert.Equal(t, "HIGH", report.Verdict.Confidence.Level)
	assert.Contains(t, report.Verdict.Recommendation, "ANALYZE")
	assert.Empty(t, report.IndependentFindings)

	// REC-3: this synthetic fixture has only one join node (no chain), so
	// ExtractFanoutChain should still degrade gracefully to a single-node
	// chain (or none) without crashing.
	for _, nm := range report.NodeMetrics {
		if nm.PlanNodeId == "join1" {
			assert.NotEmpty(t, nm.JoinKeyStats, "join-key fingerprint must be populated for the generic fixture too, no sample-specific literal required")
		}
	}
}

// TestAnalyzeJavaEngineFixture is a SYNTHETIC Java (non-native) engine fixture:
// unprefixed runtimeStats keys, a populated (non-null) stageGcStatistics with
// real GC time. Engine detection must be data-driven: this fixture must be
// classified as non-native and D1 must be able to elevate.
func TestAnalyzeJavaEngineFixture(t *testing.T) {
	q := baseQuery("synthetic-java")
	qs := q["queryStats"].(map[string]any)
	qs["elapsedTime"] = 100000.0
	qs["totalCpuTime"] = 5000.0
	qs["totalScheduledTime"] = 10000.0
	qs["stageGcStatistics"] = []any{
		map[string]any{"stageId": 0, "fullGcTasks": 2, "totalFullGcSec": 20.0},
	}
	// Java-style operator + runtimeStats keys: no "S<n>-" prefix, no short
	// Velox operator tokens.
	qs["operatorSummaries"] = []any{
		operatorSummary(0, "n1", "HashJoinOperator", 100, 100),
	}
	ops := qs["operatorSummaries"].([]any)
	ops[0].(map[string]any)["runtimeStats"] = map[string]any{
		"getOutputWallNanos": map[string]any{"name": "getOutputWallNanos", "unit": "NANO", "sum": 10, "count": 1, "max": 10, "min": 10},
	}
	qs["runtimeStats"] = map[string]any{
		"optimizerTimeNanos": map[string]any{"name": "optimizerTimeNanos", "unit": "NANO", "sum": 100, "count": 1, "max": 100, "min": 100},
	}

	report, err := Analyze(marshal(t, q), DefaultThresholds())
	require.NoError(t, err)
	var gc *CategoryResult
	for _, c := range report.Categories {
		if c.Category == CatGCPauses {
			gc = c
		}
	}
	require.NotNil(t, gc)
	assert.Equal(t, 2, gc.Severity, "Java engine with 20s full-GC over 100s elapsed (share 0.2) must elevate D1")
	assert.NotContains(t, gc.Note, "native")
}

// TestAnalyzeNoFramesIsFullyStatic covers FP-13: a lone terminal JSON with no
// snapshots yields the full static diagnosis (no progression section, no
// error).
func TestAnalyzeNoFramesIsFullyStatic(t *testing.T) {
	report, err := Analyze(marshal(t, baseQuery("q-static")), DefaultThresholds())
	require.NoError(t, err)
	assert.Nil(t, report.Progression)
	assert.NotNil(t, report.Verdict)
}

// TestAnalyzeSnapshotsOnlyNoFrames covers the error path when no frame parses.
func TestAnalyzeSnapshotsOnlyNoFrames(t *testing.T) {
	_, err := AnalyzeSnapshotsOnly(nil, DefaultThresholds())
	assert.Error(t, err)
}

// synthCardinalityExplosionQuery builds the same generic, fully-synthetic
// cardinality-explosion fixture as TestAnalyzeGenericCardinalityExplosion, as
// a []byte, reusable by both the terminal-only and terminal+snapshots tests.
func synthCardinalityExplosionQuery(t *testing.T, queryId string) []byte {
	q := baseQuery(queryId)
	qs := q["queryStats"].(map[string]any)
	qs["elapsedTime"] = 600000.0
	qs["totalCpuTime"] = 480000.0
	qs["totalScheduledTime"] = 500000.0
	qs["rawInputDataSize"] = float64(1_000_000)
	qs["operatorSummaries"] = []any{
		operatorSummary(1, "join1", "LookupJoinOperator", 1000, 5_000_000),
		operatorSummary(1, "join1", "HashBuilderOperator", 200, 0),
	}
	q["planStatsAndCosts"].(map[string]any)["stats"] = map[string]any{
		"join1": nodeEstimate(2.79e22, "LOW", lowConfNoNdv()),
	}
	q["outputStage"] = map[string]any{
		"stageId": queryId + ".1",
		"plan":    map[string]any{"jsonRepresentation": `{"id":"join1","name":"InnerJoin","identifier":"","children":[]}`},
		"latestAttemptExecutionInfo": map[string]any{
			"state": "FINISHED",
			"tasks": []any{},
		},
		"subStages": []any{},
	}
	return marshal(t, q)
}

// synthRunawayFrame builds one live/RUNNING snapshot frame with no measured
// actual for the join node (empty operatorSummaries, matching real RUNNING
// native-stage behavior, §2) but a frozen-throughput, climbing-CPU signature
// for the RunawayComputation detector, localized to stage 1 via
// S1-taskScheduledTimeNanos.
func synthRunawayFrame(t *testing.T, queryId string, cpuMs float64, stageScheduledNanos int64) []byte {
	q := baseQuery(queryId)
	qs := q["queryStats"].(map[string]any)
	qs["state"] = "RUNNING"
	q["state"] = "RUNNING"
	qs["elapsedTime"] = 600000.0
	qs["totalCpuTime"] = cpuMs
	qs["totalScheduledTime"] = 500000.0
	qs["processedInputPositions"] = 1000
	qs["progressPercentage"] = 50.0
	qs["operatorSummaries"] = []any{} // RUNNING stage: no operator summaries at all
	qs["runtimeStats"] = map[string]any{
		"S1-taskScheduledTimeNanos": map[string]any{"name": "S1-taskScheduledTimeNanos", "unit": "NANO", "sum": stageScheduledNanos, "count": 1, "max": stageScheduledNanos, "min": stageScheduledNanos},
	}
	q["planStatsAndCosts"].(map[string]any)["stats"] = map[string]any{
		"join1": nodeEstimate(2.79e22, "LOW", lowConfNoNdv()),
	}
	q["outputStage"] = map[string]any{
		"stageId": queryId + ".1",
		"plan":    map[string]any{"jsonRepresentation": `{"id":"join1","name":"InnerJoin","identifier":"","children":[]}`},
		"latestAttemptExecutionInfo": map[string]any{
			"state": "RUNNING",
			"tasks": []any{},
		},
		"subStages": []any{},
	}
	return marshal(t, q)
}

// TestAnalyzeWithSnapshotsFullPipeline drives AnalyzeWithSnapshots end-to-end
// from in-memory frame files: a terminal file with a measured cardinality
// explosion plus several live frames exhibiting the RunawayComputation
// signature. Covers the terminal+snapshots combination (progression populated
// on top of a measured-actual verdict, confidence upgraded to/kept at HIGH).
func TestAnalyzeWithSnapshotsFullPipeline(t *testing.T) {
	dir := t.TempDir()
	terminal := synthCardinalityExplosionQuery(t, "q-snap")

	var frameFiles []*FrameFile
	for i, cpu := range []float64{100000, 220000, 340000, 460000} {
		path := filepath.Join(dir, fmt.Sprintf("snapshot_%06d_%d.json", i+1, int64(i)*30000))
		require.NoError(t, os.WriteFile(path, synthRunawayFrame(t, "q-snap", cpu, int64(100+i*120)), 0644))
		frameFiles = append(frameFiles, &FrameFile{Seq: i + 1, TimestampMs: int64(i) * 30000, Path: path})
	}

	report, err := AnalyzeWithSnapshots(terminal, frameFiles, DefaultThresholds())
	require.NoError(t, err)
	require.NotNil(t, report.Progression)
	assert.Equal(t, 4, report.Progression.FrameCount)
	require.NotNil(t, report.Verdict)
	assert.Equal(t, CatPlanPath, report.Verdict.Category)
	assert.Equal(t, "join1", report.Verdict.NodeId)
	assert.Equal(t, "HIGH", report.Verdict.Confidence.Level, "a measured actual from the terminal file must still allow HIGH even with snapshots attached")
}

// TestAnalyzeWithSnapshotsMalformedFrameWarns covers a malformed frame file
// surfacing as a warning on the report, not a fatal error.
func TestAnalyzeWithSnapshotsMalformedFrameWarns(t *testing.T) {
	dir := t.TempDir()
	terminal := synthCardinalityExplosionQuery(t, "q-warn")
	badPath := filepath.Join(dir, "snapshot_000001_1000.json")
	require.NoError(t, os.WriteFile(badPath, []byte("not json"), 0644))

	report, err := AnalyzeWithSnapshots(terminal, []*FrameFile{{Seq: 1, TimestampMs: 1000, Path: badPath}}, DefaultThresholds())
	require.NoError(t, err)
	assert.NotEmpty(t, report.Warnings)
	assert.Nil(t, report.Progression, "zero successfully parsed frames must degrade to no progression section")
}

// TestAnalyzeSnapshotsOnlyFullPipeline drives the Mode-2 (frames-only, no
// terminal file) path end-to-end: the highest-seq frame is the current state,
// and confidence is capped at MEDIUM since no measured actual can exist.
func TestAnalyzeSnapshotsOnlyFullPipeline(t *testing.T) {
	dir := t.TempDir()
	var frameFiles []*FrameFile
	for i, cpu := range []float64{100000, 220000, 340000, 460000} {
		path := filepath.Join(dir, fmt.Sprintf("snapshot_%06d_%d.json", i+1, int64(i)*30000))
		require.NoError(t, os.WriteFile(path, synthRunawayFrame(t, "q-only", cpu, int64(100+i*120)), 0644))
		frameFiles = append(frameFiles, &FrameFile{Seq: i + 1, TimestampMs: int64(i) * 30000, Path: path})
	}
	report, err := AnalyzeSnapshotsOnly(frameFiles, DefaultThresholds())
	require.NoError(t, err)
	require.NotNil(t, report.Verdict)
	assert.Equal(t, "RUNNING", report.State)
	assert.Equal(t, "MEDIUM", report.Verdict.Confidence.Level)
}

// TestAnalyzeRealJavaFixture exercises the full pipeline against a REAL Java
// Presto 0.299 query JSON (testdata/real_java_small.json, excised from a
// 191-file real Java corpus used to find and fix two engine-detection/
// classification bugs during development -- see DetectNativeRuntime's doc
// comment and the C2/B5 fixes in classify.go). Must parse without error, be
// detected as non-native, and produce a sane verdict (most files in that
// corpus are small/FINISHED queries with no dominant bottleneck or a mild one).
func TestAnalyzeRealJavaFixture(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "real_java_small.json"))
	require.NoError(t, err)

	q, err := ParseQuery(raw)
	require.NoError(t, err)
	sig := ExtractSignals(q.Info, q.Extra, q.NodeMetrics, q.PlanNodes, q.Joins, nil)
	assert.False(t, sig.NativeRuntime, "a real Java query must not be detected as native")

	report, err := Analyze(raw, DefaultThresholds())
	require.NoError(t, err)
	require.NotNil(t, report.Verdict)
	assert.NotEmpty(t, report.Verdict.Confidence.Level)
	assert.Len(t, report.Categories, len(AllCategories))

	// Never-modify guarantee even for a real fixture that happens to be read
	// twice by the same test (ParseQuery + Analyze).
	raw2, err := os.ReadFile(filepath.Join("testdata", "real_java_small.json"))
	require.NoError(t, err)
	assert.Equal(t, raw, raw2)
}

// TestAnalyzeRealSampleEvidenceEnrichments is FT-10 (design.md §10 item 10):
// on every available real sample (the acceptance terminal, its dry-run
// twin, and the newer full-cycle capture — realSamplePaths, whichever
// exist), the A3 verdict's node id is read from the report itself (never
// hard-coded in this test), and the join-key NDV-fingerprint sub-table plus
// the estimate-provenance fallback line both render, with the exact
// order_id_52 min=314/max=98319154/NDV=NaN fingerprint the cycle test
// (design.md §5.4.1 A3) specifies.
func TestAnalyzeRealSampleEvidenceEnrichments(t *testing.T) {
	found := 0
	for _, path := range realSamplePaths {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		found++
		t.Run(filepath.Base(path), func(t *testing.T) {
			report, err := Analyze(raw, DefaultThresholds())
			require.NoError(t, err)
			require.NotNil(t, report.Verdict)
			require.Equal(t, CatPlanPath, report.Verdict.Category, "the real sample must still classify as PLAN PATH")
			nodeId := report.Verdict.NodeId
			require.NotEmpty(t, nodeId, "verdict must carry the plan-node id read from data")

			// Locate the winning node's NodeMetrics entry generically (by the
			// verdict's own NodeId, never a hard-coded id).
			var nm *NodeMetrics
			for _, m := range report.NodeMetrics {
				if m.PlanNodeId == nodeId {
					nm = m
					break
				}
			}
			require.NotNil(t, nm, "NodeMetrics must contain the verdict node")
			require.NotEmpty(t, nm.JoinKeyStats, "the join-key fingerprint must be populated on the verdict node")
			assert.Equal(t, "default_join_selectivity_fallback", nm.EstimateProvenance)

			var orderIdKey *JoinKeyStat
			for i := range nm.JoinKeyStats {
				if nm.JoinKeyStats[i].Column == "order_id_52" {
					orderIdKey = &nm.JoinKeyStats[i]
				}
			}
			require.NotNil(t, orderIdKey, "the order_id_52 join key must be present in the fingerprint (design.md §5.4.1 A3 cycle-test values)")
			assert.EqualValues(t, 314, orderIdKey.Min)
			assert.EqualValues(t, 98319154, orderIdKey.Max)
			assert.True(t, math.IsNaN(float64(orderIdKey.NDV)))

			md := report.RenderMarkdown(5)
			assert.Contains(t, md, "NDV fingerprint")
			assert.Contains(t, md, "| order_id_52 | 314 | 98319154 | NaN |")
			assert.Contains(t, md, "default-join-selectivity FALLBACK")
			assert.Contains(t, md, fmt.Sprintf("planStatsAndCosts[%s]", nodeId), "the rebuilt fallback evidence line must reference the node id read from the report, not a literal")
		})
	}
	if found == 0 {
		t.Skip("no real sample files available (see realSamplePaths); skipping FT-10")
	}
}

// tieProneSamplePaths are real corpus files independently found (during
// review B1) to have >=2 NodeMetrics entries tied on (severity, families) in
// scoreA3 -- exactly the shape that exposed the pre-existing map-iteration-
// order determinism bug (present since 34f101c). Only present on the
// machines used for manual corpus validation (design.md §8); the test below
// skips gracefully everywhere else, same pattern as realSamplePaths.
var tieProneSamplePaths = []string{
	"/tmp/pbench-query-corpus/ibm-lh-lakehouse-prestissimo634-presto-s/20260618_144306_00006_su6tu.json",
}

// TestAnalyzeDeterministicAcrossRepeatedRuns is the regression test for
// review B1 (§5.1.1 "deterministic tool" contract): running the FULL
// pbench-diagnose pipeline (Analyze, exactly what `pbench diagnose` calls)
// repeatedly on the SAME bytes must always report the same verdict category,
// node id, and confidence -- never flipping due to Go's randomized map
// iteration order over sig.NodeMetrics. Exercises both a tie-prone real
// corpus file (tieProneSamplePaths, when available) and every standing
// acceptance sample (realSamplePaths), running each >=10 times.
func TestAnalyzeDeterministicAcrossRepeatedRuns(t *testing.T) {
	const runs = 25
	found := 0
	for _, path := range append(append([]string{}, tieProneSamplePaths...), realSamplePaths...) {
		raw, err := os.ReadFile(path)
		if err != nil {
			continue
		}
		found++
		t.Run(filepath.Base(path), func(t *testing.T) {
			var wantCategory, wantNodeId, wantConfidence string
			for i := 0; i < runs; i++ {
				report, err := Analyze(raw, DefaultThresholds())
				require.NoError(t, err)
				require.NotNil(t, report.Verdict)
				if i == 0 {
					wantCategory = report.Verdict.Category
					wantNodeId = report.Verdict.NodeId
					wantConfidence = report.Verdict.Confidence.Level
					continue
				}
				assert.Equal(t, wantCategory, report.Verdict.Category, "verdict category must be identical across repeated runs on identical input (run %d)", i)
				assert.Equal(t, wantNodeId, report.Verdict.NodeId, "verdict node id must be identical across repeated runs on identical input (run %d)", i)
				assert.Equal(t, wantConfidence, report.Verdict.Confidence.Level, "verdict confidence must be identical across repeated runs on identical input (run %d)", i)
			}
		})
	}
	if found == 0 {
		t.Skip("no real sample files available (see tieProneSamplePaths / realSamplePaths); skipping B1 stability regression test")
	}
}

// afterFixSamplePath is the real after-NDV-fix terminal file (design.md §5.5
// third worked example, §10 item 11(d), FT-12) -- join-key statistics are
// PRESENT (query `20260719_141741_00009_drr9h`, "ANALYZE will not change
// this"), so this exercises the severity-2/inherent-fan-out tier and the
// sev-2+measured-actual confidence path being capped by two coexisting
// independent findings. Only present on the machine used for the round-2
// cluster test (design.md §8); the test below skips gracefully otherwise.
var afterFixSamplePath = "/tmp/pbench-cycle/out-fixed/stage_139_346_260719-151741/stage_139_346_139_346.json"

// TestAnalyzeRealAfterFixTerminalSeverity2MediumIsFT12 is FT-12 (design.md
// §10 item 11(d), taxonomy-owner ruling 2026-07-19): on the real after-fix
// terminal, the A3 root fires at node 1772 with severity 2 (NOT 3 -- join-key
// NDV is present, so the missing-stats/stats-fixable signature does not
// hold), the chasm-trap (inherent-fan-out) recommendation renders (NOT
// "run ANALYZE" -- that would be wrong once stats are present), node 1772
// carries NO estimateProvenance label (the estimate is on the real NDV path,
// not the default-selectivity fallback), the fan-out chain renders, and
// confidence is MEDIUM -- capped by TWO coexisting severity-2 INDEPENDENT
// findings (B4 quick-stats/metastore latency, B3 scan throughput), NOT by the
// severity tier itself (the sev-2->HIGH path exists and is demonstrated
// separately, on a synthetic no-independents fixture, by
// TestComputeConfidenceSev2MeasuredActualHighGate in classify_test.go).
func TestAnalyzeRealAfterFixTerminalSeverity2MediumIsFT12(t *testing.T) {
	raw, err := os.ReadFile(afterFixSamplePath)
	if err != nil {
		t.Skip("no real after-fix sample available (see afterFixSamplePath); skipping FT-12")
	}
	report, err := Analyze(raw, DefaultThresholds())
	require.NoError(t, err)
	require.NotNil(t, report.Verdict)

	require.Equal(t, CatPlanPath, report.Verdict.Category, "the real after-fix sample must still classify as PLAN PATH")
	require.Equal(t, "1772", report.Verdict.NodeId)

	var a3 *CategoryResult
	for _, c := range report.Categories {
		if c.Category == CatPlanPath {
			a3 = c
		}
	}
	require.NotNil(t, a3)
	assert.Equal(t, 2, a3.Severity, "join-key NDV is present -- the missing-stats/stats-fixable signature does not hold, so severity must be 2, not 3")
	assert.True(t, a3.FromMeasuredActual, "the real terminal has a measured actual fan-out")

	assert.Contains(t, report.Verdict.Recommendation, "inherent join fan-out", "present NDV must flip the recommendation to the chasm-trap branch, never the ANALYZE branch")
	assert.NotContains(t, report.Verdict.Recommendation, "ANALYZE")

	var nm1772 *NodeMetrics
	for _, m := range report.NodeMetrics {
		if m.PlanNodeId == "1772" {
			nm1772 = m
		}
	}
	require.NotNil(t, nm1772)
	assert.Empty(t, nm1772.EstimateProvenance, "the estimate is on the real NDV path, not the default-selectivity fallback -- no provenance label")

	require.NotEmpty(t, report.FanoutChain, "the fan-out chain must render for the after-fix sample")
	// Round-8 §5.4.1 A3(iii)(ι) R8-C: the probe-lineage spine now extends
	// PAST the verdict node (1772) to the downstream annihilation row 1769
	// (INNER join vs the measured-empty lr_provider_provider build) -- the
	// verdict node is somewhere in the middle of the spine, not necessarily
	// its last row; chain byte-identity is deliberately given up (design.md
	// §10 item 17), verdict/severity/confidence invariance is what matters
	// and is asserted throughout the rest of this test.
	var containsVerdict bool
	for _, row := range report.FanoutChain {
		if row.PlanNodeId == report.Verdict.NodeId {
			containsVerdict = true
		}
	}
	assert.True(t, containsVerdict, "the chain must still contain the verdict node")
	assert.Equal(t, "1769", report.FanoutChain[len(report.FanoutChain)-1].PlanNodeId, "round-8: the spine now extends to the downstream annihilation row")

	assert.Equal(t, "MEDIUM", report.Verdict.Confidence.Level, "capped by the coexisting independent findings, not by the severity tier")

	require.Len(t, report.IndependentFindings, 2, "the real after-fix run carries exactly the two known severity-2 independents (B4, B3)")
	byCat := map[string]*CategoryResult{}
	for _, f := range report.IndependentFindings {
		byCat[f.Category] = f
	}
	require.Contains(t, byCat, CatStorageLatency)
	assert.Equal(t, 2, byCat[CatStorageLatency].Severity)
	require.Contains(t, byCat, CatIOThroughput)
	assert.Equal(t, 2, byCat[CatIOThroughput].Severity)

	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "inherent join fan-out")
	assert.Contains(t, md, "severity 2")
	assert.Contains(t, md, "Confidence MEDIUM")
	assert.Contains(t, md, "fan-out chain")
	assert.Contains(t, md, "NDV fingerprint")
	assert.Contains(t, md, "PRESENT", "the fingerprint note must say join-key statistics are present, not NaN")
}

// TestAnalyzeRealAfterFixTerminalFanoutChainEnrichmentIsFT12Round5 is FT-12's
// round-5 addition (design.md §5.4.1 A3(iii), §10 item 14): on the real
// after-fix terminal, the fan-out-chain enrichment must reproduce the
// round-4 manual findings exactly, from data alone -- node 1784 resolves to
// detail1toN with build table delivery_order_order_provider and a joinKeys
// entry whose SQL line is 14; node 1781 is the auto-flagged finding that
// changed the round-4 fix from pre-aggregate to dedup -- dimensionNonUnique,
// build table admin_system_city, buildRows ~= 1792 and buildKeyNdv ~= 115
// (both read from the TableScan's own actual/estimate, not the join node's
// own task-replicated HashBuilder summary). The dimensionNonUnique per-row
// hint must render verbatim in the md chain table on that row, and the
// verdict's own recommendation (already pinned by
// TestAnalyzeRealAfterFixTerminalSeverity2MediumIsFT12) must be completely
// unaffected by this row-level flag.
func TestAnalyzeRealAfterFixTerminalFanoutChainEnrichmentIsFT12Round5(t *testing.T) {
	raw, err := os.ReadFile(afterFixSamplePath)
	if err != nil {
		t.Skip("no real after-fix sample available (see afterFixSamplePath); skipping FT-12 round-5")
	}
	report, err := Analyze(raw, DefaultThresholds())
	require.NoError(t, err)
	require.NotEmpty(t, report.FanoutChain)

	byNode := map[string]FanoutChainNode{}
	for _, row := range report.FanoutChain {
		byNode[row.PlanNodeId] = row
	}

	row1784, ok := byNode["1784"]
	require.True(t, ok, "node 1784 must be on the chain")
	assert.Equal(t, "ng_delivery.delivery_order_order_provider", row1784.Build)
	require.NotEmpty(t, row1784.JoinKeys)
	assert.Contains(t, row1784.JoinKeys, "id=order_id")
	require.NotNil(t, row1784.SqlLine, "row 1784's SQL line must be resolved from the raw plan")
	assert.Equal(t, 14, *row1784.SqlLine)
	assert.Equal(t, "detail1toN", row1784.BuildKind)

	row1781, ok := byNode["1781"]
	require.True(t, ok, "node 1781 must be on the chain")
	assert.Equal(t, "ng_public.admin_system_city", row1781.Build)
	require.Equal(t, "dimensionNonUnique", row1781.BuildKind, "the round-4 finding that changed the fix from pre-aggregate to dedup must auto-flag")
	require.NotNil(t, row1781.BuildRows)
	assert.InDelta(t, 1792, *row1781.BuildRows, 1, "buildRows must read the TableScan's own actual, not the join's task-replicated HashBuilder summary (28,672)")
	require.NotNil(t, row1781.BuildKeyNdv)
	assert.InDelta(t, 115, *row1781.BuildKeyNdv, 1)

	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "ng_public.admin_system_city")
	assert.Contains(t, md, dimensionNonUniqueHint, "the flagged row's hint must render verbatim in the chain table")
	assert.Contains(t, md, "inherent join fan-out", "the verdict's OWN recommendation must stay the chasm-trap branch, unaffected by row 1781's flag")
}

// TestAnalyzeRealAfterFixTerminalRound6IsFT12Round6 is FT-12's round-6
// addition (design.md §5.4.1 A3(iii)(δ/ε/ζ), §10 item 15): on the real
// after-fix terminal, node 1778 (looker_scratch.lr_admin_system_country)
// must auto-flag buildKind=="emptyBuild" (a MEASURED zero-row build -- the
// real "0/126 -> unique" mislabel round 6 fixes) AND deadJoin==true (LEFT
// join, zero build survivors, buildKind emptyBuild -- provably 1:1/empty,
// post-review's tightened deadJoin condition) with the verbatim
// §5.1.1 hint rendered; the chain's overall end-to-end multiplication must
// be reported at ~2.55e6x; every other row's buildKind must be unchanged
// from round 5 (1784/1775/1772 detail1toN, 1781 dimensionNonUnique); and the
// verdict must stay exactly sev2/MEDIUM/chasm-trap, untouched by any of
// this.
func TestAnalyzeRealAfterFixTerminalRound6IsFT12Round6(t *testing.T) {
	raw, err := os.ReadFile(afterFixSamplePath)
	if err != nil {
		t.Skip("no real after-fix sample available (see afterFixSamplePath); skipping FT-12 round-6")
	}
	report, err := Analyze(raw, DefaultThresholds())
	require.NoError(t, err)
	require.NotEmpty(t, report.FanoutChain)

	byNode := map[string]FanoutChainNode{}
	for _, row := range report.FanoutChain {
		byNode[row.PlanNodeId] = row
	}

	row1778, ok := byNode["1778"]
	require.True(t, ok, "node 1778 must be on the chain")
	assert.Equal(t, "looker_scratch.lr_admin_system_country", row1778.Build)
	assert.Equal(t, "emptyBuild", row1778.BuildKind, "a MEASURED zero-row build must auto-flag emptyBuild, fixing the round-5 0/126 -> unique mislabel")
	assert.True(t, row1778.DeadJoin, "LEFT + zero build survivors + provably-1:1 buildKind (emptyBuild) must auto-flag deadJoin")
	assert.Nil(t, row1778.BuildKeyDupFactor, "an emptyBuild row must never carry a dupFactor")
	require.NotNil(t, row1778.BuildRows)
	assert.EqualValues(t, 0, *row1778.BuildRows)
	assert.Equal(t, "measured", row1778.BuildRowsSource)

	// Every other row's buildKind is unchanged from round 5.
	assert.Equal(t, "detail1toN", byNode["1784"].BuildKind)
	assert.Equal(t, "dimensionNonUnique", byNode["1781"].BuildKind)
	assert.Equal(t, "detail1toN", byNode["1775"].BuildKind)
	assert.Equal(t, "detail1toN", byNode["1772"].BuildKind)

	require.NotNil(t, report.ChainFanout)
	assert.InDelta(t, 2.55e6, report.ChainFanout.EndToEnd, 5e3)
	assert.EqualValues(t, 174400, report.ChainFanout.In)
	assert.EqualValues(t, 445201868645, report.ChainFanout.Out)

	// Verdict is completely unaffected by any row-level flag.
	require.NotNil(t, report.Verdict)
	assert.Equal(t, 2, func() int {
		for _, c := range report.Categories {
			if c.Category == CatPlanPath {
				return c.Severity
			}
		}
		return -1
	}())
	assert.Equal(t, "MEDIUM", report.Verdict.Confidence.Level)
	assert.Contains(t, report.Verdict.Recommendation, "inherent join fan-out")

	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "emptyBuild")
	assert.Contains(t, md, deadJoinHint, "the deadJoin hint must render verbatim in the chain table")
	assert.Contains(t, md, "chain fan-out end-to-end ≈ 2.55e6x")
	assert.Contains(t, md, "severity 2")
	assert.Contains(t, md, "Confidence MEDIUM")
}

// TestAnalyzeRealAfterFixTerminalWithSnapshotsIsFT13d is FT-13(d) (design.md
// §10 item 12, §5.6/§5.4.4): running the FULL terminal+snapshots pipeline
// (AnalyzeWithSnapshots, exactly what `pbench diagnose` does when a sibling
// .snapshots/ directory is present) against the real after-fix run must
// render a well-formed RunawayComputation progression line (the resolved
// suspect node id, or the explicit "none identified" fallback -- never a
// dangling "suspect node ", round-3 Issue 1), show A3 emerging at or before
// frame 1 (round-3 Issue 2 -- its live-computable estimate/runaway signal is
// present from the start, never timed by the terminal-only measured-actual
// family), and raise NO spurious temporal-conflict line (the round-3 bug: a
// false "C1 before A3" conflict manufactured by mistiming A3's emergence).
func TestAnalyzeRealAfterFixTerminalWithSnapshotsIsFT13d(t *testing.T) {
	raw, err := os.ReadFile(afterFixSamplePath)
	if err != nil {
		t.Skip("no real after-fix sample available (see afterFixSamplePath); skipping FT-13(d)")
	}
	snapshotsDir := strings.TrimSuffix(afterFixSamplePath, ".json") + ".snapshots"
	frameFiles, discoverWarnings := DiscoverFrames(snapshotsDir)
	if len(frameFiles) == 0 {
		t.Skip("no real after-fix snapshots directory available; skipping FT-13(d)")
	}
	assert.Empty(t, discoverWarnings, "the real snapshot filenames must all be well-formed")

	report, err := AnalyzeWithSnapshots(raw, frameFiles, DefaultThresholds())
	require.NoError(t, err)
	require.NotNil(t, report.Verdict)
	require.Equal(t, CatPlanPath, report.Verdict.Category)
	require.Equal(t, "1772", report.Verdict.NodeId)
	require.NotNil(t, report.Progression)

	// Issue 2: A3 must have a live-computable emergence timestamp, at or
	// before frame 1 (the frames are relative to the first frame's
	// timestamp, so "at or before frame 1" reads as ~0s here).
	a3T, ok := report.Progression.EmergenceByCategory[CatPlanPath]
	require.True(t, ok, "A3's emergence must be recorded from its live-computable estimate/runaway signal")
	assert.InDelta(t, 0.0, a3T, 1e-9, "A3 must emerge at or before frame 1, never timed by the terminal-only measured actual")

	// Issue 2's net effect: no spurious temporal-conflict line at all on this
	// file now that A3 is timed correctly.
	assert.Empty(t, report.Progression.TemporalConflicts, "A3 emerging at/before its symptoms must not manufacture a temporal conflict")

	md := report.RenderMarkdown(5)
	assert.NotContains(t, md, "TEMPORAL CONFLICT", "no spurious conflict must render on this file")
	assert.NotRegexp(t, `suspect node\s*\n`, md, "the RunawayComputation line must never end with a dangling \"suspect node\" (round-3 Issue 1)")
	if report.Progression.Runaway != nil && report.Progression.Runaway.Fired {
		hasResolvedId := report.Progression.Runaway.SuspectNodeId != ""
		if hasResolvedId {
			assert.Contains(t, md, fmt.Sprintf("suspect node %s", report.Progression.Runaway.SuspectNodeId))
		} else {
			// The after-fix node 1772 no longer carries the missing-stats
			// signature pickSuspectNode searches for (join-key NDV is present,
			// round-3 Issue 3 -- verified genuine data, not a bug/regression),
			// so localization legitimately cannot name a suspect on this
			// specific file; the line must still render the explicit fallback,
			// never drop the id silently.
			assert.Contains(t, md, "suspect node: none identified")
		}
	}
}

// TestAnalyzeRealHealthyFixtureIsFT15b is FT-15b (design.md §5.4.2/§5.5's
// fourth worked example, §10 item 16): the round-7 healthy-query guards
// against the real, bundled healthy fixture (a formerly-timing-out query
// that FINISHES in 18.47s post-engine-short-circuit-fix, testdata/
// real_native_healthy.json, copied verbatim from
// /tmp/pbench-cycle/diagnosis-healthy/terminal.json) -- and, in the same
// test, the four-run acceptance invariance against the real standing
// pathological samples, confirming round 7 changed nothing about them.
func TestAnalyzeRealHealthyFixtureIsFT15b(t *testing.T) {
	t.Run("healthy fixture: top-line, gated categories, verdict unchanged (still PLAN TIME/MEDIUM, no new verdict enum)", func(t *testing.T) {
		raw, err := os.ReadFile(filepath.Join("testdata", "real_native_healthy.json"))
		require.NoError(t, err)

		report, err := Analyze(raw, DefaultThresholds())
		require.NoError(t, err)
		require.NotNil(t, report.Verdict)

		assert.True(t, report.Summary.Healthy)
		require.NotEmpty(t, report.Summary.HealthyNote)
		assert.Contains(t, report.Summary.HealthyNote, "No significant execution bottleneck detected")
		assert.Contains(t, report.Summary.HealthyNote, "planning", "the largest component must be read from the actual verdict root, not hard-coded")

		// Verdict stays the EXISTING PLAN TIME category -- no new HEALTHY
		// verdict enum, per the design's explicit ruling.
		assert.Equal(t, CatPlanTime, report.Verdict.Category)
		assert.Equal(t, "MEDIUM", report.Verdict.Confidence.Level)

		byCat := map[string]*CategoryResult{}
		for _, c := range report.Categories {
			byCat[c.Category] = c
		}
		// The skew floor + idle-driver + healthy gates must all have fired.
		assert.Equal(t, 0, byCat[CatSkew].Severity)
		assert.Contains(t, byCat[CatSkew].Note, "too small for meaningful skew")
		assert.Equal(t, 0, byCat[CatWaitBackpressure].Severity)
		assert.Contains(t, byCat[CatWaitBackpressure].Note, "idle-driver artifact")
		assert.Equal(t, 0, byCat[CatIOThroughput].Severity)
		assert.Contains(t, byCat[CatIOThroughput].Note, "idle-driver artifact")
		assert.Equal(t, 0, byCat[CatStorageLatency].Severity)
		assert.Contains(t, byCat[CatStorageLatency].Note, "idle-driver artifact")
		assert.Equal(t, 0, byCat[CatScheduling].Severity)
		assert.Contains(t, byCat[CatScheduling].Note, "idle-driver artifact")

		// A3's sev-1 independent finding survives the round-7 guards (A-group
		// is never touched).
		require.Len(t, report.IndependentFindings, 1)
		assert.Equal(t, CatPlanPath, report.IndependentFindings[0].Category)
		assert.Equal(t, 1, report.IndependentFindings[0].Severity)

		md := report.RenderMarkdown(5)
		assert.True(t, strings.HasPrefix(md, "# Query diagnosis:"))
		lines := strings.Split(md, "\n")
		require.GreaterOrEqual(t, len(lines), 3)
		assert.Equal(t, report.Summary.HealthyNote, lines[2], "the healthy top-line must be the report's literal first content line above the verdict")
		assert.Contains(t, md, "PLAN TIME:")
		assert.Contains(t, md, "task CPU too small for meaningful skew")
		assert.Contains(t, md, "idle-driver artifact")
	})

	t.Run("four-run acceptance invariance: round 7 changes nothing about the locked pathological verdicts", func(t *testing.T) {
		type expected struct {
			path       string
			category   string
			nodeId     string
			severity   int
			confidence string
			snapshots  bool // AnalyzeWithSnapshots/AnalyzeSnapshotsOnly instead of Analyze
		}
		cases := []expected{
			{afterFixSamplePath, CatPlanPath, "1772", 2, "MEDIUM", false},
		}
		for _, c := range cases {
			raw, err := os.ReadFile(c.path)
			if err != nil {
				t.Skipf("no real sample available at %s; skipping this case", c.path)
				continue
			}
			report, err := Analyze(raw, DefaultThresholds())
			require.NoError(t, err)
			require.NotNil(t, report.Verdict)
			assert.False(t, report.Summary.Healthy, "%s must not be classified healthy", c.path)
			assert.Equal(t, c.category, report.Verdict.Category, c.path)
			assert.Equal(t, c.nodeId, report.Verdict.NodeId, c.path)
			assert.Equal(t, c.confidence, report.Verdict.Confidence.Level, c.path)
			// drr9h's B3/B4 sev-2 independents must survive -- they are what
			// preserve the MEDIUM cap; round 7 must not have touched them.
			var byCat = map[string]*CategoryResult{}
			for _, f := range report.IndependentFindings {
				byCat[f.Category] = f
			}
			require.Contains(t, byCat, CatStorageLatency)
			assert.Equal(t, 2, byCat[CatStorageLatency].Severity)
			require.Contains(t, byCat, CatIOThroughput)
			assert.Equal(t, 2, byCat[CatIOThroughput].Severity)
		}

		// realSamplePaths' own confidence levels are NOT uniform: the first
		// two are the officially locked "muisn"/"terminal" sev3/HIGH
		// acceptance targets; its third entry (a residual before-fix cycle
		// file, confirmed MEDIUM back in round 3 -- capped by an unrelated
		// independent finding, not part of the four officially locked
		// targets) is checked here too, but for its OWN already-established
		// expectation, so round 7 regressions on it are still caught without
		// conflating it with the four locked acceptance verdicts.
		expectedConfidence := map[string]string{
			realSamplePaths[0]: "HIGH",   // muisn
			realSamplePaths[1]: "HIGH",   // terminal
			realSamplePaths[2]: "MEDIUM", // before-fix residual (independent-finding-capped)
		}
		for _, p := range realSamplePaths {
			raw, err := os.ReadFile(p)
			if err != nil {
				continue
			}
			report, err := Analyze(raw, DefaultThresholds())
			require.NoError(t, err)
			require.NotNil(t, report.Verdict)
			assert.False(t, report.Summary.Healthy, "%s must not be classified healthy (state FAILED)", p)
			assert.Equal(t, CatPlanPath, report.Verdict.Category, p)
			assert.Equal(t, "1772", report.Verdict.NodeId, p)
			assert.Equal(t, 3, func() int {
				for _, cc := range report.Categories {
					if cc.Category == CatPlanPath {
						return cc.Severity
					}
				}
				return -1
			}(), p)
			assert.Equal(t, expectedConfidence[p], report.Verdict.Confidence.Level, p)
		}

		// Snapshot-only (RUNNING) acceptance: sev3/MEDIUM, unaffected (state
		// RUNNING is never gated by either round-7 guard).
		snapshotsDir := "/tmp/pbench-phase1-test/out/feature/stage_139_346_260718-150403/stage_139_346_139_346.snapshots"
		if frameFiles, _ := DiscoverFrames(snapshotsDir); len(frameFiles) > 0 {
			report, err := AnalyzeSnapshotsOnly(frameFiles, DefaultThresholds())
			require.NoError(t, err)
			require.NotNil(t, report.Verdict)
			assert.False(t, report.Summary.Healthy)
			assert.Equal(t, CatPlanPath, report.Verdict.Category)
			assert.Equal(t, "MEDIUM", report.Verdict.Confidence.Level)
		}
	})
}
