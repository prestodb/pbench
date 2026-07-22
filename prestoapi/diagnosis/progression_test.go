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
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prestodb/presto-go-client/v2/queryjson"
	"pbench/prestoapi/plan_node"
)

// makeFrame builds a ParsedFrame directly (bypassing JSON) so RunawayComputation
// tests can control every input series precisely.
func makeFrame(seq int, tMs int64, cpuSec float64, processed int64, progress, memBytes, spill float64, stageSched map[int]int64) *ParsedFrame {
	rts := map[string]*RuntimeMetric{}
	for stage, v := range stageSched {
		rts[queryStageKey(stage)] = &RuntimeMetric{Sum: v}
	}
	progressCopy := progress
	processedCopy := processed
	q := &Query{
		Info: &queryjson.QueryInfo{QueryStats: &queryjson.QueryStats{PeakUserMemoryReservation: queryjson.SISize(memBytes)}},
		Extra: &ExtraInfo{QueryStats: &ExtraQueryStats{
			ProcessedInputPositions: &processedCopy,
			ProgressPercentage:      &progressCopy,
			RuntimeStats:            rts,
		}},
		NodeMetrics: map[string]*NodeMetrics{},
	}
	return &ParsedFrame{
		Seq: seq, TimestampMs: tMs, Query: q,
		Signals: &Signals{CpuSec: cpuSec, SpillBytes: spill},
	}
}

func queryStageKey(stage int) string {
	return "S" + itoa(stage) + "-taskScheduledTimeNanos"
}

func itoa(n int) string {
	if n == 0 {
		return "0"
	}
	neg := n < 0
	if neg {
		n = -n
	}
	var buf [20]byte
	i := len(buf)
	for n > 0 {
		i--
		buf[i] = byte('0' + n%10)
		n /= 10
	}
	if neg {
		i--
		buf[i] = '-'
	}
	return string(buf[i:])
}

// TestDiscoverFramesSortingAndMalformed covers frame discovery: sort by seq,
// malformed names skipped with a warning, duplicate seqs keep the later
// timestamp.
func TestDiscoverFramesSortingAndMalformed(t *testing.T) {
	dir := t.TempDir()
	names := []string{
		"snapshot_0002_2000.json",
		"snapshot_0001_1000.json",
		"snapshot_0001_1500.json", // duplicate seq, later timestamp -> should win
		"not_a_snapshot.json",
		"snapshot_bogus_x.json",
	}
	for _, n := range names {
		require.NoError(t, os.WriteFile(filepath.Join(dir, n), []byte(`{}`), 0644))
	}
	frames, warnings := DiscoverFrames(dir)
	require.Len(t, frames, 2)
	assert.Equal(t, 1, frames[0].Seq)
	assert.EqualValues(t, 1500, frames[0].TimestampMs)
	assert.Equal(t, 2, frames[1].Seq)
	assert.NotEmpty(t, warnings)

	_, errWarnings := DiscoverFrames(filepath.Join(dir, "does-not-exist"))
	assert.NotEmpty(t, errWarnings)
}

// TestDetectRunawayFires covers the positive case: throughput/progress/memory
// flat + spill 0 over >= RunawayFramesMin frames while CPU/scheduled climbs,
// localized to the dominant stage.
func TestDetectRunawayFires(t *testing.T) {
	th := DefaultThresholds()
	frames := []*ParsedFrame{
		makeFrame(1, 0, 100, 1000, 50, 1e9, 0, map[int]int64{9: 100}),
		makeFrame(2, 30000, 220, 1000, 50, 1e9, 0, map[int]int64{9: 220}),
		makeFrame(3, 60000, 340, 1000, 50, 1e9, 0, map[int]int64{9: 340}),
		makeFrame(4, 90000, 460, 1000, 50, 1e9, 0, map[int]int64{9: 460}),
	}
	r := DetectRunaway(frames, th)
	require.True(t, r.Fired)
	assert.Equal(t, 9, r.StageNum)
	assert.InDelta(t, 1.0, r.StageShare, 1e-9)
}

// TestDetectRunawayDoesNotFireWhenProgressing covers the negative case: memory
// or throughput genuinely growing must NOT fire (a healthy, progressing query).
func TestDetectRunawayDoesNotFireWhenProgressing(t *testing.T) {
	th := DefaultThresholds()
	frames := []*ParsedFrame{
		makeFrame(1, 0, 100, 1000, 10, 1e9, 0, map[int]int64{9: 100}),
		makeFrame(2, 30000, 220, 2000, 20, 1.2e9, 0, map[int]int64{9: 220}), // processed input growing
		makeFrame(3, 60000, 340, 3000, 30, 1.4e9, 0, map[int]int64{9: 340}),
		makeFrame(4, 90000, 460, 4000, 40, 1.6e9, 0, map[int]int64{9: 460}),
	}
	r := DetectRunaway(frames, th)
	assert.False(t, r.Fired)
}

// TestDetectRunawayDoesNotFireWhenCpuFlat covers the negative case: throughput
// flat is not enough on its own -- CPU must also be climbing fast enough.
func TestDetectRunawayDoesNotFireWhenCpuFlat(t *testing.T) {
	th := DefaultThresholds()
	frames := []*ParsedFrame{
		makeFrame(1, 0, 100, 1000, 50, 1e9, 0, map[int]int64{9: 100}),
		makeFrame(2, 30000, 101, 1000, 50, 1e9, 0, map[int]int64{9: 101}),
		makeFrame(3, 60000, 102, 1000, 50, 1e9, 0, map[int]int64{9: 102}),
		makeFrame(4, 90000, 103, 1000, 50, 1e9, 0, map[int]int64{9: 103}),
	}
	r := DetectRunaway(frames, th)
	assert.False(t, r.Fired)
}

// TestDetectRunawayDoesNotFireWithSpill covers the negative case: any spill
// disqualifies the runaway signature (it is not a pure cardinality explosion).
func TestDetectRunawayDoesNotFireWithSpill(t *testing.T) {
	th := DefaultThresholds()
	frames := []*ParsedFrame{
		makeFrame(1, 0, 100, 1000, 50, 1e9, 500, map[int]int64{9: 100}),
		makeFrame(2, 30000, 220, 1000, 50, 1e9, 500, map[int]int64{9: 220}),
		makeFrame(3, 60000, 340, 1000, 50, 1e9, 500, map[int]int64{9: 340}),
	}
	r := DetectRunaway(frames, th)
	assert.False(t, r.Fired)
}

// TestDetectRunawayInsufficientFrames covers the < RunawayFramesMin
// degradation.
func TestDetectRunawayInsufficientFrames(t *testing.T) {
	th := DefaultThresholds()
	frames := []*ParsedFrame{
		makeFrame(1, 0, 100, 1000, 50, 1e9, 0, map[int]int64{9: 100}),
		makeFrame(2, 30000, 220, 1000, 50, 1e9, 0, map[int]int64{9: 220}),
	}
	r := DetectRunaway(frames, th)
	assert.False(t, r.Fired)
}

// TestAnalyzeProgressionDegradesGracefully covers 0-frame and 1-frame
// operation (§5.6).
func TestAnalyzeProgressionDegradesGracefully(t *testing.T) {
	th := DefaultThresholds()
	p0 := AnalyzeProgression(nil, nil, th)
	assert.Equal(t, 0, p0.FrameCount)
	assert.Nil(t, p0.Runaway)

	p1 := AnalyzeProgression([]*ParsedFrame{makeFrame(1, 0, 1, 1, 1, 1, 0, nil)}, nil, th)
	assert.Equal(t, 1, p1.FrameCount)
	assert.True(t, p1.Insufficient)
}

// TestCheckTemporalConsistency covers §5.4.4: a symptom that demonstrably
// precedes the claimed root triggers an explicit temporal-conflict finding.
func TestCheckTemporalConsistency(t *testing.T) {
	root := newResult(CatPlanPath, 3, "")
	sym := newResult(CatCPU, 2, "")
	consistent, conflicts := CheckTemporalConsistency(root, []*CategoryResult{sym},
		map[string]float64{CatPlanPath: 60, CatCPU: 150})
	assert.True(t, consistent)
	assert.Empty(t, conflicts)

	consistent2, conflicts2 := CheckTemporalConsistency(root, []*CategoryResult{sym},
		map[string]float64{CatPlanPath: 150, CatCPU: 60})
	assert.False(t, consistent2)
	require.Len(t, conflicts2, 1)

	c3, _ := CheckTemporalConsistency(nil, nil, nil)
	assert.True(t, c3)
}

// TestParseFramesSkipsUnparsable covers §5.6: frames with unparsable JSON (or
// an unreadable path) are skipped and counted in the returned warnings, not
// fatal to the whole batch.
func TestParseFramesSkipsUnparsable(t *testing.T) {
	dir := t.TempDir()
	goodPath := filepath.Join(dir, "snapshot_000001_1000.json")
	badPath := filepath.Join(dir, "snapshot_000002_2000.json")
	require.NoError(t, os.WriteFile(goodPath, marshal(t, baseQuery("q1")), 0644))
	require.NoError(t, os.WriteFile(badPath, []byte(`not valid json`), 0644))

	files := []*FrameFile{
		{Seq: 1, TimestampMs: 1000, Path: goodPath},
		{Seq: 2, TimestampMs: 2000, Path: badPath},
		{Seq: 3, TimestampMs: 3000, Path: filepath.Join(dir, "does-not-exist.json")},
	}
	parsed, warnings := ParseFrames(files)
	require.Len(t, parsed, 1)
	assert.Equal(t, 1, parsed[0].Seq)
	assert.Len(t, warnings, 2, "one unparsable + one unreadable frame must both be warned about, not fatal")
}

// TestPickSuspectNode covers the RunawayComputation localization step: among
// the plan nodes inside the runaway stage, the LOW-confidence/NaN-NDV node is
// named as the suspect (worst estimated output wins on a tie); a stage with
// no qualifying node, or no stage-tree/plan-stats data at all, degrades to "".
func TestPickSuspectNode(t *testing.T) {
	extra := &ExtraInfo{
		OutputStage: &StageNode{
			StageId: "q1.9",
			Plan: &StagePlan{JsonRepresentation: `{
				"id": "1", "name": "LeftJoin", "identifier": "",
				"children": [
					{"id": "2", "name": "TableScan", "identifier": "", "children": []}
				]
			}`},
			LatestAttemptExecutionInfo: &StageExecutionInfo{State: "RUNNING"},
		},
		PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
			"1": {Confident: "LOW", OutputRowCount: 1e20, JoinNodeStatsEstimate: &JoinNodeStatsEstimate{}},
			"2": {Confident: "HIGH", OutputRowCount: 100}, // not a suspect: HIGH confidence
		}},
	}
	nan := plan_node.JsonFloat64(nanValue())
	extra.PlanStatsAndCosts.Stats["1"].JoinNodeStatsEstimate = &JoinNodeStatsEstimate{
		NullJoinBuildKeyCount: nan, JoinBuildKeyCount: nan, NullJoinProbeKeyCount: nan, JoinProbeKeyCount: nan,
	}
	nodeMetrics := ComputeNodeMetrics(extra, nil)
	frame := &ParsedFrame{Query: &Query{Extra: extra, NodeMetrics: nodeMetrics}}

	suspect := pickSuspectNode(frame, 9)
	assert.Equal(t, "1", suspect, "the LOW-confidence, all-NaN-join-stats node must be named as the suspect")

	// Stage number that does not exist in the tree -> no node ids -> "".
	assert.Equal(t, "", pickSuspectNode(frame, 42))

	// No OutputStage/PlanStatsAndCosts at all -> "".
	emptyFrame := &ParsedFrame{Query: &Query{Extra: &ExtraInfo{}, NodeMetrics: map[string]*NodeMetrics{}}}
	assert.Equal(t, "", pickSuspectNode(emptyFrame, 9))
}

// TestA3LiveEmergenceDetected covers the round-3 Issue 2 helper directly: the
// estimate family (estimateBad on any node) or RunawayComputation firing both
// count as "detected"; a frame with neither -- even one carrying a measured
// actual fan-out, the terminal-only family -- must NOT be detected.
func TestA3LiveEmergenceDetected(t *testing.T) {
	assert.False(t, a3LiveEmergenceDetected(baselineSignals()), "a quiet baseline has no live signal")

	estimateOnly := baselineSignals()
	estimateOnly.NodeMetrics["1772"] = &NodeMetrics{PlanNodeId: "1772", Confidence: "LOW", IsLowConfidence: true}
	assert.True(t, a3LiveEmergenceDetected(estimateOnly), "the estimate family (LOW + missing NDV) alone must be detected")

	runawayOnly := baselineSignals()
	runawayOnly.Runaway = &RunawaySignal{Fired: true, StageNum: 9, SuspectNodeId: "1772"}
	assert.True(t, a3LiveEmergenceDetected(runawayOnly), "RunawayComputation firing alone must be detected")

	actualOnly := baselineSignals()
	fanout := 742.7
	actualOnly.NodeMetrics["1772"] = &NodeMetrics{
		PlanNodeId: "1772", Confidence: "LOW", MinVariableNDV: floatPtr(56), // NDV present -> no estimate family
		ActualFanout: &fanout,
	}
	assert.False(t, a3LiveEmergenceDetected(actualOnly), "the terminal-only measured-actual fan-out family must NEVER count as a live signal")
}

// TestAnalyzeProgressionA3EmergenceUsesLiveSignalsNotTerminalActual is UT-16,
// the round-3 regression test for Issue 2 (§5.6/§5.4.4): a drr9h-shaped
// fixture -- RunawayComputation fires; node 1772's estimate family (LOW
// confidence, no NDV) is present in EVERY frame; its measured-actual fan-out
// (the terminal-only family) appears ONLY on the last frame, mimicking a
// finished-mid-run stage or the terminal file folded in -- must show A3
// emerging at frame 1 (~t=0s), never the last frame. A live-computable C1
// symptom present from frame 1 too must NOT trigger a temporal conflict
// against A3 (both emerge at t=0 -- root no later than symptom), matching the
// real after-fix file's corrected behavior (no more spurious "C1 at t=0
// before A3 at t=601s").
func TestAnalyzeProgressionA3EmergenceUsesLiveSignalsNotTerminalActual(t *testing.T) {
	th := DefaultThresholds()
	frames := []*ParsedFrame{
		makeFrame(1, 0, 100, 1000, 50, 1e9, 0, map[int]int64{9: 100}),
		makeFrame(2, 30000, 220, 1000, 50, 1e9, 0, map[int]int64{9: 220}),
		makeFrame(3, 60000, 340, 1000, 50, 1e9, 0, map[int]int64{9: 340}),
		makeFrame(4, 90000, 460, 1000, 50, 1e9, 0, map[int]int64{9: 460}),
	}
	for i, f := range frames {
		f.Signals.ElapsedSec = 90
		// A live-computable C1 symptom present from frame 1 (per-driver
		// average blocked time close to elapsed the whole way through, the
		// real drr9h shape).
		f.Signals.BlockedWallSec = 88
		f.Signals.TotalDrivers = 1
		// The estimate family: present from frame 1, byte-identical live
		// (§5.4.1) -- LOW confidence, no NDV info at all.
		f.Signals.NodeMetrics = map[string]*NodeMetrics{
			"1772": {PlanNodeId: "1772", Confidence: "LOW", IsLowConfidence: true},
		}
		if i == len(frames)-1 {
			// The terminal-only measured actual arrives ONLY on the last
			// frame -- must NOT be what times A3's emergence.
			fanout := 742.7
			in, out := int64(599440009), int64(445201868645)
			f.Signals.NodeMetrics["1772"].ActualFanout = &fanout
			f.Signals.NodeMetrics["1772"].ActualIn = &in
			f.Signals.NodeMetrics["1772"].ActualOut = &out
		}
	}

	p := AnalyzeProgression(frames, nil, th)
	require.NotNil(t, p.Runaway)
	require.True(t, p.Runaway.Fired, "precondition: RunawayComputation must fire on this fixture")

	a3T, ok := p.EmergenceByCategory[CatPlanPath]
	require.True(t, ok, "A3 must have an emergence timestamp -- its estimate family is live-computable")
	assert.Equal(t, 0.0, a3T, "A3 must emerge at frame 1 (~t=0s), NOT the last frame where the terminal-only actual fan-out first appears")

	c1T, ok := p.EmergenceByCategory[CatWaitBackpressure]
	require.True(t, ok)
	assert.Equal(t, 0.0, c1T)

	root := newResult(CatPlanPath, 3, "")
	sym := newResult(CatWaitBackpressure, 2, "")
	consistent, conflicts := CheckTemporalConsistency(root, []*CategoryResult{sym}, p.EmergenceByCategory)
	assert.True(t, consistent, "root and symptom emerge at the same time -> no spurious conflict")
	assert.Empty(t, conflicts)
}
