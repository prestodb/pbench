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
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"pbench/prestoapi/plan_node"
)

// baselineSignals returns a Signals value with every category at its
// "not elevated" baseline, generic and query-agnostic: every test below
// mutates exactly the field(s) needed to trigger the category under test.
func baselineSignals() *Signals {
	return &Signals{
		ElapsedSec:        100,
		ScheduledSec:      100,
		CpuSec:            10,
		RawInputBytes:     1000,
		RawInputPositions: 1000000,
		TotalSplits:       10,
		NodeMetrics:       map[string]*NodeMetrics{},
		NodeStageId:       map[string]int{},
		StageCpuSec:       map[int]float64{},
		StageTaskCpuSec:   map[int][]float64{},
	}
}

func byCategory(results []*CategoryResult, cat string) *CategoryResult {
	for _, r := range results {
		if r.Category == cat {
			return r
		}
	}
	return nil
}

// TestClassifyAllCategoriesMapped covers UT-12 baseline: every taxonomy
// category (14 total) is always returned, D2 marked N/A, none elevated on a
// fully quiet baseline.
func TestClassifyAllCategoriesMapped(t *testing.T) {
	th := DefaultThresholds()
	results := Classify(baselineSignals(), th)
	assert.Len(t, results, len(AllCategories))
	for _, cat := range AllCategories {
		r := byCategory(results, cat)
		require.NotNil(t, r, "category %s must always be present", cat)
		if cat == CatLockContention {
			assert.True(t, r.NA)
			assert.Equal(t, -1, r.Severity)
		} else {
			assert.LessOrEqual(t, r.Severity, 1, "category %s should not be elevated on a quiet baseline", cat)
		}
	}
}

// TestScoreA1PlanTime, one synthetic fixture per category (UT-12).
func TestScoreA1PlanTime(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.PlanningSec = 60 // 60/100 = 0.6 >= PlanTimeRatioHigh(0.5)
	r := scoreA1(sig, th)
	assert.Equal(t, 3, r.Severity)
}

func TestScoreA2QueueAdmission(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.QueueSec = 80 // ratio 0.8 >= High(0.5)
	r := scoreA2(sig, th)
	assert.Equal(t, 3, r.Severity)
}

func TestScoreB1CPU(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.CpuSec = 90
	sig.ScheduledSec = 100 // ratio 0.9 >= CpuEfficiencyHigh(0.8)
	r := scoreB1(sig, th)
	assert.Equal(t, 2, r.Severity)
}

func TestScoreB2MemorySpill(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.SpillBytes = 1
	r := scoreB2(sig, th)
	assert.Equal(t, 2, r.Severity)
	assert.Zero(t, scoreB2(baselineSignals(), th).Severity)
}

func TestScoreB3IOThroughput(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.ScanWallSec = 60
	sig.ScheduledSec = 100 // 0.6 >= ScanWallShareHigh(0.5)
	r := scoreB3(sig, th)
	assert.Equal(t, 2, r.Severity)
}

func TestScoreB4StorageLatencyRelevanceGate(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.ScanWallSec = 50
	sig.ScheduledSec = 100 // scan relevance 0.5 >= StageRelevanceShare(0.2): relevant
	sig.ScanWaitSec = 40   // wait share 0.8 >= ScanWaitShareHigh(0.3)
	r := scoreB4(sig, th)
	assert.Equal(t, 2, r.Severity)

	// Same wait share, but scanning itself is a negligible fraction of the
	// query -> gate closes, must NOT elevate (real-data-verified refinement).
	sig2 := baselineSignals()
	sig2.ScanWallSec = 5
	sig2.ScheduledSec = 1000 // relevance 0.005 < 0.2
	sig2.ScanWaitSec = 4
	r2 := scoreB4(sig2, th)
	assert.Zero(t, r2.Severity)
}

// TestScoreB5BoosterSemantics covers UT-13: wall share alone elevates; bytes
// ratio alone does NOT; bytes ratio boosts severity only when wall share is
// already elevated.
func TestScoreB5BoosterSemantics(t *testing.T) {
	th := DefaultThresholds()

	wallShareOnly := baselineSignals()
	wallShareOnly.ExchangeWallSec = 50
	wallShareOnly.ScheduledSec = 100 // 0.5 >= ShuffleShareHigh(0.4)
	wallShareOnly.ShuffledBytes = 100
	wallShareOnly.RawInputBytes = 1000 // ratio 0.1, below booster
	r := scoreB5(wallShareOnly, th)
	assert.Equal(t, 2, r.Severity)

	bytesRatioOnly := baselineSignals()
	bytesRatioOnly.ExchangeWallSec = 1
	bytesRatioOnly.ScheduledSec = 100 // wall share 0.01, not elevated
	bytesRatioOnly.ShuffledBytes = 5000
	bytesRatioOnly.RawInputBytes = 1000 // ratio 5.0, way above booster threshold
	r2 := scoreB5(bytesRatioOnly, th)
	assert.Zero(t, r2.Severity, "bytes ratio alone must never elevate B5")

	both := baselineSignals()
	both.ExchangeWallSec = 50
	both.ScheduledSec = 100
	both.ShuffledBytes = 5000
	both.RawInputBytes = 1000 // ratio 5.0 >= ShuffleBytesPerInputHigh(2.0), booster applies
	r3 := scoreB5(both, th)
	assert.Equal(t, 3, r3.Severity)
	assert.Equal(t, 2, r3.SignalFamilies)
}

func TestScoreC1WaitBackpressure(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.BlockedWallSec = 60
	sig.ScheduledSec = 100
	r := scoreC1(sig, th)
	assert.Equal(t, 2, r.Severity)
}

func TestScoreC2Skew(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.StageCpuSec = map[int]float64{1: 90, 2: 10}
	sig.StageTaskCpuSec = map[int][]float64{
		1: {1, 1, 1, 50}, // median 1, max 50 -> ratio 50, relevant (90% of total CPU)
		2: {5, 5},
	}
	r := scoreC2(sig, th)
	assert.Equal(t, 3, r.Severity)
	assert.Equal(t, "1", r.StageId)
}

// TestScoreC2SkewMinMaxTaskCpuSecGuard is UT-13's round-7 near-zero-
// denominator guard (design.md §5.4.2, SkewMinMaxTaskCpuSec=1.0): the
// max/median ratio is genuine skew and must still fire when the median is
// ~0 under a genuinely HEAVY max (the guard is deliberately on the max, not
// the median/total); but when even that winning stage's own max is itself
// below the floor, the ratio is measuring noise and C2 must degrade to
// severity 0/informational -- exactly the real healthy-fixture shape (ratio
// 21371 from a ~0 median with total CPU 1.77s).
func TestScoreC2SkewMinMaxTaskCpuSecGuard(t *testing.T) {
	th := DefaultThresholds()

	t.Run("max below floor -> severity 0, informational note, even at an enormous ratio", func(t *testing.T) {
		sig := baselineSignals()
		sig.StageCpuSec = map[int]float64{1: 1.769}
		// median ~0.0000283, max 0.605 (< SkewMinMaxTaskCpuSec 1.0) -> ratio
		// ~21371, mirroring the real fixture exactly, but max itself is tiny.
		sig.StageTaskCpuSec = map[int][]float64{1: {0.0000283, 0.0000283, 0.605}}
		r := scoreC2(sig, th)
		assert.Equal(t, 0, r.Severity)
		assert.Contains(t, r.Note, "task CPU too small for meaningful skew")
		assert.Contains(t, r.Note, "informational")
	})

	t.Run("boundary: max exactly at the floor -> NOT gated, normal scoring", func(t *testing.T) {
		sig := baselineSignals()
		sig.StageCpuSec = map[int]float64{1: 90}
		sig.StageTaskCpuSec = map[int][]float64{1: {1, 1, 1, 1.0}} // max == SkewMinMaxTaskCpuSec exactly
		r := scoreC2(sig, th)
		assert.NotContains(t, r.Note, "too small for meaningful skew", "the floor is a strict less-than -- exactly at the floor must compute normally")
	})

	t.Run("heavy max with near-zero median -> genuine skew, still fires (guard is on MAX, never median)", func(t *testing.T) {
		sig := baselineSignals()
		sig.StageCpuSec = map[int]float64{1: 90, 2: 10}
		sig.StageTaskCpuSec = map[int][]float64{
			1: {0.0001, 0.0001, 0.0001, 50}, // median ~0, max 50 (>= floor) -> huge ratio, genuine
			2: {5, 5},
		}
		r := scoreC2(sig, th)
		assert.Equal(t, 3, r.Severity, "a heavy max with a near-zero median must still elevate -- this is real skew, not noise")
		assert.NotContains(t, r.Note, "too small for meaningful skew")
	})

	t.Run("no relevant/qualifying stage at all -> unaffected (severity 0, ordinary zero-ratio note)", func(t *testing.T) {
		sig := baselineSignals()
		r := scoreC2(sig, th)
		assert.Equal(t, 0, r.Severity)
		assert.NotContains(t, r.Note, "too small for meaningful skew", "no candidate stage was ever found -- this is not the guard firing, just the ordinary no-signal baseline")
	})
}

func TestScoreC3SchedulingTinySplitsElevatesUnderParallelismDoesNot(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.RawInputPositions = 100
	sig.TotalSplits = 1000 // avg 0.1 rows/split, way below TinySplitRows -> elevates
	r := scoreC3(sig, th)
	assert.Equal(t, 2, r.Severity)
	assert.Contains(t, r.Note, "core count unknown", "under-parallelism half is informational only and never elevates on its own")

	sig2 := baselineSignals()
	sig2.RawInputPositions = 1_000_000
	sig2.TotalSplits = 1 // huge avg rows/split (under-parallelism symptom) -- must NOT elevate
	r2 := scoreC3(sig2, th)
	assert.Zero(t, r2.Severity)
}

func TestScoreC4OutputResult(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.FinishingSec = 30
	sig.ElapsedSec = 100 // 0.3 >= FinishShareHigh(0.2)
	r := scoreC4(sig, th)
	assert.Equal(t, 2, r.Severity)
}

func TestScoreD1GCNativeAlwaysNoGC(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.NativeRuntime = true
	sig.GcDataAvailable = true
	sig.GcTimeSec = 1000 // even with "GC-looking" data, native must report no-GC with confidence
	sig.ElapsedSec = 100
	r := scoreD1(sig, th)
	assert.Zero(t, r.Severity)
	assert.Contains(t, r.Note, "no GC (native workers)")
}

func TestScoreD1GCJavaElevates(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.NativeRuntime = false
	sig.GcDataAvailable = true
	sig.GcTimeSec = 10
	sig.ElapsedSec = 100 // share 0.1 >= GcShareHigh(0.05)
	r := scoreD1(sig, th)
	assert.Equal(t, 2, r.Severity)
}

func TestScoreD1GCNoData(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.NativeRuntime = false
	sig.GcDataAvailable = false
	r := scoreD1(sig, th)
	assert.Zero(t, r.Severity)
	assert.Contains(t, r.Note, "no GC data available")
}

func TestScoreD2AlwaysNA(t *testing.T) {
	r := scoreD2()
	assert.True(t, r.NA)
	assert.Equal(t, -1, r.Severity)
	assert.Contains(t, r.Note, "N/A")
}

// TestScoreA3EstimateFamilyLiveAvailable covers the estimate family being
// available from any frame (byte-identical live and terminal): LOW confidence +
// missing NDV / all-NaN join stats elevates even with no actual at all.
func TestScoreA3EstimateFamilyLiveAvailable(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.NodeMetrics["77"] = &NodeMetrics{
		PlanNodeId: "77", Confidence: "LOW", IsLowConfidence: true,
		HasJoinStats: true, AllNaNJoinStats: true, MinVariableNDV: nil,
	}
	r := scoreA3(sig, th)
	assert.GreaterOrEqual(t, r.Severity, 1)
	assert.Equal(t, "77", r.NodeId)
	assert.False(t, r.FromMeasuredActual)
}

// TestScoreA3ActualFamilyMotivatingSignature covers the severity-3 "motivating
// signature": fan-out >= FanoutHigh AND (LOW confidence or missing NDV).
func TestScoreA3ActualFamilyMotivatingSignature(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	fanout := 500.0
	in, out := int64(100), int64(50000)
	sig.NodeMetrics["88"] = &NodeMetrics{
		PlanNodeId: "88", Confidence: "LOW", IsLowConfidence: true,
		ActualIn: &in, ActualOut: &out, ActualFanout: &fanout,
		HasJoinStats: true, AllNaNJoinStats: true,
	}
	r := scoreA3(sig, th)
	assert.Equal(t, 3, r.Severity)
	assert.True(t, r.FromMeasuredActual)
	assert.GreaterOrEqual(t, r.SignalFamilies, 2)
}

// TestScoreA3RunawayLiveElevationPath covers the §5.4.2 "A3 live elevation
// path": RunawayComputation fires and the suspect node inside the runaway
// stage carries the estimate-family signature -> severity 3 with NO measured
// actual at all.
func TestScoreA3RunawayLiveElevationPath(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.NodeMetrics["99"] = &NodeMetrics{
		PlanNodeId: "99", Confidence: "LOW", IsLowConfidence: true,
		HasJoinStats: true, AllNaNJoinStats: true,
	}
	sig.Runaway = &RunawaySignal{Fired: true, StageNum: 9, StageShare: 1.0, SuspectNodeId: "99"}
	r := scoreA3(sig, th)
	assert.Equal(t, 3, r.Severity)
	assert.False(t, r.FromMeasuredActual, "the live path must not claim a measured actual")
	assert.Equal(t, "99", r.NodeId)
}

// TestScoreA3RunawayLiveElevationPathSev2WhenStatsPresent is the §10 item
// 11(d) regression test for the live-elevation 3-vs-2 gate: when
// RunawayComputation fires but the suspect node does NOT carry the
// missing-stats signature (estimateBad false -- NDV present, no NaN join
// key), the live A3 elevation must NOT be dropped and must NOT be forced to
// severity 3 -- it fires at severity 2, with the temporal-runaway family
// still standing in as the second corroborating family (families == 2, no
// measured actual claimed).
func TestScoreA3RunawayLiveElevationPathSev2WhenStatsPresent(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.NodeMetrics["99"] = &NodeMetrics{
		PlanNodeId: "99", Confidence: "LOW", IsLowConfidence: true,
		MinVariableNDV: floatPtr(56), // NDV present -- NOT the missing-stats signature
	}
	sig.Runaway = &RunawaySignal{Fired: true, StageNum: 9, StageShare: 1.0, SuspectNodeId: "99"}
	r := scoreA3(sig, th)
	require.Equal(t, "99", r.NodeId, "the runaway candidate must not be dropped just because estimateBad is false")
	assert.Equal(t, 2, r.Severity, "estimateBad false on the suspect node -> severity 2, never dropped, never forced to 3")
	assert.Equal(t, 2, r.SignalFamilies)
	assert.False(t, r.FromMeasuredActual)
}

// joinNodeWithKeyNDV wires up sig.PlanNodes/sig.NodeEstimates with a single
// join-predicate column so nodeHasNaNJoinKeyNDV can resolve it -- the same
// shape TestExtractJoinKeyStats (joinkeys_test.go) uses, needed here to
// isolate the join-key-*specific* NaN-NDV signature from the generic
// "no NDV info at all" (MinVariableNDV == nil) signature.
func joinNodeWithKeyNDV(sig *Signals, nodeId string, ndv float64) {
	sig.PlanNodes = map[string]*plan_node.PlanNode{
		nodeId: {Id: nodeId, Name: "LeftJoin", Identifier: `[("id" = "order_id_52")]`},
	}
	sig.NodeEstimates = map[string]*NodeEstimate{
		nodeId: {VariableStatistics: map[string]plan_node.VariableStatistics{
			"id<bigint>":          {LowValue: 1, HighValue: 100, DistinctValuesCount: plan_node.JsonFloat64(ndv)},
			"order_id_52<bigint>": {LowValue: 314, HighValue: 98319154, DistinctValuesCount: plan_node.JsonFloat64(ndv)},
		}},
	}
}

// TestScoreA3SeverityTieringNaNJoinKeyNDV is UT-13's severity-tiering case 1
// (design.md §5.4.2/§10 item 11(d), taxonomy-owner ruling 2026-07-19): high
// fan-out + the join-key-specific missing-stats signature (distinctValuesCount
// NaN on a join key, generic MinVariableNDV otherwise present) -> severity 3
// ("catastrophic AND stats-fixable").
func TestScoreA3SeverityTieringNaNJoinKeyNDV(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	fanout := th.FanoutHigh + 1
	in, out := int64(1000), int64(float64(1000)*fanout)
	sig.NodeMetrics["301"] = &NodeMetrics{
		PlanNodeId: "301", Confidence: "LOW", IsLowConfidence: true,
		ActualIn: &in, ActualOut: &out, ActualFanout: &fanout,
		MinVariableNDV: floatPtr(56), // present generically -- isolates on the join-key-specific check
	}
	joinNodeWithKeyNDV(sig, "301", math.NaN())
	r := scoreA3(sig, th)
	require.Equal(t, "301", r.NodeId)
	assert.Equal(t, 3, r.Severity, "NaN join-key NDV + high fan-out is the missing-stats signature -> severity 3")
}

// TestScoreA3SeverityTieringNDVPresentAfterFix is UT-13's severity-tiering
// case 2 (the after-NDV-fix shape, §5.4.2 ruling): high fan-out, LOW
// confidence, but join-key NDV is now PRESENT (real NDV path, not the
// default-selectivity fallback) -> severity 2 ("catastrophic but
// inherent/structural"), never 3 -- this is the exact real after-fix shape
// (fan-out 742x, NDV present at 66,258, confident=LOW).
func TestScoreA3SeverityTieringNDVPresentAfterFix(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	fanout := th.FanoutHigh + 1
	in, out := int64(1000), int64(float64(1000)*fanout)
	sig.NodeMetrics["302"] = &NodeMetrics{
		PlanNodeId: "302", Confidence: "LOW", IsLowConfidence: true,
		ActualIn: &in, ActualOut: &out, ActualFanout: &fanout,
		MinVariableNDV: floatPtr(56),
	}
	joinNodeWithKeyNDV(sig, "302", 66258) // join-key NDV present, real value, no NaN anywhere
	r := scoreA3(sig, th)
	require.Equal(t, "302", r.NodeId)
	assert.Equal(t, 2, r.Severity, "NDV present (after-fix shape) + high fan-out -> severity 2, never 3")
}

// TestScoreA3SeverityTieringBareLowNeverSev3 is UT-13's severity-tiering case
// 3: bare confident==LOW plus high fan-out, with NDV fully present (no
// missing-stats signature anywhere, and no join-key data to even check) ->
// severity 2, never 3 -- bare LOW is source-verified quick-stats provenance
// noise, not a sev-3 trigger on its own (§5.4.2).
func TestScoreA3SeverityTieringBareLowNeverSev3(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	fanout := th.FanoutHigh + 1
	in, out := int64(1000), int64(float64(1000)*fanout)
	sig.NodeMetrics["303"] = &NodeMetrics{
		PlanNodeId: "303", Confidence: "LOW", IsLowConfidence: true,
		ActualIn: &in, ActualOut: &out, ActualFanout: &fanout,
		MinVariableNDV: floatPtr(56), // NDV present -- no missing-stats signature at all
	}
	r := scoreA3(sig, th)
	require.Equal(t, "303", r.NodeId)
	assert.Equal(t, 2, r.Severity, "bare confident==LOW is never a sev-3 trigger on its own")
}

// TestScoreA3JoinNodeStatsEstimateNeverInRankedEvidence covers REC-2 (design.md
// §5.4.1 A3, §2, §10 item 11): joinNodeStatsEstimate all-NaN must NEVER appear
// as a ranked-evidence line, even when it is (as here) the ONLY thing distinct
// about the node -- source-verified as an HBO/null-skew field, not a CBO
// cardinality input. It is at most surfaced as a separate, non-causal
// informational line by report.go, outside ranked evidence entirely.
func TestScoreA3JoinNodeStatsEstimateNeverInRankedEvidence(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	fanout := 500.0
	in, out := int64(100), int64(50000)
	sig.NodeMetrics["1772"] = &NodeMetrics{
		PlanNodeId: "1772", Confidence: "LOW", IsLowConfidence: true,
		ActualIn: &in, ActualOut: &out, ActualFanout: &fanout,
		MinVariableNDV: floatPtr(56), // NOT nil -- generic all-variable NDV is present
		HasJoinStats:   true, AllNaNJoinStats: true,
	}
	r := scoreA3(sig, th)
	require.Equal(t, "1772", r.NodeId)
	for _, e := range r.Evidence {
		assert.NotContains(t, e.Text, "joinNodeStatsEstimate", "joinNodeStatsEstimate must never appear in ranked evidence (REC-2)")
	}
}

// TestNodeIdLess covers the final tie-break decider: numeric comparison when
// both ids parse as integers (the real, verified shape of Presto plan-node
// ids), a plain string fallback otherwise -- always a total order.
func TestNodeIdLess(t *testing.T) {
	assert.True(t, nodeIdLess("9", "10"), "numeric comparison: 9 < 10 even though \"10\" < \"9\" lexicographically")
	assert.False(t, nodeIdLess("10", "9"))
	assert.False(t, nodeIdLess("5", "5"))
	assert.True(t, nodeIdLess("abc", "xyz"), "non-numeric ids fall back to string comparison, never panic")
	assert.True(t, nodeIdLess("5", "abc"), "mixed numeric/non-numeric ids still produce a total order via the string fallback")
}

// TestScoreA3DeterministicTieBreak is the regression test for review B1 (a
// pre-existing determinism bug present since 34f101c, not introduced by
// REC-1/2/3): scoreA3 iterates sig.NodeMetrics, a Go map whose iteration
// order is deliberately randomized -- the ORIGINAL tie-break kept whichever
// equally-(severity,families)-scored candidate was encountered first, so the
// reported "where" node id could flip between identical runs on an identical
// input whenever >=2 nodes tied. This drives scoreA3 directly, many times,
// over a fixture with three nodes tied at (severity=2, families=1) but
// different fan-outs plus a genuine (severity,families,fanout) 3-way tie, and
// asserts the SAME winner every time, chosen by the documented total order:
// severity, then families, then fan-out (higher wins), then planNodeId
// (numeric ascending) as the absolute final decider.
func TestScoreA3DeterministicTieBreak(t *testing.T) {
	th := DefaultThresholds()
	buildSig := func() *Signals {
		sig := baselineSignals()
		// Three nodes all reaching severity 2 via fanout>=FanoutHigh alone
		// (no estimateBad corroboration -> exactly one family, "fanout", for
		// all three): tied on (severity, families), broken only by fan-out.
		f1, f2, f3 := th.FanoutHigh+1, th.FanoutHigh+50, th.FanoutHigh+10
		in := int64(1000)
		out1, out2, out3 := int64(float64(in)*f1), int64(float64(in)*f2), int64(float64(in)*f3)
		sig.NodeMetrics["30"] = &NodeMetrics{PlanNodeId: "30", ActualIn: &in, ActualOut: &out1, ActualFanout: &f1}
		sig.NodeMetrics["10"] = &NodeMetrics{PlanNodeId: "10", ActualIn: &in, ActualOut: &out2, ActualFanout: &f2}
		sig.NodeMetrics["20"] = &NodeMetrics{PlanNodeId: "20", ActualIn: &in, ActualOut: &out3, ActualFanout: &f3}
		return sig
	}
	var firstWinner string
	for i := 0; i < 20; i++ {
		r := scoreA3(buildSig(), th)
		if i == 0 {
			firstWinner = r.NodeId
			assert.Equal(t, "10", firstWinner, "node 10 has the largest fan-out among the tied candidates and must win deterministically")
		} else {
			assert.Equal(t, firstWinner, r.NodeId, "the A3 winning node must be identical across repeated runs on the identical input")
		}
		assert.Equal(t, 2, r.Severity)
	}

	// A genuine full tie (identical severity, families, AND fan-out): the
	// final decider is the smallest planNodeId, numerically.
	buildFullTieSig := func() *Signals {
		sig := baselineSignals()
		f := th.FanoutHigh + 1
		in, out := int64(1000), int64(float64(1000)*f)
		for _, id := range []string{"300", "100", "200"} {
			inCopy, outCopy, fCopy := in, out, f
			sig.NodeMetrics[id] = &NodeMetrics{PlanNodeId: id, ActualIn: &inCopy, ActualOut: &outCopy, ActualFanout: &fCopy}
		}
		return sig
	}
	for i := 0; i < 20; i++ {
		r := scoreA3(buildFullTieSig(), th)
		assert.Equal(t, "100", r.NodeId, "on a full tie, the smallest planNodeId (numeric) must win deterministically")
	}
}

func floatPtr(v float64) *float64 { return &v }

func intPtr(v int) *int { return &v }

// TestAttributeRootOverSymptom covers UT-13: A3+B1+B5+C1 all elevated -> root
// = A3, others attributed with reasons (the root-over-symptom proof).
func TestAttributeRootOverSymptom(t *testing.T) {
	th := DefaultThresholds()
	a3 := newResult(CatPlanPath, 3, "plan path")
	a3.NodeId = "1"
	a3.StageId = "9"
	b1 := newResult(CatCPU, 2, "cpu")
	b5 := newResult(CatNetworkShuffle, 2, "shuffle")
	c1 := newResult(CatWaitBackpressure, 2, "wait")
	all := []*CategoryResult{a3, b1, b5, c1, scoreD2()}
	sig := baselineSignals()
	sig.StageCpuSec = map[int]float64{9: 90}

	root, independents, attributed := Attribute(all, sig, th)
	require.NotNil(t, root)
	assert.Equal(t, CatPlanPath, root.Category)
	assert.Empty(t, independents)
	assert.Len(t, attributed, 3)
	for _, a := range attributed {
		assert.Equal(t, CatPlanPath, a.AttributedTo)
		assert.NotEmpty(t, a.AttributionReason)
	}
}

// TestAttributeIndependentCoCause covers the "queue-dominant + bad-plan"
// worked example (§5.5): A2 (upstream of A3 in CausalOrder) elevated at
// severity >= RootSeverityMin becomes the root; the A3 finding has no
// attribution rule FROM A2, so it is reported as an independent finding, not
// forced under the queue verdict.
func TestAttributeIndependentCoCause(t *testing.T) {
	th := DefaultThresholds()
	a2 := newResult(CatQueueAdmission, 3, "queue dominant")
	a3 := newResult(CatPlanPath, 3, "bad plan")
	root, independents, attributed := Attribute([]*CategoryResult{a2, a3}, baselineSignals(), th)
	require.NotNil(t, root)
	assert.Equal(t, CatQueueAdmission, root.Category)
	require.Len(t, independents, 1)
	assert.Equal(t, CatPlanPath, independents[0].Category)
	assert.Empty(t, attributed)
}

// TestAttributeFallbackAndNoDominant covers the max-severity-<2 fallback and
// the "nothing elevated at all" NO DOMINANT BOTTLENECK case.
func TestAttributeFallbackAndNoDominant(t *testing.T) {
	th := DefaultThresholds()
	weak := newResult(CatCPU, 1, "mildly elevated")
	root, _, _ := Attribute([]*CategoryResult{weak}, baselineSignals(), th)
	require.NotNil(t, root)
	assert.Equal(t, CatCPU, root.Category)

	none := newResult(CatCPU, 0, "quiet")
	root2, ind2, att2 := Attribute([]*CategoryResult{none}, baselineSignals(), th)
	assert.Nil(t, root2)
	assert.Nil(t, ind2)
	assert.Nil(t, att2)
}

// TestComputeConfidenceLevels covers §5.4.3's rules, including the
// snapshot-only MEDIUM cap and the independent-co-cause MEDIUM cap.
func TestComputeConfidenceLevels(t *testing.T) {
	high := newResult(CatPlanPath, 3, "")
	high.SignalFamilies = 2
	high.FromMeasuredActual = true
	c := ComputeConfidence(high, nil, false)
	assert.Equal(t, "HIGH", c.Level)

	// Same root, but derived from snapshots only (no measured actual) -> capped
	// at MEDIUM even though severity/families would otherwise reach HIGH.
	liveOnly := newResult(CatPlanPath, 3, "")
	liveOnly.SignalFamilies = 2
	liveOnly.FromMeasuredActual = false
	c2 := ComputeConfidence(liveOnly, nil, true)
	assert.Equal(t, "MEDIUM", c2.Level)

	// Independent severity-2+ finding caps at MEDIUM even with a strong root.
	indep := newResult(CatQueueAdmission, 2, "")
	c3 := ComputeConfidence(high, []*CategoryResult{indep}, false)
	assert.Equal(t, "MEDIUM", c3.Level)

	fallback := newResult(CatCPU, 1, "")
	c4 := ComputeConfidence(fallback, nil, false)
	assert.Equal(t, "LOW", c4.Level)

	c5 := ComputeConfidence(nil, nil, false)
	assert.Equal(t, "LOW", c5.Level)

	// Category-specific forced-LOW gaps (§5.4.3: needed reference value absent).
	mem := newResult(CatMemorySpill, 3, "")
	mem.SignalFamilies = 2
	mem.FromMeasuredActual = true
	c6 := ComputeConfidence(mem, nil, false)
	assert.Equal(t, "LOW", c6.Level)

	out := newResult(CatOutputResult, 3, "")
	out.SignalFamilies = 2
	out.FromMeasuredActual = true
	c7 := ComputeConfidence(out, nil, false)
	assert.Equal(t, "LOW", c7.Level)
}

// TestComputeConfidenceSev2MeasuredActualHighGate is UT-13's synthetic
// confidence fixture for §10 item 11(d)/§5.4.3's decoupling ruling: severity
// grades fixability, confidence grades certainty, so a root severity 2 backed
// by a measured actual satisfies the same certainty bar as severity 3 and can
// reach HIGH -- but ONLY when no severity-2+ independent finding coexists
// (pins both sides of the cap, per the coordinator's explicit request: the
// same fixture with vs. without a coexisting independent).
func TestComputeConfidenceSev2MeasuredActualHighGate(t *testing.T) {
	sev2Measured := newResult(CatPlanPath, 2, "")
	sev2Measured.SignalFamilies = 2
	sev2Measured.FromMeasuredActual = true

	// No coexisting sev-2+ independent finding -> HIGH.
	c := ComputeConfidence(sev2Measured, nil, false)
	assert.Equal(t, "HIGH", c.Level, "sev 2 + measured actual + 2 families + no sev-2+ independent must reach HIGH")
	assert.Contains(t, c.Reasons, "root severity 2 with the measured-actual family present")

	// The SAME fixture, but with a coexisting severity-2 independent finding
	// (as the real after-fix run has, §5.5 third worked example) -> capped at
	// MEDIUM; the cap is unchanged by this gate.
	indep := newResult(CatIOThroughput, 2, "")
	c2 := ComputeConfidence(sev2Measured, []*CategoryResult{indep}, false)
	assert.Equal(t, "MEDIUM", c2.Level, "a coexisting sev-2+ independent finding must still cap sev-2-measured at MEDIUM")

	// Sanity: sev 2 WITHOUT a measured actual must NOT reach HIGH (only sev 3,
	// or sev 2 + measured, may) -- the pre-existing MEDIUM ceiling for a plain
	// live/estimate-only sev-2 root is unchanged by this gate.
	sev2Live := newResult(CatPlanPath, 2, "")
	sev2Live.SignalFamilies = 2
	sev2Live.FromMeasuredActual = false
	c3 := ComputeConfidence(sev2Live, nil, false)
	assert.Equal(t, "MEDIUM", c3.Level, "sev 2 without a measured actual must not reach HIGH")
}

// idleDriverSignals builds a Signals value shaped exactly like the real
// round-7 healthy fixture (state FINISHED, cpu/scheduled 0.002, 3050 idle
// driver slots) with C1/B3/B4/C3/B5 all pre-elevated by their own ordinary
// scoring rules, so the idle-driver gate has something real to suppress.
func idleDriverSignals() *Signals {
	sig := baselineSignals()
	sig.State = "FINISHED"
	sig.ElapsedSec = 18.47
	sig.ScheduledSec = 755.4
	sig.CpuSec = 1.77 // cpu/scheduled = 0.00234, well below IdleDriverCpuScheduledMax
	sig.BlockedWallSec = 197856
	sig.TotalDrivers = 3050
	sig.ScanWallSec = 753.611 // share vs scheduled = 0.998 >= ScanWallShareHigh -> B3 elevates
	sig.ScanWaitSec = 855.272 // share vs scan wall > ScanWaitShareHigh -> B4 elevates
	sig.RawInputPositions = 2739122
	sig.TotalSplits = 3050 // avg rows/split 898 -- NOT tiny, so C3's OWN tiny-splits rule does not elevate...
	sig.ExchangeWallSec = 400
	sig.ShuffledBytes = 500 // B5 elevated too, to prove the gate excludes it
	sig.RawInputBytes = 100
	// Deliberately disqualify the (broader) healthy predicate so these tests
	// isolate the idle-driver gate's own narrower C1/B3/B4/C3-only scope --
	// otherwise cpu=1.77s alone would also satisfy HealthyCpuFloorSec and the
	// healthy predicate's separate, broader net would zero B5 too, masking
	// exactly the distinction this test exists to prove.
	sig.PeakUserMemoryBytes = 999999999
	return sig
}

// TestApplyHealthyQueryGuardsIdleDriverGate covers UT-13's round-7 idle-
// driver gate (design.md §5.4.2, IdleDriverCpuScheduledMax=0.05): fires
// EXACTLY on C1/B3/B4/C3 when state=="FINISHED" AND cpu/scheduled is below
// the floor; never on B5; never on a non-FINISHED state (even at an
// extremely low ratio); never when the ratio is not actually below the
// floor.
func TestApplyHealthyQueryGuardsIdleDriverGate(t *testing.T) {
	th := DefaultThresholds()

	t.Run("positive: FINISHED + cpu/scheduled 0.002 -> C1/B3/B4/C3 forced to severity 0 with idle-driver-artifact notes; B5 untouched", func(t *testing.T) {
		sig := idleDriverSignals()
		categories := Classify(sig, th)
		byCat := byCategoryMap(categories)
		require.Greater(t, byCat[CatWaitBackpressure].Severity, 0, "precondition: C1 must be genuinely elevated before the gate runs")
		require.Greater(t, byCat[CatIOThroughput].Severity, 0, "precondition: B3 must be genuinely elevated before the gate runs")
		require.Greater(t, byCat[CatStorageLatency].Severity, 0, "precondition: B4 must be genuinely elevated before the gate runs")
		require.Greater(t, byCat[CatNetworkShuffle].Severity, 0, "precondition: B5 must be genuinely elevated before the gate runs (to prove it survives)")

		healthy := ApplyHealthyQueryGuards(categories, sig, th)
		assert.False(t, healthy, "this fixture deliberately fails the healthy predicate (peak memory set far above the floor) to isolate the idle-driver gate's own narrower scope")

		assert.Equal(t, 0, byCat[CatWaitBackpressure].Severity)
		assert.Contains(t, byCat[CatWaitBackpressure].Note, "idle-driver artifact")
		assert.Equal(t, 0, byCat[CatIOThroughput].Severity)
		assert.Contains(t, byCat[CatIOThroughput].Note, "idle-driver artifact")
		assert.Equal(t, 0, byCat[CatStorageLatency].Severity)
		assert.Equal(t, "idle-driver artifact — informational", byCat[CatStorageLatency].Note)
		assert.Equal(t, 0, byCat[CatScheduling].Severity)
		assert.Contains(t, byCat[CatScheduling].Note, "idle-driver artifact")

		assert.Greater(t, byCat[CatNetworkShuffle].Severity, 0, "B5 must NEVER be gated by the idle-driver rule -- its exchange busy-wall signal is not idle-inflated the same way")
	})

	t.Run("negative: FAILED state + cpu/scheduled 0.002 -> NOT gated (a FAILED query's idleness can be the symptom)", func(t *testing.T) {
		sig := idleDriverSignals()
		sig.State = "FAILED"
		categories := Classify(sig, th)
		byCat := byCategoryMap(categories)
		before := byCat[CatWaitBackpressure].Severity
		require.Greater(t, before, 0)
		ApplyHealthyQueryGuards(categories, sig, th)
		assert.Equal(t, before, byCat[CatWaitBackpressure].Severity, "FAILED must never be gated, even at an extremely low cpu/scheduled ratio")
		assert.NotContains(t, byCat[CatWaitBackpressure].Note, "idle-driver artifact")
	})

	t.Run("negative: RUNNING (live frame) state + cpu/scheduled 0.002 -> NOT gated", func(t *testing.T) {
		sig := idleDriverSignals()
		sig.State = "RUNNING"
		categories := Classify(sig, th)
		byCat := byCategoryMap(categories)
		before := byCat[CatIOThroughput].Severity
		require.Greater(t, before, 0)
		ApplyHealthyQueryGuards(categories, sig, th)
		assert.Equal(t, before, byCat[CatIOThroughput].Severity, "a live/RUNNING frame must never be gated")
	})

	t.Run("negative: FINISHED + cpu/scheduled 0.424 (well above the floor) -> NOT gated", func(t *testing.T) {
		sig := idleDriverSignals()
		sig.State = "FINISHED"
		sig.CpuSec = 0.424 * sig.ScheduledSec // exactly at the design's real drr9h-shaped ratio, well above 0.05
		categories := Classify(sig, th)
		byCat := byCategoryMap(categories)
		before := byCat[CatStorageLatency].Severity
		require.Greater(t, before, 0)
		ApplyHealthyQueryGuards(categories, sig, th)
		assert.Equal(t, before, byCat[CatStorageLatency].Severity, "a normal (non-idle) cpu/scheduled ratio must never be gated")
	})

	t.Run("boundary: cpu/scheduled exactly at the floor -> NOT gated (strict less-than)", func(t *testing.T) {
		sig := idleDriverSignals()
		sig.CpuSec = th.IdleDriverCpuScheduledMax * sig.ScheduledSec // ratio == floor exactly
		categories := Classify(sig, th)
		byCat := byCategoryMap(categories)
		before := byCat[CatWaitBackpressure].Severity
		ApplyHealthyQueryGuards(categories, sig, th)
		assert.Equal(t, before, byCat[CatWaitBackpressure].Severity, "exactly at the floor must not be gated -- the condition is a strict less-than")
	})
}

// byCategoryMap indexes a Classify() result by category code, for the
// round-7 guard tests above.
func byCategoryMap(categories []*CategoryResult) map[string]*CategoryResult {
	m := map[string]*CategoryResult{}
	for _, c := range categories {
		m[c.Category] = c
	}
	return m
}

// TestApplyHealthyQueryGuardsHealthyPredicate covers UT-13's round-7 healthy
// predicate (design.md §5.4.2, HealthyCpuFloorSec=10s/HealthyMemFloorBytes=
// 256MB): boundary edges on both floors, FINISHED-only, and the BROADER net
// (every remaining elevated B*/C* category, not just C1/B3/B4/C3) -- proven
// via B5, which the idle-driver gate never touches but the healthy predicate
// still must, and via A1/A2/A3 staying completely untouched.
func TestApplyHealthyQueryGuardsHealthyPredicate(t *testing.T) {
	th := DefaultThresholds()

	healthySig := func() *Signals {
		sig := baselineSignals()
		sig.State = "FINISHED"
		sig.ElapsedSec = 18.47
		sig.CpuSec = 1.77
		sig.ScheduledSec = 100 // a NORMAL, non-idle ratio (1.77/100 = 0.0177 -- still below 0.05, so pick a bigger ScheduledSec to isolate the healthy-only path)
		sig.PeakUserMemoryBytes = 393216
		sig.PlanningSec = 7.13 // A1 stays elevated -- healthy must never touch A-group
		return sig
	}

	t.Run("positive: FINISHED + cpu<=floor + mem<=floor -> healthy true", func(t *testing.T) {
		sig := healthySig()
		categories := Classify(sig, th)
		assert.True(t, ApplyHealthyQueryGuards(categories, sig, th))
	})

	t.Run("boundary: cpu exactly at HealthyCpuFloorSec, mem exactly at HealthyMemFloorBytes -> still healthy", func(t *testing.T) {
		sig := healthySig()
		sig.CpuSec = th.HealthyCpuFloorSec
		sig.PeakUserMemoryBytes = th.HealthyMemFloorBytes
		categories := Classify(sig, th)
		assert.True(t, ApplyHealthyQueryGuards(categories, sig, th), "the predicate is <=, so exactly at either floor is still healthy")
	})

	t.Run("negative: cpu just above the floor -> not healthy", func(t *testing.T) {
		sig := healthySig()
		sig.CpuSec = th.HealthyCpuFloorSec + 0.001
		categories := Classify(sig, th)
		assert.False(t, ApplyHealthyQueryGuards(categories, sig, th))
	})

	t.Run("negative: mem just above the floor -> not healthy", func(t *testing.T) {
		sig := healthySig()
		sig.PeakUserMemoryBytes = th.HealthyMemFloorBytes + 1
		categories := Classify(sig, th)
		assert.False(t, ApplyHealthyQueryGuards(categories, sig, th))
	})

	t.Run("negative: not FINISHED -> never healthy, regardless of cpu/mem", func(t *testing.T) {
		sig := healthySig()
		sig.State = "FAILED"
		categories := Classify(sig, th)
		assert.False(t, ApplyHealthyQueryGuards(categories, sig, th))
	})

	t.Run("healthy suppresses EVERY remaining elevated B*/C* category, a broader net than the idle-driver gate alone (B5 proof)", func(t *testing.T) {
		sig := healthySig()
		sig.ExchangeWallSec = 60 // wallShare 60/... -- force B5 elevated on a NORMAL (non-idle) ratio the idle-driver gate would never touch
		sig.ScheduledSec = 100
		categories := Classify(sig, th)
		byCat := byCategoryMap(categories)
		require.Greater(t, byCat[CatNetworkShuffle].Severity, 0, "precondition: B5 must be genuinely elevated")

		healthy := ApplyHealthyQueryGuards(categories, sig, th)
		require.True(t, healthy)
		assert.Equal(t, 0, byCat[CatNetworkShuffle].Severity, "the healthy predicate's net covers EVERY B*/C* category, including B5 which the idle-driver gate deliberately excludes")
		assert.Contains(t, byCat[CatNetworkShuffle].Note, "informational")
	})

	t.Run("healthy never touches A1/A2/A3 (pre-execution) -- the A3 sev-1 finding survives", func(t *testing.T) {
		sig := healthySig()
		sig.QueueSec = 20 // A2 elevated too
		categories := Classify(sig, th)
		byCat := byCategoryMap(categories)
		beforeA1, beforeA2, beforeA3 := byCat[CatPlanTime].Severity, byCat[CatQueueAdmission].Severity, byCat[CatPlanPath].Severity
		require.Greater(t, beforeA1, 0)

		healthy := ApplyHealthyQueryGuards(categories, sig, th)
		require.True(t, healthy)
		assert.Equal(t, beforeA1, byCat[CatPlanTime].Severity, "A1 must be completely untouched by the healthy predicate")
		assert.Equal(t, beforeA2, byCat[CatQueueAdmission].Severity, "A2 must be completely untouched")
		assert.Equal(t, beforeA3, byCat[CatPlanPath].Severity, "A3 must be completely untouched")
	})

	t.Run("already-zero categories are left alone (note untouched, no double-suffix)", func(t *testing.T) {
		sig := healthySig()
		categories := Classify(sig, th)
		byCat := byCategoryMap(categories)
		originalB2Note := byCat[CatMemorySpill].Note
		require.Equal(t, 0, byCat[CatMemorySpill].Severity, "precondition: B2 is not elevated on this fixture")

		ApplyHealthyQueryGuards(categories, sig, th)
		assert.Equal(t, originalB2Note, byCat[CatMemorySpill].Note, "a category that was never elevated must not be rewritten")
	})
}

// TestApplyHealthyQueryGuardsFourFixtureInvariance is UT-13's four-fixture
// invariance replay (design.md §10 item 16): the four locked pathological
// acceptance signatures (before-fix/muisn-shaped, terminal-shaped, drr9h
// after-fix-shaped, and the RUNNING snapshot-only shape) are all either
// non-FINISHED or comfortably outside both the idle-driver and healthy
// floors -- ApplyHealthyQueryGuards must be a complete no-op on every one of
// them (healthy==false, zero category mutated). The real end-to-end
// re-verification against the actual sample files themselves is FT-15b
// (analyze_test.go).
func TestApplyHealthyQueryGuardsFourFixtureInvariance(t *testing.T) {
	th := DefaultThresholds()
	fixtures := []struct {
		name             string
		state            string
		cpuSec, schedSec float64
	}{
		{"muisn/before-fix-shaped (FAILED)", "FAILED", 8424, 8424 / 0.804},
		{"terminal-shaped (FAILED)", "FAILED", 8460, 8460 / 0.816},
		{"drr9h after-fix-shaped (FAILED)", "FAILED", 8316, 8316 / 0.424},
		{"snapshot-only-shaped (RUNNING)", "RUNNING", 100, 100 / 0.5},
	}
	for _, f := range fixtures {
		t.Run(f.name, func(t *testing.T) {
			sig := baselineSignals()
			sig.State = f.state
			sig.CpuSec = f.cpuSec
			sig.ScheduledSec = f.schedSec
			sig.ElapsedSec = f.cpuSec // any positive, non-gating value
			sig.StageCpuSec = map[int]float64{9: f.cpuSec}
			sig.StageTaskCpuSec = map[int][]float64{9: {f.cpuSec / 4, f.cpuSec / 4, f.cpuSec / 2}} // real max well above SkewMinMaxTaskCpuSec
			categories := Classify(sig, th)
			snapshot := make(map[string]int, len(categories))
			notes := make(map[string]string, len(categories))
			for _, c := range categories {
				snapshot[c.Category] = c.Severity
				notes[c.Category] = c.Note
			}

			healthy := ApplyHealthyQueryGuards(categories, sig, th)
			assert.False(t, healthy, "none of the four locked acceptance shapes are healthy (all FAILED/RUNNING, or CPU far above the healthy floor)")
			for _, c := range categories {
				assert.Equal(t, snapshot[c.Category], c.Severity, "category %s severity must be unchanged", c.Category)
				assert.Equal(t, notes[c.Category], c.Note, "category %s note must be unchanged", c.Category)
			}
		})
	}
}

// TestThresholdsOverrideChangesOutcome covers --thresholds: a JSON override
// file changes classification deterministically.
func TestThresholdsOverrideChangesOutcome(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "thresholds.json")
	require.NoError(t, os.WriteFile(path, []byte(`{"PlanTimeRatioWarn": 0.01, "PlanTimeRatioHigh": 0.02}`), 0644))
	th, err := LoadThresholds(path)
	require.NoError(t, err)
	sig := baselineSignals()
	sig.PlanningSec = 5 // ratio 0.05, would not elevate under defaults (0.3/0.5)
	r := scoreA1(sig, th)
	assert.Equal(t, 3, r.Severity, "overridden thresholds must change the outcome")

	// A blank path returns the built-in defaults unchanged.
	def, err := LoadThresholds("")
	require.NoError(t, err)
	assert.Equal(t, DefaultThresholds().PlanTimeRatioWarn, def.PlanTimeRatioWarn)

	_, err = LoadThresholds(filepath.Join(dir, "missing.json"))
	assert.Error(t, err)
}

// TestBroadcastLarge covers the join-distribution booster signal.
func TestBroadcastLarge(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	sig.PlanNodes = map[string]*plan_node.PlanNode{
		"5": {Id: "5", Name: "InnerJoin", Identifier: `[REPLICATED]("a" = "b")`},
	}
	sig.NodeCosts = map[string]*NodeCost{"5": {MaxMemory: 200 * 1024 * 1024}}
	assert.True(t, broadcastLarge("5", sig, th))

	// Not a join -> never broadcast.
	sig2 := baselineSignals()
	assert.False(t, broadcastLarge("missing", sig2, th))
}
