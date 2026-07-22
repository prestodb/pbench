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
	"encoding/json"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"pbench/prestoapi/plan_node"
)

// TestRecommendationLookupTable covers §5.1.1: recommendations come only from
// the fixed lookup table keyed by triggering signal; a table miss returns "".
func TestRecommendationLookupTable(t *testing.T) {
	th := DefaultThresholds()

	t.Run("A3 missing NDV -> ANALYZE", func(t *testing.T) {
		sig := baselineSignals()
		sig.NodeMetrics["1"] = &NodeMetrics{
			PlanNodeId: "1", Confidence: "LOW", IsLowConfidence: true,
			JoinKeyStats: []JoinKeyStat{{Column: "order_id_52", Min: 314, Max: 98319154, NDV: plan_node.JsonFloat64(nanValue())}},
		}
		root := newResult(CatPlanPath, 3, "")
		root.NodeId = "1"
		got := recommendationFor(root, sig, nil, th)
		assert.Contains(t, got, "ANALYZE")
	})

	// REC-1 (round 2): present join-key NDV + large fan-out -> the fixed
	// inherent-fan-out/chasm-trap template, never ANALYZE (round-2 validated:
	// after the engine NDV fix, keys had real NDVs yet fan-out stayed ~742x).
	t.Run("A3 present NDV + large fan-out -> chasm-trap", func(t *testing.T) {
		sig := baselineSignals()
		fanout := th.FanoutHigh + 1
		sig.NodeMetrics["1"] = &NodeMetrics{
			PlanNodeId: "1", Confidence: "LOW", IsLowConfidence: true, ActualFanout: &fanout,
			JoinKeyStats: []JoinKeyStat{{Column: "order_id_52", Min: 314, Max: 98319154, NDV: 64352}},
		}
		root := newResult(CatPlanPath, 3, "")
		root.NodeId = "1"
		got := recommendationFor(root, sig, nil, th)
		assert.Contains(t, got, "chasm-trap")
		assert.Contains(t, got, "pre-aggregate each detail table")
		assert.NotContains(t, got, "ANALYZE")
	})

	t.Run("A3 present NDV but fan-out not large -> table miss, no recommendation", func(t *testing.T) {
		sig := baselineSignals()
		fanout := th.FanoutModerate
		sig.NodeMetrics["1"] = &NodeMetrics{
			PlanNodeId: "1", Confidence: "LOW", IsLowConfidence: true, ActualFanout: &fanout,
			JoinKeyStats: []JoinKeyStat{{Column: "order_id_52", Min: 314, Max: 98319154, NDV: 64352}},
		}
		root := newResult(CatPlanPath, 3, "")
		root.NodeId = "1"
		assert.Equal(t, "", recommendationFor(root, sig, nil, th))
	})

	t.Run("A3 broadcast -> distribution", func(t *testing.T) {
		sig := baselineSignals()
		sig.PlanNodes = map[string]*plan_node.PlanNode{
			"2": {Id: "2", Name: "InnerJoin", Identifier: `[REPLICATED]("a" = "b")`},
		}
		sig.NodeCosts = map[string]*NodeCost{"2": {MaxMemory: 200 * 1024 * 1024}}
		root := newResult(CatPlanPath, 3, "")
		root.NodeId = "2"
		got := recommendationFor(root, sig, nil, th)
		assert.Contains(t, got, "broadcast")
	})

	t.Run("A3 via RunawayComputation -> runaway template", func(t *testing.T) {
		sig := baselineSignals()
		root := newResult(CatPlanPath, 3, "")
		root.NodeId = "5"
		root.FromMeasuredActual = false
		runaway := &RunawaySignal{Fired: true, StageNum: 9, SuspectNodeId: "5"}
		got := recommendationFor(root, sig, runaway, th)
		assert.Contains(t, got, "runaway computation in stage S9")
	})

	t.Run("A2 queue -> resource-group", func(t *testing.T) {
		root := newResult(CatQueueAdmission, 3, "")
		got := recommendationFor(root, baselineSignals(), nil, th)
		assert.Contains(t, got, "resource-group")
	})

	t.Run("B2 with spill -> memory pressure", func(t *testing.T) {
		sig := baselineSignals()
		sig.SpillBytes = 100
		root := newResult(CatMemorySpill, 2, "")
		assert.Contains(t, recommendationFor(root, sig, nil, th), "memory pressure with spill")
	})

	t.Run("B2 without spill -> table miss, no recommendation", func(t *testing.T) {
		root := newResult(CatMemorySpill, 2, "")
		assert.Equal(t, "", recommendationFor(root, baselineSignals(), nil, th))
	})

	t.Run("B5 -> join distribution", func(t *testing.T) {
		root := newResult(CatNetworkShuffle, 2, "")
		assert.Contains(t, recommendationFor(root, baselineSignals(), nil, th), "join distribution")
	})

	t.Run("C2 skew -> partition key distribution", func(t *testing.T) {
		root := newResult(CatSkew, 2, "")
		assert.Contains(t, recommendationFor(root, baselineSignals(), nil, th), "partition/join key distribution")
	})

	t.Run("C3 tiny splits -> splits too small", func(t *testing.T) {
		sig := baselineSignals()
		sig.RawInputPositions = 10
		sig.TotalSplits = 1000
		root := newResult(CatScheduling, 2, "")
		assert.Contains(t, recommendationFor(root, sig, nil, th), "splits too small")
	})

	t.Run("C3 without tiny splits -> table miss", func(t *testing.T) {
		root := newResult(CatScheduling, 2, "")
		assert.Equal(t, "", recommendationFor(root, baselineSignals(), nil, th))
	})

	t.Run("any other category -> NONE", func(t *testing.T) {
		root := newResult(CatCPU, 2, "")
		assert.Equal(t, "", recommendationFor(root, baselineSignals(), nil, th))
	})
}

// TestFanoutChainDimensionNonUniqueHintOnlyOnFlaggedRowsVerdictUnchanged is
// UT-14's round-5 regression test (design.md §5.1.1's new per-row template,
// §5.4.1 A3(iii), §10 item 14): the dimensionNonUnique hint renders VERBATIM
// on a flagged row and ONLY that row -- a detail1toN row and a row with no
// buildKind at all must render clean, hint-free cells -- and the verdict's
// OWN recommendation (the two-way NaN-NDV/chasm-trap branch, unrelated to any
// per-row flag) is completely unaffected by any chain row's buildKind.
func TestFanoutChainDimensionNonUniqueHintOnlyOnFlaggedRowsVerdictUnchanged(t *testing.T) {
	ndvDim, ndvDetail := 115.0, 64352.0
	rowsDim, rowsDetail := int64(1792), int64(96_043_860)
	dupDim, dupDetail := 15.58, 1492.48
	rowsEmpty, ndvEmpty := int64(0), 126.0
	chain := []FanoutChainNode{
		{PlanNodeId: "n-detail", Operator: "HashBuilderOperator+LookupJoinOperator", In: 1, Out: 1, Fanout: 1,
			Build: "s.detail_table", JoinKeys: []string{"id=order_id"},
			BuildRows: &rowsDetail, BuildRowsSource: "measured", BuildKeyNdv: &ndvDetail, BuildKeyDupFactor: &dupDetail, BuildKind: "detail1toN"},
		{PlanNodeId: "n-dim", Operator: "HashBuilderOperator+LookupJoinOperator", In: 1, Out: 1, Fanout: 1,
			Build: "s.dim_table", JoinKeys: []string{"city_id=id"},
			BuildRows: &rowsDim, BuildRowsSource: "measured", BuildKeyNdv: &ndvDim, BuildKeyDupFactor: &dupDim, BuildKind: "dimensionNonUnique"},
		{PlanNodeId: "n-unknown", Operator: "HashBuilderOperator+LookupJoinOperator", In: 1, Out: 1, Fanout: 1,
			Build: "unknown", JoinKeys: []string{}},
		{PlanNodeId: "n-dead", Operator: "HashBuilderOperator+LookupJoinOperator", In: 1, Out: 1, Fanout: 1,
			Build: "s.country_dim", JoinKeys: []string{"country_code=code"},
			BuildRows: &rowsEmpty, BuildRowsSource: "measured", BuildKeyNdv: &ndvEmpty, BuildKind: "emptyBuild", DeadJoin: true},
	}
	table := renderFanoutChainTable(chain, nil)

	// The hint must appear EXACTLY once, verbatim, and only on the n-dim row.
	assert.Equal(t, 1, strings.Count(table, dimensionNonUniqueHint), "the hint string must render exactly once, only on the flagged row")
	assert.Equal(t, 1, strings.Count(table, deadJoinHint), "the deadJoin hint string must render exactly once, only on the flagged row")
	lines := strings.Split(table, "\n")
	var detailLine, dimLine, unknownLine, deadLine string
	for _, l := range lines {
		switch {
		case strings.Contains(l, "n-detail"):
			detailLine = l
		case strings.Contains(l, "n-dim"):
			dimLine = l
		case strings.Contains(l, "n-unknown"):
			unknownLine = l
		case strings.Contains(l, "n-dead"):
			deadLine = l
		}
	}
	assert.Contains(t, dimLine, dimensionNonUniqueHint)
	assert.Contains(t, dimLine, "⚠")
	assert.NotContains(t, dimLine, deadJoinHint, "a dimensionNonUnique-only row must not also carry the deadJoin hint")
	assert.NotContains(t, detailLine, dimensionNonUniqueHint, "a detail1toN row must never carry the dimension-dedup hint")
	assert.NotContains(t, detailLine, "⚠")
	assert.NotContains(t, unknownLine, dimensionNonUniqueHint, "a row with no buildKind at all must never carry the hint")
	assert.NotContains(t, unknownLine, deadJoinHint)
	assert.Contains(t, deadLine, "emptyBuild", "the factual emptyBuild label must still render")
	assert.Contains(t, deadLine, deadJoinHint)
	assert.NotContains(t, deadLine, dimensionNonUniqueHint, "an emptyBuild row must never carry the unrelated dimension-dedup hint")

	// The verdict's own recommendation is a completely separate lookup keyed
	// on the ROOT's NDV-present/NaN branch -- it must render unchanged
	// regardless of any chain row's buildKind flag.
	report := &Report{
		QueryId: "q1", State: "FAILED",
		Verdict: &Verdict{
			Category: CatPlanPath, CategoryName: "Plan path", NodeId: "n-detail",
			Summary:        "node n-detail: plan-path signature (severity 2)",
			Recommendation: "inherent join fan-out (possible Looker symmetric-aggregate / chasm-trap): pre-aggregate each detail table to one row per join key before joining, and apply the filter to all join inputs, not just one.",
			Confidence:     Confidence{Level: "MEDIUM"},
		},
		Categories: Classify(baselineSignals(), DefaultThresholds()),
		Evidence:   []EvidenceItem{{Text: table}},
	}
	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "Recommendation: inherent join fan-out")
	assert.NotContains(t, md, "Recommendation: "+dimensionNonUniqueHint, "the per-row hint must never leak into the verdict's own recommendation line")
	assert.Contains(t, md, dimensionNonUniqueHint, "the hint still renders somewhere -- inside the chain table evidence, not the verdict")
}

// TestJoinNodeStatsEstimateInformationalNoteOutsideEvidence covers REC-2
// (design.md §5.4.1 A3, §2, §10 item 11): joinNodeStatsEstimate all-NaN is
// rendered ONLY as a standalone informational line -- never inside the
// ranked evidence list, never in JSON evidence[] -- and only for the A3
// verdict's own node.
func TestJoinNodeStatsEstimateInformationalNoteOutsideEvidence(t *testing.T) {
	report := &Report{
		QueryId: "q1", State: "FAILED",
		Verdict:  &Verdict{Category: CatPlanPath, CategoryName: "Plan path", NodeId: "1772", Confidence: Confidence{Level: "HIGH"}},
		Evidence: []EvidenceItem{{Text: "node 1772: actual fan-out out/in = 4.18e+11/5.59e+08 = 746.4x"}},
		NodeMetrics: []*NodeMetrics{
			{PlanNodeId: "1772", HasJoinStats: true, AllNaNJoinStats: true},
			{PlanNodeId: "999", HasJoinStats: true, AllNaNJoinStats: false}, // a different node -- must not leak in
		},
	}
	note := report.joinNodeStatsEstimateInformationalNote()
	assert.Contains(t, note, "joinNodeStatsEstimate all NaN")
	assert.Contains(t, note, "HBO")
	assert.Contains(t, note, "Informational (not root-cause evidence)")

	md := report.RenderMarkdown(5)
	verdictEnd := strings.Index(md, "## Root-cause evidence")
	findingsStart := strings.Index(md, "## ⚠ INDEPENDENT FINDINGS")
	require.Greater(t, findingsStart, verdictEnd)
	infoIdx := strings.Index(md, "Informational (not root-cause evidence)")
	require.Greater(t, infoIdx, verdictEnd, "the informational note must be outside/after the ranked evidence block")
	require.Less(t, infoIdx, findingsStart, "the informational note must render before INDEPENDENT FINDINGS, not inside it")
	// It must not be one of the numbered ranked-evidence lines.
	evidenceBlock := md[strings.Index(md, "## Root-cause evidence"):findingsStart]
	assert.NotContains(t, evidenceBlock[:strings.Index(evidenceBlock, "Informational")], "2.", "must not be numbered as ranked evidence")

	raw, err := report.RenderJSON()
	require.NoError(t, err)
	var round map[string]any
	require.NoError(t, json.Unmarshal(raw, &round))
	evidence := round["evidence"].([]any)
	for _, e := range evidence {
		assert.NotContains(t, e.(map[string]any)["text"].(string), "joinNodeStatsEstimate", "JSON evidence[] must never carry the informational note")
	}

	t.Run("no A3 verdict -> no note", func(t *testing.T) {
		r2 := &Report{Verdict: &Verdict{CategoryName: "NO DOMINANT BOTTLENECK"}}
		assert.Equal(t, "", r2.joinNodeStatsEstimateInformationalNote())
	})

	t.Run("verdict node's join stats are not all-NaN -> no note", func(t *testing.T) {
		r3 := &Report{
			Verdict:     &Verdict{Category: CatPlanPath, NodeId: "1772"},
			NodeMetrics: []*NodeMetrics{{PlanNodeId: "1772", HasJoinStats: true, AllNaNJoinStats: false}},
		}
		assert.Equal(t, "", r3.joinNodeStatsEstimateInformationalNote())
	})
}

// TestRenderMarkdownIndependentFindingsAlwaysPresent covers UT-14: the
// INDEPENDENT FINDINGS section is always rendered directly under the verdict,
// "(none)" when empty.
func TestRenderMarkdownIndependentFindingsAlwaysPresent(t *testing.T) {
	report := &Report{
		QueryId: "q1", State: "FINISHED",
		Verdict:    &Verdict{CategoryName: "NO DOMINANT BOTTLENECK", Confidence: Confidence{Level: "LOW"}},
		Categories: Classify(baselineSignals(), DefaultThresholds()),
	}
	md := report.RenderMarkdown(5)
	verdictIdx := strings.Index(md, "## Verdict")
	findingsIdx := strings.Index(md, "## ⚠ INDEPENDENT FINDINGS")
	require.Greater(t, findingsIdx, verdictIdx)
	assert.Contains(t, md, "(none)")

	report.IndependentFindings = []*CategoryResult{newResult(CatQueueAdmission, 3, "queue dominant")}
	md2 := report.RenderMarkdown(5)
	assert.Contains(t, md2, "A2 Queue / admission")
	findingsSection := md2[strings.Index(md2, "INDEPENDENT FINDINGS"):]
	if end := strings.Index(findingsSection, "## Category"); end >= 0 {
		findingsSection = findingsSection[:end]
	}
	assert.NotContains(t, findingsSection, "(none)")
}

// TestRenderMarkdownCategoryTableAlwaysFull covers the "every category
// reported as mapped, never omitted" rule, including D2 as N/A.
func TestRenderMarkdownCategoryTableAlwaysFull(t *testing.T) {
	report := &Report{
		Verdict:    &Verdict{CategoryName: "NO DOMINANT BOTTLENECK", Confidence: Confidence{Level: "LOW"}},
		Categories: Classify(baselineSignals(), DefaultThresholds()),
	}
	md := report.RenderMarkdown(5)
	for _, cat := range AllCategories {
		assert.Contains(t, md, CategoryNames[cat])
	}
	assert.Contains(t, md, "N/A")
}

// TestRenderMarkdownWithProgressionRunawayAndConflicts covers UT-14/W3: the
// progression section, including a fired RunawayComputation, per-category
// emergence lines, and an explicit temporal-conflict line, plus the
// insufficient-frames degradation.
func TestRenderMarkdownWithProgressionRunawayAndConflicts(t *testing.T) {
	report := &Report{
		QueryId:    "q1",
		Verdict:    &Verdict{Category: CatPlanPath, CategoryName: "Plan path", NodeId: "1", Confidence: Confidence{Level: "MEDIUM"}},
		Categories: Classify(baselineSignals(), DefaultThresholds()),
		Progression: &Progression{
			FrameCount: 3,
			Series: []SeriesPoint{
				{Seq: 1, TimeSec: 0, OutputRows: 10, CpuSec: 1, UserMemoryBytes: 100},
				{Seq: 2, TimeSec: 30, OutputRows: 10, CpuSec: 5, UserMemoryBytes: 100},
			},
			Runaway:             &RunawaySignal{Fired: true, StageNum: 9, StageShare: 1.0, SuspectNodeId: "1"},
			EmergenceByCategory: map[string]float64{CatPlanPath: 0, CatCPU: 60},
			TemporalConflicts:   []string{"B1 (CPU) emerged at ~t=0s, BEFORE the claimed root A3 (Plan path) at ~t=60s"},
		},
		Warnings: []string{"frame 4: parse error"},
	}
	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "## Progression")
	assert.Contains(t, md, "RunawayComputation fired: stage S9")
	assert.Contains(t, md, "suspect node 1\n", "the resolved suspect node id must be rendered (round-3 fix, Issue 1)")
	assert.Contains(t, md, "Plan path emerged at ~t=0s")
	assert.Contains(t, md, "CPU emerged at ~t=60s")
	assert.Contains(t, md, "TEMPORAL CONFLICT")
	assert.Contains(t, md, "## Warnings")
	assert.Contains(t, md, "frame 4: parse error")

	insufficient := &Report{
		QueryId:     "q2",
		Verdict:     &Verdict{CategoryName: "NO DOMINANT BOTTLENECK", Confidence: Confidence{Level: "LOW"}},
		Categories:  Classify(baselineSignals(), DefaultThresholds()),
		Progression: &Progression{FrameCount: 1, Insufficient: true},
	}
	md2 := insufficient.RenderMarkdown(5)
	assert.Contains(t, md2, "insufficient frames for rates")
}

// TestRenderMarkdownRunawaySuspectNodeNoneIdentified is the round-3
// regression test for Issue 1 (§5.6 rendering rule): when RunawayComputation
// fires but localization could not resolve a suspect node inside the runaway
// stage (SuspectNodeId == ""), the progression line must render the explicit
// "suspect node: none identified" fallback -- it must NEVER end with a
// dangling "suspect node " (the round-3 real output did exactly that: the id
// was populated in the verdict but silently dropped from this sentence).
func TestRenderMarkdownRunawaySuspectNodeNoneIdentified(t *testing.T) {
	report := &Report{
		QueryId:    "q1",
		Verdict:    &Verdict{Category: CatPlanPath, CategoryName: "Plan path", NodeId: "1772", Confidence: Confidence{Level: "MEDIUM"}},
		Categories: Classify(baselineSignals(), DefaultThresholds()),
		Progression: &Progression{
			FrameCount: 3,
			Series:     []SeriesPoint{{Seq: 1, TimeSec: 0}},
			Runaway:    &RunawaySignal{Fired: true, StageNum: 9, StageShare: 1.0, SuspectNodeId: ""},
		},
	}
	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "suspect node: none identified")
	assert.NotContains(t, md, "suspect node \n", "must never render a dangling \"suspect node \" with no id and no fallback text")
	assert.NotRegexp(t, `suspect node\s*\n`, md, "must never render a bare/dangling \"suspect node\" with nothing after it")
}

// TestRenderJSONFieldStability covers UT-14: JSON schema field stability
// (round-trips through the stable, documented field names).
func TestRenderJSONFieldStability(t *testing.T) {
	report := &Report{
		QueryId: "q1", State: "FINISHED",
		Verdict: &Verdict{Category: CatPlanPath, CategoryName: "Plan path", NodeId: "1772",
			Recommendation: "run ANALYZE on join inputs / review join order",
			Confidence:     Confidence{Level: "HIGH", Reasons: []string{"root severity 3"}}},
		Categories: Classify(baselineSignals(), DefaultThresholds()),
	}
	b, err := report.RenderJSON()
	require.NoError(t, err)
	var round map[string]any
	require.NoError(t, json.Unmarshal(b, &round))
	assert.Equal(t, "q1", round["queryId"])
	verdict := round["verdict"].(map[string]any)
	assert.Equal(t, "A3", verdict["category"])
	assert.Equal(t, "1772", verdict["nodeId"])
	confidence := verdict["confidence"].(map[string]any)
	assert.Equal(t, "HIGH", confidence["level"])
}

// ut14CamelCaseFixture builds one rich Report that exercises every reused
// internal struct embedded in the report (NodeMetrics and, via Progression,
// RunawaySignal/SeriesPoint) with every field populated -- including the four
// round-4 hidden ones (PipelineId, OperatorId, HasJoinStats, AllNaNJoinStats)
// -- so the SAME report value backs both the camelCase JSON audit and the
// markdown byte-identical golden below (design.md §10 item 13, round-4 JSON
// camelCase lock).
func ut14CamelCaseFixture() *Report {
	fanout := 742.7
	in, out := int64(599440009), int64(445201868645)
	estOut := 6.36e14
	errRatio := 0.0007
	ndv := 66258.0
	pipelineId, operatorId := 0, 5
	return &Report{
		QueryId: "q-ut14", State: "FAILED", ToolVersion: ToolVersion,
		Summary: Summary{WallSec: 609.6, CpuSec: 8316, PeakUserMemoryBytes: 22698902159},
		Verdict: &Verdict{
			Category: CatPlanPath, CategoryName: "Plan path", NodeId: "1772",
			Operator: "HashBuilderOperator+LookupJoinOperator", StageId: "9",
			Summary:        "node 1772: plan-path signature (severity 2)",
			Recommendation: "inherent join fan-out (possible Looker symmetric-aggregate / chasm-trap): pre-aggregate each detail table to one row per join key before joining, and apply the filter to all join inputs, not just one.",
			Confidence:     Confidence{Level: "MEDIUM", Reasons: []string{"a severity-2+ independent finding coexists, capping confidence at MEDIUM"}},
		},
		Categories: Classify(baselineSignals(), DefaultThresholds()),
		Evidence: []EvidenceItem{
			{Text: "node 1772: actual fan-out out/in = 4.45e+11/5.99e+08 = 742.7x", Values: map[string]float64{"fanout": fanout}},
		},
		NodeMetrics: []*NodeMetrics{
			{
				PlanNodeId: "1772", OperatorTypes: []string{"HashBuilderOperator", "LookupJoinOperator"},
				ActualIn: &in, ActualOut: &out, ActualFanout: &fanout, EstimatedOut: &estOut,
				Confidence: "LOW", MinVariableNDV: &ndv, EstimateErrorRatio: &errRatio, IsLowConfidence: true,
				HasJoinStats: true, AllNaNJoinStats: true,
				PipelineId: &pipelineId, OperatorId: &operatorId,
				JoinKeyStats: []JoinKeyStat{{Column: "order_id_52", Min: 1839, Max: 98319154, NDV: plan_node.JsonFloat64(ndv)}},
			},
		},
		FanoutChain: []FanoutChainNode{
			{
				PlanNodeId: "1772", Operator: "HashBuilderOperator+LookupJoinOperator", In: in, Out: out, Fanout: fanout,
				// Round-5 enrichment fields (design.md §5.4.1 A3(iii), §10 item
				// 14): populated here so TestRenderJSONAllKeysCamelCase's
				// recursive audit also covers them.
				Build: "ng_delivery.etl_delivery_order_all_states_together", JoinKeys: []string{"id=order_id_52"},
				SqlLine: intPtr(18), BuildRows: &out, BuildRowsSource: "measured",
				BuildKeyNdv: &ndv, BuildKeyDupFactor: floatPtr(1637.66), BuildKind: "detail1toN",
			},
			{
				// Round-6 (design.md §5.4.1 A3(iii)(δ/ζ), §10 item 15): a
				// second row exercising emptyBuild + deadJoin together, so the
				// recursive camelCase audit also covers the new fields.
				PlanNodeId: "1778", Operator: "HashBuilderOperator+LookupJoinOperator", In: 272324086, Out: 272324086, Fanout: 1,
				Build: "looker_scratch.lr_admin_system_country", JoinKeys: []string{"country_code=code"}, SqlLine: intPtr(16),
				BuildRows: int64Ptr(0), BuildRowsSource: "measured", BuildKeyNdv: floatPtr(126),
				BuildKind: "emptyBuild", DeadJoin: true,
			},
		},
		ChainFanout: &ChainFanout{In: 174400, Out: int64(445201868645), EndToEnd: 2552763.0, NodeProduct: floatPtr(2552763.0)},
		Progression: &Progression{
			FrameCount: 3,
			Series:     []SeriesPoint{{Seq: 1, TimeSec: 0, OutputRows: 0, CpuSec: 267, UserMemoryBytes: 22698902159}},
			Runaway:    &RunawaySignal{Fired: true, StageNum: 9, StageShare: 1.0, SuspectNodeId: ""},
			EmergenceByCategory: map[string]float64{
				CatPlanPath:         0,
				CatWaitBackpressure: 0,
			},
		},
		IndependentFindings: []*CategoryResult{
			newResult(CatStorageLatency, 2, "scan wait 13181.095s / scan wall 10415.395s (share 1.266)"),
			newResult(CatIOThroughput, 2, "scan wall 10415.395s / scheduled 19620.000s (share 0.531)"),
		},
		Warnings: []string{"frame 4: parse error"},
	}
}

// walkJSONKeys recursively visits every JSON object key in a decoded value
// (nested maps/slices), calling visit(key, parentKey) for each -- parentKey
// is the enclosing object key whose value the current map is (empty at the
// document root), letting a caller special-case a map whose KEYS are
// themselves meaningful data values rather than Go struct field names.
func walkJSONKeys(v any, parentKey string, visit func(key, parentKey string)) {
	switch val := v.(type) {
	case map[string]any:
		for k, sub := range val {
			visit(k, parentKey)
			walkJSONKeys(sub, k, visit)
		}
	case []any:
		for _, item := range val {
			walkJSONKeys(item, parentKey, visit)
		}
	}
}

// TestRenderJSONAllKeysCamelCase is the UT-14 rewrite (design.md §10 item 13,
// round-4 JSON camelCase lock): a recursive walk over the ENTIRE marshaled
// report FAILS if any key matches ^[A-Z] -- the exact defect class that let
// NodeMetrics/RunawaySignal leak Go's PascalCase field names verbatim
// (previously invisible because no test checked key casing at all, only
// round-tripped a few already-camelCase fields). Also pins canonical goldens
// for the camelCase names and asserts the four round-4 hidden fields
// (PipelineId, OperatorId, HasJoinStats, AllNaNJoinStats) are absent.
//
// One deliberate, documented exception: progression.emergenceByCategory's
// map KEYS are taxonomy CODE VALUES ("A3", "C1", ...) -- the exact same
// convention as categories[].category's own VALUE -- not a Go struct
// field-name leak, so they are excluded from the ^[A-Z] audit here.
func TestRenderJSONAllKeysCamelCase(t *testing.T) {
	report := ut14CamelCaseFixture()
	b, err := report.RenderJSON()
	require.NoError(t, err)

	var decoded map[string]any
	require.NoError(t, json.Unmarshal(b, &decoded))

	leakRe := regexp.MustCompile(`^[A-Z]`)
	var leaks []string
	walkJSONKeys(decoded, "", func(key, parentKey string) {
		if parentKey == "emergenceByCategory" {
			return // taxonomy code VALUES used as map keys, not a field-name leak
		}
		if leakRe.MatchString(key) {
			leaks = append(leaks, key)
		}
	})
	assert.Empty(t, leaks, "no marshaled JSON key may start with an uppercase letter (PascalCase leak)")

	// Canonical nodeMetrics[] golden (design.md §5.5/§10 item 13 locked schema).
	nodeMetrics := decoded["nodeMetrics"].([]any)
	require.Len(t, nodeMetrics, 1)
	nm := nodeMetrics[0].(map[string]any)
	for _, wantKey := range []string{
		"planNodeId", "operatorTypes", "actualIn", "actualOut", "fanout",
		"estimatedOut", "estimateErrorRatio", "confidence", "isLowConfidence",
		"minNdv", "joinKeyStats",
	} {
		assert.Contains(t, nm, wantKey, "canonical nodeMetrics[] field %q must be present", wantKey)
	}
	assert.Equal(t, "1772", nm["planNodeId"])
	assert.InDelta(t, 742.7, nm["fanout"], 1e-9)
	assert.Equal(t, "LOW", nm["confidence"])
	assert.InDelta(t, 66258.0, nm["minNdv"], 1e-9)
	// The four round-4 hidden fields must be entirely absent, under ANY name.
	for _, hidden := range []string{"pipelineId", "operatorId", "hasJoinStats", "allNaNJoinStats", "PipelineId", "OperatorId", "HasJoinStats", "AllNaNJoinStats"} {
		assert.NotContains(t, nm, hidden, "hidden internal field %q must never appear in JSON", hidden)
	}

	// Canonical progression.runaway golden.
	progression := decoded["progression"].(map[string]any)
	runaway := progression["runaway"].(map[string]any)
	assert.Equal(t, map[string]any{"fired": true, "stageNum": float64(9), "stageShare": float64(1), "suspectNodeId": ""}, runaway)

	// Canonical fanoutChain[] golden, round 5 (design.md §5.4.1 A3(iii)/§10
	// item 14): build/joinKeys always present; the optional facts present
	// here (this fixture populates all of them) must round-trip camelCase.
	fanoutChain := decoded["fanoutChain"].([]any)
	require.Len(t, fanoutChain, 2)
	fc := fanoutChain[0].(map[string]any)
	for _, wantKey := range []string{
		"planNodeId", "operator", "in", "out", "fanout",
		"build", "joinKeys", "sqlLine", "buildRows", "buildRowsSource",
		"buildKeyNdv", "buildKeyDupFactor", "buildKind",
	} {
		assert.Contains(t, fc, wantKey, "canonical fanoutChain[] field %q must be present", wantKey)
	}
	assert.Equal(t, "ng_delivery.etl_delivery_order_all_states_together", fc["build"])
	assert.Equal(t, []any{"id=order_id_52"}, fc["joinKeys"])
	assert.Equal(t, "detail1toN", fc["buildKind"])
	assert.Equal(t, "measured", fc["buildRowsSource"])

	// Round-6 canonical fanoutChain[] golden (design.md §5.4.1 A3(iii)(δ/ζ),
	// §10 item 15): the emptyBuild+deadJoin row -- buildKeyDupFactor must be
	// ABSENT (emptyBuild omits it), deadJoin must be present and camelCase.
	fc2row := fanoutChain[1].(map[string]any)
	assert.Equal(t, "emptyBuild", fc2row["buildKind"])
	assert.Equal(t, true, fc2row["deadJoin"])
	assert.NotContains(t, fc2row, "buildKeyDupFactor", "buildKeyDupFactor must be omitted on an emptyBuild row")
	assert.Contains(t, fc2row, "buildKeyNdv", "buildKeyNdv itself is still a factual read on an emptyBuild row")

	// Canonical chainFanout golden (design.md §5.4.1 A3(iii)(ε), §10 item 15).
	chainFanout := decoded["chainFanout"].(map[string]any)
	for _, wantKey := range []string{"in", "out", "endToEnd", "nodeProduct"} {
		assert.Contains(t, chainFanout, wantKey, "canonical chainFanout field %q must be present", wantKey)
	}
	assert.EqualValues(t, 174400, chainFanout["in"])

	t.Run("chainFanout absent (nil) -> key omitted entirely, never null", func(t *testing.T) {
		bareReport := ut14CamelCaseFixture()
		bareReport.ChainFanout = nil
		b2, err := bareReport.RenderJSON()
		require.NoError(t, err)
		var decoded2 map[string]any
		require.NoError(t, json.Unmarshal(b2, &decoded2))
		assert.NotContains(t, decoded2, "chainFanout")
	})

	t.Run("chainFanout.nodeProduct absent -> key omitted, other fields still present", func(t *testing.T) {
		bareReport := ut14CamelCaseFixture()
		bareReport.ChainFanout = &ChainFanout{In: 1, Out: 2, EndToEnd: 2}
		b2, err := bareReport.RenderJSON()
		require.NoError(t, err)
		var decoded2 map[string]any
		require.NoError(t, json.Unmarshal(b2, &decoded2))
		cf2 := decoded2["chainFanout"].(map[string]any)
		assert.NotContains(t, cf2, "nodeProduct")
		assert.Contains(t, cf2, "endToEnd")
	})

	t.Run("optional fanoutChain fields omitted (not blank) when absent", func(t *testing.T) {
		bareReport := ut14CamelCaseFixture()
		bareReport.FanoutChain = []FanoutChainNode{{PlanNodeId: "9", Operator: "LookupJoinOperator", In: 1, Out: 1, Fanout: 1, Build: "unknown", JoinKeys: []string{}}}
		b2, err := bareReport.RenderJSON()
		require.NoError(t, err)
		var decoded2 map[string]any
		require.NoError(t, json.Unmarshal(b2, &decoded2))
		fc2 := decoded2["fanoutChain"].([]any)[0].(map[string]any)
		assert.Contains(t, fc2, "build")
		assert.Contains(t, fc2, "joinKeys")
		for _, absentKey := range []string{"sqlLine", "buildRows", "buildRowsSource", "buildKeyNdv", "buildKeyDupFactor", "buildKind", "deadJoin"} {
			assert.NotContains(t, fc2, absentKey, "optional fanoutChain field %q must be OMITTED, not rendered blank/null, when its source data is absent", absentKey)
		}
	})

	// Belt-and-suspenders raw-text check (design.md's explicit ask): the four
	// hidden Go field names must not appear anywhere in the raw JSON text.
	raw := string(b)
	for _, hidden := range []string{"PipelineId", "OperatorId", "HasJoinStats", "AllNaNJoinStats"} {
		assert.NotContains(t, raw, hidden)
	}
}

// TestRenderMarkdownByteIdenticalAcrossJSONTagChange is the round-4
// "scope guard" regression test (design.md §10 item 13): the markdown
// renderer never touches encoding/json or any json tag, so pinning its exact
// output on a fixture that ALSO carries every JSON-tag-affected field
// (NodeMetrics/RunawaySignal, including the four now-hidden-from-JSON
// fields) proves the camelCase JSON fix is serialization-only -- no
// verdict/severity/confidence/value or markdown-text change.
func TestRenderMarkdownByteIdenticalAcrossJSONTagChange(t *testing.T) {
	report := ut14CamelCaseFixture()
	md := report.RenderMarkdown(5)
	assert.Equal(t, ut14GoldenMarkdown, md, "markdown output must be byte-identical -- this round's fix is JSON tags only")
}

// TestRenderMarkdownHealthyTopLineGoldenAbsentOnPathological is UT-14's
// round-7 render golden (design.md §5.4.2/§5.5's fourth worked example, §10
// item 16): the healthy top-line is the report's literal FIRST line, above
// the verdict, present ONLY when Summary.Healthy is true; the pathological
// golden fixture (ut14CamelCaseFixture, state FAILED, Healthy left at its
// zero value) must render with NO top-line at all -- proving this is a
// strictly additive, opt-in rendering path.
func TestRenderMarkdownHealthyTopLineGoldenAbsentOnPathological(t *testing.T) {
	pathological := ut14CamelCaseFixture()
	mdPathological := pathological.RenderMarkdown(5)
	assert.NotContains(t, mdPathological, "No significant execution bottleneck detected", "the pathological golden must never carry the healthy top-line")

	healthy := &Report{
		QueryId: "q-healthy", State: "FINISHED", ToolVersion: ToolVersion,
		Summary: Summary{
			WallSec: 18.47, CpuSec: 1.77, PeakUserMemoryBytes: 393216,
			Healthy:     true,
			HealthyNote: "No significant execution bottleneck detected — query FINISHED in 18.5s; largest component: planning 7.1s (38.6% of elapsed).",
		},
		Verdict: &Verdict{
			Category: CatPlanTime, CategoryName: "Plan time",
			Summary:    "planning 7.130s (of which analysis 6.880s) / elapsed 18.470s (ratio 0.386)",
			Confidence: Confidence{Level: "MEDIUM", Reasons: []string{"single evidence family backs the root"}},
		},
		Categories: Classify(baselineSignals(), DefaultThresholds()),
	}
	mdHealthy := healthy.RenderMarkdown(5)
	lines := strings.Split(mdHealthy, "\n")
	require.GreaterOrEqual(t, len(lines), 5)
	assert.Equal(t, "# Query diagnosis: q-healthy", lines[0])
	assert.Equal(t, "State: FINISHED | wall 18.47s | cpu 1.77s | peak user mem 393216 bytes", lines[1])
	assert.Equal(t, healthy.Summary.HealthyNote, lines[2], "the healthy top-line must be the literal third line (right after the header/state line), ABOVE the verdict")
	assert.Equal(t, "", lines[3], "exactly one blank line must separate the top-line from the verdict section")
	assert.Equal(t, "## Verdict  (root cause; confidence: MEDIUM)", lines[4])
	assert.Contains(t, mdHealthy, "## Verdict  (root cause; confidence: MEDIUM)\nPLAN TIME:")

	t.Run("Healthy=false with a stray HealthyNote set anyway -> still no top-line (Healthy is the gate, not the note's mere presence)", func(t *testing.T) {
		r := &Report{
			QueryId: "q1", State: "FINISHED",
			Summary:    Summary{HealthyNote: "should never render"},
			Verdict:    &Verdict{CategoryName: "Plan time", Confidence: Confidence{Level: "LOW"}},
			Categories: Classify(baselineSignals(), DefaultThresholds()),
		}
		md := r.RenderMarkdown(5)
		assert.NotContains(t, md, "should never render")
	})
}

// TestReportSummaryHealthyJSONCamelCaseAndOmitempty covers UT-14's round-7
// JSON contract (design.md §10 item 16): summary.healthy/summary.healthyNote
// are camelCase and BOTH omitempty -- present only on a healthy report,
// entirely absent (never `false`/`""`) on every pathological one.
func TestReportSummaryHealthyJSONCamelCaseAndOmitempty(t *testing.T) {
	t.Run("healthy report -> both fields present, camelCase", func(t *testing.T) {
		r := &Report{
			QueryId: "q1", State: "FINISHED",
			Summary:    Summary{WallSec: 18.47, CpuSec: 1.77, PeakUserMemoryBytes: 393216, Healthy: true, HealthyNote: "No significant execution bottleneck detected."},
			Verdict:    &Verdict{CategoryName: "Plan time", Confidence: Confidence{Level: "MEDIUM"}},
			Categories: Classify(baselineSignals(), DefaultThresholds()),
		}
		b, err := r.RenderJSON()
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(b, &decoded))
		summary := decoded["summary"].(map[string]any)
		assert.Equal(t, true, summary["healthy"])
		assert.Equal(t, "No significant execution bottleneck detected.", summary["healthyNote"])

		leakRe := regexp.MustCompile(`^[A-Z]`)
		var leaks []string
		walkJSONKeys(decoded, "", func(key, parentKey string) {
			if parentKey == "emergenceByCategory" {
				return
			}
			if leakRe.MatchString(key) {
				leaks = append(leaks, key)
			}
		})
		assert.Empty(t, leaks, "the recursive ^[A-Z] audit must stay clean on a healthy report too")
	})

	t.Run("pathological report (Healthy left at its zero value) -> both fields OMITTED, never false/blank", func(t *testing.T) {
		report := ut14CamelCaseFixture()
		b, err := report.RenderJSON()
		require.NoError(t, err)
		var decoded map[string]any
		require.NoError(t, json.Unmarshal(b, &decoded))
		summary := decoded["summary"].(map[string]any)
		assert.NotContains(t, summary, "healthy")
		assert.NotContains(t, summary, "healthyNote")
	})
}

// ut14GoldenMarkdown is the exact markdown rendered from ut14CamelCaseFixture
// BEFORE and unaffected by the round-4 JSON-tag change (captured once,
// pinned verbatim -- the whole point of this golden is that it must never
// change from a serialization-only fix).
var ut14GoldenMarkdown = `# Query diagnosis: q-ut14
State: FAILED | wall 609.60s | cpu 8316.00s | peak user mem 22698902159 bytes

## Verdict  (root cause; confidence: MEDIUM)
PLAN PATH — node 1772 (HashBuilderOperator+LookupJoinOperator), stage 9: node 1772: plan-path signature (severity 2)
Recommendation: inherent join fan-out (possible Looker symmetric-aggregate / chasm-trap): pre-aggregate each detail table to one row per join key before joining, and apply the filter to all join inputs, not just one.
Confidence MEDIUM: a severity-2+ independent finding coexists, capping confidence at MEDIUM

## Root-cause evidence (ranked)
1. node 1772: actual fan-out out/in = 4.45e+11/5.99e+08 = 742.7x

Informational (not root-cause evidence): joinNodeStatsEstimate all NaN — HBO null-skew fields, not a CBO cardinality input; NaN = no HBO history, not the cause.

## ⚠ INDEPENDENT FINDINGS (co-causes NOT explained by the verdict)
- B4 Storage / connector latency (sev 2): scan wait 13181.095s / scan wall 10415.395s (share 1.00 [raw 1.266])
- B3 IO — scan throughput (sev 2): scan wall 10415.395s / scheduled 19620.000s (share 0.531)

## Contributing / derived symptoms (attributed to node 1772)
(none)

## Category assessment
| Group | Category | Severity | Note |
|---|---|---|---|
| A | Plan time | 0 | planning 0.000s (of which analysis 0.000s) / elapsed 100.000s (ratio 0.000) |
| A | Queue / admission | 0 | queued+admission 0.000s / elapsed 100.000s (ratio 0.000) |
| A | Plan path | 0 | no elevated plan-node signature found |
| B | CPU | 0 | totalCpuTime 10.000s, cpu/scheduled 0.100 |
| B | Memory / spill | 0 | peak user memory 0 bytes (limit unknown); spilled 0 bytes |
| B | IO — scan throughput | 0 | scan wall 0.000s / scheduled 100.000s (share 0.000) |
| B | Storage / connector latency | 0 | scan wait 0.000s / scan wall 0.000s (share 0.000); scan wall/scheduled 0.000 (relevance gate 0.20) |
| B | Network / exchange / shuffle | 0 | exchange wall share 0.000 (threshold 0.40); shuffled 0 bytes / raw input 1000 bytes (ratio 0.00) |
| C | Wait / backpressure | 0 | avg blocked/driver 0.000s (blocked 0.000s / 0 drivers) / elapsed 100.000s (share 0.000), direction unknown; fullyBlocked=false |
| C | Skew / imbalance | 0 | max/median task CPU ratio 0.00 (relevant stages only) |
| C | Scheduling / parallelism | 0 | avg rows/split 100000 (splits=10); under-parallelism: core count unknown, SplitsPerCoreLow=1 informational only until --cluster-info exists |
| C | Output / result delivery | 0 | finishing 0.000s / elapsed 100.000s (share 0.000); root stage tasks=0; client fetch rate inferred from tail timing |
| D | GC pauses | 0 | no GC data available in this query JSON |
| D | Lock / contention | N/A | N/A — see memory arbitration / Execution-Wait |

## Progression (from 3 snapshots) — temporal causal evidence
- t=0s: outputRows=0 cpu=267.00s userMemory=22698902159 bytes
- RunawayComputation fired: stage S9 holds 100% of scheduled-time growth, suspect node: none identified
- Plan path emerged at ~t=0s
- Wait / backpressure emerged at ~t=0s

## Warnings
- frame 4: parse error
`

// TestRenderMarkdownClampsShareAboveOneRawJSONUnchanged covers review W5 /
// design §10 item 2c: a "share"-labeled value above 1.0 (the benign
// wall-vs-scheduled dimensional mix on B3/B4, bounded and driver-count-
// independent -- NOT the driver-scaling bug already fixed for B5/C1, and it
// never affects classification since every share THRESHOLD is itself < 1.0)
// must be clamped to "1.00 (raw X.X)" for markdown DISPLAY ONLY; the JSON
// output must keep the raw, unclamped numeric text unchanged (no schema
// change, classify.go untouched).
func TestRenderMarkdownClampsShareAboveOneRawJSONUnchanged(t *testing.T) {
	th := DefaultThresholds()
	sig := baselineSignals()
	// B3: scanWall > scheduled -- e.g. a query whose scan busy-wall (summed
	// across many driver instances) exceeds the query-level scheduled-time
	// aggregate by a modest, bounded factor (~1.8x), the exact benign mix the
	// review flagged (as opposed to the driver-scaling bug class, which
	// produced shares in the hundreds).
	sig.ScanWallSec = 180
	sig.ScheduledSec = 100
	b3 := scoreB3(sig, th)
	require.Equal(t, "scan wall 180.000s / scheduled 100.000s (share 1.800)", b3.Note)

	// B4: scanWait > scanWall -- same benign dimensional mix, different
	// category.
	sig2 := baselineSignals()
	sig2.ScanWaitSec = 150
	sig2.ScanWallSec = 100
	sig2.ScheduledSec = 200 // relevance gate: scanWall/scheduled = 0.5 >= 0.2, so B4 can evaluate its own share
	b4 := scoreB4(sig2, th)
	require.Contains(t, b4.Note, "share 1.500")

	report := &Report{
		QueryId: "q-share", State: "FINISHED",
		Verdict:    &Verdict{Category: CatIOThroughput, CategoryName: "IO — scan throughput", Summary: b3.Note, Confidence: Confidence{Level: "MEDIUM"}},
		Categories: []*CategoryResult{b3, b4},
	}

	md := report.RenderMarkdown(5)
	assert.NotContains(t, md, "share 1.800", "the raw >1.0 value must not appear unclamped in markdown")
	assert.NotContains(t, md, "share 1.500")
	assert.Contains(t, md, "share 1.00 [raw 1.8]", "clamped display must show 1.00 with the raw value alongside")
	assert.Contains(t, md, "share 1.00 [raw 1.5]")
	// Verdict headline also goes through the same clamp (it renders
	// Verdict.Summary, which is root.Note verbatim).
	assert.Contains(t, md, "IO — SCAN THROUGHPUT")

	// JSON keeps the raw, unclamped numeric text -- no schema change, no
	// value substitution.
	raw, err := report.RenderJSON()
	require.NoError(t, err)
	var round map[string]any
	require.NoError(t, json.Unmarshal(raw, &round))
	categories := round["categories"].([]any)
	notesJoined := ""
	for _, c := range categories {
		notesJoined += c.(map[string]any)["note"].(string) + "\n"
	}
	assert.Contains(t, notesJoined, "share 1.800", "JSON note text must retain the raw value")
	assert.Contains(t, notesJoined, "share 1.500")
	assert.NotContains(t, notesJoined, "raw 1.8", "JSON must never show the markdown-only clamped/raw annotation")

	// A share at or below 1.0 is never rewritten.
	assert.Equal(t, "scan wall 50.000s / scheduled 100.000s (share 0.500)", clampShareForDisplayTestHelper(t, "scan wall 50.000s / scheduled 100.000s (share 0.500)"))
}

// clampShareForDisplayTestHelper exercises clampShareForDisplay directly
// (package-private, same-package test) for the boundary/no-op case.
func clampShareForDisplayTestHelper(t *testing.T, s string) string {
	t.Helper()
	return clampShareForDisplay(s)
}

// TestRenderNDVFingerprintSubTableGolden covers UT-14: the NDV-fingerprint
// sub-table renders in markdown as a ranked-evidence item and round-trips
// through JSON as plain evidence text (no separate table schema — the same
// string carries the markdown table in both formats, exactly like every
// other evidence Text).
func TestRenderNDVFingerprintSubTableGolden(t *testing.T) {
	stats := []JoinKeyStat{
		{Column: "id", Min: 1839, Max: 98319154, NDV: 66312},
		{Column: "order_id_52", Min: 314, Max: 98319154, NDV: plan_node.JsonFloat64(nanValue())},
	}
	table := renderJoinKeyFingerprintTable(stats, true)
	assert.Contains(t, table, "NDV fingerprint")
	assert.Contains(t, table, "| join key | min | max | distinctValuesCount |")
	assert.Contains(t, table, "| order_id_52 | 314 | 98319154 | NaN |")
	assert.Contains(t, table, "| id | 1839 | 98319154 | 66312 |")

	presentTable := renderJoinKeyFingerprintTable(stats, false)
	assert.Contains(t, presentTable, "PRESENT")

	report := &Report{
		QueryId: "q1", State: "FAILED",
		Verdict:    &Verdict{Category: CatPlanPath, CategoryName: "Plan path", NodeId: "42", Confidence: Confidence{Level: "HIGH"}},
		Categories: []*CategoryResult{{Category: CatPlanPath, Group: "A", Name: "Plan path", Severity: 3}},
		Evidence:   []EvidenceItem{{Text: table}},
	}
	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "order_id_52 | 314 | 98319154 | NaN")

	raw, err := report.RenderJSON()
	require.NoError(t, err)
	var round map[string]any
	require.NoError(t, json.Unmarshal(raw, &round))
	evidence := round["evidence"].([]any)[0].(map[string]any)
	assert.Contains(t, evidence["text"].(string), "order_id_52 | 314 | 98319154 | NaN")
}

// TestRenderEstimateProvenanceFallbackLabelGolden covers UT-14: the
// estimate-provenance fallback line renders in both formats, and the
// nodeMetrics[].estimateProvenance JSON field is present only when the
// fallback fired (omitted otherwise, per design.md §5.5).
func TestRenderEstimateProvenanceFallbackLabelGolden(t *testing.T) {
	report := &Report{
		QueryId: "q1", State: "FAILED",
		Verdict: &Verdict{Category: CatPlanPath, CategoryName: "Plan path", NodeId: "1772", Confidence: Confidence{Level: "HIGH"}},
		Evidence: []EvidenceItem{{
			Text: "planStatsAndCosts[1772]: outputRowCount=2.79e+22 = default-join-selectivity FALLBACK (join-key NDV unavailable — engine guesswork, not a measured cardinality), confident=LOW",
		}},
		NodeMetrics: []*NodeMetrics{
			{PlanNodeId: "1772", EstimateProvenance: "default_join_selectivity_fallback"},
			{PlanNodeId: "999"}, // no fallback -> field must be omitted
		},
	}
	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "default-join-selectivity FALLBACK")
	assert.Contains(t, md, "join-key NDV unavailable")

	raw, err := report.RenderJSON()
	require.NoError(t, err)
	var round map[string]any
	require.NoError(t, json.Unmarshal(raw, &round))
	nms := round["nodeMetrics"].([]any)
	first := nms[0].(map[string]any)
	assert.Equal(t, "default_join_selectivity_fallback", first["estimateProvenance"])
	second := nms[1].(map[string]any)
	assert.NotContains(t, second, "estimateProvenance", "omitempty must drop the field when the fallback did not fire")
}

// TestRenderB4QuickStatsAnnotationGolden covers UT-14: the B4 quick-stats
// annotation flows through the existing categories[].note text in both
// formats (no schema change).
func TestRenderB4QuickStatsAnnotationGolden(t *testing.T) {
	sig := baselineSignals()
	sig.ScanWaitSec = 60
	sig.ScanWallSec = 100
	sig.ScheduledSec = 200
	sig.QuickStatsDetected = true
	b4 := scoreB4(sig, DefaultThresholds())
	require.Contains(t, b4.Note, "quick-stats / metastore latency")

	report := &Report{
		QueryId: "q1", State: "FINISHED",
		Verdict:    &Verdict{CategoryName: "NO DOMINANT BOTTLENECK", Confidence: Confidence{Level: "LOW"}},
		Categories: []*CategoryResult{b4},
	}
	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "quick-stats / metastore latency")

	raw, err := report.RenderJSON()
	require.NoError(t, err)
	var round map[string]any
	require.NoError(t, json.Unmarshal(raw, &round))
	cats := round["categories"].([]any)
	assert.Contains(t, cats[0].(map[string]any)["note"].(string), "quick-stats / metastore latency")
}
