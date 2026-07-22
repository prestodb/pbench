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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"pbench/prestoapi/plan_node"
)

func vstat(low, high, ndv float64) plan_node.VariableStatistics {
	return plan_node.VariableStatistics{
		LowValue: plan_node.JsonFloat64(low), HighValue: plan_node.JsonFloat64(high),
		NullsFraction: 0, AverageRowSize: plan_node.JsonFloat64(nanValue()), DistinctValuesCount: plan_node.JsonFloat64(ndv),
	}
}

// TestStripTypeSuffix covers the "<type>" suffix stripping that lets
// variableStatistics map keys ("order_id_52<bigint>") match a join
// predicate's bare column name ("order_id_52").
func TestStripTypeSuffix(t *testing.T) {
	assert.Equal(t, "order_id_52", stripTypeSuffix("order_id_52<bigint>"))
	assert.Equal(t, "state", stripTypeSuffix("state<varchar(255)>"))
	assert.Equal(t, "no_suffix", stripTypeSuffix("no_suffix"))
	assert.Equal(t, "", stripTypeSuffix(""))
}

// TestExtractJoinKeyStats covers UT-11: join keys resolved via
// plan_node.ParseJoins-style predicate parsing, matched against
// variableStatistics with the "<type>" suffix stripped -- the real cycle
// shape (`[("id" = "order_id_52")]`) plus negative cases.
func TestExtractJoinKeyStats(t *testing.T) {
	joinNode := &plan_node.PlanNode{Id: "1772", Name: "LeftJoin", Identifier: `[("id" = "order_id_52")]`}
	vs := map[string]plan_node.VariableStatistics{
		"id<bigint>":          vstat(1839, 98319154, 66312),
		"order_id_52<bigint>": vstat(314, 98319154, math.NaN()),
		"unrelated<varchar>":  vstat(0, 1, 5), // present but not a join key -> excluded
	}

	stats := ExtractJoinKeyStats(joinNode, vs)
	require.Len(t, stats, 2)
	byCol := map[string]JoinKeyStat{}
	for _, s := range stats {
		byCol[s.Column] = s
	}
	require.Contains(t, byCol, "order_id_52")
	orderId := byCol["order_id_52"]
	assert.EqualValues(t, 314, orderId.Min)
	assert.EqualValues(t, 98319154, orderId.Max)
	assert.True(t, math.IsNaN(float64(orderId.NDV)))
	require.Contains(t, byCol, "id")
	assert.EqualValues(t, 66312, byCol["id"].NDV)
	assert.NotContains(t, byCol, "unrelated", "a variableStatistics entry that is not a join-predicate column must be excluded")

	assert.True(t, HasNaNJoinKeyNDV(stats))

	t.Run("identical min/max across join keys is genuine per-key data, not a resolution bug (round-3, Issue 3, §5.4.1)", func(t *testing.T) {
		// The real after-fix node 1772 fingerprint (design.md §10 item 12(c)):
		// two sides of one equi-join share the exact same min=1839/max=98319154
		// range (expected -- they are compared by the join predicate), but their
		// NDV (66312 vs 66258) and nullsFraction (0.0 vs 3.06e-4, not part of the
		// JoinKeyStat schema but confirming independent per-key resolution in the
		// raw data) genuinely differ. Verified data-correct, no code change --
		// this fixture pins that ExtractJoinKeyStats resolves each join key to
		// its OWN row, never collapsing same-range columns into one.
		identicalRangeVs := map[string]plan_node.VariableStatistics{
			"id<bigint>":          vstat(1839, 98319154, 66312),
			"order_id_52<bigint>": vstat(1839, 98319154, 66258),
		}
		identicalRangeStats := ExtractJoinKeyStats(joinNode, identicalRangeVs)
		require.Len(t, identicalRangeStats, 2, "both join keys must resolve as separate rows despite sharing identical min/max")
		byRangeCol := map[string]JoinKeyStat{}
		for _, s := range identicalRangeStats {
			byRangeCol[s.Column] = s
		}
		require.Contains(t, byRangeCol, "id")
		require.Contains(t, byRangeCol, "order_id_52")
		assert.EqualValues(t, 1839, byRangeCol["id"].Min)
		assert.EqualValues(t, 98319154, byRangeCol["id"].Max)
		assert.EqualValues(t, 66312, byRangeCol["id"].NDV)
		assert.EqualValues(t, 1839, byRangeCol["order_id_52"].Min)
		assert.EqualValues(t, 98319154, byRangeCol["order_id_52"].Max)
		assert.EqualValues(t, 66258, byRangeCol["order_id_52"].NDV, "order_id_52's own NDV must survive independently of id's -- proving no collapse to one column")
		assert.NotEqual(t, byRangeCol["id"].NDV, byRangeCol["order_id_52"].NDV, "the two keys' NDVs must stay distinct even though their ranges are identical")
		assert.False(t, HasNaNJoinKeyNDV(identicalRangeStats), "both NDVs are present (post-NDV-fix shape) -- no NaN trigger")

		table := renderJoinKeyFingerprintTable(identicalRangeStats, HasNaNJoinKeyNDV(identicalRangeStats))
		assert.Contains(t, table, "| id | 1839 | 98319154 | 66312 |")
		assert.Contains(t, table, "| order_id_52 | 1839 | 98319154 | 66258 |")
	})

	t.Run("non-join node -> nil, no error", func(t *testing.T) {
		nonJoin := &plan_node.PlanNode{Id: "5", Name: "TableScan", Identifier: "some scan identifier"}
		assert.Nil(t, ExtractJoinKeyStats(nonJoin, vs))
	})

	t.Run("nil node -> nil", func(t *testing.T) {
		assert.Nil(t, ExtractJoinKeyStats(nil, vs))
	})

	t.Run("join node with unparsable identifier -> nil, no panic", func(t *testing.T) {
		bad := &plan_node.PlanNode{Id: "9", Name: "InnerJoin", Identifier: "not a valid predicate at all {{{"}
		assert.Nil(t, ExtractJoinKeyStats(bad, vs))
	})

	t.Run("join keys with no matching variableStatistics entry -> empty, no NaN trigger", func(t *testing.T) {
		emptyVs := map[string]plan_node.VariableStatistics{}
		stats := ExtractJoinKeyStats(joinNode, emptyVs)
		assert.Empty(t, stats)
		assert.False(t, HasNaNJoinKeyNDV(stats))
	})

	t.Run("all-numeric join keys (no NaN NDV) -> fingerprint present but not triggered", func(t *testing.T) {
		allNumeric := map[string]plan_node.VariableStatistics{
			"id<bigint>":          vstat(1, 2, 3),
			"order_id_52<bigint>": vstat(1, 2, 3),
		}
		stats := ExtractJoinKeyStats(joinNode, allNumeric)
		require.Len(t, stats, 2)
		assert.False(t, HasNaNJoinKeyNDV(stats), "no NaN NDV among the join keys must not trigger the sub-table")
	})
}

// TestFormatFingerprintValue covers the special-value rendering (NaN,
// +/-Infinity, whole numbers without a trailing decimal point).
func TestFormatFingerprintValue(t *testing.T) {
	assert.Equal(t, "NaN", formatFingerprintValue(plan_node.JsonFloat64(math.NaN())))
	assert.Equal(t, "Infinity", formatFingerprintValue(plan_node.JsonFloat64(math.Inf(1))))
	assert.Equal(t, "-Infinity", formatFingerprintValue(plan_node.JsonFloat64(math.Inf(-1))))
	assert.Equal(t, "314", formatFingerprintValue(plan_node.JsonFloat64(314)))
	assert.Equal(t, "98319154", formatFingerprintValue(plan_node.JsonFloat64(98319154)))
	assert.Equal(t, "1.5", formatFingerprintValue(plan_node.JsonFloat64(1.5)))
}

// TestEnrichA3EvidenceProvenanceDetection covers UT-11's estimate-provenance
// detection: LOW + NaN join-key NDV + outputRowCount >= FallbackEstimateMin
// -> "default_join_selectivity_fallback"; each condition absent -> the field
// stays unset (a negative case per condition).
func TestEnrichA3EvidenceProvenanceDetection(t *testing.T) {
	th := DefaultThresholds()
	joinNode := &plan_node.PlanNode{Id: "1772", Name: "LeftJoin", Identifier: `[("id" = "order_id_52")]`}
	planNodes := map[string]*plan_node.PlanNode{"1772": joinNode}
	vs := map[string]plan_node.VariableStatistics{
		"id<bigint>":          vstat(1839, 98319154, 66312),
		"order_id_52<bigint>": vstat(314, 98319154, math.NaN()),
	}

	newRoot := func() *CategoryResult {
		r := newResult(CatPlanPath, 3, "")
		r.NodeId = "1772"
		r.Evidence = []EvidenceItem{{
			Text:   "planStatsAndCosts[1772]: outputRowCount=2.79e+22, confident=LOW",
			Values: map[string]float64{"estimatedOutputRowCount": 2.79e22},
		}}
		return r
	}

	t.Run("all conditions met -> fallback label + fingerprint table appended", func(t *testing.T) {
		root := newRoot()
		extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
			"1772": {Confident: "LOW", OutputRowCount: plan_node.JsonFloat64(2.79e22), VariableStatistics: vs},
		}}}
		nodeMetrics := map[string]*NodeMetrics{"1772": {PlanNodeId: "1772"}}
		enrichA3Evidence(root, nodeMetrics, nil, extra, planNodes, th)

		assert.Equal(t, "default_join_selectivity_fallback", nodeMetrics["1772"].EstimateProvenance)
		require.NotEmpty(t, nodeMetrics["1772"].JoinKeyStats)
		require.Len(t, root.Evidence, 2, "the outputRowCount line stays one item (rewritten in place) + the new fingerprint table item")
		assert.Contains(t, root.Evidence[0].Text, "default-join-selectivity FALLBACK")
		assert.Contains(t, root.Evidence[1].Text, "NDV fingerprint")
		assert.Contains(t, root.Evidence[1].Text, "order_id_52")
	})

	t.Run("confident=HIGH -> fingerprint table still renders, no fallback label", func(t *testing.T) {
		root := newRoot()
		extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
			"1772": {Confident: "HIGH", OutputRowCount: plan_node.JsonFloat64(2.79e22), VariableStatistics: vs},
		}}}
		nodeMetrics := map[string]*NodeMetrics{"1772": {PlanNodeId: "1772"}}
		enrichA3Evidence(root, nodeMetrics, nil, extra, planNodes, th)

		assert.Empty(t, nodeMetrics["1772"].EstimateProvenance)
		assert.NotEmpty(t, nodeMetrics["1772"].JoinKeyStats, "the fingerprint condition does not require confident==LOW")
		assert.NotContains(t, root.Evidence[0].Text, "FALLBACK")
	})

	t.Run("outputRowCount below FallbackEstimateMin -> no fallback label", func(t *testing.T) {
		root := newRoot()
		extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
			"1772": {Confident: "LOW", OutputRowCount: plan_node.JsonFloat64(1e6), VariableStatistics: vs},
		}}}
		nodeMetrics := map[string]*NodeMetrics{"1772": {PlanNodeId: "1772"}}
		enrichA3Evidence(root, nodeMetrics, nil, extra, planNodes, th)

		assert.Empty(t, nodeMetrics["1772"].EstimateProvenance)
		assert.NotEmpty(t, nodeMetrics["1772"].JoinKeyStats)
	})

	// REC-1/REC-2 (round 2): the fingerprint now renders for BOTH NaN and
	// present join-key NDV (present NDV drives the REC-1 chasm-trap
	// recommendation branch, report.go) -- only the fallback-provenance
	// label stays NaN-gated.
	t.Run("present join-key NDV -> fingerprint still renders, no fallback label", func(t *testing.T) {
		root := newRoot()
		allNumeric := map[string]plan_node.VariableStatistics{
			"id<bigint>":          vstat(1, 2, 3),
			"order_id_52<bigint>": vstat(1, 2, 3),
		}
		extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
			"1772": {Confident: "LOW", OutputRowCount: plan_node.JsonFloat64(2.79e22), VariableStatistics: allNumeric},
		}}}
		nodeMetrics := map[string]*NodeMetrics{"1772": {PlanNodeId: "1772"}}
		enrichA3Evidence(root, nodeMetrics, nil, extra, planNodes, th)

		assert.Empty(t, nodeMetrics["1772"].EstimateProvenance, "present NDV must never get the fallback label, regardless of confident/outputRowCount")
		assert.NotEmpty(t, nodeMetrics["1772"].JoinKeyStats)
		assert.False(t, HasNaNJoinKeyNDV(nodeMetrics["1772"].JoinKeyStats))
		require.Len(t, root.Evidence, 2, "the fingerprint item is still appended even with present NDV")
		assert.Contains(t, root.Evidence[1].Text, "PRESENT")
		assert.NotContains(t, root.Evidence[0].Text, "FALLBACK")
	})

	t.Run("no join keys resolvable at all -> neither enrichment fires", func(t *testing.T) {
		root := newRoot()
		extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
			"1772": {Confident: "LOW", OutputRowCount: plan_node.JsonFloat64(2.79e22), VariableStatistics: map[string]plan_node.VariableStatistics{}},
		}}}
		nodeMetrics := map[string]*NodeMetrics{"1772": {PlanNodeId: "1772"}}
		enrichA3Evidence(root, nodeMetrics, nil, extra, planNodes, th)

		assert.Empty(t, nodeMetrics["1772"].EstimateProvenance)
		assert.Empty(t, nodeMetrics["1772"].JoinKeyStats)
		require.Len(t, root.Evidence, 1, "no fingerprint item appended")
	})

	t.Run("root is not A3 -> no-op", func(t *testing.T) {
		root := newResult(CatCPU, 2, "")
		root.NodeId = "1772"
		extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
			"1772": {Confident: "LOW", OutputRowCount: plan_node.JsonFloat64(2.79e22), VariableStatistics: vs},
		}}}
		nodeMetrics := map[string]*NodeMetrics{"1772": {PlanNodeId: "1772"}}
		enrichA3Evidence(root, nodeMetrics, nil, extra, planNodes, th)
		assert.Empty(t, nodeMetrics["1772"].JoinKeyStats)
	})

	t.Run("root is nil -> no-op, no panic", func(t *testing.T) {
		enrichA3Evidence(nil, map[string]*NodeMetrics{}, nil, &ExtraInfo{}, planNodes, th)
	})

	t.Run("no plan-node lookup available -> no-op (non-join-detectable), no panic", func(t *testing.T) {
		root := newRoot()
		extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
			"1772": {Confident: "LOW", OutputRowCount: plan_node.JsonFloat64(2.79e22), VariableStatistics: vs},
		}}}
		nodeMetrics := map[string]*NodeMetrics{"1772": {PlanNodeId: "1772"}}
		enrichA3Evidence(root, nodeMetrics, nil, extra, nil, th)
		assert.Empty(t, nodeMetrics["1772"].JoinKeyStats)
	})
}

// TestFingerprintSurvivesDefaultTopTruncationWithRunaway is a regression test
// for reviewer finding M1: appending the fingerprint sub-table LAST meant
// that in modes carrying extra optional A3 evidence lines (estimate_error_ratio,
// and/or a RunawayComputation line), it fell past the default `--top 5`
// truncation in the rendered markdown -- the JSON nodeMetrics.joinKeyStats
// still had it, but the human/LLM reading the default report did not see the
// headline "which join key lacks NDV" signal. Reproduces the exact real-data
// shape (6 evidence items before enrichment: fanout, outputRowCount, the
// all-NaN-variables note, joinNodeStatsEstimate ALL NaN, estimate_error_ratio,
// RunawayComputation) and asserts the fingerprint still renders under the
// default top-N by being inserted right after the outputRowCount line
// (design.md §5.5's own worked-example position, item 3) rather than
// appended at the end.
func TestFingerprintSurvivesDefaultTopTruncationWithRunaway(t *testing.T) {
	const defaultTop = 5
	th := DefaultThresholds()
	joinNode := &plan_node.PlanNode{Id: "1772", Name: "LeftJoin", Identifier: `[("id" = "order_id_52")]`}
	planNodes := map[string]*plan_node.PlanNode{"1772": joinNode}
	vs := map[string]plan_node.VariableStatistics{
		"id<bigint>":          vstat(1839, 98319154, 66312),
		"order_id_52<bigint>": vstat(314, 98319154, math.NaN()),
	}
	extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
		"1772": {Confident: "LOW", OutputRowCount: plan_node.JsonFloat64(2.79e22), VariableStatistics: vs},
	}}}

	root := newResult(CatPlanPath, 3, "")
	root.NodeId = "1772"
	root.Evidence = []EvidenceItem{
		{Text: "node 1772: actual fan-out out/in = 4.18e+11/5.59e+08 = 746.4x"},
		{Text: "planStatsAndCosts[1772]: outputRowCount=2.79e+22, confident=LOW", Values: map[string]float64{"estimatedOutputRowCount": 2.79e22}},
		{Text: "planStatsAndCosts[1772].variableStatistics: distinctValuesCount=NaN on all observed variables"},
		{Text: "estimate_error_ratio = 1.49e-11 (actual/estimate)"},
		{Text: "RunawayComputation: stage S9 holds 100% of the query's scheduled-time growth, no measured fan-out available live"},
	}
	require.Len(t, root.Evidence, 5, "precondition: 5 items precede enrichment, matching the real terminal+snapshots shape (post-REC-2: no joinNodeStatsEstimate line in ranked evidence)")

	nodeMetrics := map[string]*NodeMetrics{"1772": {PlanNodeId: "1772"}}
	enrichA3Evidence(root, nodeMetrics, nil, extra, planNodes, th)
	require.Len(t, root.Evidence, 6, "the fingerprint item is inserted, not swapped in")

	report := &Report{
		QueryId: "q1", State: "FAILED",
		Verdict:    &Verdict{Category: CatPlanPath, CategoryName: "Plan path", NodeId: "1772", Confidence: Confidence{Level: "HIGH"}},
		Categories: []*CategoryResult{root},
		Evidence:   root.Evidence,
	}
	md := report.RenderMarkdown(defaultTop)
	assert.Contains(t, md, "NDV fingerprint", "the fingerprint sub-table must render within the default --top even with a RunawayComputation line also present")
	assert.Contains(t, md, "order_id_52 | 314 | 98319154 | NaN")
	// The RunawayComputation line (originally item 6, now item 7 after the
	// fingerprint's insertion) is the one legitimately truncated by the
	// default top-5 -- proving this is a real truncation (not a no-op top),
	// while the higher-priority fingerprint survives it.
	assert.NotContains(t, md, "RunawayComputation: stage S9", "sanity check that truncation is genuinely happening at top=5")
}

// nodeMetricsFor builds a minimal *NodeMetrics with the actual in/out and
// operator-type data ExtractFanoutChain needs.
func nodeMetricsFor(planNodeId string, in, out int64, ops ...string) *NodeMetrics {
	inCopy, outCopy := in, out
	return &NodeMetrics{PlanNodeId: planNodeId, ActualIn: &inCopy, ActualOut: &outCopy, OperatorTypes: ops}
}

// nodeMetricsForChain is nodeMetricsFor plus the physical pipeline/operator
// position ExtractFanoutChain's tie-break uses to disambiguate a passthrough
// hop from its predecessor when they coincidentally report the same value.
func nodeMetricsForChain(planNodeId string, in, out int64, pipelineId, operatorId int, ops ...string) *NodeMetrics {
	m := nodeMetricsFor(planNodeId, in, out, ops...)
	p, o := pipelineId, operatorId
	m.PipelineId = &p
	m.OperatorId = &o
	return m
}

// TestExtractFanoutChainDifferentNodeIds covers UT-11/FT-12 (design.md §10
// item 11): the fan-out chain algorithm is driven entirely by data (actual
// in/out linkage + operator-type pattern matching), never by hard-coded node
// ids -- this fixture uses completely different, arbitrary node ids/stage
// numbers than any real sample to prove genericity, plus negative cases
// (live/no-actuals, non-join verdict, wrong stage, unrelated join in the same
// stage that must NOT be pulled into the chain).
func TestExtractFanoutChainDifferentNodeIds(t *testing.T) {
	nodeStageId := map[string]int{
		"n-scan": 3, "n-alpha": 3, "n-beta": 3, "n-gamma": 3, "n-omega": 3,
		"n-unrelated": 3, "n-other-stage": 7,
	}
	nodeMetrics := map[string]*NodeMetrics{
		// A 3-hop chain ending at the verdict node "n-omega", with arbitrary
		// (non-sequential, non-numeric-lookalike) ids and a passthrough hop
		// ("n-beta", 1.0x) whose output value COINCIDENTALLY equals its own
		// predecessor's output -- disambiguated only by pipeline+operatorId
		// order (same pipeline 0, increasing operatorId 1/2/3/4), the real
		// case verified against actual data (a node like 1778 sits directly
		// between two others while reporting the same value as one of them).
		"n-alpha": nodeMetricsForChain("n-alpha", 500, 40000, 0, 1, "HashBuilderOperator", "LookupJoinOperator"),
		"n-beta":  nodeMetricsForChain("n-beta", 40000, 40000, 0, 2, "HashBuilderOperator", "LookupJoinOperator"), // 1.0x passthrough hop
		"n-gamma": nodeMetricsForChain("n-gamma", 40000, 9_000_000, 0, 3, "HashBuilderOperator", "LookupJoinOperator"),
		"n-omega": nodeMetricsForChain("n-omega", 9_000_000, 4_000_000_000, 0, 4, "HashBuilderOperator", "LookupJoinOperator"),
		// A join in the SAME stage whose output does not feed n-alpha -- must
		// not be pulled into the chain by accident.
		"n-unrelated": nodeMetricsFor("n-unrelated", 10, 999_999, "HashBuilderOperator", "LookupJoinOperator"),
		// A join with the matching in/out linkage but in a DIFFERENT stage --
		// must not be pulled in either.
		"n-other-stage": nodeMetricsFor("n-other-stage", 1, 500, "LookupJoinOperator"),
	}

	chain := ExtractFanoutChain(nodeMetrics, nodeStageId, "n-omega")
	require.Len(t, chain, 4)
	gotIds := make([]string, len(chain))
	for i, c := range chain {
		gotIds[i] = c.PlanNodeId
	}
	assert.Equal(t, []string{"n-alpha", "n-beta", "n-gamma", "n-omega"}, gotIds, "chain must be in upstream-to-downstream join order, ending at the verdict node")
	assert.InDelta(t, 80.0, chain[0].Fanout, 1e-9)
	assert.InDelta(t, 1.0, chain[1].Fanout, 1e-9)
	assert.InDelta(t, 225.0, chain[2].Fanout, 1e-9)
	assert.InDelta(t, 444.44, chain[3].Fanout, 0.01)
	for _, c := range chain {
		assert.NotContains(t, []string{"n-unrelated", "n-other-stage"}, c.PlanNodeId)
	}

	t.Run("no actuals (live/snapshot-only) -> nil, no crash", func(t *testing.T) {
		live := map[string]*NodeMetrics{"n-omega": {PlanNodeId: "n-omega", OperatorTypes: []string{"LookupJoinOperator"}}}
		assert.Nil(t, ExtractFanoutChain(live, nodeStageId, "n-omega"))
	})

	t.Run("verdict node is not a join -> nil", func(t *testing.T) {
		nonJoin := map[string]*NodeMetrics{"n-scan": nodeMetricsFor("n-scan", 100, 100, "TableScanOperator")}
		assert.Nil(t, ExtractFanoutChain(nonJoin, nodeStageId, "n-scan"))
	})

	t.Run("verdict node id unknown -> nil", func(t *testing.T) {
		assert.Nil(t, ExtractFanoutChain(nodeMetrics, nodeStageId, "does-not-exist"))
	})

	t.Run("verdict node has no stage mapping -> nil", func(t *testing.T) {
		assert.Nil(t, ExtractFanoutChain(nodeMetrics, map[string]int{}, "n-omega"))
	})

	t.Run("single-node chain (no predecessor found) -> chain of one", func(t *testing.T) {
		solo := map[string]*NodeMetrics{"n-omega": nodeMetricsFor("n-omega", 9_000_000, 4_000_000_000, "LookupJoinOperator")}
		soloStage := map[string]int{"n-omega": 3}
		chain := ExtractFanoutChain(solo, soloStage, "n-omega")
		require.Len(t, chain, 1)
		assert.Equal(t, "n-omega", chain[0].PlanNodeId)
	})
}

// TestRenderFanoutChainTableGolden covers UT-14: the fan-out chain md table
// renders with the exact node ids/values passed in, from data, never a
// hard-coded literal. Round-5 (design.md §5.4.1 A3(iii)/§10 item 14): the
// table now carries the enriched build/joinKeys/sqlLine/buildRows-keyNdv/
// kind columns, with the dimensionNonUnique per-row hint appearing verbatim
// ONLY on a flagged row; a row with no enrichment data at all still renders
// every cell non-blank (graceful "…"/"?" placeholders). Round-6 (§10 item
// 15): an emptyBuild+deadJoin row renders both labels combined with " · ",
// the deadJoin hint verbatim, and -- when a chainFanout is supplied -- the
// trailing end-to-end summary line.
func TestRenderFanoutChainTableGolden(t *testing.T) {
	sqlLine14, sqlLine15 := 14, 15
	buildRowsDetail, buildRowsDim, buildRowsEmpty := int64(96_000_000), int64(1792), int64(0)
	ndvDetail, ndvDim, ndvEmpty := 64352.0, 115.0, 126.0
	dupFactorDetail, dupFactorDim := buildRowsDetail/int64(ndvDetail), float64(buildRowsDim)/ndvDim
	chain := []FanoutChainNode{
		{
			PlanNodeId: "n-alpha", Operator: "HashBuilderOperator+LookupJoinOperator", In: 500, Out: 40000, Fanout: 80,
			Build: "ng_delivery.delivery_order_order_provider", JoinKeys: []string{"id=order_id"}, SqlLine: &sqlLine14,
			BuildRows: &buildRowsDetail, BuildRowsSource: "measured", BuildKeyNdv: &ndvDetail,
			BuildKeyDupFactor: floatPtr(float64(dupFactorDetail)), BuildKind: "detail1toN",
		},
		{
			PlanNodeId: "n-mid", Operator: "HashBuilderOperator+LookupJoinOperator", In: 1_000, Out: 18_600, Fanout: 18.6,
			Build: "ng_public.admin_system_city", JoinKeys: []string{"city_id=expr_22"}, SqlLine: &sqlLine15,
			BuildRows: &buildRowsDim, BuildRowsSource: "estimate", BuildKeyNdv: &ndvDim,
			BuildKeyDupFactor: &dupFactorDim, BuildKind: "dimensionNonUnique",
		},
		{
			PlanNodeId: "n-empty", Operator: "HashBuilderOperator+LookupJoinOperator", In: 100, Out: 100, Fanout: 1.0,
			Build: "looker_scratch.lr_admin_system_country", JoinKeys: []string{"country_code=code"},
			BuildRows: &buildRowsEmpty, BuildRowsSource: "measured", BuildKeyNdv: &ndvEmpty,
			BuildKind: "emptyBuild", DeadJoin: true,
		},
		{PlanNodeId: "n-omega", Operator: "HashBuilderOperator+LookupJoinOperator", In: 9_000_000, Out: 4_000_000_000, Fanout: 444.44},
	}
	cf := &ChainFanout{In: 500, Out: 4_000_000_000, EndToEnd: 8_000_000, NodeProduct: floatPtr(7_995_555)}
	table := renderFanoutChainTable(chain, cf)
	assert.Contains(t, table, "fan-out chain")
	assert.Contains(t, table, "| node | fan-out | build (join key, line) | buildRows/keyNdv | kind |")
	assert.Contains(t, table, "| n-alpha | 80.0x | ng_delivery.delivery_order_order_provider (id=order_id, L14) | 96000000 / 64352 | detail1toN |")
	assert.Contains(t, table, "| n-mid | 18.6x | ng_public.admin_system_city (city_id=expr_22, L15) | 1792 / 115 | ⚠ dimensionNonUnique — "+dimensionNonUniqueHint+" |")
	// Round 6: emptyBuild (no dupFactor -- the cell shows "0 / 126" with no
	// dupFactor rendered anywhere) combined with the deadJoin hint, joined by
	// " · ", both verbatim.
	assert.Contains(t, table, "| n-empty | 1.0x | looker_scratch.lr_admin_system_country (country_code=code) | 0 / 126 | emptyBuild · ⚠ deadJoin — "+deadJoinHint+" |")
	// A row with none of the round-5/6 fields populated (e.g. a live/
	// estimate-less fixture) still renders every cell as a non-blank
	// placeholder -- never an empty/missing column.
	assert.Contains(t, table, "| n-omega | 444.4x |  (?) | … | … |")
	// Round 6: the chain fan-out end-to-end summary line, scientific notation
	// at/above 1e4, with the corroboration clause since NodeProduct is set.
	assert.Contains(t, table, "chain fan-out end-to-end ≈ 8.00e6x (500 → 4,000,000,000 rows across 4 joins; per-node product corroborates)")

	t.Run("nil chainFanout -> no trailing line at all", func(t *testing.T) {
		table := renderFanoutChainTable(chain, nil)
		assert.NotContains(t, table, "chain fan-out end-to-end")
	})
}

// TestRenderChainFanoutLineFormatting covers UT-14's number-formatting
// helpers directly: below-1e4 ratios render in plain decimal (never
// scientific), NodeProduct==nil drops the corroboration clause, and negative
// int64s still get comma-grouped correctly.
func TestRenderChainFanoutLineFormatting(t *testing.T) {
	assert.Equal(t, "9999.00", formatChainFanoutRatio(9999))
	assert.Equal(t, "1.00e4", formatChainFanoutRatio(10000))
	assert.Equal(t, "2.55e6", formatChainFanoutRatio(2552763.0))

	assert.Equal(t, "445,201,868,645", formatWithCommas(445201868645))
	assert.Equal(t, "123", formatWithCommas(123))
	assert.Equal(t, "-1,234", formatWithCommas(-1234))
	assert.Equal(t, "0", formatWithCommas(0))

	line := renderChainFanoutLine(&ChainFanout{In: 1, Out: 2, EndToEnd: 2}, 2)
	assert.Equal(t, "chain fan-out end-to-end ≈ 2.00x (1 → 2 rows across 2 joins)", line, "no NodeProduct -> the corroboration clause must be dropped entirely")
}
