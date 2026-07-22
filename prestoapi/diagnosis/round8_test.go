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
	"math"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"pbench/prestoapi/plan_node"
)

// ---------------------------------------------------------------------------
// UT-11 round-8 R8-A: result-correctness-risk classification.
// ---------------------------------------------------------------------------

// TestResultCorrectnessRisk_FilterMaskMustFlag is the review-C1 case: a mask
// that does NOT trace to any MarkDistinctNode.markerVariable (e.g. one
// produced by ImplementFilteredAggregations for a plain SQL FILTER) confers
// NO duplicate-insensitivity -- it must classify SENSITIVE, never silently
// treated as DISTINCT-qualified just because a `mask` field is present.
func TestResultCorrectnessRisk_FilterMaskMustFlag(t *testing.T) {
	j := &RawPlanNode{Type: ".JoinNode", Id: "j1", Kind: "INNER",
		Left:  &RawPlanNode{Type: ".TableScanNode", Id: "probe"},
		Right: &RawPlanNode{Type: ".TableScanNode", Id: "build"},
	}
	agg := &RawPlanNode{Type: ".AggregationNode", Id: "agg1", Step: "SINGLE", Source: j,
		Aggregations: map[string]RawAggregation{
			"filtered_sum<double>": {Call: RawCall{DisplayName: "sum"}, Mask: &RawVariable{Name: "filter_mask_not_a_marker"}},
		},
	}
	idx := buildIndexFromStages(map[string]*RawPlanNode{"1": agg})
	flagged, hedged := classifyResultCorrectnessRisk(idx, nil, "j1", DefaultThresholds())
	assert.True(t, flagged["sum"], "a FILTER-produced mask must classify SENSITIVE, not insensitive")
	assert.False(t, hedged)
}

// TestResultCorrectnessRisk_MarkDistinctTraceInsensitive is the positive
// mirror: a mask that DOES trace to a MarkDistinctNode.markerVariable
// (Presto's own COUNT(DISTINCT) lowering) is genuinely DISTINCT-qualified and
// must NOT be flagged.
func TestResultCorrectnessRisk_MarkDistinctTraceInsensitive(t *testing.T) {
	j := &RawPlanNode{Type: ".JoinNode", Id: "j1", Kind: "INNER",
		Left:  &RawPlanNode{Type: ".TableScanNode", Id: "probe"},
		Right: &RawPlanNode{Type: ".TableScanNode", Id: "build"},
	}
	agg := &RawPlanNode{Type: ".AggregationNode", Id: "agg1", Step: "SINGLE", Source: j,
		Aggregations: map[string]RawAggregation{
			"distinct_count<bigint>": {Call: RawCall{DisplayName: "count"}, Mask: &RawVariable{Name: "mask1"}},
		},
	}
	markDistinct := &RawPlanNode{Type: ".MarkDistinctNode", Id: "md1", MarkerVariable: &RawVariable{Name: "mask1"}}
	idx := buildIndexFromStages(map[string]*RawPlanNode{"1": agg, "99": markDistinct})
	flagged, hedged := classifyResultCorrectnessRisk(idx, nil, "j1", DefaultThresholds())
	assert.Empty(t, flagged, "a mask tracing to a genuine MarkDistinctNode marker must never be flagged")
	assert.False(t, hedged)
}

// TestResultCorrectnessRisk_AllowListAndUnknownSensitive covers the fixed
// allow-list (min/max/bool_or/bool_and/approx_distinct/arbitrary/any_value/
// max_by/min_by never flagged regardless of DISTINCT) and the fail-safe
// default (an unrecognized function name classifies SENSITIVE, never
// silently ignored).
func TestResultCorrectnessRisk_AllowListAndUnknownSensitive(t *testing.T) {
	j := &RawPlanNode{Type: ".JoinNode", Id: "j1", Kind: "INNER",
		Left:  &RawPlanNode{Type: ".TableScanNode", Id: "probe"},
		Right: &RawPlanNode{Type: ".TableScanNode", Id: "build"},
	}
	agg := &RawPlanNode{Type: ".AggregationNode", Id: "agg1", Step: "SINGLE", Source: j,
		Aggregations: map[string]RawAggregation{
			"a<double>": {Call: RawCall{DisplayName: "max"}},
			"b<double>": {Call: RawCall{DisplayName: "some_unknown_udf"}},
		},
	}
	idx := buildIndexFromStages(map[string]*RawPlanNode{"1": agg})
	flagged, _ := classifyResultCorrectnessRisk(idx, nil, "j1", DefaultThresholds())
	assert.False(t, flagged["max"], "max is on the fixed allow-list -- never flagged")
	assert.True(t, flagged["some_unknown_udf"], "an unknown function must default to SENSITIVE, never silently pass")
}

// TestResultCorrectnessRisk_PartialSingleStepOnly is the review-C2 case
// (the 15_118 negative's own shape): only SINGLE/PARTIAL aggregation steps
// are classified; a FINAL/INTERMEDIATE step -- whose inputs are partial
// aggregation STATE, not rows -- must never be examined even when it carries
// an unmasked, otherwise-sensitive function name.
func TestResultCorrectnessRisk_PartialSingleStepOnly(t *testing.T) {
	j := &RawPlanNode{Type: ".JoinNode", Id: "j1", Kind: "INNER",
		Left:  &RawPlanNode{Type: ".TableScanNode", Id: "probe"},
		Right: &RawPlanNode{Type: ".TableScanNode", Id: "build"},
	}
	for _, step := range []string{"FINAL", "INTERMEDIATE"} {
		aggFinal := &RawPlanNode{Type: ".AggregationNode", Id: "aggF", Step: step, Source: j,
			Aggregations: map[string]RawAggregation{"x<bigint>": {Call: RawCall{DisplayName: "count"}}},
		}
		idx := buildIndexFromStages(map[string]*RawPlanNode{"1": aggFinal})
		flagged, _ := classifyResultCorrectnessRisk(idx, nil, "j1", DefaultThresholds())
		assert.Empty(t, flagged, "step %s must never be classified", step)
	}
	for _, step := range []string{"SINGLE", "PARTIAL"} {
		aggOk := &RawPlanNode{Type: ".AggregationNode", Id: "aggOk", Step: step, Source: j,
			Aggregations: map[string]RawAggregation{"x<bigint>": {Call: RawCall{DisplayName: "count"}}},
		}
		idx := buildIndexFromStages(map[string]*RawPlanNode{"1": aggOk})
		flagged, _ := classifyResultCorrectnessRisk(idx, nil, "j1", DefaultThresholds())
		assert.True(t, flagged["count"], "step %s must be classified", step)
	}
}

// TestResultCorrectnessRisk_WindowFunctionsInvisible (review W7): the walk
// only ever inspects AggregationNode.Aggregations -- a WindowNode-shaped
// ancestor between the verdict and the real aggregation is simply climbed
// through transparently (never itself examined, never mistaken for a stop
// point), and the walk still reaches and classifies the genuine
// AggregationNode beyond it.
func TestResultCorrectnessRisk_WindowFunctionsInvisible(t *testing.T) {
	j := &RawPlanNode{Type: ".JoinNode", Id: "j1", Kind: "INNER",
		Left:  &RawPlanNode{Type: ".TableScanNode", Id: "probe"},
		Right: &RawPlanNode{Type: ".TableScanNode", Id: "build"},
	}
	windowNode := &RawPlanNode{Type: ".WindowNode", Id: "win1", Source: j}
	agg := &RawPlanNode{Type: ".AggregationNode", Id: "agg1", Step: "SINGLE", Source: windowNode,
		Aggregations: map[string]RawAggregation{"x<double>": {Call: RawCall{DisplayName: "sum"}}},
	}
	idx := buildIndexFromStages(map[string]*RawPlanNode{"1": agg})
	flagged, _ := classifyResultCorrectnessRisk(idx, nil, "j1", DefaultThresholds())
	assert.True(t, flagged["sum"], "the walk must climb transparently through the WindowNode and still classify the real aggregation")
}

// TestArgumentLineage_W1Qualifier covers the round-8 W1 argument-lineage
// qualifier in full: with exactly ONE fanning spine join, a sensitive
// aggregate whose argument resolves to that join's BUILD side is intended
// 1:N expansion (not flagged); one resolving to the PROBE side is flagged
// full-strength; one that resolves to neither is flagged with the hedged
// wording. With >=2 fanning spine joins, every sensitive aggregate is
// flagged full-strength unconditionally, regardless of lineage.
func TestArgumentLineage_W1Qualifier(t *testing.T) {
	buildScan := &RawPlanNode{Type: ".TableScanNode", Id: "build_scan", OutputVariables: []RawVariable{{Name: "build_amt"}}}
	probeScan := &RawPlanNode{Type: ".TableScanNode", Id: "probe_scan", OutputVariables: []RawVariable{{Name: "probe_amt"}}}
	fanJoin := &RawPlanNode{Type: ".JoinNode", Id: "fan_join", Kind: "INNER", Left: probeScan, Right: buildScan}
	agg := &RawPlanNode{Type: ".AggregationNode", Id: "agg1", Step: "SINGLE", Source: fanJoin,
		Aggregations: map[string]RawAggregation{
			"a<double>": {Call: RawCall{DisplayName: "sum", Arguments: []RawVariable{{Name: "build_amt"}}}},    // build-side
			"b<double>": {Call: RawCall{DisplayName: "avg", Arguments: []RawVariable{{Name: "probe_amt"}}}},    // probe-side
			"c<double>": {Call: RawCall{DisplayName: "stddev", Arguments: []RawVariable{{Name: "unknown_v"}}}}, // unresolved
		},
	}
	idx := buildIndexFromStages(map[string]*RawPlanNode{"1": agg})

	t.Run("exactly one fanning spine join -- real lineage tracing applies", func(t *testing.T) {
		chain := []FanoutChainNode{{PlanNodeId: "fan_join", Fanout: 50}}
		flagged, hedged := classifyResultCorrectnessRisk(idx, chain, "fan_join", DefaultThresholds())
		assert.False(t, flagged["sum"], "sole-fanning-join build-side argument -- intended expansion, not flagged")
		assert.True(t, flagged["avg"], "probe-side argument -- flagged full-strength")
		assert.True(t, flagged["stddev"], "unresolvable lineage -- flagged, hedged")
		assert.True(t, hedged)
	})

	t.Run(">=2 fanning spine joins -- flagged full-strength unconditionally, no lineage tracing needed", func(t *testing.T) {
		chain := []FanoutChainNode{{PlanNodeId: "fan_join", Fanout: 50}, {PlanNodeId: "other_join", Fanout: 20}}
		flagged, hedged := classifyResultCorrectnessRisk(idx, chain, "fan_join", DefaultThresholds())
		assert.True(t, flagged["sum"], "with a second fanning join, even the build-side argument is duplicated by the OTHER join")
		assert.True(t, flagged["avg"])
		assert.True(t, flagged["stddev"])
		assert.False(t, hedged, "the >=2 rule never needs the hedge -- it flags unconditionally")
	})
}

// TestBuildResultCorrectnessRisk_NoteAndSortedUniqueAggregates covers the
// top-level assembly: sorted-unique aggregate names (determinism, review
// W5) and the verbatim §5.1.1 note template.
func TestBuildResultCorrectnessRisk_NoteAndSortedUniqueAggregates(t *testing.T) {
	j := &RawPlanNode{Type: ".JoinNode", Id: "j1", Kind: "INNER",
		Left:  &RawPlanNode{Type: ".TableScanNode", Id: "probe"},
		Right: &RawPlanNode{Type: ".TableScanNode", Id: "build"},
	}
	agg := &RawPlanNode{Type: ".AggregationNode", Id: "agg1", Step: "SINGLE", Source: j,
		Aggregations: map[string]RawAggregation{
			"a<double>": {Call: RawCall{DisplayName: "sum"}},
			"b<double>": {Call: RawCall{DisplayName: "avg"}},
			"c<double>": {Call: RawCall{DisplayName: "avg"}}, // duplicate display name -- must collapse to one
		},
	}
	idx := buildIndexFromStages(map[string]*RawPlanNode{"1": agg})
	risk := buildResultCorrectnessRisk(idx, nil, "j1", DefaultThresholds())
	require.NotNil(t, risk)
	assert.Equal(t, []string{"avg", "sum"}, risk.Aggregates, "sorted, unique display names")
	assert.Equal(t, "j1", risk.NodeId)
	assert.Contains(t, risk.Note, "RESULT-CORRECTNESS RISK: fan-out at node j1 likely INFLATES avg, sum")
	assert.Contains(t, risk.Note, "result may be incorrect (not just slow)")
}

// TestBuildResultCorrectnessRisk_NilWhenNothingFlagged covers the negative
// path: no raw plan data at all, or an aggregation that resolves entirely
// insensitive, both yield a nil result (never an empty-but-non-nil struct).
func TestBuildResultCorrectnessRisk_NilWhenNothingFlagged(t *testing.T) {
	assert.Nil(t, buildResultCorrectnessRisk(nil, nil, "j1", DefaultThresholds()))
	assert.Nil(t, buildResultCorrectnessRisk(&rawPlanIndex{}, nil, "j1", DefaultThresholds()))
}

// ---------------------------------------------------------------------------
// UT-11 round-8 R8-B: deadWeightFanout + precedence over deadJoin.
// ---------------------------------------------------------------------------

func TestDeadWeightFanoutFlag(t *testing.T) {
	leftZeroSurvivors := &RawPlanNode{
		Kind:            "LEFT",
		OutputVariables: []RawVariable{{Name: "a"}, {Name: "b"}},
		Right:           &RawPlanNode{OutputVariables: []RawVariable{{Name: "code"}}},
	}
	th := DefaultThresholds()

	t.Run("LEFT + zero survivors + fanout >= FanoutModerate -> flagged", func(t *testing.T) {
		assert.True(t, deadWeightFanoutFlag(leftZeroSurvivors, th.FanoutModerate, th))
		assert.True(t, deadWeightFanoutFlag(leftZeroSurvivors, th.FanoutModerate*10, th))
	})
	t.Run("fanout below FanoutModerate -> never flagged", func(t *testing.T) {
		assert.False(t, deadWeightFanoutFlag(leftZeroSurvivors, th.FanoutModerate-0.01, th))
	})
	t.Run("INNER join -> never flagged even with a huge fanout", func(t *testing.T) {
		inner := &RawPlanNode{Kind: "INNER", OutputVariables: []RawVariable{{Name: "a"}}, Right: &RawPlanNode{OutputVariables: []RawVariable{{Name: "code"}}}}
		assert.False(t, deadWeightFanoutFlag(inner, 1000, th))
	})
	t.Run("a build-side column survives downstream -> never flagged", func(t *testing.T) {
		survivorJoin := &RawPlanNode{Kind: "LEFT", OutputVariables: []RawVariable{{Name: "code"}}, Right: &RawPlanNode{OutputVariables: []RawVariable{{Name: "code"}}}}
		assert.False(t, deadWeightFanoutFlag(survivorJoin, 1000, th))
	})
	t.Run("nil join -> never flagged", func(t *testing.T) {
		assert.False(t, deadWeightFanoutFlag(nil, 1000, th))
	})
}

// TestEnrichFanoutChainRows_DeadWeightFanoutPrecedenceOverDeadJoin covers
// review W2's exclusivity hardening end-to-end via enrichFanoutChainRows: a
// LEFT join, zero build survivors, MEASURED fanout >= FanoutModerate, and a
// buildKind that would otherwise satisfy deadJoin's own condition (3) must
// render ONLY deadWeightFanout -- never both -- and deadJoin's own measured
// in==out guard must hold on a genuine 1:1 row (drr9h's real row 1778 shape).
func TestEnrichFanoutChainRows_DeadWeightFanoutPrecedenceOverDeadJoin(t *testing.T) {
	th := DefaultThresholds()

	t.Run("fanning LEFT join with zero survivors -> deadWeightFanout, never deadJoin", func(t *testing.T) {
		scan := &RawPlanNode{Type: ".TableScanNode", Id: "scan1", Table: &RawTableHandle{ConnectorHandle: RawConnectorHandle{SchemaName: "s", TableName: "dim"}}, OutputVariables: []RawVariable{{Name: "dim_col"}}}
		join := &RawPlanNode{
			Type: ".JoinNode", Id: "fanning", Kind: "LEFT",
			OutputVariables: []RawVariable{{Name: "probe_col"}}, // build column "dim_col" NOT in this list -- 0 survivors
			Right:           scan,
			Criteria:        []RawJoinCriterion{{Left: RawVariable{Name: "probe_key"}, Right: RawVariable{Name: "dim_col"}}},
		}
		idx := buildIndexFromStages(map[string]*RawPlanNode{"1": join})
		chain := []FanoutChainNode{{PlanNodeId: "fanning", In: 100, Out: 100 * int64(th.FanoutModerate), Fanout: th.FanoutModerate}}
		nodeMetrics := map[string]*NodeMetrics{"scan1": {PlanNodeId: "scan1", ActualIn: int64Ptr(50), ActualOut: int64Ptr(50)}}
		enrichFanoutChainRows(chain, idx, nil, nil, nodeMetrics, th)
		assert.True(t, chain[0].DeadWeightFanout)
		assert.False(t, chain[0].DeadJoin, "deadWeightFanout must suppress deadJoin on the same row")
	})

	t.Run("real 1:1 LEFT join with zero survivors and measured in==out -> deadJoin fires, deadWeightFanout does not", func(t *testing.T) {
		scan := &RawPlanNode{Type: ".TableScanNode", Id: "scan2", Table: &RawTableHandle{ConnectorHandle: RawConnectorHandle{SchemaName: "looker_scratch", TableName: "lr_admin_system_country"}}, OutputVariables: []RawVariable{{Name: "code"}}}
		join := &RawPlanNode{
			Type: ".JoinNode", Id: "dead", Kind: "LEFT",
			OutputVariables: []RawVariable{{Name: "probe_col"}},
			Right:           scan,
			Criteria:        []RawJoinCriterion{{Left: RawVariable{Name: "probe_key"}, Right: RawVariable{Name: "code"}}},
		}
		idx := buildIndexFromStages(map[string]*RawPlanNode{"1": join})
		chain := []FanoutChainNode{{PlanNodeId: "dead", In: 272324086, Out: 272324086, Fanout: 1}}
		nodeMetrics := map[string]*NodeMetrics{"scan2": {PlanNodeId: "scan2", ActualIn: int64Ptr(0), ActualOut: int64Ptr(0)}}
		enrichFanoutChainRows(chain, idx, nil, nil, nodeMetrics, th)
		assert.Equal(t, "emptyBuild", chain[0].BuildKind)
		assert.False(t, chain[0].DeadWeightFanout)
		assert.True(t, chain[0].DeadJoin, "drr9h's real row 1778 shape must still fire deadJoin")
	})
}

// ---------------------------------------------------------------------------
// UT-11 round-8 R8-C: probe-lineage spine walk, annihilation, truncation.
// ---------------------------------------------------------------------------

// TestExtractProbeSpineIds_CrossStageBothDirections builds a small 3-stage
// synthetic plan exercising BOTH the upstream (probe/left) and downstream
// (sink/parent) cross-stage extensions, proving the walk is driven by the
// actual plan topology, not by node-id coincidence.
func TestExtractProbeSpineIds_CrossStageBothDirections(t *testing.T) {
	// Stage "10" (most upstream): a bare scan -- the walk's true source.
	upstreamScan := &RawPlanNode{Type: ".TableScanNode", Id: "u_scan"}

	// Stage "1": verdict join, probe side crosses into stage 10.
	upstreamRemote := &RawPlanNode{Type: ".RemoteSourceNode", Id: "u_remote", SourceFragmentIds: []string{"10"}}
	verdict := &RawPlanNode{Type: ".JoinNode", Id: "verdict", Kind: "INNER", Left: upstreamRemote, Right: &RawPlanNode{Type: ".TableScanNode", Id: "v_build"}}

	// Stage "2": downstream join, probe side crosses INTO stage 1 (verdict's
	// stage) via a RemoteSourceNode -- the downstream extension.
	downstreamRemote := &RawPlanNode{Type: ".RemoteSourceNode", Id: "d_remote", SourceFragmentIds: []string{"1"}}
	downstreamProject := &RawPlanNode{Type: ".ProjectNode", Id: "d_proj", Source: downstreamRemote}
	downstreamJoin := &RawPlanNode{Type: ".JoinNode", Id: "downstream", Kind: "INNER", Left: downstreamProject, Right: &RawPlanNode{Type: ".TableScanNode", Id: "d_build"}}

	idx := buildIndexFromStages(map[string]*RawPlanNode{"10": upstreamScan, "1": verdict, "2": downstreamJoin})

	nodeMetrics := map[string]*NodeMetrics{
		"verdict":    {PlanNodeId: "verdict", OperatorTypes: []string{"LookupJoinOperator"}, ActualIn: int64Ptr(100), ActualOut: int64Ptr(1000)},
		"downstream": {PlanNodeId: "downstream", OperatorTypes: []string{"LookupJoinOperator"}, ActualIn: int64Ptr(1000), ActualOut: int64Ptr(5000)},
	}
	ids := extractProbeSpineIds(idx, nodeMetrics, "verdict")
	require.Equal(t, []string{"verdict", "downstream"}, ids, "probe order: verdict (source-nearest here) then the downstream cross-stage join")
}

// TestExtractProbeSpineIds_StopsAtMultiSourceRemote covers review W5: a
// multi-source RemoteSourceNode (naming more than one fragment) is a
// deterministic stop, never a guessed branch.
func TestExtractProbeSpineIds_StopsAtMultiSourceRemote(t *testing.T) {
	multiRemote := &RawPlanNode{Type: ".RemoteSourceNode", Id: "multi", SourceFragmentIds: []string{"5", "6"}}
	verdict := &RawPlanNode{Type: ".JoinNode", Id: "verdict", Kind: "INNER", Left: multiRemote, Right: &RawPlanNode{Type: ".TableScanNode", Id: "v_build"}}
	idx := buildIndexFromStages(map[string]*RawPlanNode{"1": verdict})
	nodeMetrics := map[string]*NodeMetrics{
		"verdict": {PlanNodeId: "verdict", OperatorTypes: []string{"LookupJoinOperator"}, ActualIn: int64Ptr(100), ActualOut: int64Ptr(1000)},
	}
	ids := extractProbeSpineIds(idx, nodeMetrics, "verdict")
	assert.Equal(t, []string{"verdict"}, ids, "a multi-source RemoteSourceNode must stop the upstream walk deterministically")
}

// TestExtractProbeSpineIds_StopsWhenStreamBecomesBuildInput covers the
// explicit termination rule: if the probe stream (this stage's root) would
// otherwise cross into an upstream join's BUILD (right) side, the walk stops
// there rather than continuing.
func TestExtractProbeSpineIds_StopsWhenStreamBecomesBuildInput(t *testing.T) {
	verdict := &RawPlanNode{Type: ".JoinNode", Id: "verdict", Kind: "INNER", Left: &RawPlanNode{Type: ".TableScanNode", Id: "v_probe"}, Right: &RawPlanNode{Type: ".TableScanNode", Id: "v_build"}}
	remote := &RawPlanNode{Type: ".RemoteSourceNode", Id: "remote_to_verdict", SourceFragmentIds: []string{"1"}}
	// The consumer join reads the verdict's stage as its BUILD (Right) side.
	consumer := &RawPlanNode{Type: ".JoinNode", Id: "consumer", Kind: "INNER", Left: &RawPlanNode{Type: ".TableScanNode", Id: "c_probe"}, Right: remote}

	idx := buildIndexFromStages(map[string]*RawPlanNode{"1": verdict, "2": consumer})
	nodeMetrics := map[string]*NodeMetrics{
		"verdict":  {PlanNodeId: "verdict", OperatorTypes: []string{"LookupJoinOperator"}, ActualIn: int64Ptr(100), ActualOut: int64Ptr(1000)},
		"consumer": {PlanNodeId: "consumer", OperatorTypes: []string{"LookupJoinOperator"}, ActualIn: int64Ptr(1000), ActualOut: int64Ptr(5000)},
	}
	ids := extractProbeSpineIds(idx, nodeMetrics, "verdict")
	assert.Equal(t, []string{"verdict"}, ids, "the walk must stop rather than follow the stream into a BUILD-side edge")
}

// TestExtractProbeSpineIds_FallsBackWhenNoRawPlan covers the graceful
// degradation: no raw structured plan at all (idx has zero entries) yields
// nil, and callers (enrichA3Evidence) fall back to the legacy
// same-stage numeric-matching ExtractFanoutChain in that case.
func TestExtractProbeSpineIds_FallsBackWhenNoRawPlan(t *testing.T) {
	idx := &rawPlanIndex{byNodeId: map[string]*RawPlanNode{}}
	nodeMetrics := map[string]*NodeMetrics{
		"verdict": {PlanNodeId: "verdict", OperatorTypes: []string{"LookupJoinOperator"}, ActualIn: int64Ptr(100), ActualOut: int64Ptr(1000)},
	}
	assert.Nil(t, extractProbeSpineIds(idx, nodeMetrics, "verdict"))
}

// TestAnnihilationEvidenceLines covers the R8-C annihilation-line rule: an
// INNER join against a measured-empty build whose measured out==0 gets its
// own dedicated evidence line; ordinary rows (including a LEFT+emptyBuild
// row, which legitimately has fanout 1.0/out>0) never produce one.
func TestAnnihilationEvidenceLines(t *testing.T) {
	chain := []FanoutChainNode{
		{PlanNodeId: "ordinary", In: 100, Out: 500, Fanout: 5, kind: "INNER"},
		{PlanNodeId: "left_empty", In: 100, Out: 100, Fanout: 1, kind: "LEFT", BuildKind: "emptyBuild"},
		{PlanNodeId: "annihilated", In: 445201868645, Out: 0, Fanout: 0, kind: "INNER", BuildKind: "emptyBuild", Build: "looker_scratch.lr_provider_provider"},
	}
	lines := annihilationEvidenceLines(chain)
	require.Len(t, lines, 1)
	assert.Contains(t, lines[0], "INNER join annihilated")
	assert.Contains(t, lines[0], "lr_provider_provider")
	assert.Contains(t, lines[0], "0 output")
}

// TestComputeChainFanout_TruncatesAtLastPositiveOut covers the round-8
// truncation rule: a trailing annihilation row (out==0) must never zero the
// end-to-end headline -- endToEnd/nodeProduct are computed over the prefix
// ending at the LAST row with out>0.
func TestComputeChainFanout_TruncatesAtLastPositiveOut(t *testing.T) {
	chain := []FanoutChainNode{
		{PlanNodeId: "a", In: 174_400, Out: 14_609_408, Fanout: 83.76954128440367},
		{PlanNodeId: "b", In: 14_609_408, Out: 272_324_086, Fanout: 18.640323139719282},
		{PlanNodeId: "c", In: 272_324_086, Out: 272_324_086, Fanout: 1},
		{PlanNodeId: "d", In: 272_324_086, Out: 599_440_009, Fanout: 2.2012008478750573},
		{PlanNodeId: "e", In: 599_440_009, Out: 445_201_868_645, Fanout: 742.696286468593},
		{PlanNodeId: "f", In: 445_201_868_645, Out: 0, Fanout: 0}, // annihilation row
	}
	cf := computeChainFanout(chain)
	require.NotNil(t, cf)
	assert.Equal(t, int64(174_400), cf.In)
	assert.Equal(t, int64(445_201_868_645), cf.Out, "out must be the LAST row with out>0 (e), never the literal last row (f, out=0)")
	assert.InDelta(t, 2552763.0082855504, cf.EndToEnd, 1e-3)
	require.NotNil(t, cf.NodeProduct)
	assert.InDelta(t, 2552763.0082855504, *cf.NodeProduct, 1e-3, "nodeProduct must also be computed over the truncated range")
}

// TestComputeChainFanout_NilWhenNoRowHasPositiveOut is the defensive edge:
// if EVERY row is annihilated, there is nothing meaningful to report.
func TestComputeChainFanout_NilWhenNoRowHasPositiveOut(t *testing.T) {
	chain := []FanoutChainNode{
		{PlanNodeId: "a", In: 100, Out: 0, Fanout: 0},
		{PlanNodeId: "b", In: 100, Out: 0, Fanout: 0},
	}
	assert.Nil(t, computeChainFanout(chain))
}

// ---------------------------------------------------------------------------
// UT-11 round-8 R8-D: skewCause per-column attribution.
// ---------------------------------------------------------------------------

// TestComputeSkewCause_AnyComponentDegenerateFires covers the C5 multi-column
// rule end-to-end: a two-component partition key where ONE component traces
// to an emptyBuild-LEFT join's output (degenerate) and the OTHER shows
// non-degenerate pre-join stats -- the note fires with BOTH per-column facts,
// but does NOT claim the full single-partition mechanism (not all degenerate).
func TestComputeSkewCause_AnyComponentDegenerateFires(t *testing.T) {
	// Child fragment "4" (producing stage) emits partitioning key [colA, colB].
	childRoot := &RawPlanNode{Type: ".ProjectNode", Id: "child_root"}
	// Skewed stage "3": one RemoteSourceNode reading the child fragment.
	remote := &RawPlanNode{Type: ".RemoteSourceNode", Id: "remote1", SourceFragmentIds: []string{"4"}}
	skewedRoot := &RawPlanNode{Type: ".MarkDistinctNode", Id: "skewed_root", Source: remote}

	idx := buildIndexFromStages(map[string]*RawPlanNode{"3": skewedRoot, "4": childRoot})
	idx.partitioningByStage["4"] = &RawPartitioningScheme{Partitioning: &RawPartitioning{Arguments: []RawVariable{{Name: "colA"}, {Name: "colB"}}}}

	// colA traces to the build side of a LEFT join flagged emptyBuild on the chain.
	leftJoin := &RawPlanNode{Type: ".JoinNode", Id: "left_join", Kind: "LEFT", Right: &RawPlanNode{OutputVariables: []RawVariable{{Name: "colA"}}}}
	idx.byNodeId["left_join"] = leftJoin
	chain := []FanoutChainNode{{PlanNodeId: "left_join", BuildKind: "emptyBuild", Build: "looker_scratch.lr_dim", buildScanNodeId: "dim_scan"}}

	estimates := map[string]*NodeEstimate{
		"some_node": {VariableStatistics: map[string]plan_node.VariableStatistics{
			"colB<integer>": vstat(1, 1000, 189120),
		}},
	}

	c2 := &CategoryResult{Category: CatSkew, Severity: 3, StageId: "3"}
	res := computeSkewCause(c2, idx, chain, estimates, DefaultThresholds())
	require.NotEmpty(t, res.note)
	assert.Contains(t, res.note, "colA")
	assert.Contains(t, res.note, "traces to the empty")
	assert.Contains(t, res.note, "colB")
	assert.Contains(t, res.note, "non-degenerate")
	assert.NotContains(t, res.note, "single-partition mechanism", "not ALL components are degenerate -- must not over-claim")
	require.Len(t, res.implicated, 1)
	assert.Equal(t, "dim_scan", res.implicated[0].planNodeId)
}

// TestComputeSkewCause_AllComponentsDegenerateClaimsMechanism covers the
// full-claim boundary: when EVERY resolved component is degenerate, the note
// additionally claims the single-partition mechanism.
func TestComputeSkewCause_AllComponentsDegenerateClaimsMechanism(t *testing.T) {
	remote := &RawPlanNode{Type: ".RemoteSourceNode", Id: "remote1", SourceFragmentIds: []string{"4"}}
	skewedRoot := &RawPlanNode{Type: ".MarkDistinctNode", Id: "skewed_root", Source: remote}
	childRoot := &RawPlanNode{Type: ".ProjectNode", Id: "child_root"}
	idx := buildIndexFromStages(map[string]*RawPlanNode{"3": skewedRoot, "4": childRoot})
	idx.partitioningByStage["4"] = &RawPartitioningScheme{Partitioning: &RawPartitioning{Arguments: []RawVariable{{Name: "colA"}}}}

	leftJoin := &RawPlanNode{Type: ".JoinNode", Id: "left_join", Kind: "LEFT", Right: &RawPlanNode{OutputVariables: []RawVariable{{Name: "colA"}}}}
	idx.byNodeId["left_join"] = leftJoin
	chain := []FanoutChainNode{{PlanNodeId: "left_join", BuildKind: "emptyBuild", Build: "looker_scratch.lr_dim", buildScanNodeId: "dim_scan"}}

	c2 := &CategoryResult{Category: CatSkew, Severity: 3, StageId: "3"}
	res := computeSkewCause(c2, idx, chain, nil, DefaultThresholds())
	require.NotEmpty(t, res.note)
	assert.Contains(t, res.note, "single-partition mechanism")
}

// TestComputeSkewCause_GatedOnC2Severity covers the round-7 gate: skewCause
// is evaluated ONLY when C2 survived the round-7 magnitude gate (severity >
// 0) -- never reached on the healthy fixture's own C2==0 result.
func TestComputeSkewCause_GatedOnC2Severity(t *testing.T) {
	c2 := &CategoryResult{Category: CatSkew, Severity: 0, StageId: "3"}
	res := computeSkewCause(c2, &rawPlanIndex{}, nil, nil, DefaultThresholds())
	assert.Empty(t, res.note)
	assert.Nil(t, res.implicated)
}

// ---------------------------------------------------------------------------
// UT-11 round-8 R8-E: dataFreshnessRisks implication gate.
// ---------------------------------------------------------------------------

// TestComputeDataFreshnessRisks_ImplicationGate covers the core rule: an
// input on the fan-out chain (emptyBuild) is surfaced; an input that is NOT
// on the chain/skew trace at all is never surfaced, even if it would
// otherwise qualify -- the healthy fixture's own real-but-unimplicated empty
// scan is the motivating negative case (asserted against the real fixture in
// the FT-15c test below).
func TestComputeDataFreshnessRisks_ImplicationGate(t *testing.T) {
	th := DefaultThresholds()
	rows0 := int64(0)
	chain := []FanoutChainNode{
		{PlanNodeId: "j1", Build: "looker_scratch.lr_a", buildScanNodeId: "22", BuildKind: "emptyBuild", BuildRows: &rows0},
		{PlanNodeId: "j2", Build: "ng.dim_b", buildScanNodeId: "9", BuildKind: "detail1toN", BuildRows: int64Ptr(1000), BuildKeyNdv: floatPtr(1)},
	}
	risks := computeDataFreshnessRisks(chain, nil, th)
	require.Len(t, risks, 2)
	// Sorted by NUMERIC planNodeId (review W5): "9" before "22".
	assert.Equal(t, "9", risks[0].PlanNodeId)
	assert.Equal(t, "ndvCollapsed", risks[0].Reason)
	assert.Equal(t, "22", risks[1].PlanNodeId)
	assert.Equal(t, "emptyScan", risks[1].Reason)
}

// TestComputeDataFreshnessRisks_SkewTraceImplicationAndDedup covers the
// third implication channel (an R8-D skew trace) and de-duplication when the
// same table is implicated by more than one channel.
func TestComputeDataFreshnessRisks_SkewTraceImplicationAndDedup(t *testing.T) {
	th := DefaultThresholds()
	rows0 := int64(0)
	chain := []FanoutChainNode{
		{PlanNodeId: "j1", Build: "looker_scratch.lr_a", buildScanNodeId: "22", BuildKind: "emptyBuild", BuildRows: &rows0},
	}
	skewImplicated := []implicatedInput{
		{table: "looker_scratch.lr_a", planNodeId: "22", reason: "emptyScan", rows: 0}, // duplicate of the chain entry
		{table: "looker_scratch.lr_b", planNodeId: "30", reason: "emptyScan", rows: 0}, // skew-only, not on the chain
	}
	risks := computeDataFreshnessRisks(chain, skewImplicated, th)
	require.Len(t, risks, 2, "the duplicate (node 22) must be deduplicated, not double-counted")
	ids := []string{risks[0].PlanNodeId, risks[1].PlanNodeId}
	assert.Contains(t, ids, "22")
	assert.Contains(t, ids, "30")
}

// ---------------------------------------------------------------------------
// UT-11 round-8 R8-F: progress-at-failure, informational-only.
// ---------------------------------------------------------------------------

func TestProgressAtFailureLine(t *testing.T) {
	th := DefaultThresholds()
	t.Run("at/above NearMissProgressMin -- the factual template, explicitly NOT a near-miss claim", func(t *testing.T) {
		line := progressAtFailureLine(98.0697130554215, th.NearMissProgressMin)
		assert.Contains(t, line, "98.1% of scheduled work completed")
		assert.Contains(t, line, "likely close to finishing")
		assert.Contains(t, line, "driver progress is not time-remaining")
	})
	t.Run("below NearMissProgressMin -- the bare number, no interpretation clause at all", func(t *testing.T) {
		line := progressAtFailureLine(60.21857923497268, th.NearMissProgressMin)
		assert.Equal(t, "progress at failure: 60.2% of scheduled work completed", line)
		assert.NotContains(t, line, "close to finishing")
	})
	t.Run("exactly at the threshold fires the factual template", func(t *testing.T) {
		line := progressAtFailureLine(th.NearMissProgressMin, th.NearMissProgressMin)
		assert.Contains(t, line, "close to finishing")
	})
}

// ---------------------------------------------------------------------------
// UT-14: render goldens for the round-8 fields, camelCase + omitempty.
// ---------------------------------------------------------------------------

// TestRound8FieldsCamelCaseAndOmitempty covers UT-14: every new round-8 JSON
// field is camelCase (covered generically by the existing recursive ^[A-Z]
// audit elsewhere, re-asserted here directly against a round-8-populated
// report), and every optional round-8 field is OMITTED (never rendered as a
// zero value) when its source data does not fire.
func TestRound8FieldsCamelCaseAndOmitempty(t *testing.T) {
	report := &Report{
		QueryId: "q1", State: "FAILED",
		Summary: Summary{WallSec: 1, CpuSec: 1, PeakUserMemoryBytes: 1},
		Verdict: &Verdict{Category: CatPlanPath, CategoryName: "Plan path", NodeId: "1544", Summary: "x", Confidence: Confidence{Level: "MEDIUM", Reasons: []string{"x"}}},
	}
	b, err := report.RenderJSON()
	require.NoError(t, err)
	s := string(b)
	assert.NotContains(t, s, "resultCorrectnessRisk")
	assert.NotContains(t, s, "dataFreshnessRisks")
	assert.NotContains(t, s, "progressAtFailure")
	assert.NotContains(t, s, "deadWeightFanout")

	pct := 98.1
	report.Summary.ProgressAtFailure = &pct
	report.ResultCorrectnessRisk = &ResultCorrectnessRisk{NodeId: "1544", Aggregates: []string{"sum"}, Note: "n"}
	report.DataFreshnessRisks = []DataFreshnessRisk{{Table: "t", PlanNodeId: "9", Reason: "emptyScan", Rows: 0}}
	report.FanoutChain = []FanoutChainNode{{PlanNodeId: "1544", DeadWeightFanout: true}}
	b, err = report.RenderJSON()
	require.NoError(t, err)
	s = string(b)
	assert.Contains(t, s, `"progressAtFailure": 98.1`)
	assert.Contains(t, s, `"resultCorrectnessRisk"`)
	assert.Contains(t, s, `"aggregates"`)
	assert.Contains(t, s, `"dataFreshnessRisks"`)
	assert.Contains(t, s, `"planNodeId": "9"`)
	assert.Contains(t, s, `"deadWeightFanout": true`)

	leakRe := mustNoUppercaseLeak(t, b)
	_ = leakRe
}

// mustNoUppercaseLeak decodes b and fails the test if any JSON key matches
// ^[A-Z] anywhere in the structure (the same mechanical audit report_test.go
// already runs, re-applied here to a round-8-populated report specifically).
func mustNoUppercaseLeak(t *testing.T, b []byte) bool {
	var decoded map[string]any
	require.NoError(t, json.Unmarshal(b, &decoded))
	var leaks []string
	walkJSONKeys(decoded, "", func(key, parentKey string) {
		if parentKey == "emergenceByCategory" {
			return
		}
		if len(key) > 0 && key[0] >= 'A' && key[0] <= 'Z' {
			leaks = append(leaks, key)
		}
	})
	assert.Empty(t, leaks, "no round-8 JSON key may start with an uppercase letter")
	return true
}

// ---------------------------------------------------------------------------
// FT-15c: round-8 acceptance against the real ground-truth terminals.
// ---------------------------------------------------------------------------

// ft15cDrr9hPath is deliberately NOT its own literal -- it is the exact same
// fixture as afterFixSamplePath (analyze_test.go), reused by symbol so the
// two never drift if the fixture location ever moves (review S1).
var ft15cDrr9hPath = afterFixSamplePath

const (
	ft15c258463Path  = "/tmp/pbench-cycle/query3/terminal.json"
	ft15c15118Path   = "/tmp/pbench-cycle/query2/terminal.json"
	ft15cHealthyPath = "/tmp/pbench-cycle/diagnosis-healthy/terminal.json"
)

// TestFT15c_258463RoundEightSignals is FT-15c(a): the six round-8 signals
// fire as designed on 258_463, and verdict/severity/confidence stay
// byte-identical to the pre-R8 diagnosis.md (PLAN PATH node 1544, sev 2,
// MEDIUM).
func TestFT15c_258463RoundEightSignals(t *testing.T) {
	raw, err := os.ReadFile(ft15c258463Path)
	if err != nil {
		t.Skip("no real 258_463 sample available; skipping FT-15c(a)")
	}
	report, err := Analyze(raw, DefaultThresholds())
	require.NoError(t, err)
	require.NotNil(t, report.Verdict)

	// Verdict invariance.
	assert.Equal(t, CatPlanPath, report.Verdict.Category)
	assert.Equal(t, "1544", report.Verdict.NodeId)
	var a3 *CategoryResult
	for _, c := range report.Categories {
		if c.Category == CatPlanPath {
			a3 = c
		}
	}
	require.NotNil(t, a3)
	assert.Equal(t, 2, a3.Severity)
	assert.Equal(t, "MEDIUM", report.Verdict.Confidence.Level)

	// R8-A: sum only.
	require.NotNil(t, report.ResultCorrectnessRisk)
	assert.Equal(t, []string{"sum"}, report.ResultCorrectnessRisk.Aggregates)

	// R8-B/R8-C: 5-row spine, node 1544 flagged deadWeightFanout, others not.
	require.Len(t, report.FanoutChain, 5)
	byNode := map[string]FanoutChainNode{}
	for _, row := range report.FanoutChain {
		byNode[row.PlanNodeId] = row
	}
	for _, id := range []string{"1544", "1541", "1538", "1535", "1532"} {
		assert.Contains(t, byNode, id)
	}
	assert.True(t, byNode["1544"].DeadWeightFanout)
	for _, id := range []string{"1541", "1538", "1535", "1532"} {
		assert.False(t, byNode[id].DeadWeightFanout, "node %s must not be flagged", id)
	}
	require.NotNil(t, report.ChainFanout)
	assert.InDelta(t, 16162.386427231784, report.ChainFanout.EndToEnd, 1)

	// R8-D: skewCause per-column note on C2.
	var c2 *CategoryResult
	for _, c := range report.Categories {
		if c.Category == CatSkew {
			c2 = c
		}
	}
	require.NotNil(t, c2)
	assert.Contains(t, c2.Note, "name_259")
	assert.Contains(t, c2.Note, "user_id_269")

	// R8-E: implicated empty PDT.
	require.Len(t, report.DataFreshnessRisks, 1)
	assert.Contains(t, report.DataFreshnessRisks[0].Table, "lr_admin_system_country")

	// R8-F: factual near-miss template (98.1%).
	require.NotNil(t, report.Summary.ProgressAtFailure)
	assert.InDelta(t, 98.1, *report.Summary.ProgressAtFailure, 0.1)
	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "likely close to finishing")
}

// TestFT15c_139346RoundEightSignals is FT-15c(b): drr9h/139_346 -- avg ONLY,
// 6-row chain incl. the 1769 annihilation row + evidence line, chainFanout
// stays 2.55e6x via truncation, both empty PDTs implicated, verdict
// unchanged (sev2/MEDIUM).
func TestFT15c_139346RoundEightSignals(t *testing.T) {
	raw, err := os.ReadFile(ft15cDrr9hPath)
	if err != nil {
		t.Skip("no real 139_346/drr9h sample available; skipping FT-15c(b)")
	}
	report, err := Analyze(raw, DefaultThresholds())
	require.NoError(t, err)
	require.NotNil(t, report.Verdict)

	assert.Equal(t, CatPlanPath, report.Verdict.Category)
	assert.Equal(t, "1772", report.Verdict.NodeId)
	var a3 *CategoryResult
	for _, c := range report.Categories {
		if c.Category == CatPlanPath {
			a3 = c
		}
	}
	require.NotNil(t, a3)
	assert.Equal(t, 2, a3.Severity)
	assert.Equal(t, "MEDIUM", report.Verdict.Confidence.Level)

	require.NotNil(t, report.ResultCorrectnessRisk)
	assert.Equal(t, []string{"avg"}, report.ResultCorrectnessRisk.Aggregates, "the MarkDistinct-masked sums must NOT be named")

	require.Len(t, report.FanoutChain, 6)
	assert.Equal(t, "1769", report.FanoutChain[5].PlanNodeId)
	assert.Equal(t, int64(0), report.FanoutChain[5].Out)

	require.NotNil(t, report.ChainFanout)
	assert.InDelta(t, 2552763.0082855504, report.ChainFanout.EndToEnd, 1, "truncation rule: unaffected by the annihilation row")

	require.Len(t, report.DataFreshnessRisks, 2)
	tables := []string{report.DataFreshnessRisks[0].Table, report.DataFreshnessRisks[1].Table}
	assert.Contains(t, tables, "looker_scratch.lr_admin_system_country")
	assert.Contains(t, tables, "looker_scratch.lr_provider_provider")

	md := report.RenderMarkdown(5)
	assert.Contains(t, md, "annihilated")
	assert.Contains(t, md, "1769")

	require.NotNil(t, report.Summary.ProgressAtFailure)
	assert.InDelta(t, 75.0, *report.Summary.ProgressAtFailure, 0.1)
	assert.NotContains(t, progressAtFailureLine(*report.Summary.ProgressAtFailure, DefaultThresholds().NearMissProgressMin), "close to finishing")
}

// TestFT15c_15118NegativeCase is FT-15c(c): 15_118 -- correctness-risk does
// NOT fire (masked counts insensitive at the PARTIAL step; FINAL counts
// never classified; its `min` is a window function, invisible), verdict
// A3/2066/sev3/HIGH unchanged.
func TestFT15c_15118NegativeCase(t *testing.T) {
	raw, err := os.ReadFile(ft15c15118Path)
	if err != nil {
		t.Skip("no real 15_118 sample available; skipping FT-15c(c)")
	}
	report, err := Analyze(raw, DefaultThresholds())
	require.NoError(t, err)
	require.NotNil(t, report.Verdict)

	assert.Equal(t, CatPlanPath, report.Verdict.Category)
	assert.Equal(t, "2066", report.Verdict.NodeId)
	var a3 *CategoryResult
	for _, c := range report.Categories {
		if c.Category == CatPlanPath {
			a3 = c
		}
	}
	require.NotNil(t, a3)
	assert.Equal(t, 3, a3.Severity)
	assert.Equal(t, "HIGH", report.Verdict.Confidence.Level)

	assert.Nil(t, report.ResultCorrectnessRisk, "15_118 must NOT fire the correctness risk")

	require.NotNil(t, report.Summary.ProgressAtFailure)
	assert.InDelta(t, 60.2, *report.Summary.ProgressAtFailure, 0.1)
}

// TestFT15c_HealthyNoRoundEightSignals is FT-15c(d): none of the six round-8
// signals are present on the healthy re-run -- R8-A/B/C unreachable (non-A3
// verdict), R8-D gated (round-7 floor), R8-E suppressed by the implication
// gate (node-22 empty scan is real but un-implicated), R8-F absent
// (FINISHED) -- round-7 output otherwise unchanged.
func TestFT15c_HealthyNoRoundEightSignals(t *testing.T) {
	raw, err := os.ReadFile(ft15cHealthyPath)
	if err != nil {
		t.Skip("no real healthy sample available; skipping FT-15c(d)")
	}
	report, err := Analyze(raw, DefaultThresholds())
	require.NoError(t, err)
	require.NotNil(t, report.Verdict)

	assert.Equal(t, CatPlanTime, report.Verdict.Category, "round-7 verdict must be unchanged")
	assert.Equal(t, "MEDIUM", report.Verdict.Confidence.Level)
	assert.True(t, report.Summary.Healthy)

	assert.Nil(t, report.ResultCorrectnessRisk)
	assert.Empty(t, report.DataFreshnessRisks, "node 22's real empty scan is un-implicated -- must stay suppressed")
	assert.Nil(t, report.Summary.ProgressAtFailure, "FINISHED -- never set")
	assert.Empty(t, report.FanoutChain)
	assert.Nil(t, report.ChainFanout)
}

// ---------------------------------------------------------------------------
// Additional coverage-targeted unit tests for small round-8 helpers.
// ---------------------------------------------------------------------------

// TestProbeSideVisited covers every branch of the probe-side walk directly:
// nil join/no-left-child, immediate TableScanNode, single-source Exchange
// pass-through, a single-fragment RemoteSourceNode crossing stages, a
// multi-fragment RemoteSourceNode stopping, and an unrecognized/ambiguous
// shape stopping.
func TestProbeSideVisited(t *testing.T) {
	assert.Nil(t, probeSideVisited(nil, &rawPlanIndex{}))
	assert.Nil(t, probeSideVisited(&RawPlanNode{}, &rawPlanIndex{}))

	t.Run("immediate TableScanNode", func(t *testing.T) {
		j := &RawPlanNode{Left: &RawPlanNode{Type: ".TableScanNode", Id: "scan1"}}
		visited := probeSideVisited(j, &rawPlanIndex{})
		require.Len(t, visited, 1)
		assert.Equal(t, "scan1", visited[0].Id)
	})

	t.Run("single-source Exchange pass-through to a TableScanNode", func(t *testing.T) {
		scan := &RawPlanNode{Type: ".TableScanNode", Id: "scan2"}
		exch := &RawPlanNode{Type: ".ExchangeNode", Id: "exch1", Sources: []*RawPlanNode{scan}}
		j := &RawPlanNode{Left: exch}
		visited := probeSideVisited(j, &rawPlanIndex{})
		require.Len(t, visited, 2)
		assert.Equal(t, "exch1", visited[0].Id)
		assert.Equal(t, "scan2", visited[1].Id)
	})

	t.Run("single-fragment RemoteSourceNode crosses into the child stage", func(t *testing.T) {
		childRoot := &RawPlanNode{Type: ".TableScanNode", Id: "child_scan"}
		remote := &RawPlanNode{Type: ".RemoteSourceNode", Id: "remote1", SourceFragmentIds: []string{"9"}}
		j := &RawPlanNode{Left: remote}
		idx := &rawPlanIndex{byStage: map[string]*RawPlanNode{"9": childRoot}}
		visited := probeSideVisited(j, idx)
		require.Len(t, visited, 2)
		assert.Equal(t, "remote1", visited[0].Id)
		assert.Equal(t, "child_scan", visited[1].Id)
	})

	t.Run("multi-fragment RemoteSourceNode stops", func(t *testing.T) {
		remote := &RawPlanNode{Type: ".RemoteSourceNode", Id: "remote2", SourceFragmentIds: []string{"9", "10"}}
		j := &RawPlanNode{Left: remote}
		visited := probeSideVisited(j, &rawPlanIndex{})
		require.Len(t, visited, 1)
		assert.Equal(t, "remote2", visited[0].Id)
	})

	t.Run("unresolved single-fragment RemoteSourceNode stops", func(t *testing.T) {
		remote := &RawPlanNode{Type: ".RemoteSourceNode", Id: "remote3", SourceFragmentIds: []string{"999"}}
		j := &RawPlanNode{Left: remote}
		visited := probeSideVisited(j, &rawPlanIndex{byStage: map[string]*RawPlanNode{}})
		require.Len(t, visited, 1)
	})

	t.Run("ambiguous/unrecognized shape stops", func(t *testing.T) {
		multiSource := &RawPlanNode{Type: ".UnionNode", Id: "u1", Sources: []*RawPlanNode{{Id: "s1"}, {Id: "s2"}}}
		j := &RawPlanNode{Left: multiSource}
		visited := probeSideVisited(j, &rawPlanIndex{})
		require.Len(t, visited, 1)
		assert.Equal(t, "u1", visited[0].Id)
	})
}

// TestAggregationArgumentName covers all three resolution paths: Call's own
// Arguments (preferred), the aggregation entry's own Arguments (fallback),
// and neither present (empty).
func TestAggregationArgumentName(t *testing.T) {
	assert.Equal(t, "x", aggregationArgumentName(RawAggregation{Call: RawCall{Arguments: []RawVariable{{Name: "x"}}}}))
	assert.Equal(t, "y", aggregationArgumentName(RawAggregation{Arguments: []RawVariable{{Name: "y"}}}))
	assert.Equal(t, "", aggregationArgumentName(RawAggregation{}))
}

// TestFormatFingerprintValue2 covers NaN/Inf/-Inf, whole numbers, and
// fractional values.
func TestFormatFingerprintValue2(t *testing.T) {
	assert.Equal(t, "NaN", formatFingerprintValue2(nanValue()))
	assert.Equal(t, "Infinity", formatFingerprintValue2(math.Inf(1)))
	assert.Equal(t, "-Infinity", formatFingerprintValue2(math.Inf(-1)))
	assert.Equal(t, "189120", formatFingerprintValue2(189120))
	assert.Equal(t, "0", formatFingerprintValue2(0))
	assert.Contains(t, formatFingerprintValue2(1.5), "1.5")
}
