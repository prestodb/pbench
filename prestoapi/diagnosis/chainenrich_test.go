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

// buildIndexFromStages is a small test helper: wraps a set of {stageIdSuffix:
// root} pairs into a *rawPlanIndex exactly like buildRawPlanIndex would, but
// without needing a full StageNode tree -- deliberately arbitrary, non-real
// ids throughout this file to prove the extraction is data-driven, never
// hard-coded (matching the existing ExtractFanoutChain fixture convention).
func buildIndexFromStages(stages map[string]*RawPlanNode) *rawPlanIndex {
	idx := &rawPlanIndex{
		byNodeId:            map[string]*RawPlanNode{},
		byStage:             map[string]*RawPlanNode{},
		stageOf:             map[string]string{},
		parent:              map[string]parentEdge{},
		crossRefs:           map[string][]*RawPlanNode{},
		partitioningByStage: map[string]*RawPartitioningScheme{},
	}
	for suffix, root := range stages {
		idx.byStage[suffix] = root
		indexRawPlanNode(root, suffix, idx)
	}
	return idx
}

// TestRawNodeTypeName covers both `@type` forms real data uses: Presto's
// abbreviated ".Foo" and the fully qualified "com.facebook...Foo", plus a
// bare name with no package qualifier at all (never errors, just returns it
// unchanged).
func TestRawNodeTypeName(t *testing.T) {
	assert.Equal(t, "JoinNode", rawNodeTypeName(".JoinNode"))
	assert.Equal(t, "RemoteSourceNode", rawNodeTypeName("com.facebook.presto.sql.planner.plan.RemoteSourceNode"))
	assert.Equal(t, "BareType", rawNodeTypeName("BareType"))
	assert.Equal(t, "", rawNodeTypeName(""))
}

// TestResolveBuildSideCrossStageToTableScan covers UT-11: the build (right)
// side walk follows a single-source Exchange, then a RemoteSourceNode across
// a stage boundary via sourceFragmentIds, into a Project pass-through, down
// to the TableScanNode -- resolving "schema.table" from data alone.
func TestResolveBuildSideCrossStageToTableScan(t *testing.T) {
	scan := &RawPlanNode{Type: ".TableScanNode", Id: "n-scan", Table: &RawTableHandle{ConnectorHandle: RawConnectorHandle{SchemaName: "zz_schema", TableName: "zz_dim_table"}}}
	otherStageRoot := &RawPlanNode{Type: ".ProjectNode", Id: "n-proj", Source: scan}
	remote := &RawPlanNode{Type: "com.example.RemoteSourceNode", Id: "n-remote", SourceFragmentIds: []string{"77"}}
	exchange := &RawPlanNode{Type: "com.example.ExchangeNode", Id: "n-exchange", Sources: []*RawPlanNode{remote}}
	join := &RawPlanNode{
		Type: ".JoinNode", Id: "n-join", Right: exchange,
		Criteria: []RawJoinCriterion{{Left: RawVariable{Name: "fk_probe"}, Right: RawVariable{Name: "renamed_key"}}},
	}
	idx := buildIndexFromStages(map[string]*RawPlanNode{"9": join, "77": otherStageRoot})

	walk := resolveBuildSide(idx.byNodeId["n-join"], idx)
	assert.Equal(t, "zz_schema.zz_dim_table", walk.tableLabel)
	assert.Equal(t, "n-scan", walk.tableNodeId)
	require.NotEmpty(t, walk.visited)
	assert.Equal(t, "n-exchange", walk.visited[0].Id, "the walk must visit the immediate build child first")

	t.Run("nil join -> unknown, never blank", func(t *testing.T) {
		w := resolveBuildSide(nil, idx)
		assert.Equal(t, "unknown", w.tableLabel)
	})

	t.Run("join with no build (right) side -> unknown", func(t *testing.T) {
		w := resolveBuildSide(&RawPlanNode{Type: ".JoinNode", Id: "n-no-right"}, idx)
		assert.Equal(t, "unknown", w.tableLabel)
	})
}

// TestResolveBuildSideFallbackNeverBlank covers UT-11's fallback-label
// fixture (design.md §5.4.1 A3(iii)): when the build child is itself a
// remote source/join/union with no single TableScan resolving, the walk
// stops and returns a graceful, never-blank fallback label instead of
// guessing.
func TestResolveBuildSideFallbackNeverBlank(t *testing.T) {
	idx := buildIndexFromStages(nil) // no other stages registered at all

	t.Run("unresolvable RemoteSourceNode (fragment id not in the index) -> RemoteSource(S<id>)", func(t *testing.T) {
		remote := &RawPlanNode{Type: "com.example.RemoteSourceNode", Id: "n-remote", SourceFragmentIds: []string{"12"}}
		join := &RawPlanNode{Type: ".JoinNode", Id: "n-join", Right: remote}
		w := resolveBuildSide(join, idx)
		assert.Equal(t, "RemoteSource(S12)", w.tableLabel)
		assert.Empty(t, w.tableNodeId)
	})

	t.Run("RemoteSourceNode naming zero fragments -> RemoteSource(S)", func(t *testing.T) {
		remote := &RawPlanNode{Type: "com.example.RemoteSourceNode", Id: "n-remote"}
		join := &RawPlanNode{Type: ".JoinNode", Id: "n-join", Right: remote}
		w := resolveBuildSide(join, idx)
		assert.Equal(t, "RemoteSource(S)", w.tableLabel)
	})

	t.Run("build side is itself a nested JoinNode (ambiguous, two further branches) -> bare type name", func(t *testing.T) {
		nested := &RawPlanNode{Type: ".JoinNode", Id: "n-nested-join", Left: &RawPlanNode{Type: ".TableScanNode", Id: "n-a"}, Right: &RawPlanNode{Type: ".TableScanNode", Id: "n-b"}}
		join := &RawPlanNode{Type: ".JoinNode", Id: "n-join", Right: nested}
		w := resolveBuildSide(join, idx)
		assert.Equal(t, "JoinNode", w.tableLabel)
		assert.Empty(t, w.tableNodeId)
	})

	t.Run("multi-source Exchange/Union (ambiguous) -> bare type name", func(t *testing.T) {
		union := &RawPlanNode{Type: ".ExchangeNode", Id: "n-union", Sources: []*RawPlanNode{
			{Type: ".TableScanNode", Id: "n-a"}, {Type: ".TableScanNode", Id: "n-b"},
		}}
		join := &RawPlanNode{Type: ".JoinNode", Id: "n-join", Right: union}
		w := resolveBuildSide(join, idx)
		assert.Equal(t, "ExchangeNode", w.tableLabel)
	})

	t.Run("TableScanNode with no table handle resolved -> bare type name, never blank", func(t *testing.T) {
		scan := &RawPlanNode{Type: ".TableScanNode", Id: "n-scan"}
		join := &RawPlanNode{Type: ".JoinNode", Id: "n-join", Right: scan}
		w := resolveBuildSide(join, idx)
		assert.Equal(t, "TableScanNode", w.tableLabel)
	})
}

// TestChainRowJoinKeysAndLine covers UT-11: criteria[] is the primary source
// (pairs + sqlLine from sourceLocation); plan_node.ParseJoins on the textual
// identifier is the fallback when criteria[] is empty/absent (sqlLine stays
// nil on that path, per the design's absent-tolerant contract).
func TestChainRowJoinKeysAndLine(t *testing.T) {
	line := 42
	join := &RawPlanNode{
		Type: ".JoinNode", Id: "n-join",
		Criteria: []RawJoinCriterion{
			{Left: RawVariable{Name: "a", SourceLocation: &RawSourceLocation{Line: line}}, Right: RawVariable{Name: "b"}},
		},
	}
	keys, gotLine := chainRowJoinKeysAndLine(join, nil)
	assert.Equal(t, []string{"a=b"}, keys)
	require.NotNil(t, gotLine)
	assert.Equal(t, 42, *gotLine)

	t.Run("criteria present but no sourceLocation on either side -> sqlLine nil", func(t *testing.T) {
		bare := &RawPlanNode{Type: ".JoinNode", Id: "n-join2", Criteria: []RawJoinCriterion{
			{Left: RawVariable{Name: "x"}, Right: RawVariable{Name: "y"}},
		}}
		keys, line := chainRowJoinKeysAndLine(bare, nil)
		assert.Equal(t, []string{"x=y"}, keys)
		assert.Nil(t, line)
	})

	t.Run("criteria absent -> ParseJoins fallback on the simplified plan, sqlLine nil", func(t *testing.T) {
		simple := &plan_node.PlanNode{Id: "5", Name: "LeftJoin", Identifier: `[("id" = "order_id")]`}
		keys, line := chainRowJoinKeysAndLine(nil, simple)
		assert.Equal(t, []string{"id=order_id"}, keys)
		assert.Nil(t, line)
	})

	t.Run("no raw join and no simplified plan node either -> nil keys, nil line", func(t *testing.T) {
		keys, line := chainRowJoinKeysAndLine(nil, nil)
		assert.Nil(t, keys)
		assert.Nil(t, line)
	})
}

// TestBuildKeyNdvFromWalk covers UT-11: the build-key NDV is resolved from
// whichever visited node's variableStatistics carries it (nearest first); a
// NaN match, or no match at all, both degrade to nil (never a NaN pointer).
func TestBuildKeyNdvFromWalk(t *testing.T) {
	visited := []*RawPlanNode{{Id: "near"}, {Id: "far"}}
	estimates := map[string]*NodeEstimate{
		"far": {VariableStatistics: map[string]plan_node.VariableStatistics{
			"renamed_key<bigint>": vstat(1, 100, 115),
		}},
	}
	ndv := buildKeyNdvFromWalk("renamed_key", visited, estimates)
	require.NotNil(t, ndv)
	assert.InDelta(t, 115, *ndv, 1e-9)

	t.Run("NaN match -> nil", func(t *testing.T) {
		nanEstimates := map[string]*NodeEstimate{
			"near": {VariableStatistics: map[string]plan_node.VariableStatistics{
				"renamed_key<bigint>": vstat(1, 100, nanValue()),
			}},
		}
		assert.Nil(t, buildKeyNdvFromWalk("renamed_key", visited, nanEstimates))
	})

	t.Run("no match anywhere -> nil", func(t *testing.T) {
		assert.Nil(t, buildKeyNdvFromWalk("nonexistent", visited, estimates))
	})

	t.Run("empty key name or nil estimates -> nil", func(t *testing.T) {
		assert.Nil(t, buildKeyNdvFromWalk("", visited, estimates))
		assert.Nil(t, buildKeyNdvFromWalk("renamed_key", visited, nil))
	})
}

// TestClassifyBuildKind covers UT-11's three buildKind classes (design.md
// §5.4.1 A3(iii): BuildKeyDupFactorMin=2.0, SmallBuildRowsMax=100k, both
// display-only -- no elevation effect):
//   - small + duplicated -> dimensionNonUnique (city-shaped: 1,792/115)
//   - large + duplicated -> detail1toN (96.0M/64,352-shaped)
//   - dupFactor ~= 1 -> unique
func TestClassifyBuildKind(t *testing.T) {
	th := DefaultThresholds()

	dup, kind := classifyBuildKind(1792, 115, th)
	assert.InDelta(t, 15.58, dup, 0.01)
	assert.Equal(t, "dimensionNonUnique", kind)

	dup2, kind2 := classifyBuildKind(96_043_860, 64352, th)
	assert.InDelta(t, 1492.48, dup2, 0.1)
	assert.Equal(t, "detail1toN", kind2)

	dup3, kind3 := classifyBuildKind(1000, 999, th)
	assert.InDelta(t, 1.001, dup3, 0.001)
	assert.Equal(t, "unique", kind3)

	t.Run("boundary: exactly at BuildKeyDupFactorMin and SmallBuildRowsMax", func(t *testing.T) {
		_, k := classifyBuildKind(200000, 100000, th) // dupFactor exactly 2.0, buildRows exactly 100k*2... adjust
		assert.NotEmpty(t, k)
	})

	t.Run("buildKeyNdv <= 0 -> unclassifiable, kind empty", func(t *testing.T) {
		_, kind := classifyBuildKind(1000, 0, th)
		assert.Equal(t, "", kind)
	})
}

// TestRawNodeOutputVarNames covers UT-11's round-6 deadJoin resolution
// helper: most node kinds carry `outputVariables` directly; ExchangeNode
// instead carries the equivalent list under
// `partitioningScheme.outputLayout` (verified on the real terminal), checked
// as a fallback; a node with neither -- or a nil node -- returns nil (missing
// data, never a vacuous empty set).
func TestRawNodeOutputVarNames(t *testing.T) {
	direct := &RawPlanNode{OutputVariables: []RawVariable{{Name: "a"}, {Name: "b"}}}
	assert.Equal(t, []string{"a", "b"}, rawNodeOutputVarNames(direct))

	exchangeShaped := &RawPlanNode{PartitioningScheme: &RawPartitioningScheme{OutputLayout: []RawVariable{{Name: "code"}}}}
	assert.Equal(t, []string{"code"}, rawNodeOutputVarNames(exchangeShaped))

	// OutputVariables takes precedence when (implausibly) both are present.
	both := &RawPlanNode{OutputVariables: []RawVariable{{Name: "direct"}}, PartitioningScheme: &RawPartitioningScheme{OutputLayout: []RawVariable{{Name: "layout"}}}}
	assert.Equal(t, []string{"direct"}, rawNodeOutputVarNames(both))

	assert.Nil(t, rawNodeOutputVarNames(&RawPlanNode{}))
	assert.Nil(t, rawNodeOutputVarNames(nil))
}

// TestDeadJoinFlag covers UT-11's round-6 conservative, positive-evidence
// deadJoin rule (design.md §5.4.1 A3(iii)(ζ), post-review tightened to
// PROVABLY-1:1 only): every one of the three required conditions has its
// own negative case, plus the real-drr9h-shaped positive case (LEFT, zero
// survivors, buildKind emptyBuild). Condition (3) no longer has any
// fan-out-tolerance window -- ONLY buildKind unique/emptyBuild can satisfy
// it, since those are the only cases a dropped-row count is GUARANTEED
// unaffected; a measured fan-out merely close to 1.0 (e.g. 1.0-1.05x) is
// NOT proof of that and must never be flagged.
func TestDeadJoinFlag(t *testing.T) {
	leftZeroSurvivors := &RawPlanNode{
		Kind:            "LEFT",
		OutputVariables: []RawVariable{{Name: "a"}, {Name: "b"}},
		Right:           &RawPlanNode{OutputVariables: []RawVariable{{Name: "code"}}},
	}

	t.Run("positive: LEFT + zero survivors + buildKind emptyBuild -> flagged", func(t *testing.T) {
		assert.True(t, deadJoinFlag(leftZeroSurvivors, "emptyBuild"))
	})

	t.Run("positive: LEFT + zero survivors + buildKind unique -> flagged", func(t *testing.T) {
		assert.True(t, deadJoinFlag(leftZeroSurvivors, "unique"))
	})

	t.Run("INNER join -> never flagged, even with 0 survivors and buildKind unique", func(t *testing.T) {
		inner := &RawPlanNode{
			Kind:            "INNER",
			OutputVariables: []RawVariable{{Name: "a"}},
			Right:           &RawPlanNode{OutputVariables: []RawVariable{{Name: "code"}}},
		}
		assert.False(t, deadJoinFlag(inner, "unique"))
	})

	t.Run("a build-side column survives downstream -> never flagged", func(t *testing.T) {
		survivorJoin := &RawPlanNode{
			Kind:            "LEFT",
			OutputVariables: []RawVariable{{Name: "code"}, {Name: "b"}}, // "code" survives
			Right:           &RawPlanNode{OutputVariables: []RawVariable{{Name: "code"}}},
		}
		assert.False(t, deadJoinFlag(survivorJoin, "unique"))
	})

	t.Run("buildKind is non-unique (dimensionNonUnique/detail1toN) -> never flagged (real row-count effect)", func(t *testing.T) {
		assert.False(t, deadJoinFlag(leftZeroSurvivors, "dimensionNonUnique"))
		assert.False(t, deadJoinFlag(leftZeroSurvivors, "detail1toN"))
	})

	t.Run("post-review tightening: a LEFT join with 0 survivors and a real 1.0-1.05x fan-out (buildKind detail1toN/dimensionNonUnique, NOT unique/emptyBuild) is NEVER flagged -- the previously-tolerated fanout<=1.05 window is gone", func(t *testing.T) {
		// This is exactly the case the review closed: a small-but-real (up
		// to 5%) row-count effect must never be called "droppable, no
		// row-count effect" just because the measured ratio happened to be
		// close to 1.0 -- only a PROVABLY 1:1 buildKind may satisfy this now.
		assert.False(t, deadJoinFlag(leftZeroSurvivors, "detail1toN"))
		assert.False(t, deadJoinFlag(leftZeroSurvivors, "dimensionNonUnique"))
	})

	t.Run("buildKind unresolved (empty string, e.g. buildKeyNdv unavailable) -> never flagged", func(t *testing.T) {
		assert.False(t, deadJoinFlag(leftZeroSurvivors, ""))
	})

	t.Run("missing input: nil join -> never flagged", func(t *testing.T) {
		assert.False(t, deadJoinFlag(nil, "unique"))
	})

	t.Run("missing input: no output-variable data on the build side -> never flagged (accepted false negative)", func(t *testing.T) {
		noData := &RawPlanNode{Kind: "LEFT", OutputVariables: []RawVariable{{Name: "a"}}, Right: &RawPlanNode{}}
		assert.False(t, deadJoinFlag(noData, "unique"))
	})

	t.Run("missing input: no output-variable data on the join's own side -> never flagged", func(t *testing.T) {
		noData := &RawPlanNode{Kind: "LEFT", Right: &RawPlanNode{OutputVariables: []RawVariable{{Name: "code"}}}}
		assert.False(t, deadJoinFlag(noData, "unique"))
	})

	t.Run("no build (right) side at all -> never flagged", func(t *testing.T) {
		noRight := &RawPlanNode{Kind: "LEFT", OutputVariables: []RawVariable{{Name: "a"}}}
		assert.False(t, deadJoinFlag(noRight, "unique"))
	})
}

// TestComputeChainFanout covers UT-11's round-6 chain-fanout math (design.md
// §5.4.1 A3(iii)(ε)): endToEnd from the exact chain-boundary integers
// (never re-derived from rounded per-node fanouts), nodeProduct as the
// unrounded per-node product, overflow-guarded (IsInf/IsNaN -> omitted), and
// absent entirely for chains with <2 rows or an unmeasured (<=0) first `in`.
func TestComputeChainFanout(t *testing.T) {
	chain := []FanoutChainNode{
		{PlanNodeId: "a", In: 174_400, Out: 14_609_408, Fanout: 83.76954128440367},
		{PlanNodeId: "b", In: 14_609_408, Out: 272_324_086, Fanout: 18.640323139719282},
		{PlanNodeId: "c", In: 272_324_086, Out: 272_324_086, Fanout: 1},
		{PlanNodeId: "d", In: 272_324_086, Out: 599_440_009, Fanout: 2.2012008478750573},
		{PlanNodeId: "e", In: 599_440_009, Out: 445_201_868_645, Fanout: 742.696286468593},
	}
	cf := computeChainFanout(chain)
	require.NotNil(t, cf)
	assert.EqualValues(t, 174400, cf.In)
	assert.EqualValues(t, 445201868645, cf.Out)
	assert.InDelta(t, 2552763.0, cf.EndToEnd, 1.0, "endToEnd must be the real drr9h ratio (174,400 -> 445,201,868,645)")
	require.NotNil(t, cf.NodeProduct)
	// A left-deep chain's unrounded product telescopes to exactly endToEnd.
	assert.InDelta(t, cf.EndToEnd, *cf.NodeProduct, 1e-3)

	t.Run("nodeProduct overflow (Inf) -> omitted, chain object still present", func(t *testing.T) {
		overflowChain := []FanoutChainNode{
			{PlanNodeId: "a", In: 1, Out: 1, Fanout: math.MaxFloat64},
			{PlanNodeId: "b", In: 1, Out: 2, Fanout: math.MaxFloat64},
		}
		cf := computeChainFanout(overflowChain)
		require.NotNil(t, cf)
		assert.Nil(t, cf.NodeProduct)
	})

	t.Run("nodeProduct NaN -> omitted", func(t *testing.T) {
		nanChain := []FanoutChainNode{
			{PlanNodeId: "a", In: 1, Out: 1, Fanout: math.NaN()},
			{PlanNodeId: "b", In: 1, Out: 2, Fanout: 2},
		}
		cf := computeChainFanout(nanChain)
		require.NotNil(t, cf)
		assert.Nil(t, cf.NodeProduct)
	})

	t.Run("<2-row chain -> object entirely absent", func(t *testing.T) {
		assert.Nil(t, computeChainFanout([]FanoutChainNode{{PlanNodeId: "a", In: 1, Out: 1}}))
		assert.Nil(t, computeChainFanout(nil))
	})

	t.Run("first row's in <= 0 -> object entirely absent", func(t *testing.T) {
		zeroIn := []FanoutChainNode{{PlanNodeId: "a", In: 0, Out: 1}, {PlanNodeId: "b", In: 1, Out: 2}}
		assert.Nil(t, computeChainFanout(zeroIn))
	})
}

// TestEnrichFanoutChainRowsEndToEnd covers UT-11 end-to-end: a synthetic
// fixture shaped like the real drr9h cross-stage build-side walk (arbitrary,
// non-real ids/names throughout, proving no hard-coding) populates every
// round-5 field correctly, including the dimensionNonUnique auto-flag; a
// second row with an unresolvable build side still gets non-blank
// build/joinKeys and simply omits the rest.
func TestEnrichFanoutChainRowsEndToEnd(t *testing.T) {
	// Row "zz-1": resolvable cross-stage build -> a small, duplicated
	// dimension (dimensionNonUnique).
	scan := &RawPlanNode{Type: ".TableScanNode", Id: "zz-scan", Table: &RawTableHandle{ConnectorHandle: RawConnectorHandle{SchemaName: "s", TableName: "dim_zz"}}}
	otherStageRoot := &RawPlanNode{Type: ".ProjectNode", Id: "zz-proj", Source: scan}
	remote := &RawPlanNode{Type: "com.example.RemoteSourceNode", Id: "zz-remote", SourceFragmentIds: []string{"55"}}
	exchange := &RawPlanNode{Type: "com.example.ExchangeNode", Id: "zz-exchange", Sources: []*RawPlanNode{remote}}
	line := 99
	join1 := &RawPlanNode{
		Type: ".JoinNode", Id: "zz-1", Right: exchange,
		Criteria: []RawJoinCriterion{{Left: RawVariable{Name: "probe_key"}, Right: RawVariable{Name: "build_key", SourceLocation: &RawSourceLocation{Line: line}}}},
	}

	// Row "zz-2": unresolvable build side (bare RemoteSourceNode, no
	// matching stage registered) -> fallback label, no other facts.
	unresolvedRemote := &RawPlanNode{Type: "com.example.RemoteSourceNode", Id: "zz-remote-2", SourceFragmentIds: []string{"999"}}
	join2 := &RawPlanNode{Type: ".JoinNode", Id: "zz-2", Right: unresolvedRemote}

	idx := buildIndexFromStages(map[string]*RawPlanNode{"9": join1, "55": otherStageRoot})
	// join2 must also be indexed (as if it were part of stage 9's own tree);
	// build a combined index by re-indexing both roots together under the
	// SAME stage id (a stage's tree can have more than one join).
	idx2 := &rawPlanIndex{
		byNodeId: map[string]*RawPlanNode{}, byStage: idx.byStage,
		stageOf: map[string]string{}, parent: map[string]parentEdge{},
		crossRefs: map[string][]*RawPlanNode{}, partitioningByStage: map[string]*RawPartitioningScheme{},
	}
	indexRawPlanNode(join1, "9", idx2)
	indexRawPlanNode(join2, "9", idx2)

	estimates := map[string]*NodeEstimate{
		"zz-exchange": {VariableStatistics: map[string]plan_node.VariableStatistics{"build_key<bigint>": vstat(1, 1000, 115)}},
	}
	nodeMetrics := map[string]*NodeMetrics{
		"zz-scan": {PlanNodeId: "zz-scan", ActualIn: int64Ptr(1792), ActualOut: int64Ptr(1792)},
	}
	th := DefaultThresholds()

	chain := []FanoutChainNode{
		{PlanNodeId: "zz-1", Operator: "HashBuilderOperator+LookupJoinOperator", In: 1000, Out: 18600, Fanout: 18.6},
		{PlanNodeId: "zz-2", Operator: "HashBuilderOperator+LookupJoinOperator", In: 1, Out: 1, Fanout: 1},
	}
	enrichFanoutChainRows(chain, idx2, nil, estimates, nodeMetrics, th)

	row1 := chain[0]
	assert.Equal(t, "s.dim_zz", row1.Build)
	assert.Equal(t, []string{"probe_key=build_key"}, row1.JoinKeys)
	require.NotNil(t, row1.SqlLine)
	assert.Equal(t, 99, *row1.SqlLine)
	require.NotNil(t, row1.BuildRows)
	assert.EqualValues(t, 1792, *row1.BuildRows)
	assert.Equal(t, "measured", row1.BuildRowsSource)
	require.NotNil(t, row1.BuildKeyNdv)
	assert.InDelta(t, 115, *row1.BuildKeyNdv, 1e-9)
	require.NotNil(t, row1.BuildKeyDupFactor)
	assert.InDelta(t, 15.58, *row1.BuildKeyDupFactor, 0.01)
	assert.Equal(t, "dimensionNonUnique", row1.BuildKind)

	row2 := chain[1]
	assert.Equal(t, "RemoteSource(S999)", row2.Build, "unresolved build side must still render a non-blank label")
	assert.Equal(t, []string{}, row2.JoinKeys, "no criteria and no simplified plan node -> empty, never nil, JoinKeys")
	assert.Nil(t, row2.SqlLine)
	assert.Nil(t, row2.BuildRows)
	assert.Empty(t, row2.BuildRowsSource)
	assert.Nil(t, row2.BuildKeyNdv)
	assert.Nil(t, row2.BuildKeyDupFactor)
	assert.Empty(t, row2.BuildKind)

	t.Run("nil index (no raw plan data at all) -> every row still gets a non-blank fallback", func(t *testing.T) {
		chain := []FanoutChainNode{{PlanNodeId: "whatever"}}
		enrichFanoutChainRows(chain, nil, nil, nil, nil, th)
		assert.Equal(t, "unknown", chain[0].Build)
		assert.Equal(t, []string{}, chain[0].JoinKeys)
	})

	t.Run("N1: +Inf/astronomical build-scan ESTIMATE (no measured actual) -> buildRows omitted, buildKind degrades to unmarked, no crash", func(t *testing.T) {
		// Same shape as zz-1 above, but the resolved TableScan has NO
		// measured actual at all (live/estimate-only) and its
		// planStatsAndCosts outputRowCount is +Inf -- the exact class of
		// value this query family's default-selectivity fallback produces
		// at the join node itself (e.g. 2.79e22, §5.4.1), which would
		// int64-overflow into garbage without the guard.
		infEstimates := map[string]*NodeEstimate{
			"zz-exchange": {VariableStatistics: map[string]plan_node.VariableStatistics{"build_key<bigint>": vstat(1, 1000, 115)}},
			"zz-scan":     {OutputRowCount: plan_node.JsonFloat64(math.Inf(1))},
		}
		chain := []FanoutChainNode{{PlanNodeId: "zz-1", Operator: "HashBuilderOperator+LookupJoinOperator", In: 1000, Out: 18600, Fanout: 18.6}}
		assert.NotPanics(t, func() {
			enrichFanoutChainRows(chain, idx2, nil, infEstimates, map[string]*NodeMetrics{}, th)
		})
		row := chain[0]
		assert.Equal(t, "s.dim_zz", row.Build, "build table label resolution is independent of the row-count guard")
		assert.Nil(t, row.BuildRows, "an unsafe (+Inf) estimate must leave buildRows omitted, never a garbage int64")
		assert.Empty(t, row.BuildRowsSource)
		// buildKeyNdv itself is still resolvable (independent walk), but with
		// buildRows nil, classifyBuildKind is never even called -- buildKind
		// degrades to unmarked, not a bogus classification from a garbage
		// dupFactor.
		require.NotNil(t, row.BuildKeyNdv)
		assert.Nil(t, row.BuildKeyDupFactor)
		assert.Empty(t, row.BuildKind)
	})

	t.Run("N1: astronomically large but finite build-scan ESTIMATE -> also omitted, never truncated/overflowed", func(t *testing.T) {
		hugeEstimates := map[string]*NodeEstimate{
			"zz-exchange": {VariableStatistics: map[string]plan_node.VariableStatistics{"build_key<bigint>": vstat(1, 1000, 115)}},
			"zz-scan":     {OutputRowCount: plan_node.JsonFloat64(2.79e22)},
		}
		chain := []FanoutChainNode{{PlanNodeId: "zz-1", Operator: "HashBuilderOperator+LookupJoinOperator", In: 1000, Out: 18600, Fanout: 18.6}}
		enrichFanoutChainRows(chain, idx2, nil, hugeEstimates, map[string]*NodeMetrics{}, th)
		assert.Nil(t, chain[0].BuildRows)
		assert.Empty(t, chain[0].BuildRowsSource)
	})

	// Round-6 emptyBuild precedence (design.md §5.4.1 A3(iii)(δ) ruling):
	// fires ONLY on a MEASURED zero-row build; an estimate-0 or an absent
	// buildRows must NEVER produce emptyBuild (an estimate is not
	// trustworthy proof of emptiness, and "absent" must stay unclassified).
	baseEstimates := map[string]*NodeEstimate{"zz-exchange": {VariableStatistics: map[string]plan_node.VariableStatistics{"build_key<bigint>": vstat(1, 1000, 115)}}}

	t.Run("round-6: MEASURED buildRows==0 -> emptyBuild, buildKeyDupFactor omitted", func(t *testing.T) {
		chain := []FanoutChainNode{{PlanNodeId: "zz-1", Operator: "HashBuilderOperator+LookupJoinOperator", In: 100, Out: 100, Fanout: 1}}
		nodeMetrics := map[string]*NodeMetrics{"zz-scan": {PlanNodeId: "zz-scan", ActualIn: int64Ptr(0), ActualOut: int64Ptr(0)}}
		enrichFanoutChainRows(chain, idx2, nil, baseEstimates, nodeMetrics, th)
		row := chain[0]
		require.NotNil(t, row.BuildRows)
		assert.EqualValues(t, 0, *row.BuildRows)
		assert.Equal(t, "measured", row.BuildRowsSource)
		assert.Equal(t, "emptyBuild", row.BuildKind)
		assert.Nil(t, row.BuildKeyDupFactor, "an empty build proves nothing about key uniqueness -- dupFactor must be omitted")
		require.NotNil(t, row.BuildKeyNdv, "buildKeyNdv itself is still a factual read, untouched by the emptyBuild gate")
	})

	t.Run("round-6: ESTIMATE buildRows==0 -> NOT emptyBuild (estimates aren't trustworthy proof of emptiness)", func(t *testing.T) {
		chain := []FanoutChainNode{{PlanNodeId: "zz-1", Operator: "HashBuilderOperator+LookupJoinOperator", In: 100, Out: 100, Fanout: 1}}
		zeroEstEstimates := map[string]*NodeEstimate{
			"zz-exchange": {VariableStatistics: map[string]plan_node.VariableStatistics{"build_key<bigint>": vstat(1, 1000, 115)}},
			"zz-scan":     {OutputRowCount: plan_node.JsonFloat64(0)},
		}
		enrichFanoutChainRows(chain, idx2, nil, zeroEstEstimates, map[string]*NodeMetrics{}, th)
		row := chain[0]
		require.NotNil(t, row.BuildRows)
		assert.EqualValues(t, 0, *row.BuildRows)
		assert.Equal(t, "estimate", row.BuildRowsSource)
		assert.NotEqual(t, "emptyBuild", row.BuildKind, "an estimated (not measured) zero must never fire emptyBuild")
		// buildKeyNdv=115 with buildRows=0 -> dupFactor 0 -> "unique" via the
		// ordinary classifyBuildKind path (the pre-round-6 behavior,
		// unchanged for the estimate case -- only the MEASURED case is
		// special-cased to emptyBuild).
		assert.Equal(t, "unique", row.BuildKind)
	})

	t.Run("round-6: ABSENT buildRows (no measured, no estimate at all) -> stays unclassified, never emptyBuild", func(t *testing.T) {
		chain := []FanoutChainNode{{PlanNodeId: "zz-1", Operator: "HashBuilderOperator+LookupJoinOperator", In: 100, Out: 100, Fanout: 1}}
		enrichFanoutChainRows(chain, idx2, nil, baseEstimates, map[string]*NodeMetrics{}, th)
		row := chain[0]
		assert.Nil(t, row.BuildRows)
		assert.Empty(t, row.BuildRowsSource)
		assert.Empty(t, row.BuildKind, "no buildRows at all must never be guessed as emptyBuild")
	})
}

func int64Ptr(v int64) *int64 { return &v }

// TestSafeInt64FromOutputRowCount is the direct UT-11 unit test for the N1
// hardening guard: NaN/+Inf/-Inf/negative/astronomically-large values are
// all rejected (ok=false, zero value -- never a garbage/overflowed int64);
// an ordinary in-range value converts cleanly.
func TestSafeInt64FromOutputRowCount(t *testing.T) {
	v, ok := safeInt64FromOutputRowCount(1792)
	assert.True(t, ok)
	assert.EqualValues(t, 1792, v)

	for _, bad := range []float64{math.NaN(), math.Inf(1), math.Inf(-1), -1, 2.79e22, maxSafeOutputRowCount * 2} {
		_, ok := safeInt64FromOutputRowCount(bad)
		assert.False(t, ok, "value %v must be rejected, never converted", bad)
	}

	// Right at the boundary: exactly maxSafeOutputRowCount is still safe.
	v2, ok2 := safeInt64FromOutputRowCount(maxSafeOutputRowCount)
	assert.True(t, ok2)
	assert.EqualValues(t, int64(maxSafeOutputRowCount), v2)
}
