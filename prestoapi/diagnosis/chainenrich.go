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
	"strconv"
	"strings"

	"pbench/prestoapi/plan_node"
)

// rawPlanIndex indexes a query's per-stage raw structured plans
// (StagePlan.Root, model.go) for the round-5 fan-out-chain row enrichment
// (design.md §5.4.1 A3(iii), §10 item 14): byNodeId locates a node by its
// plan-node id WITHIN its own stage's root (a stage's own recursive walk
// never crosses a RemoteSourceNode boundary -- that is a DIFFERENT stage's
// tree), and byStage resolves a RemoteSourceNode's sourceFragmentIds entry to
// that other stage's own root, letting the build-side walk below follow a
// join's build input across stage boundaries to its source TableScan, the
// same cross-stage stitching `qi.PrepareForInsert()` does for the simplified
// plan (§5.1).
type rawPlanIndex struct {
	byNodeId map[string]*RawPlanNode
	byStage  map[string]*RawPlanNode

	// Round-8 additions (§5.4.1 A3(iii)(η/ι) R8-A/R8-C shared infra):
	//
	// stageOf maps a node id to the fragment-id suffix of the stage that owns
	// it (the same convention byStage's keys use) -- needed to resolve
	// cross-stage consumer edges from a node's OWN stage, not just from a
	// stage's root.
	stageOf map[string]string
	// parent maps a node id to its immediate parent WITHIN THE SAME STAGE
	// (absent for a stage's own root -- a plan fragment is a tree, so this is
	// always at most one edge, never a fork). Used by both the R8-C
	// downstream probe-spine walk (climbing toward the sink) and the R8-A
	// nearest-aggregation walk (climbing toward the consuming aggregation).
	parent map[string]parentEdge
	// crossRefs maps a fragment-id suffix to EVERY RemoteSourceNode (in any
	// OTHER stage) whose own SourceFragmentIds includes it -- i.e. every
	// consumer edge reading that stage's output. A stage genuinely read by
	// more than one consumer (a broadcast fan-out) or a RemoteSourceNode that
	// itself names multiple fragments (a fan-in multi-source read) are both
	// deterministic STOP conditions for the linear R8-C spine walk (review
	// W5: "never guess a branch"); R8-A's nearest-aggregation walk instead
	// explores every branch, since it must classify each downstream path.
	crossRefs map[string][]*RawPlanNode
	// partitioningByStage maps a fragment-id suffix to that fragment's OWN
	// output PartitioningScheme (round-8, §5.4.5 R8-D) -- the design's W4
	// correction: a skewed stage's INPUT partition key is its CHILD
	// (producing) fragment's OWN scheme, resolved via this map keyed by the
	// child's fragment id (never the skewed stage's own entry).
	partitioningByStage map[string]*RawPartitioningScheme
}

// parentEdge records a raw-plan node's immediate parent within its own stage,
// plus which child slot it occupies -- "left"/"right" distinguish a JoinNode's
// probe vs. build side (needed by the R8-C rule that the probe-lineage spine
// walk terminates the moment the stream becomes a BUILD input of an upstream
// join), "source"/"sources" cover every other pass-through node shape.
type parentEdge struct {
	node *RawPlanNode
	side string
}

// buildRawPlanIndex walks every stage's raw structured plan (when present)
// and indexes every node by id, plus each stage's own root by its numeric
// stage-id suffix (matching RemoteSourceNode.sourceFragmentIds' own
// convention, verified against real data: a fragment id of "12" names the
// stage whose StageId ends in ".12"). Absent-tolerant throughout -- a nil
// outputStage, or any stage/plan/root along the way being nil, is simply
// skipped, never an error; a query with no raw structured plan at all yields
// an index with zero entries, and every caller below degrades to a
// never-blank fallback label in that case.
func buildRawPlanIndex(outputStage *StageNode) *rawPlanIndex {
	idx := &rawPlanIndex{
		byNodeId:            map[string]*RawPlanNode{},
		byStage:             map[string]*RawPlanNode{},
		stageOf:             map[string]string{},
		parent:              map[string]parentEdge{},
		crossRefs:           map[string][]*RawPlanNode{},
		partitioningByStage: map[string]*RawPartitioningScheme{},
	}
	for _, s := range outputStage.Flatten() {
		if s == nil || s.Plan == nil || s.Plan.Root == nil {
			continue
		}
		suffix := s.StageId
		if n, ok := NumericStageId(s.StageId); ok {
			suffix = strconv.Itoa(n)
		}
		idx.byStage[suffix] = s.Plan.Root
		if s.Plan.PartitioningScheme != nil {
			idx.partitioningByStage[suffix] = s.Plan.PartitioningScheme
		}
		indexRawPlanNode(s.Plan.Root, suffix, idx)
	}
	return idx
}

// indexRawPlanNode recursively registers every node reachable from n (within
// its OWN stage only) into byNodeId/stageOf/parent, plus every RemoteSourceNode
// encountered into crossRefs (round-8 additions above). Deliberately does not
// FOLLOW SourceFragmentIds itself -- that cross-stage jump is resolved on
// demand (byStage for the build/probe-side walks, crossRefs for the R8-C/R8-A
// downstream walks), not during indexing: each stage's own root is indexed
// once, from its own recursive walk, when buildRawPlanIndex reaches it via
// Flatten().
func indexRawPlanNode(n *RawPlanNode, stageSuffix string, idx *rawPlanIndex) {
	if n == nil {
		return
	}
	if n.Id != "" {
		idx.byNodeId[n.Id] = n
		idx.stageOf[n.Id] = stageSuffix
	}
	if rawNodeIsType(n, "RemoteSourceNode") {
		for _, frag := range n.SourceFragmentIds {
			idx.crossRefs[frag] = append(idx.crossRefs[frag], n)
		}
	}
	if n.Left != nil {
		idx.parent[n.Left.Id] = parentEdge{node: n, side: "left"}
	}
	if n.Right != nil {
		idx.parent[n.Right.Id] = parentEdge{node: n, side: "right"}
	}
	if n.Source != nil {
		idx.parent[n.Source.Id] = parentEdge{node: n, side: "source"}
	}
	for _, s := range n.Sources {
		if s != nil {
			idx.parent[s.Id] = parentEdge{node: n, side: "sources"}
		}
	}
	indexRawPlanNode(n.Left, stageSuffix, idx)
	indexRawPlanNode(n.Right, stageSuffix, idx)
	indexRawPlanNode(n.Source, stageSuffix, idx)
	for _, s := range n.Sources {
		indexRawPlanNode(s, stageSuffix, idx)
	}
}

// buildSideWalkResult holds what resolveBuildSide found while walking from a
// JoinNode's build (right) side down toward a TableScanNode.
type buildSideWalkResult struct {
	tableLabel  string         // never blank: a real "schema.table", or a graceful fallback
	tableNodeId string         // the resolved TableScanNode's id; "" when unresolved (fallback case)
	visited     []*RawPlanNode // every node visited along the way, nearest first (for the buildKeyNdv search)
}

// resolveBuildSide walks from a JoinNode's build (right) child down to its
// TableScanNode (round-5, §5.4.1 A3(iii): "the build-side (right) input's
// fully-qualified table name, resolved by walking the raw structured plan
// fragment from the JoinNode's build child down to its TableScan"):
// single-child pass-through nodes (Source) and single-source Exchange nodes
// are followed transparently; a RemoteSourceNode crosses into the OTHER
// stage's own root via its sole sourceFragmentIds entry. The walk stops,
// gracefully, the moment it can no longer make an UNAMBIGUOUS choice (a
// RemoteSourceNode naming zero or several fragments, an Exchange/Union with
// zero or several sources, a nested JoinNode on the build side, or any
// unrecognized node shape) -- returning that node's own label
// (`RemoteSource(S<fragmentId>)` or its bare `@type` name) rather than
// guessing. Never returns a blank label; a nil join or a join with no build
// (right) side at all degrades to "unknown".
func resolveBuildSide(join *RawPlanNode, idx *rawPlanIndex) buildSideWalkResult {
	if join == nil || join.Right == nil {
		return buildSideWalkResult{tableLabel: "unknown"}
	}
	var visited []*RawPlanNode
	cur := join.Right
	for depth := 0; cur != nil && depth < 64; depth++ { // depth cap: never loop forever on a malformed/cyclic plan
		visited = append(visited, cur)
		switch {
		case rawNodeIsType(cur, "TableScanNode"):
			return buildSideWalkResult{tableLabel: tableLabelFromScan(cur), tableNodeId: cur.Id, visited: visited}
		case rawNodeIsType(cur, "RemoteSourceNode"):
			if len(cur.SourceFragmentIds) != 1 {
				return buildSideWalkResult{tableLabel: fmt.Sprintf("RemoteSource(S%s)", strings.Join(cur.SourceFragmentIds, ",")), visited: visited}
			}
			next := idx.byStage[cur.SourceFragmentIds[0]]
			if next == nil {
				return buildSideWalkResult{tableLabel: fmt.Sprintf("RemoteSource(S%s)", cur.SourceFragmentIds[0]), visited: visited}
			}
			cur = next
		case cur.Source != nil && cur.Right == nil && cur.Left == nil && len(cur.Sources) == 0:
			cur = cur.Source
		case len(cur.Sources) == 1:
			cur = cur.Sources[0]
		default:
			// Ambiguous (a nested JoinNode, a multi-source Union/Exchange, or
			// an unrecognized leaf) -- stop here, never blank.
			return buildSideWalkResult{tableLabel: rawNodeTypeName(cur.Type), visited: visited}
		}
	}
	return buildSideWalkResult{tableLabel: "unknown", visited: visited}
}

// tableLabelFromScan renders "schema.table" from a TableScanNode's Table
// handle -- never blank: an unrecognized/absent connector handle falls back
// to the node's own bare type name (still, deliberately, never blank).
func tableLabelFromScan(scan *RawPlanNode) string {
	if scan.Table == nil || scan.Table.ConnectorHandle.TableName == "" {
		return rawNodeTypeName(scan.Type)
	}
	if scan.Table.ConnectorHandle.SchemaName == "" {
		return scan.Table.ConnectorHandle.TableName
	}
	return scan.Table.ConnectorHandle.SchemaName + "." + scan.Table.ConnectorHandle.TableName
}

// fallbackJoinKeyPairs is the plan_node.ParseJoins fallback for
// chainRowJoinKeysAndLine (used when the raw plan's criteria[] is
// unavailable, design.md §5.4.1 A3(iii)): parses the node's textual
// join-predicate identifier the same parser joinKeyNames (joinkeys.go) uses,
// but keeps each predicate's left/right PAIRED -- joinKeyNames flattens and
// dedupes across sides, which would lose the "left=right" pairing this
// enrichment renders.
func fallbackJoinKeyPairs(node *plan_node.PlanNode) []string {
	if node == nil || !plan_node.IsJoin[node.Name] || node.Identifier == "" {
		return nil
	}
	parsed, err := plan_node.PlanNodeJoinPredicatesParser.ParseString(node.Id, node.Identifier)
	if err != nil {
		return nil
	}
	var keys []string
	for _, exp := range parsed.GetJoins() {
		for _, pred := range exp.GetJoinPredicates() {
			left, right := pred.GetAssignments()
			for i := 0; i < len(left) && i < len(right); i++ {
				keys = append(keys, left[i]+"="+right[i])
			}
		}
	}
	return keys
}

// chainRowJoinKeysAndLine extracts the round-5 joinKeys[]/sqlLine? for one
// fan-out-chain row (design.md §5.4.1 A3(iii)): primarily from the raw
// JoinNode's own criteria[] (which also carries sourceLocation); when
// criteria[] is empty/absent, falls back to fallbackJoinKeyPairs on the same
// simplified plan the NDV fingerprint already uses -- that path never has a
// sourceLocation, so sqlLine legitimately stays nil (omitted), per the
// design's explicit absent-tolerant contract for that field.
func chainRowJoinKeysAndLine(rawJoin *RawPlanNode, simplePlanNode *plan_node.PlanNode) ([]string, *int) {
	if rawJoin != nil && len(rawJoin.Criteria) > 0 {
		keys := make([]string, 0, len(rawJoin.Criteria))
		var line *int
		for _, c := range rawJoin.Criteria {
			keys = append(keys, c.Left.Name+"="+c.Right.Name)
			if line == nil {
				if c.Left.SourceLocation != nil {
					l := c.Left.SourceLocation.Line
					line = &l
				} else if c.Right.SourceLocation != nil {
					l := c.Right.SourceLocation.Line
					line = &l
				}
			}
		}
		return keys, line
	}
	return fallbackJoinKeyPairs(simplePlanNode), nil
}

// buildKeyNdvFromWalk searches the visited build-side nodes (nearest first)
// for the build-side join key's own distinctValuesCount (design.md §5.4.1
// A3(iii)): the join node's OWN aggregated variableStatistics does not
// necessarily carry a renamed/projected build key (e.g. a CAST introducing
// "expr_22") -- it is typically present on the build side's own ancestors
// instead, which is exactly the order this walk visits them in. Stops at the
// first node whose variableStatistics resolves the (type-suffix-stripped)
// name. Returns nil (never a NaN pointer) when no visited node carries it,
// or when the only match found is itself NaN -- the per-join-key NDV
// fingerprint already covers "missing", and a NaN dupFactor could not drive
// a meaningful buildKind classification anyway, so this field is simply
// omitted in that case, per the design's absent-tolerant contract.
func buildKeyNdvFromWalk(buildKeyName string, visited []*RawPlanNode, estimates map[string]*NodeEstimate) *float64 {
	if buildKeyName == "" || estimates == nil {
		return nil
	}
	for _, n := range visited {
		if n == nil {
			continue
		}
		est := estimates[n.Id]
		if est == nil {
			continue
		}
		for key, vs := range est.VariableStatistics {
			if stripTypeSuffix(key) != buildKeyName {
				continue
			}
			if math.IsNaN(float64(vs.DistinctValuesCount)) {
				return nil
			}
			v := float64(vs.DistinctValuesCount)
			return &v
		}
	}
	return nil
}

// maxSafeOutputRowCount is the largest outputRowCount this package will ever
// convert to an int64 buildRows value (N1 hardening, reviewer finding): a
// generous safety margin below math.MaxInt64 (~9.22e18) so the conversion
// itself can never overflow/wrap, while still comfortably covering any
// realistic real row count -- this query family's own default-selectivity
// fallback estimates run into the 1e22+ range at the join node itself
// (§5.4.1), astronomically past any plausible TABLE size, so this guard
// exists specifically to reject those rather than silently truncate them
// into a garbage int64.
const maxSafeOutputRowCount = 9.2e18

// safeInt64FromOutputRowCount converts a planStatsAndCosts outputRowCount
// estimate to an int64 buildRows value, rejecting anything that is not
// finite (NaN/+Inf/-Inf) or is outside the safe int64 range -- an
// int64(math.Inf(1)) conversion in Go is undefined/implementation-defined
// garbage, not a clean error, so this check must happen BEFORE the
// conversion, never after. Returns ok=false (never a garbage value) for
// anything unsafe; the caller simply leaves buildRows unset in that case,
// the same absent-tolerant degradation as every other optional chain-row
// field.
func safeInt64FromOutputRowCount(v float64) (int64, bool) {
	if math.IsNaN(v) || math.IsInf(v, 0) || v < 0 || v > maxSafeOutputRowCount {
		return 0, false
	}
	return int64(v), true
}

// classifyBuildKind computes buildKeyDupFactor and the buildKind label from
// buildRows/buildKeyNdv (design.md §5.4.1 A3(iii), §5.4.2's two display-only
// constants -- NO elevation effect on severity/the causal engine):
// dupFactor < BuildKeyDupFactorMin -> "unique"; dupFactor >= min AND
// buildRows <= SmallBuildRowsMax -> "dimensionNonUnique" (a dimension-sized
// build duplicated on its own key -- the fan-out is a data/modeling defect:
// dedup, not pre-aggregate); dupFactor >= min AND buildRows > max ->
// "detail1toN" (a legitimate detail expansion -- pre-aggregate). Returns
// kind=="" (both fields omitted by the caller) when buildKeyNdv is not a
// usable positive number. The round-6 emptyBuild case (below) takes
// precedence over this function entirely and never calls it.
func classifyBuildKind(buildRows int64, buildKeyNdv float64, th *Thresholds) (dupFactor float64, kind string) {
	if buildKeyNdv <= 0 {
		return 0, ""
	}
	dupFactor = float64(buildRows) / buildKeyNdv
	switch {
	case dupFactor < th.BuildKeyDupFactorMin:
		return dupFactor, "unique"
	case float64(buildRows) <= th.SmallBuildRowsMax:
		return dupFactor, "dimensionNonUnique"
	default:
		return dupFactor, "detail1toN"
	}
}

// rawVariableNames extracts bare names from a []RawVariable list.
func rawVariableNames(vars []RawVariable) []string {
	if len(vars) == 0 {
		return nil
	}
	names := make([]string, 0, len(vars))
	for _, v := range vars {
		names = append(names, v.Name)
	}
	return names
}

// rawNodeOutputVarNames returns a raw plan node's own emitted column names
// (round-6, §5.4.1 A3(iii)(ζ) deadJoin resolution), or nil when neither
// source is populated -- most node kinds (JoinNode, TableScanNode,
// RemoteSourceNode, Project/Filter/etc.) serialize `outputVariables`
// directly; ExchangeNode instead carries the equivalent list under
// `partitioningScheme.outputLayout` (verified on the real terminal: the
// immediate build child under most of these joins IS an ExchangeNode),
// checked here as a fallback so the deadJoin check does not lose data at the
// single most common node shape on a join's build side. A nil result here is
// treated as "missing input" by deadJoinFlag -- never as "vacuously zero
// survivors" -- so an absent/unrecognized node shape can only ever produce a
// false negative, never a false positive.
func rawNodeOutputVarNames(n *RawPlanNode) []string {
	if n == nil {
		return nil
	}
	if names := rawVariableNames(n.OutputVariables); names != nil {
		return names
	}
	if n.PartitioningScheme != nil {
		return rawVariableNames(n.PartitioningScheme.OutputLayout)
	}
	return nil
}

// deadJoinFlag implements the round-6 §5.4.1 A3(iii)(ζ) CONSERVATIVE,
// positive-evidence-only rule for whether a chain join is a droppable "dead"
// join: fires ONLY when ALL three of the following are positively
// established from the raw plan -- any missing input (an unknown join type,
// no output-variable data on either side of the join boundary) yields
// false, an accepted false negative, never a guess:
//  1. join.Kind == "LEFT" -- dropping a LEFT join never removes probe rows;
//     an INNER join filters rows and is never flagged even with 0 survivors.
//  2. zero build survivors at the join boundary: the build child's own
//     output columns ∩ the join's own output columns == ∅, by name --
//     leaning on the engine's own column pruning (unused build columns are
//     already absent from the join's pruned output list), so "unused
//     downstream" is a positive plan-level fact, never a traced inference.
//     A survivor discarded further downstream is NOT flagged here (an
//     accepted false negative).
//  3. no row-count effect, PROVABLY 1:1 only: buildKind is
//     "unique"/"emptyBuild" (design decision, post-review tightening) -- a
//     1:1 or empty build is the only case a dropped-row count is guaranteed
//     to be unaffected. An earlier draft additionally allowed any measured
//     fanout <= a small tolerance (DeadJoinFanoutMax); that window was
//     dropped deliberately: a LEFT join with a genuine 1.0-1.05x fan-out
//     would still silently drop up to 5% of rows if removed, and "droppable
//     — remove this join" must never be printed for a case that is not
//     provably result-preserving.
func deadJoinFlag(join *RawPlanNode, buildKind string) bool {
	if join == nil || join.Kind != "LEFT" {
		return false
	}
	buildVars := rawNodeOutputVarNames(join.Right)
	joinVars := rawNodeOutputVarNames(join)
	if len(buildVars) == 0 || len(joinVars) == 0 {
		return false // missing input -- never guess
	}
	joinVarSet := make(map[string]bool, len(joinVars))
	for _, v := range joinVars {
		joinVarSet[v] = true
	}
	for _, v := range buildVars {
		if joinVarSet[v] {
			return false // a build-side column survives downstream -- not dead
		}
	}
	return buildKind == "unique" || buildKind == "emptyBuild"
}

// deadWeightFanoutFlag implements the round-8 §5.4.1 A3(iii)(θ) R8-B rule:
// the fanning complement of deadJoinFlag above. Conditions (1) LEFT join and
// (2) zero build survivors at the join boundary are IDENTICAL to deadJoin;
// condition (3) instead requires a MEASURED row fan-out >= FanoutModerate --
// the join contributes ONLY row multiplication (its build columns are dead
// weight while it fans out), as opposed to deadJoinFlag's "no row-count
// effect at all" case. The two flags are mutually exclusive by construction
// on any single row in practice (fanout>=FanoutModerate>1 rules out
// buildKind=="unique", and an emptyBuild's fanout is measured 1.0 -- verified
// review W2, guarded further by the deadJoin caller's own measured in==out
// check), but this function itself makes no such assumption -- the caller
// (enrichFanoutChainRows) is the single place precedence is enforced.
func deadWeightFanoutFlag(join *RawPlanNode, measuredFanout float64, th *Thresholds) bool {
	if join == nil || join.Kind != "LEFT" {
		return false
	}
	if measuredFanout < th.FanoutModerate {
		return false
	}
	buildVars := rawNodeOutputVarNames(join.Right)
	joinVars := rawNodeOutputVarNames(join)
	if len(buildVars) == 0 || len(joinVars) == 0 {
		return false // missing input -- never guess
	}
	joinVarSet := make(map[string]bool, len(joinVars))
	for _, v := range joinVars {
		joinVarSet[v] = true
	}
	for _, v := range buildVars {
		if joinVarSet[v] {
			return false // a build-side column survives downstream -- not dead weight
		}
	}
	return true
}

// nextUpstreamJoin implements the round-8 §5.4.1 A3(iii)(ι) R8-C
// probe-lineage walk, UPSTREAM direction (toward the driving stream's
// source): starting from a JoinNode's own probe (left) child, follows
// single-child pass-through nodes and single-source Exchange/Union nodes
// transparently, crosses a RemoteSourceNode naming EXACTLY ONE fragment into
// that other stage's own root, and returns the next JoinNode found. Returns
// nil -- deterministically, never a guess (review W5) -- at a TableScanNode
// (the walk reached the true source: no more upstream joins, a normal,
// expected end of the spine) OR at any ambiguous shape (a RemoteSourceNode
// naming zero/several fragments, a multi-source Exchange/Union, an
// unresolved cross-stage jump, or an unrecognized node shape); both "stop"
// cases are observably identical to the caller (no further spine row), which
// is exactly the design's "walk STOPS, deterministically" rule -- the reason
// for stopping does not need its own representation here.
func nextUpstreamJoin(start *RawPlanNode, idx *rawPlanIndex) *RawPlanNode {
	cur := start
	for depth := 0; cur != nil && depth < 64; depth++ {
		switch {
		case rawNodeIsType(cur, "JoinNode"):
			return cur
		case rawNodeIsType(cur, "RemoteSourceNode"):
			if len(cur.SourceFragmentIds) != 1 {
				return nil
			}
			next := idx.byStage[cur.SourceFragmentIds[0]]
			if next == nil {
				return nil
			}
			cur = next
		case cur.Source != nil && cur.Right == nil && cur.Left == nil && len(cur.Sources) == 0:
			cur = cur.Source
		case len(cur.Sources) == 1:
			cur = cur.Sources[0]
		default:
			return nil // TableScanNode (true source) or any ambiguous/unrecognized shape
		}
	}
	return nil
}

// nextDownstreamJoin implements the round-8 R8-C probe-lineage walk,
// DOWNSTREAM direction (toward the driving stream's sink): climbs from
// nodeId's immediate parent within the same stage (rawPlanIndex.parent),
// through single-child pass-through parents, until either (a) the parent is
// a JoinNode -- returned ONLY when nodeId is that join's PROBE (left) child
// (the "side" recorded by parentEdge); if nodeId is instead the join's BUILD
// (right) child, the driving stream has become a build input and the walk
// stops here, per the design's explicit termination rule; (b) nodeId has no
// local parent, i.e. it IS its own stage's root -- the walk then crosses via
// rawPlanIndex.crossRefs to the SOLE consumer stage reading this fragment,
// continuing the climb there; zero or more-than-one consumer edges (a
// genuine broadcast fan-out, or a consumer whose own RemoteSourceNode itself
// names >1 fragments) is a deterministic stop, never a guessed branch (review
// W5); or (c) a pass-through parent is itself a multi-source merge (e.g. a
// UnionNode with >1 Sources) -- also a deterministic stop, for the same
// "never guess a branch" reason. Returns nil for every stop condition; the
// caller does not need to distinguish which one occurred.
func nextDownstreamJoin(nodeId string, idx *rawPlanIndex) *RawPlanNode {
	curId := nodeId
	for depth := 0; depth < 64; depth++ {
		edge, ok := idx.parent[curId]
		if !ok {
			stage := idx.stageOf[curId]
			refs := idx.crossRefs[stage]
			if len(refs) != 1 || len(refs[0].SourceFragmentIds) != 1 {
				return nil
			}
			curId = refs[0].Id
			continue
		}
		if rawNodeIsType(edge.node, "JoinNode") {
			if edge.side == "left" {
				return edge.node
			}
			return nil // became a build input of an upstream join -- stop
		}
		if edge.side == "sources" && len(edge.node.Sources) > 1 {
			return nil // multi-source merge (e.g. UNION) -- ambiguous, stop
		}
		curId = edge.node.Id
	}
	return nil
}

// extractProbeSpineIds implements the round-8 §5.4.1 A3(iii)(ι) R8-C rule:
// "spine = EVERY join on the probe lineage ... rendered in probe order" --
// walked from the A3 verdict node in BOTH directions (nextUpstreamJoin toward
// the source, nextDownstreamJoin toward the sink) via the raw structured
// plan's actual topology (never numeric in/out coincidence), returning plan
// node ids in probe (source-to-sink) order with the verdict node itself
// always present somewhere in the middle (or at either end, when the verdict
// happens to sit at an extremity of its own spine -- verified on 258_463,
// where the verdict node 1544 IS the spine's upstream-most join). Each
// candidate id is required to already carry a measured join actual
// (hasJoinOperatorType + ActualIn/ActualOut) before being accepted -- a
// structurally-found join with no operator-summary data (never observed on
// the real fixtures, but not guaranteed impossible) stops that direction's
// walk exactly like a natural topology dead-end, rather than emitting a row
// with no measurements. Returns nil when the verdict node itself is not a
// raw JoinNode with measured actuals (the caller falls back to the legacy
// same-stage numeric-matching ExtractFanoutChain in that case, e.g. when the
// raw structured plan is entirely absent for this query).
func extractProbeSpineIds(idx *rawPlanIndex, nodeMetrics map[string]*NodeMetrics, verdictNodeId string) []string {
	hasMeasuredJoin := func(id string) bool {
		m := nodeMetrics[id]
		return m != nil && m.ActualIn != nil && m.ActualOut != nil && hasJoinOperatorType(m.OperatorTypes)
	}
	verdict := idx.byNodeId[verdictNodeId]
	if verdict == nil || !rawNodeIsType(verdict, "JoinNode") || !hasMeasuredJoin(verdictNodeId) {
		return nil
	}

	var upstream []string
	cur := verdict
	for i := 0; i < 100 && cur.Left != nil; i++ {
		next := nextUpstreamJoin(cur.Left, idx)
		if next == nil || !hasMeasuredJoin(next.Id) {
			break
		}
		upstream = append([]string{next.Id}, upstream...)
		cur = next
	}

	var downstream []string
	curId := verdictNodeId
	for i := 0; i < 100; i++ {
		next := nextDownstreamJoin(curId, idx)
		if next == nil || !hasMeasuredJoin(next.Id) {
			break
		}
		downstream = append(downstream, next.Id)
		curId = next.Id
	}

	ids := make([]string, 0, len(upstream)+1+len(downstream))
	ids = append(ids, upstream...)
	ids = append(ids, verdictNodeId)
	ids = append(ids, downstream...)
	return ids
}

// buildChainFromIds constructs the FanoutChainNode rows (In/Out/Fanout/
// Operator/kind only -- the round-5/6 enrichment fields are filled in
// afterward by enrichFanoutChainRows, exactly as for the legacy
// numeric-matching ExtractFanoutChain) for an already-ordered list of plan
// node ids (round-8 R8-C, from extractProbeSpineIds).
func buildChainFromIds(ids []string, nodeMetrics map[string]*NodeMetrics) []FanoutChainNode {
	out := make([]FanoutChainNode, 0, len(ids))
	for _, id := range ids {
		m := nodeMetrics[id]
		if m == nil || m.ActualIn == nil || m.ActualOut == nil {
			continue
		}
		var fanout float64
		if *m.ActualIn > 0 {
			fanout = float64(*m.ActualOut) / float64(*m.ActualIn)
		}
		out = append(out, FanoutChainNode{
			PlanNodeId: id,
			Operator:   strings.Join(m.OperatorTypes, "+"),
			In:         *m.ActualIn,
			Out:        *m.ActualOut,
			Fanout:     fanout,
		})
	}
	return out
}

// ChainFanout is the round-6 top-level descriptive summary of a fan-out
// chain's overall multiplication (design.md §5.4.1 A3(iii)(ε), §10 item 15):
// EndToEnd is the TRUE end-to-end ratio computed from the exact
// chain-boundary integers (Out of the last row / In of the first row) --
// NEVER re-multiplied from the displayed, rounded per-node fanouts, which
// would drift from the true value by display-rounding error alone.
// NodeProduct is the UNROUNDED per-node fanout product, included only as
// corroboration and omitted entirely on overflow (Inf/NaN). Purely
// descriptive: never consumed by any threshold, severity, or the causal
// engine.
type ChainFanout struct {
	In          int64    `json:"in"`
	Out         int64    `json:"out"`
	EndToEnd    float64  `json:"endToEnd"`
	NodeProduct *float64 `json:"nodeProduct,omitempty"`
}

// computeChainFanout implements design.md §5.4.1 A3(iii)(ε), truncated per
// the round-8 §5.4.1 A3(iii)(ι) R8-C rule: emitted only when the chain has
// >=2 rows and the first row's `in` is > 0 -- a single-row chain, or an
// unmeasured first hop, has no meaningful "end-to-end" ratio to report.
// EndToEnd/NodeProduct are computed over the prefix ending at the LAST row
// with `out > 0` (never the literal last row) -- an annihilation row (an
// INNER join against a measured-empty build that swallows the entire stream,
// R8-C) would otherwise turn a genuine multi-order-of-magnitude headline into
// a nonsense 0.0x; that row still renders in the table and gets its own
// dedicated evidence line (renderFanoutChainTable), it simply does not
// displace the end-to-end ratio's own denominator/numerator. Returns nil when
// NO row has out > 0 (nothing meaningful to truncate to).
func computeChainFanout(chain []FanoutChainNode) *ChainFanout {
	if len(chain) < 2 || chain[0].In <= 0 {
		return nil
	}
	lastPositiveIdx := -1
	for i, n := range chain {
		if n.Out > 0 {
			lastPositiveIdx = i
		}
	}
	if lastPositiveIdx < 0 {
		return nil
	}
	first, last := chain[0], chain[lastPositiveIdx]
	cf := &ChainFanout{In: first.In, Out: last.Out, EndToEnd: float64(last.Out) / float64(first.In)}
	product := 1.0
	for _, n := range chain[:lastPositiveIdx+1] {
		product *= n.Fanout
	}
	if !math.IsInf(product, 0) && !math.IsNaN(product) {
		cf.NodeProduct = &product
	}
	return cf
}

// enrichFanoutChainRows adds the round-5 per-row fields (build, joinKeys,
// sqlLine?, buildRows?/buildRowsSource?, buildKeyNdv?, buildKeyDupFactor?,
// buildKind?) to an already-extracted fan-out chain, IN PLACE (design.md
// §5.4.1 A3(iii), §10 item 14). Pure additive evidence: never touches
// PlanNodeId/Operator/In/Out/Fanout, and never affects
// severity/verdict/confidence -- this runs strictly after ExtractFanoutChain
// and Attribute() have already produced/picked their results. build and
// joinKeys always get a non-blank value via their own fallbacks (even when
// idx has no raw-plan data at all, or a specific row's join node is not
// found in it); every other field is optional and simply omitted when its
// source data cannot be resolved.
func enrichFanoutChainRows(chain []FanoutChainNode, idx *rawPlanIndex, planNodes map[string]*plan_node.PlanNode, estimates map[string]*NodeEstimate, nodeMetrics map[string]*NodeMetrics, th *Thresholds) {
	for i := range chain {
		row := &chain[i]
		var rawJoin *RawPlanNode
		if idx != nil {
			rawJoin = idx.byNodeId[row.PlanNodeId]
		}
		var simpleNode *plan_node.PlanNode
		if planNodes != nil {
			simpleNode = planNodes[row.PlanNodeId]
		}

		keys, line := chainRowJoinKeysAndLine(rawJoin, simpleNode)
		if keys == nil {
			keys = []string{}
		}
		row.JoinKeys = keys
		row.SqlLine = line

		walk := resolveBuildSide(rawJoin, idx)
		row.Build = walk.tableLabel
		row.buildScanNodeId = walk.tableNodeId

		// buildRows/buildRowsSource: the resolved TableScan's OWN measured
		// actual (terminal/finished) preferred over the planStatsAndCosts
		// estimate -- deliberately read from the TABLE SCAN itself, never
		// from this join node's own HashBuilderOperator summary:
		// operatorSummaries sums per-task, and a REPLICATED (broadcast)
		// build side is read once per task, so that sum is
		// task-count-inflated (the same driver/task-scaled-aggregate bug
		// class already fixed for B5/C1 -- verified on the real drr9h data:
		// node 1781's own HashBuilderOperator reports 28,672 = 16 x the
		// TableScan's true, unreplicated 1,792). Reading the source
		// TableScan's own actual sidesteps the inflation entirely, since a
		// scan is read exactly once per its own splits regardless of how
		// many downstream tasks a build broadcasts to.
		if walk.tableNodeId != "" {
			if m := nodeMetrics[walk.tableNodeId]; m != nil && m.ActualOut != nil {
				rows := *m.ActualOut
				row.BuildRows = &rows
				row.BuildRowsSource = "measured"
			} else if est := estimates[walk.tableNodeId]; est != nil {
				if rows, ok := safeInt64FromOutputRowCount(float64(est.OutputRowCount)); ok {
					row.BuildRows = &rows
					row.BuildRowsSource = "estimate"
				}
				// N1 hardening (reviewer finding): a NaN/+Inf/astronomically
				// large estimate (this query family produces outputRowCount
				// values like 2.79e22 at the join itself, §5.4.1's
				// default-selectivity fallback signature) would int64-overflow
				// into garbage -- safeInt64FromOutputRowCount rejects it and
				// buildRows/buildRowsSource simply stay unset (omitted), the
				// same absent-tolerant degradation as every other optional
				// chain-row field, rather than emitting a bogus row count.
			}
		}

		buildKeyName := ""
		if rawJoin != nil && len(rawJoin.Criteria) > 0 {
			buildKeyName = rawJoin.Criteria[0].Right.Name
		}
		ndv := buildKeyNdvFromWalk(buildKeyName, walk.visited, estimates)
		row.BuildKeyNdv = ndv

		// Round-6 (§5.4.1 A3(iii)(δ) ruling): emptyBuild takes PRECEDENCE over
		// every dupFactor-based kind, and fires ONLY on a MEASURED zero-row
		// build -- an estimate-0 or absent buildRows never fires it (an
		// estimate is not trustworthy proof of emptiness, and "absent" must
		// stay unclassified, never guessed as empty). buildKeyDupFactor is
		// deliberately OMITTED for an emptyBuild row: a truly empty build
		// proves nothing about key uniqueness (the previous behavior computed
		// 0/ndv -> dupFactor 0 -> the misleading "unique" label the real
		// 1778 row showed). buildKeyNdv itself is left set above (still a
		// factual, informative read) -- only the dupFactor/kind pairing is
		// gated here.
		switch {
		case row.BuildRows != nil && row.BuildRowsSource == "measured" && *row.BuildRows == 0:
			row.BuildKind = "emptyBuild"
		case row.BuildRows != nil && ndv != nil:
			dupFactor, kind := classifyBuildKind(*row.BuildRows, *ndv, th)
			if kind != "" {
				row.BuildKeyDupFactor = &dupFactor
				row.BuildKind = kind
			}
		}

		// Round-8 (§5.4.1 A3(iii)(θ) R8-B): kind, for the annihilation-line
		// detection in renderFanoutChainTable below (never serialized --
		// unexported, purely a rendering aid).
		if rawJoin != nil {
			row.kind = rawJoin.Kind
		}

		// Round-8 deadWeightFanout (§5.4.1 A3(iii)(θ) R8-B) vs round-6 deadJoin
		// (§5.4.1 A3(iii)(ζ)), evaluated last so both can read the
		// just-computed buildKind. Exclusivity hardening (review W2):
		// deadWeightFanout takes explicit PRECEDENCE over deadJoin on the same
		// row (a fanning row can never also be classified "no row-count
		// effect"), and deadJoin's own condition (3) additionally requires the
		// MEASURED fact `in == out` at this node -- never an
		// estimate-derived claim that "removing this join changes nothing".
		row.DeadWeightFanout = deadWeightFanoutFlag(rawJoin, row.Fanout, th)
		if !row.DeadWeightFanout {
			row.DeadJoin = row.In == row.Out && deadJoinFlag(rawJoin, row.BuildKind)
		}
	}
}
