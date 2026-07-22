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
	"sort"
	"strings"
)

// ResultCorrectnessRisk is the round-8 §5.4.1 A3(iii)(η) R8-A top-level
// finding: an A3 (plan-path) fan-out feeding one or more duplicate-SENSITIVE
// aggregates, meaning the query's RESULT -- not just its performance -- is at
// risk of being wrong. A correctness annotation layered on the A3 verdict:
// never a severity/category change (§5.4.5).
type ResultCorrectnessRisk struct {
	NodeId string `json:"nodeId"`
	// Aggregates is the sorted, unique set of flagged aggregate DISPLAY NAMES
	// (determinism, review W5) -- e.g. ["avg"], never a per-node/per-argument
	// breakdown.
	Aggregates []string `json:"aggregates"`
	Note       string   `json:"note"`
}

// dupInsensitiveAllowList is the round-8 §5.4.1 A3(iii)(η) fixed allow-list of
// aggregate function display names that are duplicate-INSENSITIVE regardless
// of DISTINCT qualification (review S1): each of these returns the same
// result whether or not its input rows were duplicated by an upstream
// fan-out. Every other function -- sum, avg, non-distinct count, stddev*,
// var*, array_agg, and any UNKNOWN function -- is duplicate-SENSITIVE
// (unknown => sensitive, the design's explicit fail-safe default).
var dupInsensitiveAllowList = map[string]bool{
	"min": true, "max": true, "bool_or": true, "bool_and": true,
	"approx_distinct": true, "arbitrary": true, "any_value": true,
	"max_by": true, "min_by": true,
}

// argLineage is the round-8 §5.4.1 A3(iii)(η) W1 argument-lineage qualifier's
// resolution outcome for a duplicate-sensitive aggregate's argument, relative
// to the SOLE fanning spine join (only consulted when there is exactly one).
type argLineage int

const (
	lineageUnresolved argLineage = iota
	lineageBuildSide
	lineageProbeSide
)

// resultCorrectnessRiskHedgeSuffix is appended to the note (verbatim, §5.1.1)
// when at least one flagged aggregate's argument lineage could not be
// resolved relative to the sole fanning spine join.
const resultCorrectnessRiskHedgeSuffix = " — argument lineage unresolved"

// resultCorrectnessRiskNoteTemplate is the §5.1.1 lookup-table entry for the
// A3 + duplicate-SENSITIVE-aggregate signal, rendered verbatim (never
// synthesized): "RESULT-CORRECTNESS RISK: fan-out at node N likely INFLATES
// <agg list> — result may be incorrect (not just slow); dedup the fanning
// input to one row per key before aggregating" (hedged variant appends
// resultCorrectnessRiskHedgeSuffix).
const resultCorrectnessRiskNoteTemplate = "RESULT-CORRECTNESS RISK: fan-out at node %s likely INFLATES %s — result may be incorrect (not just slow); dedup the fanning input to one row per key before aggregating"

// markDistinctMarkerNames collects every MarkDistinctNode.MarkerVariable name
// across the ENTIRE raw plan (all stages) -- the mandatory producer-trace
// proof of genuine DISTINCT qualification (review C1): a `mask` variable
// tracing to one of these names came from Presto's own COUNT(DISTINCT)
// lowering; a mask that does NOT trace here (e.g. one produced by
// ImplementFilteredAggregations for a plain SQL FILTER) confers no
// duplicate-insensitivity at all. Building this as a set makes the result
// independent of idx.byNodeId's own (map) iteration order.
func markDistinctMarkerNames(idx *rawPlanIndex) map[string]bool {
	names := map[string]bool{}
	if idx == nil {
		return names
	}
	for _, n := range idx.byNodeId {
		if rawNodeIsType(n, "MarkDistinctNode") && n.MarkerVariable != nil && n.MarkerVariable.Name != "" {
			names[n.MarkerVariable.Name] = true
		}
	}
	return names
}

// findNearestAggregations implements the round-8 §5.4.1 A3(iii)(η) walk:
// "the nearest AggregationNode on each downstream path" from the A3 verdict
// node, climbing through the raw plan's parent edges (within-stage, via
// rawPlanIndex.parent) and across stage boundaries (via rawPlanIndex.
// crossRefs, exploring EVERY consumer edge when a stage's root is read by
// more than one -- unlike R8-C's linear spine walk, which stops at such a
// fork, R8-A must classify every downstream path). A branch's climb STOPS the
// moment it reaches ANY AggregationNode, classified or not -- "never walk
// past it" (a FINAL/INTERMEDIATE step's own callers are filtered by the
// caller, buildResultCorrectnessRisk, not here).
func findNearestAggregations(idx *rawPlanIndex, verdictNodeId string) []*RawPlanNode {
	if idx == nil {
		return nil
	}
	var found []*RawPlanNode
	visited := map[string]bool{}
	var walk func(nodeId string, depth int)
	walk = func(nodeId string, depth int) {
		if depth > 200 || visited[nodeId] {
			return
		}
		visited[nodeId] = true
		edge, ok := idx.parent[nodeId]
		if !ok {
			stage := idx.stageOf[nodeId]
			for _, ref := range idx.crossRefs[stage] {
				walk(ref.Id, depth+1)
			}
			return
		}
		p := edge.node
		if rawNodeIsType(p, "AggregationNode") {
			found = append(found, p)
			return
		}
		walk(p.Id, depth+1)
	}
	walk(verdictNodeId, 0)
	return found
}

// probeSideVisited walks a JoinNode's probe (left) side the same way
// nextUpstreamJoin does, but returns every node visited along the way
// (stopping at, and including, the first JoinNode/TableScanNode/ambiguous
// shape) -- used by argumentLineage below to check whether an aggregate's
// argument variable originates on the PROBE side of the sole fanning join.
func probeSideVisited(join *RawPlanNode, idx *rawPlanIndex) []*RawPlanNode {
	if join == nil || join.Left == nil {
		return nil
	}
	var visited []*RawPlanNode
	cur := join.Left
	for depth := 0; cur != nil && depth < 64; depth++ {
		visited = append(visited, cur)
		switch {
		case rawNodeIsType(cur, "JoinNode"), rawNodeIsType(cur, "TableScanNode"):
			return visited
		case rawNodeIsType(cur, "RemoteSourceNode"):
			if len(cur.SourceFragmentIds) != 1 {
				return visited
			}
			next := idx.byStage[cur.SourceFragmentIds[0]]
			if next == nil {
				return visited
			}
			cur = next
		case cur.Source != nil && cur.Right == nil && cur.Left == nil && len(cur.Sources) == 0:
			cur = cur.Source
		case len(cur.Sources) == 1:
			cur = cur.Sources[0]
		default:
			return visited
		}
	}
	return visited
}

// aggregationArgumentName returns a raw aggregation's first argument
// variable name, preferring Call.Arguments (the function-call's own argument
// list) and falling back to the aggregation entry's own Arguments field --
// both are populated identically on the real fixtures; kept absent-tolerant
// for engine variants that might only populate one.
func aggregationArgumentName(a RawAggregation) string {
	if len(a.Call.Arguments) > 0 {
		return a.Call.Arguments[0].Name
	}
	if len(a.Arguments) > 0 {
		return a.Arguments[0].Name
	}
	return ""
}

// argumentLineage implements the round-8 §5.4.1 A3(iii)(η) W1 qualifier's
// lineage resolution for the "exactly one fanning spine join" case: searches
// the sole fanning join's build-side walk (resolveBuildSide's visited nodes)
// and probe-side walk (probeSideVisited) for a node whose own output columns
// include the aggregate's argument variable name. Build-side match =>
// intended 1:N expansion (the fan-out IS the data being aggregated);
// probe-side match => flagged full-strength; neither (or no argument at all)
// => unresolved, rendered with the hedged wording.
func argumentLineage(idx *rawPlanIndex, fanningJoinNodeId string, a RawAggregation) argLineage {
	argName := aggregationArgumentName(a)
	if argName == "" || idx == nil {
		return lineageUnresolved
	}
	join := idx.byNodeId[fanningJoinNodeId]
	if join == nil {
		return lineageUnresolved
	}
	buildWalk := resolveBuildSide(join, idx)
	for _, n := range buildWalk.visited {
		for _, v := range rawNodeOutputVarNames(n) {
			if v == argName {
				return lineageBuildSide
			}
		}
	}
	for _, n := range probeSideVisited(join, idx) {
		for _, v := range rawNodeOutputVarNames(n) {
			if v == argName {
				return lineageProbeSide
			}
		}
	}
	return lineageUnresolved
}

// classifyResultCorrectnessRisk implements the full round-8 §5.4.1
// A3(iii)(η) R8-A detection rule: classify only the nearest PARTIAL/SINGLE
// AggregationNode(s) on each downstream path from the verdict node (never
// FINAL/INTERMEDIATE); for each aggregation, duplicate-INSENSITIVE =
// `distinct==true` OR a mask tracing to a MarkDistinctNode.markerVariable OR
// the fixed allow-list; everything else is duplicate-SENSITIVE and subject
// to the W1 argument-lineage qualifier ("fanning spine join" = a chain row
// with measured fan-out >= FanoutModerate): >=2 fanning spine joins flags
// full-strength unconditionally (every aggregate is duplicated by the OTHER
// joins regardless of which one this argument traces to); exactly 1 requires
// real lineage tracing (build-side suppressed, probe-side/unresolved
// flagged, the latter hedged); 0 (not expected under a real A3 verdict, but
// handled defensively) flags full-strength. Window functions are invisible
// by construction (only AggregationNode.Aggregations is ever read, review
// W7).
func classifyResultCorrectnessRisk(idx *rawPlanIndex, chain []FanoutChainNode, verdictNodeId string, th *Thresholds) (flagged map[string]bool, anyHedged bool) {
	flagged = map[string]bool{}
	aggNodes := findNearestAggregations(idx, verdictNodeId)
	if len(aggNodes) == 0 {
		return flagged, false
	}
	markers := markDistinctMarkerNames(idx)

	var fanningRowIds []string
	for _, row := range chain {
		if row.Fanout >= th.FanoutModerate {
			fanningRowIds = append(fanningRowIds, row.PlanNodeId)
		}
	}

	for _, agg := range aggNodes {
		// Review C2: only SINGLE/PARTIAL steps consume ROWS; FINAL/
		// INTERMEDIATE steps consume partial aggregation STATE and must never
		// be classified (the 15_118 negative: its FINAL node's unmasked
		// counts are the consumed-partial-state counterparts of its PARTIAL
		// node's masked, insensitive ones).
		if agg.Step != "SINGLE" && agg.Step != "PARTIAL" {
			continue
		}
		for _, a := range agg.Aggregations {
			name := a.Call.DisplayName
			if name == "" {
				continue
			}
			if a.Distinct || (a.Mask != nil && markers[a.Mask.Name]) {
				continue // genuinely DISTINCT-qualified -- duplicate-INSENSITIVE
			}
			if dupInsensitiveAllowList[name] {
				continue
			}
			switch len(fanningRowIds) {
			case 0:
				flagged[name] = true
			case 1:
				switch argumentLineage(idx, fanningRowIds[0], a) {
				case lineageBuildSide:
					// sole-fanning-join build-side argument -- intended
					// expansion, not flagged.
				case lineageProbeSide:
					flagged[name] = true
				default:
					flagged[name] = true
					anyHedged = true
				}
			default:
				flagged[name] = true // >=2 fanning spine joins -- flag full-strength
			}
		}
	}
	return flagged, anyHedged
}

// buildResultCorrectnessRisk assembles the round-8 top-level
// ResultCorrectnessRisk finding (nil when nothing is flagged, or when the
// raw structured plan cannot resolve any downstream aggregation at all --
// e.g. no raw plan present for this query).
func buildResultCorrectnessRisk(idx *rawPlanIndex, chain []FanoutChainNode, verdictNodeId string, th *Thresholds) *ResultCorrectnessRisk {
	if idx == nil || verdictNodeId == "" {
		return nil
	}
	flagged, anyHedged := classifyResultCorrectnessRisk(idx, chain, verdictNodeId, th)
	if len(flagged) == 0 {
		return nil
	}
	names := make([]string, 0, len(flagged))
	for n := range flagged {
		names = append(names, n)
	}
	sort.Strings(names)
	note := fmt.Sprintf(resultCorrectnessRiskNoteTemplate, verdictNodeId, strings.Join(names, ", "))
	if anyHedged {
		note += resultCorrectnessRiskHedgeSuffix
	}
	return &ResultCorrectnessRisk{NodeId: verdictNodeId, Aggregates: names, Note: note}
}
