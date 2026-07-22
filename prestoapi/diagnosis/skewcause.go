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
	"sort"
	"strings"
)

// implicatedInput is a TableScan the round-8 R8-E implication gate has proof
// for (via the fan-out chain/annihilation, or via an R8-D skew trace) --
// internal plumbing only, never serialized directly (DataFreshnessRisk is the
// public shape).
type implicatedInput struct {
	table      string
	planNodeId string
	reason     string // "emptyScan" | "ndvCollapsed"
	rows       int64
}

// collectRawNodesByType returns every node of the given bare `@type` name
// reachable from root, in a deterministic depth-first order (round-8 R8-D:
// used to find every RemoteSourceNode inside a skewed stage's own tree --
// deterministic order matters here because it decides which candidate
// partition-key column resolves first when several RemoteSourceNodes
// coexist).
func collectRawNodesByType(root *RawPlanNode, bareType string) []*RawPlanNode {
	var out []*RawPlanNode
	var walk func(n *RawPlanNode)
	walk = func(n *RawPlanNode) {
		if n == nil {
			return
		}
		if rawNodeIsType(n, bareType) {
			out = append(out, n)
		}
		walk(n.Left)
		walk(n.Right)
		walk(n.Source)
		for _, s := range n.Sources {
			walk(s)
		}
	}
	walk(root)
	return out
}

// skewedStagePartitionKeyColumns implements the round-8 §5.4.5 R8-D W4
// resolution rule: "the skewed stage's INPUT partition key columns come from
// the CHILD (producing) fragments' partitioningScheme -- a fragment's own
// scheme is its OUTPUT partitioning". Finds every RemoteSourceNode inside the
// skewed stage's own tree, resolves each one's SOLE child fragment (when
// unambiguous), and returns that child's OWN partitioning-key column names,
// deduplicated in first-seen (deterministic DFS) order.
func skewedStagePartitionKeyColumns(idx *rawPlanIndex, skewedStageSuffix string) []string {
	if idx == nil || skewedStageSuffix == "" {
		return nil
	}
	stageRoot := idx.byStage[skewedStageSuffix]
	if stageRoot == nil {
		return nil
	}
	seen := map[string]bool{}
	var cols []string
	for _, rs := range collectRawNodesByType(stageRoot, "RemoteSourceNode") {
		if len(rs.SourceFragmentIds) != 1 {
			continue
		}
		childScheme := idx.partitioningByStage[rs.SourceFragmentIds[0]]
		if childScheme == nil || childScheme.Partitioning == nil {
			continue
		}
		for _, v := range childScheme.Partitioning.Arguments {
			if v.Name != "" && !seen[v.Name] {
				seen[v.Name] = true
				cols = append(cols, v.Name)
			}
		}
	}
	return cols
}

// emptyBuildLeftTrace checks whether column traces to the output of a LEFT
// join whose build side measured 0 rows (round-8 §5.4.5 R8-D: "positively
// traces to the output of a LEFT join whose build measured 0 rows") --
// approximated, as the design's evidence-layer reads always are, by checking
// the join's OWN build-side output-variable set (never a full cross-Project
// data-flow trace) for an exact name match against an already-enriched chain
// row flagged emptyBuild. Returns the resolved table label + TableScan node
// id (for R8-E's implication use) when found.
func emptyBuildLeftTrace(idx *rawPlanIndex, chain []FanoutChainNode, column string) (table, scanNodeId string, ok bool) {
	for _, row := range chain {
		if row.BuildKind != "emptyBuild" {
			continue
		}
		join := idx.byNodeId[row.PlanNodeId]
		if join == nil || join.Kind != "LEFT" {
			continue
		}
		for _, v := range rawNodeOutputVarNames(join.Right) {
			if v == column {
				return row.Build, row.buildScanNodeId, true
			}
		}
	}
	return "", "", false
}

// columnStatsAcrossPlan searches EVERY node's variableStatistics (not just
// the chain/verdict node) for an entry matching column (after stripping the
// "<type>" suffix), picking the lowest plan-node id deterministically when
// more than one node carries it (verified on real data: the same
// pre-partition column's stats are propagated, byte-identical, to several
// nodes along its own producing fragment) -- never a map-iteration-order
// pick.
func columnStatsAcrossPlan(estimates map[string]*NodeEstimate, column string) (nullsFraction, ndv float64, nodeId string, ok bool) {
	bestId := ""
	for id, est := range estimates {
		if est == nil {
			continue
		}
		for key, vs := range est.VariableStatistics {
			if stripTypeSuffix(key) != column {
				continue
			}
			if bestId == "" || nodeIdLess(id, bestId) {
				bestId = id
				nullsFraction = float64(vs.NullsFraction)
				ndv = float64(vs.DistinctValuesCount)
			}
		}
	}
	if bestId == "" {
		return 0, 0, "", false
	}
	return nullsFraction, ndv, bestId, true
}

// skewCauseResult carries the round-8 R8-D appended C2 note (empty when
// nothing is provable) plus every TableScan it traced degeneracy to, for
// R8-E's implication gate to fold in alongside the fan-out chain/annihilation
// sources.
type skewCauseResult struct {
	note       string
	implicated []implicatedInput
}

// skewComponentFact is one partition-key column's per-component finding
// (round-8 R8-D) -- degenerate (with the table/scan it traced to, when
// structural) or not (with a stats-caveat clause either way).
type skewComponentFact struct {
	column     string
	degenerate bool
	clause     string
	table      string
	scanNodeId string
}

// computeSkewCause implements the round-8 §5.4.5 R8-D rule in full:
// evaluated ONLY when C2 survived the round-7 magnitude gate (severity > 0);
// per-column factual attribution over the skewed stage's resolved
// partition-key columns (skewedStagePartitionKeyColumns) -- a component is
// "degenerate" when it EITHER traces to an emptyBuild-LEFT join's output
// (emptyBuildLeftTrace) OR its own pre-join variableStatistics show
// nullsFraction >= SkewNullKeyFractionMin / distinctValuesCount <=
// NdvCollapsedMax; the note fires when ANY component is positively
// degenerate (stating per-column facts only, with an explicit stats caveat
// for non-degenerate components, since variableStatistics are node-scoped
// PRE-JOIN values that cannot see LEFT-join NULL production), and claims the
// full single-partition mechanism only when EVERY component is degenerate.
// Nothing provable at all (no columns resolved, or none degenerate) returns
// an empty note (omitted by the caller).
func computeSkewCause(c2 *CategoryResult, idx *rawPlanIndex, chain []FanoutChainNode, estimates map[string]*NodeEstimate, th *Thresholds) skewCauseResult {
	if c2 == nil || c2.NA || c2.Severity <= 0 || c2.StageId == "" {
		return skewCauseResult{}
	}
	cols := skewedStagePartitionKeyColumns(idx, c2.StageId)
	if len(cols) == 0 {
		return skewCauseResult{}
	}

	var facts []skewComponentFact
	anyDegenerate := false
	for _, col := range cols {
		if table, scanId, ok := emptyBuildLeftTrace(idx, chain, col); ok {
			facts = append(facts, skewComponentFact{
				column: col, degenerate: true, table: table, scanNodeId: scanId,
				clause: fmt.Sprintf("partition key component `%s` traces to the empty `%s` build (NULL under LEFT join)", col, table),
			})
			anyDegenerate = true
			continue
		}
		nullsFraction, ndv, _, statsOK := columnStatsAcrossPlan(estimates, col)
		if statsOK && (nullsFraction >= th.SkewNullKeyFractionMin || ndv <= th.NdvCollapsedMax) {
			facts = append(facts, skewComponentFact{
				column: col, degenerate: true,
				clause: fmt.Sprintf("partition key component `%s` shows a collapsed key (NDV %s, nulls %.3f)", col, formatFingerprintValue2(ndv), nullsFraction),
			})
			anyDegenerate = true
			continue
		}
		if statsOK {
			facts = append(facts, skewComponentFact{
				column: col,
				clause: fmt.Sprintf("component `%s` shows non-degenerate pre-join stats (NDV %s, nulls %.1f — node-scoped, cannot see LEFT-join NULL production)", col, formatFingerprintValue2(ndv), nullsFraction),
			})
			continue
		}
		facts = append(facts, skewComponentFact{
			column: col,
			clause: fmt.Sprintf("component `%s` has no resolvable pre-join stats", col),
		})
	}
	if !anyDegenerate {
		return skewCauseResult{}
	}

	clauses := make([]string, 0, len(facts))
	var implicated []implicatedInput
	for _, f := range facts {
		clauses = append(clauses, f.clause)
		if f.degenerate && f.scanNodeId != "" {
			implicated = append(implicated, implicatedInput{table: f.table, planNodeId: f.scanNodeId, reason: "emptyScan", rows: 0})
		}
	}
	note := "skew cause: " + strings.Join(clauses, "; ")
	if allDegenerate(facts) {
		note += " — all partition-key components are degenerate, explaining the skew via a single-partition mechanism"
	}
	return skewCauseResult{note: note, implicated: implicated}
}

func allDegenerate(facts []skewComponentFact) bool {
	for _, f := range facts {
		if !f.degenerate {
			return false
		}
	}
	return len(facts) > 0
}

// formatFingerprintValue2 renders a bare float64 (never a JsonFloat64,
// unlike formatFingerprintValue) the same way -- whole numbers without a
// decimal point, NaN/Inf as their literal names.
func formatFingerprintValue2(v float64) string {
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "Infinity"
	case math.IsInf(v, -1):
		return "-Infinity"
	case v == math.Trunc(v) && math.Abs(v) < 1e15:
		return fmt.Sprintf("%.0f", v)
	default:
		return fmt.Sprintf("%g", v)
	}
}

// DataFreshnessRisk is one round-8 §5.4.5 R8-E finding: an input IMPLICATED
// in the diagnosis (on the reported fan-out-chain spine, in an R8-C
// annihilation finding, or in an R8-D skew trace) whose own TableScan
// measured 0 output rows, or whose join/partition-key column NDV collapsed
// while rows > 0 -- informational evidence only, no verdict/severity effect.
type DataFreshnessRisk struct {
	Table      string `json:"table"`
	PlanNodeId string `json:"planNodeId"`
	Reason     string `json:"reason"`
	Rows       int64  `json:"rows"`
}

// dataFreshnessRiskHint is the §5.1.1 templated hint's fixed tail, rendered
// verbatim (never free text) on every dataFreshnessRisks evidence line by
// dataFreshnessRiskLine below.
const dataFreshnessRiskHint = "if this table should have data (e.g. a Looker PDT `lr_*` scratch table), repopulate it; the fix is data, not a rewrite"

// dataFreshnessRiskLine renders one round-8 R8-E evidence line: the
// table/node-scoped fact (emptyScan or ndvCollapsed) followed by the §5.1.1
// templated hint, verbatim.
func dataFreshnessRiskLine(r DataFreshnessRisk) string {
	fact := fmt.Sprintf("%s (node %s) scanned 0 rows", r.Table, r.PlanNodeId)
	if r.Reason == "ndvCollapsed" {
		fact = fmt.Sprintf("%s (node %s) has a collapsed key NDV (%d rows)", r.Table, r.PlanNodeId, r.Rows)
	}
	return fmt.Sprintf("possible DATA/freshness issue (not query shape): %s — %s", fact, dataFreshnessRiskHint)
}

// computeDataFreshnessRisks implements the round-8 §5.4.5 R8-E implication
// gate: candidates are collected ONLY from inputs already implicated by the
// reported fan-out chain (every row, including an R8-C annihilation row --
// both are simply chain rows with BuildKind=="emptyBuild") and from the R8-D
// skewCause trace (skewImplicated) -- a real-but-UNIMPLICATED empty scan
// elsewhere in the plan (the healthy fixture's own node-22 empty PDT) is
// deliberately never surfaced (coordinator/user "healthy stays quiet"
// decision, review C4). Deduplicated by plan-node id, sorted by NUMERIC
// planNodeId (determinism, review W5) -- never by discovery order.
func computeDataFreshnessRisks(chain []FanoutChainNode, skewImplicated []implicatedInput, th *Thresholds) []DataFreshnessRisk {
	seen := map[string]bool{}
	var cands []implicatedInput
	add := func(c implicatedInput) {
		if c.planNodeId == "" || seen[c.planNodeId] {
			return
		}
		seen[c.planNodeId] = true
		cands = append(cands, c)
	}
	for _, row := range chain {
		if row.buildScanNodeId == "" {
			continue
		}
		switch {
		case row.BuildKind == "emptyBuild" && row.BuildRows != nil && *row.BuildRows == 0:
			add(implicatedInput{table: row.Build, planNodeId: row.buildScanNodeId, reason: "emptyScan", rows: 0})
		case row.BuildKeyNdv != nil && *row.BuildKeyNdv <= th.NdvCollapsedMax && row.BuildRows != nil && *row.BuildRows > 0:
			add(implicatedInput{table: row.Build, planNodeId: row.buildScanNodeId, reason: "ndvCollapsed", rows: *row.BuildRows})
		}
	}
	for _, c := range skewImplicated {
		add(c)
	}
	sort.Slice(cands, func(i, j int) bool { return nodeIdLess(cands[i].planNodeId, cands[j].planNodeId) })
	out := make([]DataFreshnessRisk, 0, len(cands))
	for _, c := range cands {
		out = append(out, DataFreshnessRisk{Table: c.table, PlanNodeId: c.planNodeId, Reason: c.reason, Rows: c.rows})
	}
	return out
}
