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
	"strconv"
	"strings"

	"pbench/prestoapi/plan_node"
)

// JoinKeyStat is one row of the A3 per-join-key NDV-fingerprint sub-table
// (design.md §5.4.1 A3, §5.4.2, §5.5 — cycle-test evidence enrichment):
// {min, max, distinctValuesCount} for a join-predicate column, read straight
// from planStatsAndCosts[node].variableStatistics. This is a factual,
// what+where evidence read only — it never affects severity/elevation.
//
// Min/Max/NDV use plan_node.JsonFloat64, not a bare float64: NDV in
// particular is expected to legitimately BE NaN (that is the whole point of
// the fingerprint — showing "stats absent", not omitting the field), and
// Go's encoding/json cannot marshal a bare float64 NaN/Inf ("json:
// unsupported value: NaN") — JsonFloat64's MarshalJSON already renders those
// as the same "NaN"/"Infinity"/"-Infinity" wire strings planStatsAndCosts
// itself uses (§2), so no new NaN-handling code is needed here.
type JoinKeyStat struct {
	Column string                `json:"column"`
	Min    plan_node.JsonFloat64 `json:"min"`
	Max    plan_node.JsonFloat64 `json:"max"`
	NDV    plan_node.JsonFloat64 `json:"ndv"`
}

// stripTypeSuffix removes the trailing "<type>" suffix Presto appends to
// variableStatistics map keys (e.g. "order_id_52<bigint>" -> "order_id_52"),
// so they can be matched against the bare column names a join predicate's
// identifier text uses (e.g. `[("id" = "order_id_52")]`).
func stripTypeSuffix(key string) string {
	if !strings.HasSuffix(key, ">") {
		return key
	}
	if idx := strings.LastIndexByte(key, '<'); idx >= 0 {
		return key[:idx]
	}
	return key
}

// joinKeyNames extracts the raw join-predicate column names for a plan node
// (both sides of every equality predicate), using the same parsing primitives
// plan_node.PlanTree.ParseJoins uses internally
// (PlanNodeJoinPredicatesParser / JoinPredicate.GetAssignments), but reading
// the RAW assignment strings directly rather than resolving them through a
// cross-stage assignment map: a bare join predicate over source columns
// (`[("id" = "order_id_52")]`, the common case) has no entry in that map, so
// plan_node.Join{Left,Right}Value would be a nil Value — the raw predicate
// text already carries the exact variable name variableStatistics keys use
// (after stripping "<type>"), which is all this evidence read needs. Returns
// nil for a non-join node or an unparsable identifier (never an error —
// evidence enrichment degrades gracefully, per §5.1.1).
func joinKeyNames(node *plan_node.PlanNode) []string {
	if node == nil || !plan_node.IsJoin[node.Name] || node.Identifier == "" {
		return nil
	}
	parsed, err := plan_node.PlanNodeJoinPredicatesParser.ParseString(node.Id, node.Identifier)
	if err != nil {
		return nil
	}
	seen := map[string]bool{}
	var names []string
	for _, exp := range parsed.GetJoins() {
		for _, pred := range exp.GetJoinPredicates() {
			left, right := pred.GetAssignments()
			for _, n := range append(left, right...) {
				if n != "" && !seen[n] {
					seen[n] = true
					names = append(names, n)
				}
			}
		}
	}
	return names
}

// ExtractJoinKeyStats resolves the per-join-key NDV fingerprint for a plan
// node: identifies its join-predicate columns via joinKeyNames, then matches
// each against variableStatistics (stripping the "<type>" suffix from its
// keys). Non-join nodes, or nodes whose predicate columns have no matching
// variableStatistics entry, yield an empty (nil) result — never an error.
func ExtractJoinKeyStats(node *plan_node.PlanNode, variableStatistics map[string]plan_node.VariableStatistics) []JoinKeyStat {
	names := joinKeyNames(node)
	if len(names) == 0 || len(variableStatistics) == 0 {
		return nil
	}
	byBareName := make(map[string]plan_node.VariableStatistics, len(variableStatistics))
	for key, vs := range variableStatistics {
		byBareName[stripTypeSuffix(key)] = vs
	}
	var stats []JoinKeyStat
	for _, name := range names {
		vs, ok := byBareName[name]
		if !ok {
			continue
		}
		stats = append(stats, JoinKeyStat{
			Column: name,
			Min:    vs.LowValue,
			Max:    vs.HighValue,
			NDV:    vs.DistinctValuesCount,
		})
	}
	return stats
}

// HasNaNJoinKeyNDV reports whether at least one join-key fingerprint row has
// a missing (NaN) distinctValuesCount — the trigger condition for rendering
// the sub-table (design.md §5.4.1 A3: "when the A3 verdict fires with >=1
// NaN-NDV join key").
func HasNaNJoinKeyNDV(stats []JoinKeyStat) bool {
	for _, s := range stats {
		if math.IsNaN(float64(s.NDV)) {
			return true
		}
	}
	return false
}

// nodeHasNaNJoinKeyNDV is the on-demand join-key-specific "NDV missing" check
// estimateBad uses in place of the retracted joinNodeStatsEstimate heuristic
// (REC-2): resolves the node's join-predicate columns from planNodes and
// matches them against its planStatsAndCosts variableStatistics, exactly like
// enrichA3Evidence's fingerprint extraction, but callable from classify.go/
// progression.go's severity-scoring paths where only a plan-node lookup and
// an estimate map (not yet a fully-built NodeMetrics.JoinKeyStats) are
// available. Never errors: missing lookups/estimates simply degrade to false.
func nodeHasNaNJoinKeyNDV(nodeId string, planNodes map[string]*plan_node.PlanNode, estimates map[string]*NodeEstimate) bool {
	if planNodes == nil || estimates == nil {
		return false
	}
	est := estimates[nodeId]
	if est == nil {
		return false
	}
	return HasNaNJoinKeyNDV(ExtractJoinKeyStats(planNodes[nodeId], est.VariableStatistics))
}

// formatFingerprintValue renders one fingerprint cell: NaN/Infinity as their
// literal names (matching planStatsAndCosts' own wire vocabulary, §2), whole
// numbers without a decimal point (e.g. "314", not "314.000000"), everything
// else with Go's shortest round-tripping representation.
func formatFingerprintValue(jv plan_node.JsonFloat64) string {
	v := float64(jv)
	switch {
	case math.IsNaN(v):
		return "NaN"
	case math.IsInf(v, 1):
		return "Infinity"
	case math.IsInf(v, -1):
		return "-Infinity"
	case v == math.Trunc(v) && math.Abs(v) < 1e15:
		return strconv.FormatFloat(v, 'f', 0, 64)
	default:
		return strconv.FormatFloat(v, 'g', -1, 64)
	}
}

// enrichA3Evidence implements the round-1+round-2 cycle-test evidence
// enrichments (design.md §10 items 10/11, §5.4.1 A3, §5.4.2 FallbackEstimateMin)
// on an already-decided A3 verdict: (i) the per-join-key NDV-fingerprint
// sub-table, inserted as a ranked-evidence item whenever >=1 join key is
// resolved -- round 2: rendered for BOTH NaN and present NDV, since present
// NDV drives the REC-1 recommendation branch (report.go); (ii) the
// "default-join-selectivity fallback" estimate-provenance label, when
// additionally join-key NDV is NaN, confident=="LOW", and outputRowCount >=
// FallbackEstimateMin; (iii) REC-3 the per-node fan-out CHAIN, when actuals
// are available. All three are pure evidence/labeling reads: root's
// Severity/SignalFamilies/FromMeasuredActual and every other causal-engine
// field are untouched — this runs strictly after Attribute() has already
// picked the verdict. Returns the fan-out chain (nil when not triggered) so
// the caller can also surface it as the top-level Report.FanoutChain field.
func enrichA3Evidence(root *CategoryResult, nodeMetrics map[string]*NodeMetrics, nodeStageId map[string]int, extra *ExtraInfo, planNodes map[string]*plan_node.PlanNode, th *Thresholds) ([]FanoutChainNode, *ChainFanout, *ResultCorrectnessRisk, *rawPlanIndex) {
	var outputStage *StageNode
	if extra != nil {
		outputStage = extra.OutputStage
	}
	idx := buildRawPlanIndex(outputStage)
	if root == nil || root.NA || root.Category != CatPlanPath || root.NodeId == "" {
		return nil, nil, nil, idx
	}
	if extra == nil || extra.PlanStatsAndCosts == nil {
		return nil, nil, nil, idx
	}
	est := extra.PlanStatsAndCosts.Stats[root.NodeId]
	if est == nil {
		return nil, nil, nil, idx
	}
	var node *plan_node.PlanNode
	if planNodes != nil {
		node = planNodes[root.NodeId]
	}
	stats := ExtractJoinKeyStats(node, est.VariableStatistics)

	nm := nodeMetrics[root.NodeId]

	// Locate the outputRowCount evidence item structurally, via its Values
	// marker, rather than matching classify.go's exact format string, so this
	// stays robust to unrelated wording changes there. Needed regardless of
	// whether the fingerprint/fallback fire, so the fan-out chain (below) has
	// a stable insertion anchor either way.
	outputRowCountIdx := -1
	for i := range root.Evidence {
		if _, ok := root.Evidence[i].Values["estimatedOutputRowCount"]; ok {
			outputRowCountIdx = i
			break
		}
	}
	insertAfter := outputRowCountIdx // running insertion cursor; grows as items are added

	if len(stats) > 0 {
		if nm != nil {
			nm.JoinKeyStats = stats
		}
		hasNaN := HasNaNJoinKeyNDV(stats)

		// Estimate-provenance fallback label: a strictly narrower condition
		// than the fingerprint sub-table (also requires NaN join-key NDV,
		// confident=="LOW", and a sufficiently astronomical outputRowCount)
		// -- display/labeling only, no elevation effect (FallbackEstimateMin).
		outputRowCount := float64(est.OutputRowCount)
		fallback := hasNaN && est.Confident == "LOW" && !math.IsNaN(outputRowCount) && outputRowCount >= th.FallbackEstimateMin
		if fallback && nm != nil {
			nm.EstimateProvenance = "default_join_selectivity_fallback"
		}
		if fallback && outputRowCountIdx >= 0 {
			// Rebuild (rather than text-patch) the line in place.
			root.Evidence[outputRowCountIdx].Text = fmt.Sprintf(
				"planStatsAndCosts[%s]: outputRowCount=%.3g = default-join-selectivity FALLBACK (join-key NDV unavailable — engine guesswork, not a measured cardinality), confident=%s",
				root.NodeId, outputRowCount, est.Confident)
		}

		// Insert the fingerprint sub-table directly after the outputRowCount
		// line (matching design.md §5.5's own worked-example ordering: items
		// 2/3) rather than appending it last. It is the headline "which
		// join key lacks/has NDV" signal, and other A3 evidence lines that
		// only apply in some modes (RunawayComputation, estimate_error_ratio)
		// would otherwise push it past the default --top truncation in the
		// terminal+snapshots / snapshot-only paths (reviewer finding M1) --
		// appending unconditionally at the end is too fragile to how many of
		// those optional lines happen to precede it.
		fingerprintItem := EvidenceItem{Text: renderJoinKeyFingerprintTable(stats, hasNaN)}
		insertAfter = insertEvidenceAfter(root, insertAfter, fingerprintItem)
	}

	// REC-3: per-node fan-out chain, terminal/finished-stage actuals only.
	// Inserted right after the fingerprint (or, if the fingerprint did not
	// fire, right after the outputRowCount line) so it stays within the
	// default --top window alongside the fingerprint, per the same M1
	// rationale above.
	//
	// Round-8 (§5.4.1 A3(iii)(ι) R8-C): when the raw structured plan resolves
	// a full probe-lineage spine via the actual plan TOPOLOGY (never numeric
	// in/out coincidence), that spine is authoritative and supersedes the
	// legacy same-stage numeric-matching walk -- it is a strict superset in
	// every case verified against the real fixtures (drr9h/258_463), and
	// degrades gracefully to the exact legacy behavior whenever the raw plan
	// cannot resolve the verdict node as a JoinNode with measured actuals
	// (e.g. no raw structured plan at all for this query).
	chain := ExtractFanoutChain(nodeMetrics, nodeStageId, root.NodeId)
	if spineIds := extractProbeSpineIds(idx, nodeMetrics, root.NodeId); len(spineIds) > 0 {
		if topologyChain := buildChainFromIds(spineIds, nodeMetrics); len(topologyChain) > 0 {
			chain = topologyChain
		}
	}
	var chainFanout *ChainFanout
	var correctnessRisk *ResultCorrectnessRisk
	if len(chain) > 0 {
		// Round-5 per-row enrichment (design.md §5.4.1 A3(iii), §10 item 14):
		// build/joinKeys/sqlLine/build-key uniqueness facts, from the raw
		// structured plan -- pure additive evidence, mutates the chain rows
		// in place before rendering/returning; never touches
		// PlanNodeId/Operator/In/Out/Fanout or any causal-engine field.
		enrichFanoutChainRows(chain, idx, planNodes, extra.PlanStatsAndCosts.Stats, nodeMetrics, th)
		// Round-6 (§5.4.1 A3(iii)(ε)): the chain fan-out end-to-end summary,
		// rendered as a continuation line of the SAME evidence item as the
		// chain table (never a separate ranked-evidence entry). Round 8: the
		// summary line is followed by any annihilation evidence line(s)
		// (§5.4.1 A3(iii)(ι) R8-C) as further lines of the SAME item.
		chainFanout = computeChainFanout(chain)
		chainText := renderFanoutChainTable(chain, chainFanout)
		for _, line := range annihilationEvidenceLines(chain) {
			chainText += "\n" + line
		}
		chainItem := EvidenceItem{Text: chainText}
		insertEvidenceAfter(root, insertAfter, chainItem)

		// Round-8 (§5.4.1 A3(iii)(η) R8-A): the result-correctness-risk
		// headline, classified from the already-built chain (fanning-spine
		// join count for the W1 argument-lineage qualifier) and the raw plan
		// index (nearest PARTIAL/SINGLE aggregation walk, mask->MarkDistinct
		// producer trace).
		correctnessRisk = buildResultCorrectnessRisk(idx, chain, root.NodeId, th)
	}
	return chain, chainFanout, correctnessRisk, idx
}

// insertEvidenceAfter inserts item into root.Evidence immediately after index
// afterIdx (appending at the end when afterIdx < 0, i.e. no anchor was
// found), and returns the index the inserted item now occupies -- so callers
// can chain a second insertion immediately after the first.
func insertEvidenceAfter(root *CategoryResult, afterIdx int, item EvidenceItem) int {
	if afterIdx < 0 {
		root.Evidence = append(root.Evidence, item)
		return len(root.Evidence) - 1
	}
	insertAt := afterIdx + 1
	root.Evidence = append(root.Evidence[:insertAt:insertAt], append([]EvidenceItem{item}, root.Evidence[insertAt:]...)...)
	return insertAt
}

// renderJoinKeyFingerprintTable renders the §5.5 markdown sub-table as a
// single evidence-item Text value (a plain string; JSON just carries the same
// markdown-table text verbatim, exactly like every other evidence Text —
// there is no separate table schema). The header text names which REC-1
// branch applies (NaN missing-stats fingerprint vs present real cardinalities)
// since round 2 renders this table for both.
func renderJoinKeyFingerprintTable(stats []JoinKeyStat, hasNaN bool) string {
	var b strings.Builder
	if hasNaN {
		b.WriteString("planStatsAndCosts variableStatistics: distinctValuesCount=NaN on join keys — NDV fingerprint (min/max present, NDV missing ⇒ stats absent, not empty data):\n")
	} else {
		b.WriteString("NDV fingerprint — join-key statistics PRESENT (ANALYZE will not change this):\n")
	}
	b.WriteString("| join key | min | max | distinctValuesCount |\n")
	b.WriteString("|---|---|---|---|\n")
	for _, s := range stats {
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", s.Column, formatFingerprintValue(s.Min), formatFingerprintValue(s.Max), formatFingerprintValue(s.NDV))
	}
	return strings.TrimRight(b.String(), "\n")
}

// FanoutChainNode is one row of the REC-3 per-node fan-out chain: a join node
// on the path to the A3 verdict node, in join (data-flow) order, with its
// measured actual in/out/fan-out from operatorSummaries.
//
// Round-5 enrichment (design.md §5.4.1 A3(iii), §10 item 14, additive
// evidence-layer only -- no severity/verdict/confidence change): Build and
// JoinKeys are always present when the chain fires (a graceful fallback
// label/empty list when the raw plan can't resolve them, never blank); the
// rest are optional and simply omitted from JSON when their source data is
// absent, per the locked schema (§5.5).
type FanoutChainNode struct {
	PlanNodeId string  `json:"planNodeId"`
	Operator   string  `json:"operator"`
	In         int64   `json:"in"`
	Out        int64   `json:"out"`
	Fanout     float64 `json:"fanout"`

	// Build is the build-side (right) input's fully-qualified table name
	// ("schema.table"), resolved by walking the raw structured plan from the
	// JoinNode's build child down to its TableScan; a graceful fallback label
	// (e.g. "RemoteSource(S12)" or the unresolved child's own node-type name)
	// when no single TableScan resolves -- never blank.
	Build string `json:"build"`
	// JoinKeys is the equi-join key pair(s) as "left=right" strings, from the
	// raw JoinNode's criteria[] (primary source) or, when that is
	// unavailable, plan_node.ParseJoins on the textual plan (the same
	// fallback path the NDV fingerprint uses).
	JoinKeys []string `json:"joinKeys"`
	// SqlLine is the join's SQL source line (criteria[].left/right.
	// sourceLocation.line), best-effort and optional: engine/version
	// dependent, omitted when absent.
	SqlLine *int `json:"sqlLine,omitempty"`
	// BuildRows is the build side's row count: the resolved TableScan's own
	// measured actual (terminal/finished) when available, else the
	// planStatsAndCosts estimate -- BuildRowsSource records which
	// ("measured"|"estimate"). Omitted when neither is resolvable.
	BuildRows       *int64 `json:"buildRows,omitempty"`
	BuildRowsSource string `json:"buildRowsSource,omitempty"`
	// BuildKeyNdv is the build-side join key's own distinctValuesCount, and
	// BuildKeyDupFactor = BuildRows/BuildKeyNdv. BuildKind classifies the
	// build key's uniqueness ("unique"|"dimensionNonUnique"|"detail1toN",
	// §5.4.2's two display-only constants) -- all three omitted together
	// when BuildKeyNdv cannot be resolved.
	//
	// Round-6 (design.md §5.4.1 A3(iii)(δ)): BuildKind gains a fourth,
	// precedence-first value "emptyBuild" (a MEASURED zero-row build --
	// BuildKeyDupFactor is omitted for it; an empty build proves nothing
	// about key uniqueness).
	BuildKeyNdv       *float64 `json:"buildKeyNdv,omitempty"`
	BuildKeyDupFactor *float64 `json:"buildKeyDupFactor,omitempty"`
	BuildKind         string   `json:"buildKind,omitempty"`
	// DeadJoin (round-6, design.md §5.4.1 A3(iii)(ζ)) is true only when the
	// conservative, positive-evidence-only rule positively fires (LEFT join,
	// zero build survivors downstream, no row-count effect); omitted (never
	// rendered as `false`) otherwise.
	DeadJoin bool `json:"deadJoin,omitempty"`
	// DeadWeightFanout (round-8, design.md §5.4.1 A3(iii)(θ) R8-B) is true
	// only when the fanning complement of DeadJoin positively fires (LEFT
	// join, zero build survivors downstream, MEASURED fan-out >=
	// FanoutModerate); takes explicit precedence over DeadJoin on the same
	// row (never both true at once).
	DeadWeightFanout bool `json:"deadWeightFanout,omitempty"`

	// kind is the raw JoinNode's own "type" attribute ("LEFT"/"INNER"/etc.,
	// round-8 R8-C annihilation-line detection) -- deliberately unexported
	// (never serialized): a rendering aid only, not part of the locked
	// fanoutChain[] schema.
	kind string
	// buildScanNodeId is the resolved build-side TableScanNode's own plan
	// node id (round-8 R8-D/R8-E plumbing) -- deliberately unexported (never
	// serialized): lets the skew-trace/data-freshness code point at the
	// SAME TableScan this row's Build/BuildRows fields already describe,
	// without re-walking the raw plan.
	buildScanNodeId string
}

// isJoinOperatorType reports whether opType looks like a join PROBE operator
// (e.g. "LookupJoinOperator", generic across engines) as opposed to a join
// BUILD-side operator (e.g. "HashBuilderOperator", which always reports 0
// output and is excluded from the chain's in/out linkage, though it still
// contributes to the node's combined "operator" display name via NodeMetrics
// elsewhere).
func isJoinOperatorType(opType string) bool {
	return strings.Contains(opType, "Join") && !strings.Contains(opType, "Build")
}

func hasJoinOperatorType(types []string) bool {
	for _, t := range types {
		if isJoinOperatorType(t) {
			return true
		}
	}
	return false
}

// ExtractFanoutChain builds the REC-3 per-node fan-out chain ending at the
// verdict node (design.md §5.4.1 A3 enrichment iii, §10 item 11): the
// join-probe nodes in the SAME stage as the verdict node, linked by their
// ACTUAL in/out linkage (a join's output feeds the next join's input
// directly within one pipeline/stage — no exchange in between, verified
// against real data), walked backwards from the verdict node until no
// predecessor is found, then reversed to join (data-flow) order. Node ids are
// read entirely from nodeMetrics/nodeStageId — never hard-coded. Returns nil
// (no crash) when actuals are unavailable (live/snapshot-only diagnosis) or
// the verdict node itself is not a join.
func ExtractFanoutChain(nodeMetrics map[string]*NodeMetrics, nodeStageId map[string]int, verdictNodeId string) []FanoutChainNode {
	verdict := nodeMetrics[verdictNodeId]
	if verdict == nil || verdict.ActualIn == nil || verdict.ActualOut == nil {
		return nil
	}
	if !hasJoinOperatorType(verdict.OperatorTypes) {
		return nil
	}
	stage, ok := nodeStageId[verdictNodeId]
	if !ok {
		return nil
	}

	type candidate struct {
		id         string
		in         int64
		out        int64
		ops        []string
		pipelineId *int
		operatorId *int
	}
	candidates := map[string]*candidate{}
	ids := make([]string, 0, len(nodeMetrics))
	for id, m := range nodeMetrics {
		if m == nil || m.ActualIn == nil || m.ActualOut == nil {
			continue
		}
		if s, ok := nodeStageId[id]; !ok || s != stage {
			continue
		}
		if !hasJoinOperatorType(m.OperatorTypes) {
			continue
		}
		candidates[id] = &candidate{id: id, in: *m.ActualIn, out: *m.ActualOut, ops: m.OperatorTypes, pipelineId: m.PipelineId, operatorId: m.OperatorId}
		ids = append(ids, id)
	}
	sort.Strings(ids) // deterministic fallback tie-break order for the predecessor search below

	// pickPredecessor resolves ties among candidates whose output value
	// exactly matches current's input (out == current.in): raw value
	// matching alone is ambiguous whenever a 1:1 passthrough hop is present,
	// because the passthrough's own output trivially equals its
	// predecessor's output too. operatorId is monotonic in physical
	// execution order within one pipeline, so among same-pipeline
	// candidates, the one with the LARGEST operatorId strictly less than
	// current's is the true immediate predecessor. Falls back to the first
	// candidate in the (already sorted) tied slice when operatorId/pipeline
	// data is unavailable, keeping the result deterministic either way.
	pickPredecessor := func(tied []*candidate, current *candidate) *candidate {
		if len(tied) == 0 {
			return nil
		}
		if current.pipelineId != nil && current.operatorId != nil {
			var best *candidate
			for _, c := range tied {
				if c.pipelineId == nil || c.operatorId == nil {
					continue
				}
				if *c.pipelineId != *current.pipelineId || *c.operatorId >= *current.operatorId {
					continue
				}
				if best == nil || *c.operatorId > *best.operatorId {
					best = c
				}
			}
			if best != nil {
				return best
			}
		}
		return tied[0]
	}

	chain := []string{verdictNodeId}
	seen := map[string]bool{verdictNodeId: true}
	current := candidates[verdictNodeId]
	for current != nil {
		var tied []*candidate
		for _, id := range ids {
			if seen[id] {
				continue
			}
			if c := candidates[id]; c.out == current.in {
				tied = append(tied, c)
			}
		}
		pred := pickPredecessor(tied, current)
		if pred == nil {
			break
		}
		chain = append([]string{pred.id}, chain...)
		seen[pred.id] = true
		current = pred
	}

	out := make([]FanoutChainNode, 0, len(chain))
	for _, id := range chain {
		c := candidates[id]
		var fanout float64
		if c.in > 0 {
			fanout = float64(c.out) / float64(c.in)
		}
		out = append(out, FanoutChainNode{
			PlanNodeId: id,
			Operator:   strings.Join(c.ops, "+"),
			In:         c.in,
			Out:        c.out,
			Fanout:     fanout,
		})
	}
	return out
}

// buildCellText renders one chain row's "build (join key, line)" md cell:
// the resolved (or gracefully-fallback-labeled) table name, its join key(s),
// and the optional SQL source line -- never blank (Build/JoinKeys are always
// populated, per FanoutChainNode's own contract; SqlLine is the only
// optional piece here and is simply omitted from the cell when absent).
func buildCellText(n FanoutChainNode) string {
	keys := "?"
	if len(n.JoinKeys) > 0 {
		keys = strings.Join(n.JoinKeys, "; ")
	}
	if n.SqlLine != nil {
		return fmt.Sprintf("%s (%s, L%d)", n.Build, keys, *n.SqlLine)
	}
	return fmt.Sprintf("%s (%s)", n.Build, keys)
}

// buildRowsCellText renders one chain row's "buildRows/keyNdv" md cell;
// "…" when neither is resolvable (never blank).
func buildRowsCellText(n FanoutChainNode) string {
	switch {
	case n.BuildRows != nil && n.BuildKeyNdv != nil:
		return fmt.Sprintf("%d / %s", *n.BuildRows, formatFingerprintValue(plan_node.JsonFloat64(*n.BuildKeyNdv)))
	case n.BuildRows != nil:
		return fmt.Sprintf("%d / …", *n.BuildRows)
	case n.BuildKeyNdv != nil:
		return fmt.Sprintf("… / %s", formatFingerprintValue(plan_node.JsonFloat64(*n.BuildKeyNdv)))
	default:
		return "…"
	}
}

// kindCellText renders one chain row's "kind" md cell: the buildKind label
// (a bare "emptyBuild" carries no hint of its own -- purely factual, round
// 6), plus -- ONLY on a dimensionNonUnique row -- the verbatim §5.1.1
// templated per-row hint (report.go's dimensionNonUniqueHint), and/or --
// independently, ONLY when DeadJoin OR DeadWeightFanout fires (round 8: the
// two are mutually exclusive by construction, enrichFanoutChainRows) -- the
// verbatim deadJoinHint/deadWeightFanoutHint, joined with " · " when both the
// buildKind label and the flag hint are present (the real 1778 row is both
// emptyBuild AND deadJoin). Absent buildKind and no flag renders "…", never
// blank.
func kindCellText(n FanoutChainNode) string {
	kindPart := "…"
	switch n.BuildKind {
	case "":
		// leave as "…" below unless a flag alone still has something to say
	case "dimensionNonUnique":
		kindPart = "⚠ " + n.BuildKind + " — " + dimensionNonUniqueHint
	default:
		kindPart = n.BuildKind
	}
	var flagPart string
	switch {
	case n.DeadWeightFanout:
		flagPart = "⚠ deadWeightFanout — " + deadWeightFanoutHint
	case n.DeadJoin:
		flagPart = "⚠ deadJoin — " + deadJoinHint
	default:
		return kindPart
	}
	if kindPart == "…" {
		return flagPart
	}
	return kindPart + " · " + flagPart
}

// annihilationEvidenceLines implements the round-8 §5.4.1 A3(iii)(ι) R8-C
// rule: a chain row that is an INNER join against a measured-empty build
// which swallows the ENTIRE driving stream (measured out == 0, despite a
// potentially astronomical in) gets its own dedicated evidence line rather
// than silently zeroing the chain's end-to-end headline (computeChainFanout's
// truncation rule handles that half) -- "the strongest possible empty-PDT
// correctness signal", per the design's own wording. Returns one line per
// qualifying row (real fixtures have at most one; the general rule does not
// assume that).
func annihilationEvidenceLines(chain []FanoutChainNode) []string {
	var lines []string
	for _, n := range chain {
		if n.kind == "INNER" && n.BuildKind == "emptyBuild" && n.Out == 0 {
			lines = append(lines, fmt.Sprintf(
				"INNER join %s vs measured-empty `%s` annihilated %s rows → 0 output — the strongest possible empty-PDT correctness signal",
				n.PlanNodeId, n.Build, formatScientific(float64(n.In))))
		}
	}
	return lines
}

// renderFanoutChainTable renders the REC-3/round-5/round-6 fan-out chain as a
// single evidence-item Text value (same plain-string convention as the
// fingerprint table above). Round-5 (design.md §5.4.1 A3(iii), §10 item 14):
// the table gains the build table/join key(s)/SQL line and the build-key
// uniqueness/kind columns; the dimensionNonUnique per-row hint renders ONLY
// on flagged rows and never touches the verdict's own recommendation.
// Round-6 (§10 item 15): the kind cell also carries the deadJoin hint on
// flagged rows, and -- when chainFanout is non-nil -- a trailing
// "chain fan-out end-to-end" summary line is appended after the table
// (descriptive only, never consumed by the causal engine).
func renderFanoutChainTable(chain []FanoutChainNode, chainFanout *ChainFanout) string {
	var b strings.Builder
	b.WriteString("fan-out chain (build table / join key / line / buildRows/keyNdv / kind per row, from operatorSummaries + the raw plan — the blow-up compounds, it is not one bad join):\n")
	b.WriteString("| node | fan-out | build (join key, line) | buildRows/keyNdv | kind |\n")
	b.WriteString("|---|---|---|---|---|\n")
	for _, n := range chain {
		fmt.Fprintf(&b, "| %s | %.1fx | %s | %s | %s |\n", n.PlanNodeId, n.Fanout, buildCellText(n), buildRowsCellText(n), kindCellText(n))
	}
	if chainFanout != nil {
		fmt.Fprintf(&b, "%s\n", renderChainFanoutLine(chainFanout, len(chain)))
	}
	return strings.TrimRight(b.String(), "\n")
}

// renderChainFanoutLine renders the round-6 chain fan-out end-to-end summary
// line (design.md §5.4.1 A3(iii)(ε)/§5.5's worked example), e.g.:
// "chain fan-out end-to-end ≈ 2.55e6x (174,400 → 445,201,868,645 rows across
// 5 joins; per-node product corroborates)". The corroboration clause is
// dropped when NodeProduct overflowed (nil).
func renderChainFanoutLine(cf *ChainFanout, nodeCount int) string {
	line := fmt.Sprintf("chain fan-out end-to-end ≈ %sx (%s → %s rows across %d joins",
		formatChainFanoutRatio(cf.EndToEnd), formatWithCommas(cf.In), formatWithCommas(cf.Out), nodeCount)
	if cf.NodeProduct != nil {
		return line + "; per-node product corroborates)"
	}
	return line + ")"
}

// formatChainFanoutRatio renders a fan-out ratio in decimal form below 1e4,
// scientific notation ("2.55e6", matching design.md's own worked example --
// NOT Go's default "2.55e+06") at/above it.
func formatChainFanoutRatio(v float64) string {
	if math.Abs(v) >= 1e4 {
		return formatScientific(v)
	}
	return strconv.FormatFloat(v, 'f', 2, 64)
}

// formatScientific renders v in "M.MMeE" form (e.g. "2.55e6") -- Go's %e verb
// produces "2.55e+06" (explicit sign, zero-padded exponent), which does not
// match the design's worked example, so the exponent is reformatted by hand.
func formatScientific(v float64) string {
	s := strconv.FormatFloat(v, 'e', 2, 64) // e.g. "2.55e+06"
	mantissa, expPart, found := strings.Cut(s, "e")
	if !found {
		return s
	}
	exp, err := strconv.Atoi(expPart) // strconv.Atoi tolerates the leading "+"/"-" and zero padding
	if err != nil {
		return s
	}
	return fmt.Sprintf("%se%d", mantissa, exp)
}

// formatWithCommas renders an int64 with thousands separators (e.g.
// 445201868645 -> "445,201,868,645"), matching design.md's worked example.
func formatWithCommas(n int64) string {
	s := strconv.FormatInt(n, 10)
	neg := strings.HasPrefix(s, "-")
	if neg {
		s = s[1:]
	}
	var out []byte
	for i := 0; i < len(s); i++ {
		if i > 0 && (len(s)-i)%3 == 0 {
			out = append(out, ',')
		}
		out = append(out, s[i])
	}
	if neg {
		return "-" + string(out)
	}
	return string(out)
}
