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
	"fmt"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

// Summary carries the top-line resource magnitudes printed under the report
// header (§5.5).
type Summary struct {
	WallSec             float64 `json:"wallSec"`
	CpuSec              float64 `json:"cpuSec"`
	PeakUserMemoryBytes float64 `json:"peakUserMemoryBytes"`
	// Healthy/HealthyNote (round 7, design.md §5.4.2/§10 item 16) are set only
	// when the healthy-query predicate holds (state=="FINISHED" AND
	// totalCpuTime <= HealthyCpuFloorSec AND peakUserMemory <=
	// HealthyMemFloorBytes) -- both omitempty, so a normal/pathological run's
	// JSON is byte-unaffected. HealthyNote is the exact text rendered as the
	// report's leading top-line (above the verdict) in markdown.
	Healthy     bool   `json:"healthy,omitempty"`
	HealthyNote string `json:"healthyNote,omitempty"`
	// ProgressAtFailure (round 8, design.md §5.4.5 R8-F) is
	// queryStats.progressPercentage, set only for a non-FINISHED terminal
	// (omitempty) -- DRIVER/SPLIT completion, not remaining work; see
	// progressAtFailureLine (report.go) for the informational-only rendering.
	ProgressAtFailure *float64 `json:"progressAtFailure,omitempty"`
}

// Verdict is the report's root-cause headline (§5.5).
type Verdict struct {
	Category       string     `json:"category"`
	CategoryName   string     `json:"categoryName"`
	NodeId         string     `json:"nodeId,omitempty"`
	Operator       string     `json:"operator,omitempty"`
	StageId        string     `json:"stageId,omitempty"`
	Summary        string     `json:"summary"`
	Recommendation string     `json:"recommendation,omitempty"`
	Confidence     Confidence `json:"confidence"`
}

// Report is the full `pbench diagnose` output (§5.5): stable field names so
// downstream tooling — including a hand-off to an LLM for "why/how to fix"
// analysis (§5.1.1) — can consume it.
type Report struct {
	QueryId             string            `json:"queryId"`
	State               string            `json:"state"`
	ToolVersion         string            `json:"toolVersion"`
	Summary             Summary           `json:"summary"`
	Verdict             *Verdict          `json:"verdict"`
	Categories          []*CategoryResult `json:"categories"`
	IndependentFindings []*CategoryResult `json:"independentFindings"`
	Evidence            []EvidenceItem    `json:"evidence,omitempty"`
	NodeMetrics         []*NodeMetrics    `json:"nodeMetrics,omitempty"`
	// FanoutChain is the REC-3 top-level enrichment (design.md §5.4.1 A3(iii),
	// §10 item 11): join nodes on the path to the A3 verdict node, in join
	// order, populated only when the verdict fires and actuals are available
	// (terminal / finished stages) — additive, no break to existing fields.
	FanoutChain []FanoutChainNode `json:"fanoutChain,omitempty"`
	// ChainFanout is the round-6 top-level descriptive summary (design.md
	// §5.4.1 A3(iii)(ε), §10 item 15) of FanoutChain's overall end-to-end
	// multiplication; populated only alongside a >=2-row FanoutChain whose
	// first row's `in` is > 0 — purely descriptive, never consumed by any
	// threshold, severity, or the causal engine.
	ChainFanout *ChainFanout `json:"chainFanout,omitempty"`
	// ResultCorrectnessRisk (round 8, design.md §5.4.1 A3(iii)(η) R8-A) is
	// the headline "the A3 fan-out likely inflated a duplicate-SENSITIVE
	// aggregate" finding -- a correctness annotation layered on the A3
	// verdict, never a severity/category change; populated only when the
	// verdict fires AND at least one downstream aggregate classifies
	// duplicate-SENSITIVE.
	ResultCorrectnessRisk *ResultCorrectnessRisk `json:"resultCorrectnessRisk,omitempty"`
	// DataFreshnessRisks (round 8, design.md §5.4.5 R8-E) lists every
	// IMPLICATED input (on the fan-out chain, in an R8-C annihilation
	// finding, or in an R8-D skew trace) whose own TableScan measured 0
	// rows, or whose join/partition-key NDV collapsed while rows > 0 --
	// informational evidence only, sorted by numeric planNodeId.
	DataFreshnessRisks []DataFreshnessRisk `json:"dataFreshnessRisks,omitempty"`
	Progression        *Progression        `json:"progression,omitempty"`
	Warnings           []string            `json:"warnings,omitempty"`

	// nearMissProgressMin (round 8, §5.4.5 R8-F rendering only) carries
	// Thresholds.NearMissProgressMin through to RenderMarkdown without
	// widening RenderMarkdown's own signature (it has many existing call
	// sites across rounds 1-7) or the JSON schema -- deliberately
	// unexported, never serialized. Zero-valued (0.0) on a Report built
	// without going through analyze.go's buildReport (e.g. a hand-built test
	// fixture); harmless in that case because Summary.ProgressAtFailure is
	// ONLY ever set in the same buildReport code path that also sets this
	// field, so RenderMarkdown never calls progressAtFailureLine on a Report
	// where the two could disagree.
	nearMissProgressMin float64
}

// recommendationFor implements the §5.1.1 fixed, bounded lookup table:
// obvious, templated one-liners keyed off the triggering signal — never
// synthesized analysis. A table miss returns "" (no recommendation line).
func recommendationFor(root *CategoryResult, sig *Signals, runaway *RunawaySignal, th *Thresholds) string {
	switch root.Category {
	case CatPlanPath:
		if runaway != nil && runaway.Fired && !root.FromMeasuredActual && runaway.SuspectNodeId == root.NodeId {
			return fmt.Sprintf("runaway computation in stage S%d; likely cardinality blow-up — inspect that stage's join/aggregation", runaway.StageNum)
		}
		if broadcastLarge(root.NodeId, sig, th) {
			return "review join distribution (broadcast vs partitioned)"
		}
		// REC-1 (design.md §5.1.1, round 2): the A3 recommendation is a
		// two-way branch keyed on join-key NDV present vs NaN/missing --
		// both are fixed templated strings, no per-query reasoning. NDV
		// present but the fan-out not "large" (< FanoutHigh) falls through
		// to no recommendation, matching the table's "any other -> NONE".
		if m := sig.NodeMetrics[root.NodeId]; m != nil && len(m.JoinKeyStats) > 0 {
			if HasNaNJoinKeyNDV(m.JoinKeyStats) {
				return "run ANALYZE / add column statistics on the join inputs"
			}
			if m.ActualFanout != nil && *m.ActualFanout >= th.FanoutHigh {
				return "inherent join fan-out (possible Looker symmetric-aggregate / chasm-trap): pre-aggregate each detail table to one row per join key before joining, and apply the filter to all join inputs, not just one."
			}
		}
	case CatQueueAdmission:
		return "cluster/resource-group contention — review resource-group limits / cluster load"
	case CatMemorySpill:
		if sig.SpillBytes > 0 {
			return "memory pressure with spill — raise memory or reduce working set"
		}
	case CatNetworkShuffle:
		return "review join distribution (broadcast vs partitioned) / repartitioning volume"
	case CatSkew:
		return "check partition/join key distribution"
	case CatScheduling:
		if sig.TotalSplits > 0 && float64(sig.RawInputPositions)/float64(sig.TotalSplits) < th.TinySplitRows {
			return "splits too small — review split sizing / file sizes"
		}
	}
	return ""
}

// dimensionNonUniqueHint is the new §5.1.1 per-row templated hint (round 5,
// fixed string — design.md §5.4.1 A3(iii)/§10 item 14): rendered ONLY on
// fanoutChain rows flagged buildKind=="dimensionNonUnique"
// (renderFanoutChainTable, joinkeys.go), via the same "templated, never free
// text" mechanism as every other entry in the §5.1.1 lookup table. This is a
// per-ROW hint on the chain table, not the verdict line -- it never alters
// verdict.recommendation.
const dimensionNonUniqueHint = "non-unique dimension (buildRows ≫ buildKeyNdv on a dimension-sized build): dedup the dimension to one row per key (GROUP BY key) or repair it at source — pre-aggregation is the wrong remedy for this row"

// deadJoinHint is the new §5.1.1 per-row templated hint (round 6, fixed
// string — design.md §5.4.1 A3(iii)(ζ)/§10 item 15): rendered ONLY on
// fanoutChain rows flagged DeadJoin (renderFanoutChainTable, joinkeys.go),
// via the same "templated, never free text" mechanism as every other entry
// in the §5.1.1 lookup table. Independent of the dimensionNonUniqueHint
// above (a row can carry either, both, or neither) -- this is a per-ROW hint
// on the chain table, not the verdict line -- it never alters
// verdict.recommendation.
const deadJoinHint = "droppable dead join (LEFT join, build columns unused downstream, no row-count effect): remove this join from the query/model"

// deadWeightFanoutHint is the round-8 §5.1.1 per-row templated hint
// (design.md §5.4.1 A3(iii)(θ) R8-B): rendered ONLY on fanoutChain rows
// flagged DeadWeightFanout (renderFanoutChainTable/kindCellText,
// joinkeys.go) -- takes precedence over deadJoinHint on the same row
// (enrichFanoutChainRows never sets both).
const deadWeightFanoutHint = "dead-weight fan-out (build columns unused downstream and the join fans out — it contributes only row multiplication): drop the join or dedup its key to one row per key"

// progressAtFailureLine implements the round-8 §5.4.5 R8-F informational-only
// rendering (review W3 -- no "near-miss"/"raise limits" claim): a non-FINISHED
// terminal's queryStats.progressPercentage is DRIVER/SPLIT completion, not
// remaining work, so at/above NearMissProgressMin the FACTUAL template names
// that explicitly ("driver progress is not time-remaining") rather than
// implying the query was nearly done; below the threshold, the bare number
// renders with no interpretation at all.
func progressAtFailureLine(pct float64, nearMissProgressMin float64) string {
	if pct >= nearMissProgressMin {
		return fmt.Sprintf("progress at failure: %.1f%% of scheduled work completed — likely close to finishing; note driver progress is not time-remaining (check the fan-out chain / runaway evidence before raising limits)", pct)
	}
	return fmt.Sprintf("progress at failure: %.1f%% of scheduled work completed", pct)
}

// RenderJSON renders the report as indented JSON (--format json). Unlike
// RenderMarkdown, this marshals the Report struct's fields verbatim: the raw
// numeric "share" values are never clamped in JSON output (no schema change,
// review W5 / design §10 item 2c).
func (r *Report) RenderJSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

// shareValueRe matches a "share"-labeled floating-point number in rendered
// note/evidence text, e.g. "(share 1.800)" or "wall share 483.917". Several
// scorers' Note/Evidence text embeds a ratio explicitly labeled "share"
// (classify.go: scoreB3, scoreB4, scoreB5, scoreC1, scoreC4, scoreD1); most
// stay under 1.0, but a benign wall-vs-scheduled dimensional mix (bounded,
// driver-count-independent — NOT the driver-scaling bug already fixed for B5/
// C1, and it never affects classification since every share THRESHOLD is
// itself < 1.0) can occasionally print a share above 1.0 (e.g. ~1.8). A field
// literally labeled "share" printing above 100% reads as a display bug even
// though the underlying number and every severity decision are unaffected.
var shareValueRe = regexp.MustCompile(`(?i)(share\s+)(-?\d+\.\d+)`)

// clampShareForDisplay is applied ONLY to the markdown text a note/evidence
// string renders as — never to the underlying CategoryResult/EvidenceItem
// struct fields, which RenderJSON marshals unchanged. Any "share <value>"
// occurrence above 1.0 is clamped to "share 1.00 [raw <value>]" (brackets so
// it does not nest inside a note's own "(share ...)" parentheses); classify.go
// and the JSON output are untouched by this function.
func clampShareForDisplay(s string) string {
	return shareValueRe.ReplaceAllStringFunc(s, func(match string) string {
		groups := shareValueRe.FindStringSubmatch(match)
		v, err := strconv.ParseFloat(groups[2], 64)
		if err != nil || v <= 1.0 {
			return match
		}
		return fmt.Sprintf("%s1.00 [raw %s]", groups[1], trimTrailingZeros(v))
	})
}

// trimTrailingZeros renders a float with up to 3 decimal places, trimming
// trailing zeros (and a trailing decimal point) for a clean "raw" display,
// e.g. 1.8 -> "1.8", 483.917 -> "483.917", 2.000 -> "2".
func trimTrailingZeros(v float64) string {
	s := strconv.FormatFloat(v, 'f', 3, 64)
	s = strings.TrimRight(s, "0")
	s = strings.TrimSuffix(s, ".")
	return s
}

// joinNodeStatsEstimateInformationalNote implements REC-2 (design.md §5.4.1
// A3, §2, §10 item 11): joinNodeStatsEstimate is an HBO/null-skew runtime
// field, never a CBO cardinality input, so it is deliberately NOT part of
// ranked evidence (classify.go no longer appends it there). If the A3
// verdict's node happens to carry an all-NaN joinNodeStatsEstimate, this
// renders it as a single non-causal informational line, outside evidence[]
// entirely — never inside the ranked list. Returns "" when the verdict is not
// A3, or the verdict node's joinNodeStatsEstimate is not all-NaN (nothing to
// caveat).
func (r *Report) joinNodeStatsEstimateInformationalNote() string {
	if r.Verdict == nil || r.Verdict.Category != CatPlanPath || r.Verdict.NodeId == "" {
		return ""
	}
	for _, nm := range r.NodeMetrics {
		if nm.PlanNodeId != r.Verdict.NodeId {
			continue
		}
		if nm.HasJoinStats && nm.AllNaNJoinStats {
			return "Informational (not root-cause evidence): joinNodeStatsEstimate all NaN — HBO null-skew fields, not a CBO cardinality input; NaN = no HBO history, not the cause."
		}
		return ""
	}
	return ""
}

// RenderMarkdown renders the report as markdown (--format md, default), in the
// exact section order/prominence of §5.5: header, verdict, ranked evidence,
// INDEPENDENT FINDINGS (always present, "(none)" when empty, directly under the
// verdict), contributing/derived symptoms, the full category table, and — when
// available — the progression section.
func (r *Report) RenderMarkdown(top int) string {
	var b strings.Builder
	fmt.Fprintf(&b, "# Query diagnosis: %s\n", r.QueryId)
	fmt.Fprintf(&b, "State: %s | wall %.2fs | cpu %.2fs | peak user mem %.0f bytes\n", r.State, r.Summary.WallSec, r.Summary.CpuSec, r.Summary.PeakUserMemoryBytes)
	// Round-7 healthy-query top-line (design.md §5.4.2/§5.5's fourth worked
	// example): the report's FIRST line, above the verdict, printed only when
	// the healthy predicate holds -- the verdict itself is otherwise entirely
	// unaffected (no new HEALTHY verdict enum).
	if r.Summary.Healthy && r.Summary.HealthyNote != "" {
		fmt.Fprintf(&b, "%s\n", r.Summary.HealthyNote)
	}
	// Round-8 §5.4.5 R8-F: progressAtFailure header line -- mutually
	// exclusive with the healthy top-line above (Healthy requires
	// state=="FINISHED"; ProgressAtFailure is only ever set for a
	// non-FINISHED terminal, analyze.go's buildReport).
	if r.Summary.ProgressAtFailure != nil {
		fmt.Fprintf(&b, "%s\n", progressAtFailureLine(*r.Summary.ProgressAtFailure, r.nearMissProgressMin))
	}
	b.WriteString("\n")

	fmt.Fprintf(&b, "## Verdict  (root cause; confidence: %s)\n", r.Verdict.Confidence.Level)
	if r.Verdict.Category == "" {
		fmt.Fprintf(&b, "%s: %s\n", r.Verdict.CategoryName, clampShareForDisplay(r.Verdict.Summary))
	} else {
		loc := ""
		if r.Verdict.NodeId != "" {
			loc = fmt.Sprintf(" — node %s", r.Verdict.NodeId)
			if r.Verdict.Operator != "" {
				loc += fmt.Sprintf(" (%s)", r.Verdict.Operator)
			}
			if r.Verdict.StageId != "" {
				loc += fmt.Sprintf(", stage %s", r.Verdict.StageId)
			}
		} else if r.Verdict.StageId != "" {
			loc = fmt.Sprintf(" — stage %s", r.Verdict.StageId)
		}
		fmt.Fprintf(&b, "%s%s: %s\n", strings.ToUpper(r.Verdict.CategoryName), loc, clampShareForDisplay(r.Verdict.Summary))
		if r.Verdict.Recommendation != "" {
			fmt.Fprintf(&b, "Recommendation: %s\n", r.Verdict.Recommendation)
		}
	}
	// Round-8 §5.4.1 A3(iii)(η) R8-A: the result-correctness-risk top-line,
	// directly under the verdict summary/recommendation (§5.5's fifth worked
	// example) -- a correctness ANNOTATION on the verdict, never a
	// severity/category change.
	if r.ResultCorrectnessRisk != nil {
		fmt.Fprintf(&b, "⚠ %s\n", r.ResultCorrectnessRisk.Note)
	}
	fmt.Fprintf(&b, "Confidence %s: %s\n\n", r.Verdict.Confidence.Level, strings.Join(r.Verdict.Confidence.Reasons, "; "))

	fmt.Fprintf(&b, "## Root-cause evidence (ranked)\n")
	if len(r.Evidence) == 0 {
		b.WriteString("(no evidence recorded)\n\n")
	} else {
		n := len(r.Evidence)
		if top > 0 && top < n {
			n = top
		}
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b, "%d. %s\n", i+1, clampShareForDisplay(r.Evidence[i].Text))
		}
		b.WriteString("\n")
	}
	if note := r.joinNodeStatsEstimateInformationalNote(); note != "" {
		fmt.Fprintf(&b, "%s\n\n", note)
	}

	b.WriteString("## ⚠ INDEPENDENT FINDINGS (co-causes NOT explained by the verdict)\n")
	if len(r.IndependentFindings) == 0 {
		b.WriteString("(none)\n\n")
	} else {
		for _, f := range r.IndependentFindings {
			fmt.Fprintf(&b, "- %s %s (sev %d): %s\n", f.Category, CategoryNames[f.Category], f.Severity, clampShareForDisplay(f.Note))
		}
		b.WriteString("\n")
	}

	attributed := make([]*CategoryResult, 0)
	for _, c := range r.Categories {
		if c.AttributedTo != "" {
			attributed = append(attributed, c)
		}
	}
	if r.Verdict.Category != "" {
		fmt.Fprintf(&b, "## Contributing / derived symptoms")
		if r.Verdict.NodeId != "" {
			fmt.Fprintf(&b, " (attributed to node %s)", r.Verdict.NodeId)
		}
		b.WriteString("\n")
		if len(attributed) == 0 {
			b.WriteString("(none)\n\n")
		} else {
			for _, c := range attributed {
				fmt.Fprintf(&b, "- %s %s (sev %d): %s — %s\n", c.Category, CategoryNames[c.Category], c.Severity, clampShareForDisplay(c.Note), c.AttributionReason)
			}
			b.WriteString("\n")
		}
	}

	b.WriteString("## Category assessment\n")
	b.WriteString("| Group | Category | Severity | Note |\n|---|---|---|---|\n")
	byCat := map[string]*CategoryResult{}
	for _, c := range r.Categories {
		byCat[c.Category] = c
	}
	for _, cat := range AllCategories {
		c := byCat[cat]
		if c == nil {
			continue
		}
		sevText := fmt.Sprintf("%d", c.Severity)
		if c.NA {
			sevText = "N/A"
		}
		fmt.Fprintf(&b, "| %s | %s | %s | %s |\n", c.Group, c.Name, sevText, clampShareForDisplay(c.Note))
	}
	b.WriteString("\n")

	// Round-8 §5.4.5 R8-E: dataFreshnessRisks, implication-gated evidence
	// lines (already sorted by numeric planNodeId by computeDataFreshnessRisks).
	if len(r.DataFreshnessRisks) > 0 {
		b.WriteString("## Data freshness\n")
		for _, dfr := range r.DataFreshnessRisks {
			fmt.Fprintf(&b, "- %s\n", dataFreshnessRiskLine(dfr))
		}
		b.WriteString("\n")
	}

	if r.Progression != nil {
		fmt.Fprintf(&b, "## Progression (from %d snapshots) — temporal causal evidence\n", r.Progression.FrameCount)
		if r.Progression.Insufficient {
			b.WriteString("insufficient frames for rates\n\n")
		} else {
			for _, s := range r.Progression.Series {
				fmt.Fprintf(&b, "- t=%.0fs: outputRows=%d cpu=%.2fs userMemory=%.0f bytes\n", s.TimeSec, s.OutputRows, s.CpuSec, s.UserMemoryBytes)
			}
			if r.Progression.Runaway != nil && r.Progression.Runaway.Fired {
				// Round-3 fix (Issue 1, §5.6 rendering rule): the line must never end
				// with a dangling "suspect node " -- render the resolved id, or an
				// explicit "none identified" when localization couldn't name one.
				suspect := "suspect node: none identified"
				if r.Progression.Runaway.SuspectNodeId != "" {
					suspect = fmt.Sprintf("suspect node %s", r.Progression.Runaway.SuspectNodeId)
				}
				fmt.Fprintf(&b, "- RunawayComputation fired: stage S%d holds %.0f%% of scheduled-time growth, %s\n", r.Progression.Runaway.StageNum, r.Progression.Runaway.StageShare*100, suspect)
			}
			cats := make([]string, 0, len(r.Progression.EmergenceByCategory))
			for c := range r.Progression.EmergenceByCategory {
				cats = append(cats, c)
			}
			sort.Strings(cats)
			for _, c := range cats {
				fmt.Fprintf(&b, "- %s emerged at ~t=%.0fs\n", CategoryNames[c], r.Progression.EmergenceByCategory[c])
			}
			for _, conflict := range r.Progression.TemporalConflicts {
				fmt.Fprintf(&b, "- TEMPORAL CONFLICT: %s\n", conflict)
			}
		}
		b.WriteString("\n")
	}
	if len(r.Warnings) > 0 {
		b.WriteString("## Warnings\n")
		for _, w := range r.Warnings {
			fmt.Fprintf(&b, "- %s\n", w)
		}
	}
	return b.String()
}
