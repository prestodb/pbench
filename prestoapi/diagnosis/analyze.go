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
	"context"
	"encoding/json"
	"fmt"
	"sort"

	"github.com/prestodb/presto-go-client/v2/queryjson"
	"pbench/prestoapi/plan_node"
)

// ToolVersion is embedded in every report for downstream tooling (§5.5).
const ToolVersion = "pbench-diagnose/1.0"

// Query bundles the two parallel parses of one query JSON file (§5.1: the same
// bytes are unmarshaled twice — once into queryjson.QueryInfo, once into
// diagnosis.ExtraInfo), plus the plan-node/join analysis derived from them.
type Query struct {
	Raw         []byte
	Info        *queryjson.QueryInfo
	Extra       *ExtraInfo
	NodeMetrics map[string]*NodeMetrics
	PlanNodes   map[string]*plan_node.PlanNode
	Joins       []plan_node.Join
}

// ParseQuery parses one query JSON file's raw bytes into a Query. It applies
// the same minimal validation as loadjson.processFile (queryId/queryStats
// present) so callers can skip invalid files with a warning (FP-14).
func ParseQuery(raw []byte) (*Query, error) {
	qi := new(queryjson.QueryInfo)
	if err := json.Unmarshal(raw, qi); err != nil {
		return nil, fmt.Errorf("failed to unmarshal query JSON: %w", err)
	}
	if qi.QueryId == "" || qi.QueryStats == nil {
		return nil, fmt.Errorf("not a valid query JSON: queryId or queryStats missing")
	}
	extra := new(ExtraInfo)
	if err := json.Unmarshal(raw, extra); err != nil {
		return nil, fmt.Errorf("failed to unmarshal extra query JSON: %w", err)
	}
	q := &Query{Raw: raw, Info: qi, Extra: extra}
	q.NodeMetrics = ComputeNodeMetrics(extra, qi.QueryStats.OperatorSummaries)

	// PrepareForInsert mutates our private *queryjson.QueryInfo copy only (never
	// the raw bytes/file, §5.7) to assemble the per-stage plan fragments into a
	// plan_node.PlanTree-shaped structure, reusing the existing join/plan-node
	// analysis in prestoapi/plan_node rather than re-implementing it.
	if err := qi.PrepareForInsert(); err == nil && qi.AssembledQueryPlanJson != "" {
		var tree plan_node.PlanTree
		// See StripEstimates (model.go): real native-engine plans carry a
		// string-enum "confident" incompatible with plan_node.PlanEstimate's bool.
		if uErr := json.Unmarshal(StripEstimates([]byte(qi.AssembledQueryPlanJson)), &tree); uErr == nil {
			q.PlanNodes = collectPlanNodes(tree)
			if joins, jErr := tree.ParseJoins(); jErr == nil {
				q.Joins = joins
			}
		}
	}
	return q, nil
}

// topCategoryReadings returns the top-N categories by severity (descending,
// N/A excluded) as ranked evidence, used for the NO DOMINANT BOTTLENECK
// verdict so its evidence section is never empty (§5.4.2 fallback, review W1).
func topCategoryReadings(categories []*CategoryResult, top int) []EvidenceItem {
	ranked := make([]*CategoryResult, 0, len(categories))
	for _, c := range categories {
		if !c.NA {
			ranked = append(ranked, c)
		}
	}
	sort.SliceStable(ranked, func(i, j int) bool { return ranked[i].Severity > ranked[j].Severity })
	if top > 0 && top < len(ranked) {
		ranked = ranked[:top]
	}
	out := make([]EvidenceItem, 0, len(ranked))
	for _, c := range ranked {
		out = append(out, EvidenceItem{
			Text:   fmt.Sprintf("%s %s (severity %d): %s", c.Category, c.Name, c.Severity, c.Note),
			Values: map[string]float64{"severity": float64(c.Severity)},
		})
	}
	return out
}

func collectPlanNodes(tree plan_node.PlanTree) map[string]*plan_node.PlanNode {
	out := map[string]*plan_node.PlanNode{}
	_ = tree.Traverse(context.Background(), func(_ context.Context, n *plan_node.PlanNode) error {
		out[n.Id] = n
		return nil
	})
	return out
}

// Analyze produces the full static diagnosis for a single query JSON file with
// no snapshot context (FP-13's first-class no-snapshot mode): complete taxonomy
// assessment, root-cause verdict, ranked evidence — only the progression section
// and the §5.4.4 temporal confidence-upgrade do not apply.
func Analyze(raw []byte, th *Thresholds) (*Report, error) {
	q, err := ParseQuery(raw)
	if err != nil {
		return nil, err
	}
	return buildReport(q, nil, nil, th, false)
}

// AnalyzeWithSnapshots produces the diagnosis for a terminal query plus its
// sibling Phase-1 snapshot frames (FP-13): the terminal is the "current state"
// for classification, and the frames feed the progression section plus the
// §5.4.4 temporal confidence-upgrade.
func AnalyzeWithSnapshots(raw []byte, frameFiles []*FrameFile, th *Thresholds) (*Report, error) {
	q, err := ParseQuery(raw)
	if err != nil {
		return nil, err
	}
	frames, warnings := ParseFrames(frameFiles)
	return buildReport(q, frames, warnings, th, false)
}

// AnalyzeSnapshotsOnly diagnoses directly from frames with no terminal file
// (Mode 2, §5.2): the highest-seq frame is the "current state" for
// classification, and confidence is capped at MEDIUM (§5.4.3) since no
// measured actual (terminal/finished-stage) can exist.
func AnalyzeSnapshotsOnly(frameFiles []*FrameFile, th *Thresholds) (*Report, error) {
	frames, warnings := ParseFrames(frameFiles)
	if len(frames) == 0 {
		return nil, fmt.Errorf("no parsable snapshot frames found")
	}
	sort.Slice(frames, func(i, j int) bool { return frames[i].TimestampMs < frames[j].TimestampMs })
	current := frames[len(frames)-1].Query
	return buildReport(current, frames, warnings, th, true)
}

func buildReport(current *Query, frames []*ParsedFrame, warnings []string, th *Thresholds, snapshotOnly bool) (*Report, error) {
	var progression *Progression
	var runaway *RunawaySignal
	if len(frames) > 0 {
		progression = AnalyzeProgression(frames, warnings, th)
		runaway = progression.Runaway
	}

	sig := ExtractSignals(current.Info, current.Extra, current.NodeMetrics, current.PlanNodes, current.Joins, runaway)
	categories := Classify(sig, th)
	// Round-7 healthy-query guards (design.md §5.4.2, §10 item 16): applied
	// at this Step-2 scoring stage, strictly BEFORE Step-3 rooting/
	// attribution below, so a suppressed category is never considered a root
	// or an independent finding.
	healthy := ApplyHealthyQueryGuards(categories, sig, th)
	root, independents, attributed := Attribute(categories, sig, th)

	// Cycle-test evidence enrichments (design.md §10 items 10/11, §5.4.1 A3):
	// additive what+where evidence on the already-decided A3 verdict node —
	// never touches severity/root selection/causal attribution, all of which
	// are already final by this point.
	fanoutChain, chainFanout, correctnessRisk, rawIdx := enrichA3Evidence(root, current.NodeMetrics, sig.NodeStageId, current.Extra, current.PlanNodes, th)

	// Round-8 §5.4.5 R8-D: skewCause, appended to C2's own note, evaluated
	// only when C2 survived the round-7 magnitude gate (computeSkewCause's
	// own guard) -- never touches C2's severity/verdict/confidence.
	var skewImplicated []implicatedInput
	for _, c := range categories {
		if c.Category != CatSkew {
			continue
		}
		var estimates map[string]*NodeEstimate
		if current.Extra != nil && current.Extra.PlanStatsAndCosts != nil {
			estimates = current.Extra.PlanStatsAndCosts.Stats
		}
		skewCause := computeSkewCause(c, rawIdx, fanoutChain, estimates, th)
		if skewCause.note != "" {
			c.Note += " — " + skewCause.note
		}
		skewImplicated = skewCause.implicated
		break
	}

	// Round-8 §5.4.5 R8-E: dataFreshnessRisks, implication-gated to inputs on
	// the fan-out chain (any row, including an R8-C annihilation row) or in
	// the R8-D skew trace above -- informational evidence only.
	freshnessRisks := computeDataFreshnessRisks(fanoutChain, skewImplicated, th)

	if progression != nil && root != nil {
		consistent, conflicts := CheckTemporalConsistency(root, attributed, progression.EmergenceByCategory)
		progression.TemporalConflicts = conflicts
		if !consistent {
			// Temporal contradiction is a first-class finding, not an assertion
			// failure (§5.4.4): re-run attribution preferring the earliest
			// emerging elevated category as an alternate root candidate is left
			// as a documented, explicit conflict rather than silently swapped,
			// so operators see both the original and the contradicting evidence.
			for _, r := range append([]*CategoryResult{}, attributed...) {
				r.Note += " [TEMPORAL CONFLICT: emerged before the claimed root — see progression]"
			}
		}
		for _, cat := range categories {
			if t, ok := progression.EmergenceByCategory[cat.Category]; ok {
				v := t
				cat.EmergenceSec = &v
			}
		}
	}

	confidence := ComputeConfidence(root, independents, snapshotOnly)
	if progression != nil && root != nil && len(progression.TemporalConflicts) == 0 && confidence.Level == "LOW" {
		// §5.4.4: temporal ordering can upgrade LOW -> MEDIUM (never MEDIUM->HIGH,
		// and never HIGH without a measured actual, already enforced by
		// ComputeConfidence's snapshotOnly cap).
		if _, ok := progression.EmergenceByCategory[root.Category]; ok {
			confidence = Confidence{Level: "MEDIUM", Reasons: append(confidence.Reasons, "temporal evidence from snapshot frames is consistent with the causal attribution (upgraded from LOW)")}
		}
	}

	report := &Report{
		ToolVersion:         ToolVersion,
		Warnings:            warnings,
		nearMissProgressMin: th.NearMissProgressMin,
	}
	if current.Info != nil {
		report.QueryId = current.Info.QueryId
		report.State = current.Info.State
	}
	report.Summary = Summary{
		WallSec:             sig.ElapsedSec,
		CpuSec:              sig.CpuSec,
		PeakUserMemoryBytes: sig.PeakUserMemoryBytes,
	}
	if healthy {
		report.Summary.Healthy = true
		report.Summary.HealthyNote = HealthyTopLine(sig, root)
	}
	// Round-8 §5.4.5 R8-F: progressAtFailure, informational-only, for any
	// non-FINISHED terminal that carries queryStats.progressPercentage
	// (absent on FINISHED runs and on frames where the field itself is
	// absent — never coerced to 0).
	if sig.State != "FINISHED" && sig.ProgressPercentage != nil {
		v := *sig.ProgressPercentage
		report.Summary.ProgressAtFailure = &v
	}
	report.Categories = categories
	report.IndependentFindings = independents
	report.Progression = progression
	report.FanoutChain = fanoutChain
	report.ResultCorrectnessRisk = correctnessRisk
	report.DataFreshnessRisks = freshnessRisks
	report.ChainFanout = chainFanout

	nodeIds := make([]string, 0, len(current.NodeMetrics))
	for id := range current.NodeMetrics {
		nodeIds = append(nodeIds, id)
	}
	sort.Strings(nodeIds)
	for _, id := range nodeIds {
		report.NodeMetrics = append(report.NodeMetrics, current.NodeMetrics[id])
	}

	if root == nil {
		// §5.4.2: "if nothing is elevated at all, the verdict is NO DOMINANT
		// BOTTLENECK with the top per-category readings for reference" — the
		// ranked-evidence section must not be empty even here (review W1).
		report.Evidence = topCategoryReadings(categories, th.Top)
		report.Verdict = &Verdict{
			Category:     "",
			CategoryName: "NO DOMINANT BOTTLENECK",
			Summary:      "no category reached the elevation threshold; see the category table for the top per-category readings",
			Confidence:   confidence,
		}
		return report, nil
	}
	report.Evidence = root.Evidence
	report.Verdict = &Verdict{
		Category:       root.Category,
		CategoryName:   CategoryNames[root.Category],
		NodeId:         root.NodeId,
		Operator:       root.Operator,
		StageId:        root.StageId,
		Summary:        root.Note,
		Recommendation: recommendationFor(root, sig, runaway, th),
		Confidence:     confidence,
	}
	return report, nil
}
