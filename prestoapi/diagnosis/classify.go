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

	"github.com/prestodb/presto-go-client/v2/queryjson"
	"pbench/prestoapi/plan_node"
)

// EvidenceItem is one ranked, numeric-evidence line for a category or verdict.
type EvidenceItem struct {
	Text   string             `json:"text"`
	Values map[string]float64 `json:"values,omitempty"`
}

// CategoryResult is the outcome of scoring one taxonomy category (§5.4.2).
// Severity is -1 for the N/A category (D2 Lock, never a verdict). 0 means not
// elevated; "elevated" means Severity >= 1.
type CategoryResult struct {
	Category           string         `json:"category"`
	Group              string         `json:"group"`
	Name               string         `json:"name"`
	Severity           int            `json:"severity"`
	NA                 bool           `json:"na,omitempty"`
	Note               string         `json:"note"`
	Evidence           []EvidenceItem `json:"evidence,omitempty"`
	SignalFamilies     int            `json:"signalFamilies,omitempty"`
	FromMeasuredActual bool           `json:"fromMeasuredActual,omitempty"`
	NodeId             string         `json:"nodeId,omitempty"`
	Operator           string         `json:"operator,omitempty"`
	StageId            string         `json:"stageId,omitempty"`
	AttributedTo       string         `json:"attributedTo,omitempty"`
	AttributionReason  string         `json:"attributionReason,omitempty"`
	Independent        bool           `json:"independent,omitempty"`
	EmergenceSec       *float64       `json:"emergenceSec,omitempty"`
}

// RunawaySignal is the live substitute for a measured actual fan-out (§5.6),
// computed by progression.go and fed into A3 scoring by classify.go's "A3 live
// elevation path" (§5.4.2).
//
// JSON tags (round-4 fix, design.md §10 item 13/§5.5's locked canonical
// schema `progression.runaway{fired, stageNum, stageShare, suspectNodeId}`):
// this struct previously had no tags at all, leaking Go's PascalCase field
// names verbatim. Serialization fix only -- no value/logic change.
type RunawaySignal struct {
	Fired         bool    `json:"fired"`
	StageNum      int     `json:"stageNum"`
	StageShare    float64 `json:"stageShare"`
	SuspectNodeId string  `json:"suspectNodeId"`
}

// Signals is every raw/derived measurement the classification engine reads,
// extracted once per frame from a Query (§5.4.2 Step 1 — Extract).
type Signals struct {
	// State is the query's own top-level state ("FINISHED"/"FAILED"/"RUNNING"),
	// needed by the round-7 healthy-query guards (§5.4.2): the idle-driver gate
	// and the healthy predicate are both explicitly FINISHED-only (a FAILED/
	// timed-out query's idleness can be the symptom, and RUNNING live frames
	// are never gated).
	State string

	ElapsedSec, PlanningSec, AnalysisSec, QueueSec, CpuSec, ScheduledSec float64
	PeakUserMemoryBytes                                                  float64
	SpillBytes                                                           float64
	RawInputBytes, ProcessedInputBytes                                   float64
	ScanWallSec, ScanWaitSec                                             float64
	ExchangeWallSec                                                      float64
	ShuffledBytes                                                        float64
	BlockedWallSec, ProducerBlockedSec, OtherBlockedSec                  float64
	TotalDrivers                                                         int
	FullyBlocked                                                         bool
	FinishingSec                                                         float64
	TotalSplits                                                          int
	RawInputPositions                                                    int64
	GcTimeSec                                                            float64
	GcDataAvailable                                                      bool
	NativeRuntime                                                        bool
	RootStageTaskCount                                                   int
	// QuickStatsDetected: B4 note-annotation-only signal (design.md §5.4.1 B4,
	// §10 item 10) — key-existence read, never affects severity.
	QuickStatsDetected bool
	// ProgressPercentage (round-8, §5.4.5 R8-F) is queryStats.progressPercentage
	// verbatim — DRIVER/SPLIT completion, not remaining work (§5.4.5's own
	// in-corpus proof: a FINISHED query can read well below 100%, and a
	// FAILED query can read high while nowhere near actually done). Nil when
	// the field is absent; never coerced to 0.
	ProgressPercentage *float64

	NodeMetrics     map[string]*NodeMetrics
	NodeStageId     map[string]int
	StageCpuSec     map[int]float64
	StageTaskCpuSec map[int][]float64
	NodeCosts       map[string]*NodeCost
	// NodeEstimates mirrors extra.PlanStatsAndCosts.Stats -- needed by
	// estimateBad's join-key-specific NDV check (REC-2), computed on demand
	// rather than cached on NodeMetrics itself (NodeMetrics is built before
	// the plan tree/PlanNodes are assembled, §5.1 ParseQuery ordering).
	NodeEstimates map[string]*NodeEstimate
	PlanNodes     map[string]*plan_node.PlanNode
	Joins         []plan_node.Join

	Runaway *RunawaySignal
}

func isScanOperator(opType string) bool { return strings.Contains(opType, "TableScan") }
func isExchangeOperator(opType string) bool {
	return strings.Contains(opType, "Exchange") || strings.Contains(opType, "PartitionedOutput")
}

// ExtractSignals computes the Signals for one frame (§5.4.2 Step 1). runaway is
// nil for a lone terminal/frame with no progression context (FP-13's first-class
// no-snapshot mode).
func ExtractSignals(qi *queryjson.QueryInfo, extra *ExtraInfo, nodeMetrics map[string]*NodeMetrics, planNodes map[string]*plan_node.PlanNode, joins []plan_node.Join, runaway *RunawaySignal) *Signals {
	sig := &Signals{
		NodeMetrics:     nodeMetrics,
		NodeStageId:     map[string]int{},
		StageCpuSec:     map[int]float64{},
		StageTaskCpuSec: map[int][]float64{},
		PlanNodes:       planNodes,
		Joins:           joins,
		Runaway:         runaway,
	}
	if qi != nil {
		sig.State = qi.State
	}
	if extra != nil && extra.PlanStatsAndCosts != nil {
		sig.NodeCosts = extra.PlanStatsAndCosts.Costs
		sig.NodeEstimates = extra.PlanStatsAndCosts.Stats
	}

	if qi != nil && qi.QueryStats != nil {
		qs := qi.QueryStats
		// FIX (review C1): totalPlanningTime already INCLUDES analysisTime as a
		// component (it is a subset, not an additive sibling) -- verified
		// against the real corpus: 98/382 files have analysisTime ==
		// totalPlanningTime exactly, and summing them there gave an impossible
		// plan-time ratio > 1.0 (e.g. 1.465 on a 4.26s FINISHED query), which
		// halved the effective A1 elevation threshold and produced ~180 false
		// A1 headline verdicts. Use totalPlanningTime alone; analysisTime is
		// kept as an informational sub-metric only (AnalysisSec), never added.
		sig.PlanningSec = qs.TotalPlanningTime.Seconds()
		sig.AnalysisSec = qs.AnalysisTime.Seconds()
		sig.QueueSec = qs.QueuedTime.Seconds()
		sig.CpuSec = qs.TotalCpuTime.Seconds()
		sig.ElapsedSec = qs.ElapsedTime.Seconds()
		sig.PeakUserMemoryBytes = float64(qs.PeakUserMemoryReservation)
		sig.RawInputBytes = float64(qs.RawInputDataSize)
		sig.RawInputPositions = qs.RawInputPositions
		sig.TotalDrivers = qs.TotalDrivers

		for _, op := range qs.OperatorSummaries {
			if op == nil {
				continue
			}
			sig.NodeStageId[op.PlanNodeId] = op.StageId
			// A rough per-stage CPU total, used only as a fallback for
			// relevance/attribution when no genuine per-task data exists for
			// this stage (overwritten below with the exact sum when it does).
			// Deliberately NOT used to seed the C2 skew series: operator-level
			// entries mix different operator TYPES within one stage (TableScan
			// vs Aggregation vs Output, etc.), which is not task skew.
			sig.StageCpuSec[op.StageId] += op.GetOutputCpu.Seconds() + op.AddInputCpu.Seconds()
			sig.SpillBytes += float64(op.SpilledDataSize)
			sig.BlockedWallSec += op.BlockedWall.Seconds()

			decoded := DecodeOperatorRuntimeStats(op.RuntimeStats)
			if isScanOperator(op.OperatorType) {
				sig.ScanWallSec += op.GetOutputWall.Seconds() + op.AddInputWall.Seconds()
				sig.ScanWaitSec += float64(SumRuntimeMetrics(decoded, "blockedWaitForSplitWallNanos", "ioWaitWallNanos", "waitForPreloadSplitNanos")) / 1e9
			}
			if isExchangeOperator(op.OperatorType) {
				// Busy time only (GetOutputWall+AddInputWall) -- deliberately
				// EXCLUDING BlockedWall. Verified against real data (both a
				// real Java corpus and the native incident sample):
				// BlockedWall on Exchange/PartitionedOutput operators is
				// itself a driver-count-scaled aggregate that is often the
				// single largest number in the whole file (e.g. tens of
				// minutes of blocked time on a query whose total scheduled
				// time is under three), and it reflects WAITING/backpressure
				// (already the dedicated C1 signal, via ProducerBlockedSec/
				// OtherBlockedSec), not "time spent doing exchange work".
				// Folding it into B5's busy-time share caused near-universal
				// false B5 elevation across a 191-file real Java corpus.
				sig.ExchangeWallSec += op.GetOutputWall.Seconds() + op.AddInputWall.Seconds()
			}
			sig.ProducerBlockedSec += float64(SumRuntimeMetrics(decoded, "blockedWaitForProducerWallNanos")) / 1e9
			// Corroborate native-runtime detection with per-operator runtimeStats
			// too (see DetectNativeRuntime): a small query's query-level rollup
			// may not itself surface a hierarchical Op.node key even when its
			// operators do.
			if !sig.NativeRuntime && DetectNativeRuntime(decoded) {
				sig.NativeRuntime = true
			}
		}
	}

	if extra != nil && extra.QueryStats != nil {
		eqs := extra.QueryStats
		if !sig.NativeRuntime {
			sig.NativeRuntime = DetectNativeRuntime(eqs.RuntimeStats)
		}
		sig.QuickStatsDetected = HasQuickStatsCounters(eqs.RuntimeStats)
		if eqs.TotalScheduledTime != nil {
			sig.ScheduledSec = eqs.TotalScheduledTime.Seconds()
		}
		if eqs.TotalBlockedTime != nil {
			sig.BlockedWallSec = eqs.TotalBlockedTime.Seconds()
		}
		if eqs.FullyBlocked != nil {
			sig.FullyBlocked = *eqs.FullyBlocked
		}
		if eqs.FinishingTime != nil {
			sig.FinishingSec = eqs.FinishingTime.Seconds()
		}
		if eqs.TotalSplits != nil {
			sig.TotalSplits = *eqs.TotalSplits
		}
		if eqs.ShuffledDataSize != nil {
			sig.ShuffledBytes = float64(*eqs.ShuffledDataSize)
		}
		// Note: eqs.QueuedTime itself is not added here -- queuedTime is
		// already counted once from qs.QueuedTime above (queryjson.QueryStats);
		// ExtraQueryStats.QueuedTime exists only for engines/shapes where the
		// queryjson field might be absent, which this code does not currently
		// need to fall back to.
		if eqs.ResourceWaitingTime != nil {
			sig.QueueSec += eqs.ResourceWaitingTime.Seconds()
		}
		if eqs.WaitingForPrerequisitesTime != nil {
			sig.QueueSec += eqs.WaitingForPrerequisitesTime.Seconds()
		}
		if eqs.DispatchingTime != nil {
			sig.QueueSec += eqs.DispatchingTime.Seconds()
		}
		if eqs.ProcessedInputDataSize != nil {
			sig.ProcessedInputBytes = float64(*eqs.ProcessedInputDataSize)
		}
		if eqs.ProgressPercentage != nil {
			v := *eqs.ProgressPercentage
			sig.ProgressPercentage = &v
		}
		for _, g := range eqs.StageGcStatistics {
			if g == nil || !g.HasGcData() {
				continue
			}
			sig.GcDataAvailable = true
			if secs, ok := g.FullGcTimeSeconds(); ok {
				sig.GcTimeSec += secs
			}
		}
	}
	sig.OtherBlockedSec = math.Max(0, sig.BlockedWallSec-sig.ProducerBlockedSec)

	if extra != nil && extra.OutputStage != nil {
		// Real per-TASK CPU (for C2 skew, §5.4.1: "per-task ... CPU across
		// outputStage...tasks[]"). This must come from genuine tasks, not from
		// operatorSummaries: an operator-level breakdown mixes different
		// OPERATOR TYPES within the same stage (e.g. TableScan vs Aggregation vs
		// Output), whose CPU costs differ by construction and are not "skew" in
		// any meaningful sense -- comparing them as if they were parallel task
		// instances of the same work produced near-universal false positives,
		// verified against a real 191-file corpus. Stages with no task data
		// (e.g. an unsupported stage shape) simply do not participate in C2.
		taskCpuByStage := map[int][]float64{}
		for _, stage := range extra.OutputStage.Flatten() {
			n, ok := NumericStageId(stage.StageId)
			if !ok || stage.LatestAttemptExecutionInfo == nil {
				continue
			}
			for _, id := range keys(stage.PlanNodeIds()) {
				if _, exists := sig.NodeStageId[id]; !exists {
					sig.NodeStageId[id] = n
				}
			}
			if stage == extra.OutputStage {
				sig.RootStageTaskCount = len(stage.LatestAttemptExecutionInfo.Tasks)
			}
			var stageCpu float64
			for _, t := range stage.LatestAttemptExecutionInfo.Tasks {
				if t == nil || t.Stats == nil {
					continue
				}
				cpuSec := float64(t.Stats.TotalCpuTimeInNanos) / 1e9
				taskCpuByStage[n] = append(taskCpuByStage[n], cpuSec)
				stageCpu += cpuSec
			}
			if len(taskCpuByStage[n]) > 0 {
				// Real per-task data supersedes the operator-level
				// approximation seeded above for both the CPU total (now exact)
				// and the skew series (now semantically correct).
				sig.StageCpuSec[n] = stageCpu
				sig.StageTaskCpuSec[n] = taskCpuByStage[n]
			}
		}
	}
	return sig
}

func keys(m map[string]bool) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}

func tier(value, warn, high float64) int {
	switch {
	case value >= high:
		return 3
	case value >= warn:
		return 2
	default:
		return 0
	}
}

func binarySeverity(cond bool) int {
	if cond {
		return 2
	}
	return 0
}

func ratio(num, den float64) float64 {
	if den <= 0 {
		return 0
	}
	return num / den
}

func newResult(cat string, severity int, note string) *CategoryResult {
	return &CategoryResult{
		Category: cat,
		Group:    CategoryGroup[cat],
		Name:     CategoryNames[cat],
		Severity: severity,
		Note:     note,
	}
}

// singleEvidence builds the single ranked evidence item every scorer emits
// (§5.4.2 Step 4 / review W1: every category verdict, not just A3, must carry
// its ranked numeric evidence — previously only A3 populated Evidence, so the
// "## Root-cause evidence" section printed "(no evidence recorded)" for any
// other verdict).
func singleEvidence(text string, values map[string]float64) []EvidenceItem {
	return []EvidenceItem{{Text: text, Values: values}}
}

// estimateBad reports the A3 "plan-estimate" family signature (§5.4.1 A3,
// available live from frame 1): a LOW-confidence estimate together with
// missing NDV -- either the generic per-node NDV (m.MinVariableNDV, "all
// observed variables are NaN") or, more precisely, the join-KEY-specific NDV
// (joinKeyNDVMissing) -- a node can have real NDV on some non-join column
// (so the generic check alone is false) while its join-predicate columns
// specifically still lack NDV, which is what actually drives cardinality
// mis-estimation. Callers compute joinKeyNDVMissing via
// nodeHasNaNJoinKeyNDV, using whichever plan-node/estimate lookup they have
// on hand (Signals during static classification, a single frame's Query
// during progression/RunawayComputation analysis).
//
// REC-2 (round-2 source-verified): joinNodeStatsEstimate is deliberately NOT
// part of this signature (nor of joinKeyNDVMissing) -- it is an HBO
// (history-based-optimizer)/null-skew runtime field, populated only by
// HistoryBasedPlanStatisticsTracker and consumed only by null-skew
// randomization + text rendering, never by the CBO's JoinStatsRule. All-NaN
// merely means "no HBO history for this query", not "missing statistics" --
// so it must not corroborate/trigger A3 elevation, the live-elevation
// signature, or a confidence family. It is at most rendered as a non-causal
// informational line outside ranked evidence (report.go).
func estimateBad(m *NodeMetrics, joinKeyNDVMissing bool) bool {
	if m == nil || m.Confidence != "LOW" {
		return false
	}
	return m.MinVariableNDV == nil || joinKeyNDVMissing
}

// broadcastLarge reports whether node id is a REPLICATED join whose build side
// cost estimate exceeds BroadcastLargeBytes (§5.4.1 A3 join-distribution signal).
func broadcastLarge(nodeId string, sig *Signals, th *Thresholds) bool {
	if sig.PlanNodes == nil {
		return false
	}
	n := sig.PlanNodes[nodeId]
	if n == nil || !plan_node.IsJoin[n.Name] {
		return false
	}
	if !strings.Contains(strings.ToUpper(n.Identifier), "REPLICATED") {
		return false
	}
	if sig.NodeCosts == nil {
		return false
	}
	cost := sig.NodeCosts[nodeId]
	if cost == nil {
		return false
	}
	return float64(cost.MaxMemory) >= th.BroadcastLargeBytes
}

// a3LiveEmergenceDetected reports whether A3's LIVE-COMPUTABLE signature is
// present in this frame's signals (§5.6/§5.4.4 round-3 fix, Issue 2): the
// estimate family (estimateBad -- confident==LOW + the missing-stats
// signature, derived from planStatsAndCosts and therefore present from frame
// 1, byte-identical live, §5.4.1) on any plan node, OR the RunawayComputation
// signal firing. This intentionally does NOT reuse scoreA3's full severity
// computation, which also folds in the terminal-only measured-actual fan-out
// family (and its EstimateErrorRatio corroborator) -- those require a
// terminal file or a finished stage and must NEVER set A3's emergence time (a
// terminal measurement is an end-state, not an onset; timing A3 by it
// manufactured the round-3 spurious "C1 before A3" conflict). Used solely for
// progression emergence timing (AnalyzeProgression below); never affects the
// actual A3 verdict/severity, which stays scoreA3's job untouched.
func a3LiveEmergenceDetected(sig *Signals) bool {
	if sig.Runaway != nil && sig.Runaway.Fired {
		return true
	}
	for nodeId, m := range sig.NodeMetrics {
		joinKeyBad := nodeHasNaNJoinKeyNDV(nodeId, sig.PlanNodes, sig.NodeEstimates)
		if estimateBad(m, joinKeyBad) {
			return true
		}
	}
	return false
}

// a3Candidate is one plan node considered for the A3 verdict.
type a3Candidate struct {
	nodeId   string
	m        *NodeMetrics
	severity int
	families int
	measured bool
}

// candidateBetter implements a TOTAL, deterministic order over A3 candidates
// (review B1, a pre-existing determinism bug present since 34f101c): the
// original tie-break kept whichever candidate was encountered FIRST among
// equal-(severity, families) ties while iterating sig.NodeMetrics, a Go map
// -- Go deliberately randomizes map iteration order, so on a corpus file
// with >=2 nodes tied at the same (severity, families) the reported "where"
// node id could flip between runs of the identical binary on the identical
// input (observed: 6/382 real corpus files). Category/severity/confidence
// were never affected, only which node got reported -- but the tool is
// documented as deterministic (§5.1.1), so this must be fixed.
//
// Ranking, highest priority first (never changes severity/family scoring
// itself, only which already-equally-scored node wins):
//  1. severity (higher wins)
//  2. families (higher wins)
//  3. actual fan-out (higher wins; a node with no measured fan-out sorts
//     last here -- the most RELEVANT "where" for a plan-path verdict is the
//     one with the bigger measured blow-up)
//  4. planNodeId, ascending (final, absolute decider -- numeric comparison
//     when both ids parse as integers, matching real Presto plan-node ids;
//     falls back to a plain string comparison otherwise, so this never
//     panics or leaves the order any less total for non-numeric ids)
func candidateBetter(c, best *a3Candidate) bool {
	if c.severity != best.severity {
		return c.severity > best.severity
	}
	if c.families != best.families {
		return c.families > best.families
	}
	cFanout, bestFanout := candidateFanout(c), candidateFanout(best)
	if cFanout != bestFanout {
		return cFanout > bestFanout
	}
	return nodeIdLess(c.nodeId, best.nodeId)
}

// candidateFanout returns the candidate's actual fan-out, or -Inf when
// unavailable (so a measured candidate always outranks an unmeasured one at
// this tie-break tier, and two unmeasured candidates fall through to the
// planNodeId decider below).
func candidateFanout(c *a3Candidate) float64 {
	if c.m == nil || c.m.ActualFanout == nil {
		return math.Inf(-1)
	}
	return *c.m.ActualFanout
}

// nodeIdLess orders two plan-node ids ascending: numerically when both parse
// as integers (the real, verified shape of Presto plan-node ids), otherwise
// by plain string comparison -- always a total order, never an error.
func nodeIdLess(a, b string) bool {
	an, aErr := strconv.Atoi(a)
	bn, bErr := strconv.Atoi(b)
	if aErr == nil && bErr == nil {
		return an < bn
	}
	return a < b
}

// scoreA3 implements §5.4.1/§5.4.2's A3 Plan path scoring: the actual family
// (terminal/finished-stage fan-out), the estimate family (available live), the
// join-distribution booster, and the live "RunawayComputation" elevation path
// that reaches severity 3 with no measured actual at all.
func scoreA3(sig *Signals, th *Thresholds) *CategoryResult {
	r := newResult(CatPlanPath, 0, "no elevated plan-node signature found")
	var best *a3Candidate
	consider := func(c *a3Candidate) {
		if best == nil || candidateBetter(c, best) {
			best = c
		}
	}

	for nodeId, m := range sig.NodeMetrics {
		c := &a3Candidate{nodeId: nodeId, m: m}
		families := map[string]bool{}
		sev := 0
		joinKeyBad := nodeHasNaNJoinKeyNDV(nodeId, sig.PlanNodes, sig.NodeEstimates)
		if m.ActualFanout != nil {
			f := *m.ActualFanout
			if f >= th.FanoutModerate {
				sev = 1
				families["fanout"] = true
			}
			if f >= th.FanoutHigh {
				sev = 2
			}
			if f >= th.FanoutHigh && estimateBad(m, joinKeyBad) {
				sev = 3
			}
			if sev >= 2 {
				c.measured = true
			}
		}
		if estimateBad(m, joinKeyBad) {
			families["estimate"] = true
			if sev < 2 {
				sev = 1
			}
		}
		if broadcastLarge(nodeId, sig, th) {
			families["broadcast"] = true
			if sev < 2 {
				sev = 2
			}
		}
		// EstimateErrorHigh (review W2, now wired): the estimate is off from
		// the measured actual by orders of magnitude — only computable when a
		// measured actual exists (terminal/finished-stage), corroborating
		// (never on its own initiating) elevation, matching the design's
		// "max(est/actual, actual/est) >= threshold" signal.
		if m.EstimateErrorRatio != nil {
			e := *m.EstimateErrorRatio
			if e > 0 && !math.IsNaN(e) && !math.IsInf(e, 0) {
				magnitude := e
				if magnitude < 1 {
					magnitude = 1 / magnitude
				}
				if magnitude >= th.EstimateErrorHigh {
					families["estimateError"] = true
					if sev < 2 {
						sev = 2
					}
				}
			}
		}
		c.severity = sev
		c.families = len(families)
		if sev > 0 {
			consider(c)
		}
	}

	// A3 live elevation path (§5.4.2, §10 item 11(d)): RunawayComputation fires
	// -> A3 is elevated with NO measured actual required, using the SAME
	// estimateBad gate as the terminal/measured path above for its 3-vs-2
	// choice: severity 3 when the suspect node also carries the missing-stats
	// signature (nodeHasNaNJoinKeyNDV, or the LOW-confidence/NDV-missing
	// fallback signature -- estimateBad); otherwise severity 2 (catastrophic but
	// inherent/structural -- NOT dropped, and never forced to 3). Either way the
	// temporal-runaway family stands in as the second corroborating family.
	if sig.Runaway != nil && sig.Runaway.Fired {
		suspectJoinKeyBad := nodeHasNaNJoinKeyNDV(sig.Runaway.SuspectNodeId, sig.PlanNodes, sig.NodeEstimates)
		if m, ok := sig.NodeMetrics[sig.Runaway.SuspectNodeId]; ok {
			sev := 2
			if estimateBad(m, suspectJoinKeyBad) {
				sev = 3
			}
			c := &a3Candidate{nodeId: sig.Runaway.SuspectNodeId, m: m, severity: sev, families: 2, measured: false}
			consider(c)
		}
	}

	if best == nil {
		return r
	}
	m := best.m
	r.Severity = best.severity
	r.SignalFamilies = best.families
	r.FromMeasuredActual = best.measured
	r.NodeId = best.nodeId
	if len(m.OperatorTypes) > 0 {
		r.Operator = strings.Join(m.OperatorTypes, "+")
	}
	if stageId, ok := sig.NodeStageId[best.nodeId]; ok {
		r.StageId = fmt.Sprintf("%d", stageId)
	}
	r.Note = fmt.Sprintf("node %s: plan-path signature (severity %d)", best.nodeId, best.severity)
	if m.ActualFanout != nil {
		r.Evidence = append(r.Evidence, EvidenceItem{
			Text:   fmt.Sprintf("node %s (%s): actual fan-out out/in = %.3g/%.3g = %.1fx", best.nodeId, r.Operator, floatOr(m.ActualOut), floatOr(m.ActualIn), *m.ActualFanout),
			Values: map[string]float64{"fanout": *m.ActualFanout},
		})
	} else {
		r.Evidence = append(r.Evidence, EvidenceItem{Text: fmt.Sprintf("node %s: actual fan-out unavailable (no terminal/finished-stage operator summary, §2)", best.nodeId)})
	}
	if m.EstimatedOut != nil {
		r.Evidence = append(r.Evidence, EvidenceItem{
			Text:   fmt.Sprintf("planStatsAndCosts[%s]: outputRowCount=%.3g, confident=%s", best.nodeId, *m.EstimatedOut, m.Confidence),
			Values: map[string]float64{"estimatedOutputRowCount": *m.EstimatedOut},
		})
	} else if m.Confidence != "" {
		r.Evidence = append(r.Evidence, EvidenceItem{Text: fmt.Sprintf("planStatsAndCosts[%s]: confident=%s, outputRowCount=NaN", best.nodeId, m.Confidence)})
	}
	if m.MinVariableNDV == nil {
		r.Evidence = append(r.Evidence, EvidenceItem{Text: fmt.Sprintf("planStatsAndCosts[%s].variableStatistics: distinctValuesCount=NaN on all observed variables", best.nodeId)})
	}
	// REC-2: joinNodeStatsEstimate is deliberately NOT added to ranked
	// evidence here (see estimateBad's doc comment) -- it is a non-causal,
	// informational-only fact, rendered (if at all) by report.go outside the
	// ranked evidence list.
	if m.EstimateErrorRatio != nil {
		r.Evidence = append(r.Evidence, EvidenceItem{
			Text:   fmt.Sprintf("estimate_error_ratio = %.3g (actual/estimate)", *m.EstimateErrorRatio),
			Values: map[string]float64{"estimateErrorRatio": *m.EstimateErrorRatio},
		})
	}
	if sig.Runaway != nil && sig.Runaway.Fired && sig.Runaway.SuspectNodeId == best.nodeId {
		r.Evidence = append(r.Evidence, EvidenceItem{
			Text:   fmt.Sprintf("RunawayComputation: stage S%d holds %.0f%% of the query's scheduled-time growth, no measured fan-out available live", sig.Runaway.StageNum, sig.Runaway.StageShare*100),
			Values: map[string]float64{"stageShare": sig.Runaway.StageShare},
		})
	}
	return r
}

func floatOr(v *int64) float64 {
	if v == nil {
		return math.NaN()
	}
	return float64(*v)
}

func scoreA1(sig *Signals, th *Thresholds) *CategoryResult {
	// totalPlanningTime already includes analysisTime as a component (§ review
	// C1); the ratio is planning/elapsed alone, analysis is reported only as an
	// informational sub-metric. Belt-and-suspenders: the ratio is clamped so a
	// build-specific quirk can never print a physically impossible >1.0 share.
	r := ratio(sig.PlanningSec, sig.ElapsedSec)
	if r > 1 {
		r = 1
	}
	res := newResult(CatPlanTime, tier(r, th.PlanTimeRatioWarn, th.PlanTimeRatioHigh),
		fmt.Sprintf("planning %.3fs (of which analysis %.3fs) / elapsed %.3fs (ratio %.3f)", sig.PlanningSec, sig.AnalysisSec, sig.ElapsedSec, r))
	res.SignalFamilies = 1
	res.Evidence = []EvidenceItem{{
		Text:   fmt.Sprintf("totalPlanningTime %.3fs (includes analysisTime %.3fs) / elapsedTime %.3fs = %.3f", sig.PlanningSec, sig.AnalysisSec, sig.ElapsedSec, r),
		Values: map[string]float64{"planningSec": sig.PlanningSec, "analysisSec": sig.AnalysisSec, "elapsedSec": sig.ElapsedSec, "ratio": r},
	}}
	return res
}

func scoreA2(sig *Signals, th *Thresholds) *CategoryResult {
	r := ratio(sig.QueueSec, sig.ElapsedSec)
	res := newResult(CatQueueAdmission, tier(r, th.QueueRatioWarn, th.QueueRatioHigh),
		fmt.Sprintf("queued+admission %.3fs / elapsed %.3fs (ratio %.3f)", sig.QueueSec, sig.ElapsedSec, r))
	res.SignalFamilies = 1
	res.Evidence = singleEvidence(
		fmt.Sprintf("queued+resourceWaiting+prerequisites+dispatching %.3fs / elapsedTime %.3fs = %.3f", sig.QueueSec, sig.ElapsedSec, r),
		map[string]float64{"queueSec": sig.QueueSec, "elapsedSec": sig.ElapsedSec, "ratio": r})
	return res
}

func scoreB1(sig *Signals, th *Thresholds) *CategoryResult {
	r := ratio(sig.CpuSec, sig.ScheduledSec)
	res := newResult(CatCPU, binarySeverity(r >= th.CpuEfficiencyHigh),
		fmt.Sprintf("totalCpuTime %.3fs, cpu/scheduled %.3f", sig.CpuSec, r))
	res.SignalFamilies = 1
	res.Evidence = singleEvidence(
		fmt.Sprintf("totalCpuTime %.3fs / totalScheduledTime %.3fs = %.3f", sig.CpuSec, sig.ScheduledSec, r),
		map[string]float64{"cpuSec": sig.CpuSec, "scheduledSec": sig.ScheduledSec, "ratio": r})
	return res
}

func scoreB2(sig *Signals, th *Thresholds) *CategoryResult {
	res := newResult(CatMemorySpill, binarySeverity(sig.SpillBytes >= th.SpillBytesAny),
		fmt.Sprintf("peak user memory %.0f bytes (limit unknown); spilled %.0f bytes", sig.PeakUserMemoryBytes, sig.SpillBytes))
	res.SignalFamilies = 1
	res.Evidence = singleEvidence(
		fmt.Sprintf("spilledDataSize %.0f bytes (threshold %.0f); peakUserMemoryReservation %.0f bytes", sig.SpillBytes, th.SpillBytesAny, sig.PeakUserMemoryBytes),
		map[string]float64{"spillBytes": sig.SpillBytes, "peakUserMemoryBytes": sig.PeakUserMemoryBytes})
	return res
}

func scoreB3(sig *Signals, th *Thresholds) *CategoryResult {
	r := ratio(sig.ScanWallSec, sig.ScheduledSec)
	res := newResult(CatIOThroughput, binarySeverity(r >= th.ScanWallShareHigh),
		fmt.Sprintf("scan wall %.3fs / scheduled %.3fs (share %.3f)", sig.ScanWallSec, sig.ScheduledSec, r))
	res.SignalFamilies = 1
	res.Evidence = singleEvidence(
		fmt.Sprintf("TableScan getOutputWall+addInputWall %.3fs / totalScheduledTime %.3fs = %.3f", sig.ScanWallSec, sig.ScheduledSec, r),
		map[string]float64{"scanWallSec": sig.ScanWallSec, "scheduledSec": sig.ScheduledSec, "ratio": r})
	return res
}

func scoreB4(sig *Signals, th *Thresholds) *CategoryResult {
	r := ratio(sig.ScanWaitSec, sig.ScanWallSec)
	// Gate on scanning itself being a relevant fraction of the query (reusing
	// StageRelevanceShare, the same relevance concept C2/C3 already apply):
	// a large wait share inside a scan stage that is itself a negligible
	// fraction of total query work should not read as a query-level bottleneck.
	scanRelevance := ratio(sig.ScanWallSec, sig.ScheduledSec)
	relevant := scanRelevance >= th.StageRelevanceShare
	// elevated is the EXACT same condition passed to binarySeverity below —
	// extracted only so the quick-stats annotation (design.md §5.4.1 B4, §10
	// item 10) can read it afterward without duplicating/risking divergence
	// from the severity decision. Severity itself is unchanged.
	elevated := relevant && sig.ScanWallSec > 0 && r >= th.ScanWaitShareHigh
	note := fmt.Sprintf("scan wait %.3fs / scan wall %.3fs (share %.3f); scan wall/scheduled %.3f (relevance gate %.2f)", sig.ScanWaitSec, sig.ScanWallSec, r, scanRelevance, th.StageRelevanceShare)
	// Quick-stats sub-cause annotation: elevated B4 + quick-stats/metastore
	// counters present in query-level runtimeStats -> a more specific note
	// than generic storage latency. Annotation only; does not affect
	// `elevated`/severity above, which was already decided.
	if elevated && sig.QuickStatsDetected {
		note += " — quick-stats / metastore latency"
	}
	res := newResult(CatStorageLatency, binarySeverity(elevated), note)
	res.SignalFamilies = 1
	res.Evidence = singleEvidence(
		fmt.Sprintf("blockedWaitForSplit+ioWait+waitForPreloadSplit %.3fs / scan wall %.3fs = %.3f (scan relevance %.3f)", sig.ScanWaitSec, sig.ScanWallSec, r, scanRelevance),
		map[string]float64{"scanWaitSec": sig.ScanWaitSec, "scanWallSec": sig.ScanWallSec, "ratio": r, "scanRelevance": scanRelevance})
	return res
}

func scoreB5(sig *Signals, th *Thresholds) *CategoryResult {
	wallShare := ratio(sig.ExchangeWallSec, sig.ScheduledSec)
	sev := binarySeverity(wallShare >= th.ShuffleShareHigh)
	bytesRatio := ratio(sig.ShuffledBytes, sig.RawInputBytes)
	families := 1
	if sev >= 2 && bytesRatio >= th.ShuffleBytesPerInputHigh {
		sev = 3
		families = 2
	}
	res := newResult(CatNetworkShuffle, sev,
		fmt.Sprintf("exchange wall share %.3f (threshold %.2f); shuffled %.0f bytes / raw input %.0f bytes (ratio %.2f)", wallShare, th.ShuffleShareHigh, sig.ShuffledBytes, sig.RawInputBytes, bytesRatio))
	res.SignalFamilies = families
	res.Evidence = singleEvidence(
		fmt.Sprintf("Exchange/PartitionedOutput busy wall %.3fs / totalScheduledTime %.3fs = %.3f; shuffledDataSize %.0f bytes / rawInputDataSize %.0f bytes = %.3f", sig.ExchangeWallSec, sig.ScheduledSec, wallShare, sig.ShuffledBytes, sig.RawInputBytes, bytesRatio),
		map[string]float64{"exchangeWallSec": sig.ExchangeWallSec, "scheduledSec": sig.ScheduledSec, "wallShare": wallShare, "bytesRatio": bytesRatio})
	return res
}

func scoreC1(sig *Signals, th *Thresholds) *CategoryResult {
	// FIX (review C2): totalBlockedTime is a driver-count-scaled cumulative
	// aggregate (isBlockedWall etc. summed across every driver instance, e.g.
	// tens of THOUSANDS of driver-seconds), the exact same bug class already
	// fixed for B5's exchange busy-time. Dividing it directly by
	// totalScheduledTime -- itself ALSO driver-scaled, but on a much smaller
	// scale -- produced shares up to ~484x on real data (374/382 corpus files
	// had totalBlockedTime > totalScheduledTime), falsely elevating C1 on
	// nearly every query and capping confidence at MEDIUM query-wide.
	//
	// Normalizing by dividing both driver-scaled aggregates (blocked/scheduled)
	// cancels the driver-count scaling algebraically and reproduces the exact
	// same broken ratio -- so the fix instead compares a PER-DRIVER AVERAGE
	// blocked time against the query's (non-driver-scaled) wall-clock elapsed
	// time: avgBlockedPerDriver = totalBlockedTime / totalDrivers. Verified
	// against real data: the acceptance incident (a genuine, severe,
	// query-timing-out backpressure case) now yields ~1.0 (606.8s average
	// blocked per driver against a 603.6s wall-clock query -- i.e. drivers
	// were blocked for essentially the entire query, correctly still
	// elevating), while ordinary corpus queries drop to sane sub-1.0 shares
	// (typically 0.003-0.2) instead of the previous 2x-484x range.
	avgBlockedPerDriver := ratio(sig.BlockedWallSec, float64(maxInt(sig.TotalDrivers, 1)))
	r := ratio(avgBlockedPerDriver, sig.ElapsedSec)
	direction := "direction unknown"
	if sig.ProducerBlockedSec > sig.OtherBlockedSec {
		direction = "producer-side (upstream slow)"
	} else if sig.OtherBlockedSec > 0 {
		direction = "consumer/buffer-side (downstream slow)"
	}
	res := newResult(CatWaitBackpressure, binarySeverity(r >= th.BlockedShareHigh),
		fmt.Sprintf("avg blocked/driver %.3fs (blocked %.3fs / %d drivers) / elapsed %.3fs (share %.3f), %s; fullyBlocked=%v",
			avgBlockedPerDriver, sig.BlockedWallSec, sig.TotalDrivers, sig.ElapsedSec, r, direction, sig.FullyBlocked))
	res.SignalFamilies = 1
	res.Evidence = []EvidenceItem{{
		Text:   fmt.Sprintf("totalBlockedTime %.3fs / totalDrivers %d = %.3fs avg/driver; avg/driver / elapsedTime %.3fs = %.3f", sig.BlockedWallSec, sig.TotalDrivers, avgBlockedPerDriver, sig.ElapsedSec, r),
		Values: map[string]float64{"blockedWallSec": sig.BlockedWallSec, "totalDrivers": float64(sig.TotalDrivers), "avgBlockedPerDriverSec": avgBlockedPerDriver, "elapsedSec": sig.ElapsedSec, "share": r},
	}}
	return res
}

func maxInt(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func scoreC2(sig *Signals, th *Thresholds) *CategoryResult {
	totalCpu := 0.0
	for _, v := range sig.StageCpuSec {
		totalCpu += v
	}
	maxRatio := 0.0
	skewStage := -1
	bestMaxTaskCpu := 0.0
	for stage, tasks := range sig.StageTaskCpuSec {
		if totalCpu <= 0 || ratio(sig.StageCpuSec[stage], totalCpu) < th.StageRelevanceShare {
			continue
		}
		if len(tasks) < 2 {
			continue
		}
		sorted := append([]float64{}, tasks...)
		sort.Float64s(sorted)
		median := sorted[len(sorted)/2]
		max := sorted[len(sorted)-1]
		if median <= 0 {
			continue
		}
		r := max / median
		if r > maxRatio {
			maxRatio = r
			skewStage = stage
			bestMaxTaskCpu = max
		}
	}

	// Round-7 near-zero-denominator guard (design.md §5.4.2,
	// SkewMinMaxTaskCpuSec): a ratio computed from a heavy max but a
	// near-zero median is genuine skew and must still fire -- the guard is
	// deliberately on the winning stage's own MAX per-task CPU, never the
	// median or total -- but when even that max is itself tiny, the ratio is
	// measuring noise, not skew (real healthy-query fixture: ratio 21371
	// from a ~0 median with only 1.77s of total query CPU).
	if skewStage >= 0 && bestMaxTaskCpu < th.SkewMinMaxTaskCpuSec {
		res := newResult(CatSkew, 0,
			fmt.Sprintf("task CPU too small for meaningful skew (max task CPU < SkewMinMaxTaskCpuSec %.1fs; total CPU %.3fs) — informational", th.SkewMinMaxTaskCpuSec, totalCpu))
		res.SignalFamilies = 1
		return res
	}

	res := newResult(CatSkew, tier(maxRatio, th.SkewRatioWarn, th.SkewRatioHigh),
		fmt.Sprintf("max/median task CPU ratio %.2f (relevant stages only)", maxRatio))
	if skewStage >= 0 {
		res.StageId = fmt.Sprintf("%d", skewStage)
	}
	res.SignalFamilies = 1
	res.Evidence = singleEvidence(
		fmt.Sprintf("stage %s: max task CPU / median task CPU = %.2f (stages below StageRelevanceShare=%.2f of query CPU excluded)", res.StageId, maxRatio, th.StageRelevanceShare),
		map[string]float64{"maxMedianRatio": maxRatio})
	return res
}

func scoreC3(sig *Signals, th *Thresholds) *CategoryResult {
	sev := 0
	avg := 0.0
	if sig.TotalSplits > 0 {
		avg = float64(sig.RawInputPositions) / float64(sig.TotalSplits)
		if avg < th.TinySplitRows {
			sev = 2
		}
	}
	res := newResult(CatScheduling, sev,
		// SplitsPerCoreLow is surfaced here for visibility even though it never
		// gates severity (design: under-parallelism is informational-only,
		// needs the cluster core count, which is not in the JSON).
		fmt.Sprintf("avg rows/split %.0f (splits=%d); under-parallelism: core count unknown, SplitsPerCoreLow=%.0f informational only until --cluster-info exists", avg, sig.TotalSplits, th.SplitsPerCoreLow))
	res.SignalFamilies = 1
	res.Evidence = singleEvidence(
		fmt.Sprintf("rawInputPositions %d / totalSplits %d = %.0f rows/split (TinySplitRows threshold %.0f)", sig.RawInputPositions, sig.TotalSplits, avg, th.TinySplitRows),
		map[string]float64{"avgRowsPerSplit": avg, "totalSplits": float64(sig.TotalSplits)})
	return res
}

func scoreC4(sig *Signals, th *Thresholds) *CategoryResult {
	r := ratio(sig.FinishingSec, sig.ElapsedSec)
	res := newResult(CatOutputResult, binarySeverity(r >= th.FinishShareHigh),
		fmt.Sprintf("finishing %.3fs / elapsed %.3fs (share %.3f); root stage tasks=%d; client fetch rate inferred from tail timing", sig.FinishingSec, sig.ElapsedSec, r, sig.RootStageTaskCount))
	res.SignalFamilies = 1
	res.Evidence = singleEvidence(
		fmt.Sprintf("finishingTime %.3fs / elapsedTime %.3fs = %.3f; root stage task count %d", sig.FinishingSec, sig.ElapsedSec, r, sig.RootStageTaskCount),
		map[string]float64{"finishingSec": sig.FinishingSec, "elapsedSec": sig.ElapsedSec, "ratio": r})
	return res
}

func scoreD1(sig *Signals, th *Thresholds) *CategoryResult {
	if sig.NativeRuntime {
		res := newResult(CatGCPauses, 0, "no GC (native workers) — inferred from prefixed runtimeStats keys / Velox operator types")
		res.SignalFamilies = 1
		res.Evidence = singleEvidence("native runtime detected from a hierarchical Operator.numericNodeId runtimeStats key shape; C++ Velox workers have no JVM GC", nil)
		return res
	}
	if !sig.GcDataAvailable {
		res := newResult(CatGCPauses, 0, "no GC data available in this query JSON")
		res.Evidence = singleEvidence("queryStats.stageGcStatistics carries no GC measurement (absent/null, not zero)", nil)
		return res
	}
	r := ratio(sig.GcTimeSec, sig.ElapsedSec)
	res := newResult(CatGCPauses, binarySeverity(r >= th.GcShareHigh),
		fmt.Sprintf("full GC time %.3fs / elapsed %.3fs (share %.3f)", sig.GcTimeSec, sig.ElapsedSec, r))
	res.SignalFamilies = 1
	res.Evidence = singleEvidence(
		fmt.Sprintf("sum(fullGcTime) %.3fs / elapsedTime %.3fs = %.3f", sig.GcTimeSec, sig.ElapsedSec, r),
		map[string]float64{"gcTimeSec": sig.GcTimeSec, "elapsedSec": sig.ElapsedSec, "ratio": r})
	return res
}

func scoreD2() *CategoryResult {
	res := newResult(CatLockContention, -1, "N/A — see memory arbitration / Execution-Wait")
	res.NA = true
	res.Evidence = singleEvidence("no lock/contention concept in Presto/Velox core; closest analog is memory-arbitration serialization (revocable memory + localArbitrationWaitWallNanos)", nil)
	return res
}

// Classify runs Step 1 (via the caller-supplied Signals) and Step 2 (elevation
// scoring) of §5.4.2 for every taxonomy category, always returning all of them
// (§5.4.1: "N/A categories ... reported as mapped, never omitted").
func Classify(sig *Signals, th *Thresholds) []*CategoryResult {
	return []*CategoryResult{
		scoreA1(sig, th),
		scoreA2(sig, th),
		scoreA3(sig, th),
		scoreB1(sig, th),
		scoreB2(sig, th),
		scoreB3(sig, th),
		scoreB4(sig, th),
		scoreB5(sig, th),
		scoreC1(sig, th),
		scoreC2(sig, th),
		scoreC3(sig, th),
		scoreC4(sig, th),
		scoreD1(sig, th),
		scoreD2(),
	}
}

// ApplyHealthyQueryGuards implements the round-7 healthy-query guards
// (design.md §5.4.2, §10 item 16), applied at this Step-2 scoring stage --
// mutating categories IN PLACE, strictly BEFORE Step-3 rooting/attribution
// (Attribute) runs on them -- so a category this function suppresses is
// never considered a root or an independent finding by construction (severity
// 0 already excludes it there, unchanged). Two gates, both gating/display-
// only (no CausalOrder/taxonomy/attribution/confidence-rule change):
//
//  1. Idle-driver gate (IdleDriverCpuScheduledMax): when sig.State ==
//     "FINISHED" AND the query's own cpu/scheduled ratio is below the floor,
//     categories C1/B3/B4/C3 (exactly these four, never B5 -- its exchange
//     busy-wall signal is not idle-inflated the same way) are downgraded to
//     severity 0 with an explicit "idle-driver artifact" note. FINISHED-only
//     by design: a FAILED/timed-out query's idleness can be the symptom
//     (deadlock, external wait), and live/RUNNING frames are never gated.
//  2. Healthy predicate (HealthyCpuFloorSec/HealthyMemFloorBytes): healthy :=
//     state=="FINISHED" AND totalCpuTime <= floor AND peakUserMemory <=
//     floor. When healthy, EVERY remaining elevated B*/C* category (a
//     broader net than gate 1 alone -- a healthy-but-not-idle query, e.g. a
//     fast query with a normal cpu/scheduled ratio, is possible) is also
//     downgraded to severity 0. Pre-execution A1/A2/A3 are NEVER touched by
//     either gate -- a fast query's planning time is exactly where A1
//     genuinely matters, per the design's explicit "no HEALTHY verdict"
//     ruling (§5.4.2/§5.4.3).
//
// Returns the healthy boolean, for the caller to build the report's leading
// top-line text from once the verdict/root is known (after Attribute()).
func ApplyHealthyQueryGuards(categories []*CategoryResult, sig *Signals, th *Thresholds) bool {
	byCat := map[string]*CategoryResult{}
	for _, c := range categories {
		byCat[c.Category] = c
	}

	cpuSchedRatio := ratio(sig.CpuSec, sig.ScheduledSec)
	if sig.State == "FINISHED" && cpuSchedRatio < th.IdleDriverCpuScheduledMax {
		if c := byCat[CatWaitBackpressure]; c != nil && c.Severity > 0 {
			c.Severity = 0
			c.Note = fmt.Sprintf("idle-driver artifact: cpu/scheduled %.3f < %.2f with state FINISHED — blocked %.3fs over %d idle driver slots is not a bottleneck — informational",
				cpuSchedRatio, th.IdleDriverCpuScheduledMax, sig.BlockedWallSec, sig.TotalDrivers)
		}
		if c := byCat[CatIOThroughput]; c != nil && c.Severity > 0 {
			c.Severity = 0
			c.Note = fmt.Sprintf("idle-driver artifact (scan wall share %.3f over idle slots) — informational", ratio(sig.ScanWallSec, sig.ScheduledSec))
		}
		if c := byCat[CatStorageLatency]; c != nil && c.Severity > 0 {
			c.Severity = 0
			c.Note = "idle-driver artifact — informational"
		}
		if c := byCat[CatScheduling]; c != nil && c.Severity > 0 {
			avg := 0.0
			if sig.TotalSplits > 0 {
				avg = float64(sig.RawInputPositions) / float64(sig.TotalSplits)
			}
			c.Severity = 0
			c.Note = fmt.Sprintf("idle-driver artifact (avg rows/split %.0f; no meaningful work per slot) — informational", avg)
		}
	}

	healthy := sig.State == "FINISHED" && sig.CpuSec <= th.HealthyCpuFloorSec && sig.PeakUserMemoryBytes <= th.HealthyMemFloorBytes
	if healthy {
		for _, c := range categories {
			if c.NA || c.Severity == 0 {
				continue
			}
			if CategoryGroup[c.Category] != "B" && CategoryGroup[c.Category] != "C" {
				continue
			}
			c.Severity = 0
			c.Note += " — informational (healthy query, no significant execution bottleneck)"
		}
	}
	return healthy
}

// round7LargestComponentLabel/Key deterministically identify, for the small
// set of categories whose own scorer already reports a "<name>Sec"+"ratio"
// evidence pair (design.md's healthy top-line, §5.4.2/§5.5's fourth worked
// example), which Values key is the PRIMARY measured quantity to quote --
// e.g. A1 reports both planningSec AND analysisSec, and only the former is
// the headline metric. A category outside this small, explicit set (e.g.
// A3, which is node-scoped, not a bare elapsed-time share) falls back to its
// own Note verbatim in verdictLargestComponentPhrase below -- deliberately
// NOT a generic map-iteration pick, which would be nondeterministic (Go map
// iteration order) whenever more than one "*Sec" key is present.
var round7LargestComponentLabel = map[string]string{
	CatPlanTime:         "planning",
	CatQueueAdmission:   "queue",
	CatWaitBackpressure: "blocked",
	CatOutputResult:     "finishing",
}
var round7LargestComponentKey = map[string]string{
	CatPlanTime:         "planningSec",
	CatQueueAdmission:   "queueSec",
	CatWaitBackpressure: "avgBlockedPerDriverSec",
	CatOutputResult:     "finishingSec",
}

// verdictLargestComponentPhrase renders the verdict root's own measured
// value as a short "<component> Xs (Y% of elapsed)" phrase for the round-7
// healthy top-line (design.md §5.4.2/§5.5's fourth worked example) --
// reusing the SAME evidence Values every "*Sec"+"ratio"-shaped scorer
// already populates, so the healthy top-line always describes the SAME root
// the normal causal engine already selected (never a separately re-derived
// ranking, which could disagree with root -- e.g. an idle-driver-gated
// category's inflated raw ratio must never win). Falls back to the root's
// own Name+Note verbatim for a category without that shape (e.g. A3, which
// is node-scoped) -- always renders something, from data, never blank.
func verdictLargestComponentPhrase(root *CategoryResult) string {
	if root == nil {
		return ""
	}
	if label, ok := round7LargestComponentLabel[root.Category]; ok {
		if key, ok := round7LargestComponentKey[root.Category]; ok && len(root.Evidence) > 0 {
			values := root.Evidence[0].Values
			v, okV := values[key]
			r, okR := values["ratio"]
			if okV && okR {
				return fmt.Sprintf("%s %.1fs (%.1f%% of elapsed)", label, v, r*100)
			}
		}
	}
	return fmt.Sprintf("%s: %s", strings.ToLower(root.Name), root.Note)
}

// HealthyTopLine renders the round-7 leading report line (design.md §5.4.2/
// §5.5's fourth worked example), printed ABOVE the verdict only when healthy
// is true: "No significant execution bottleneck detected — query FINISHED
// in <elapsed>s; largest component: <root's own measure>." All values are
// read from data (sig.ElapsedSec, the verdict root); never a hard-coded
// string.
func HealthyTopLine(sig *Signals, root *CategoryResult) string {
	return fmt.Sprintf("No significant execution bottleneck detected — query FINISHED in %.1fs; largest component: %s.",
		sig.ElapsedSec, verdictLargestComponentPhrase(root))
}

// attributionRule connects an elevated downstream category to a root category
// with an explicit, inspectable reason (§5.4.2 Step 3). When no rule connects a
// pair, the downstream category is reported as an independent finding rather
// than force-attributed.
type attributionRule struct {
	From, To string
	Reason   func(sig *Signals, from, to *CategoryResult) string
}

var attributionRules = []attributionRule{
	{CatPlanPath, CatCPU, func(sig *Signals, from, to *CategoryResult) string {
		total := 0.0
		for _, v := range sig.StageCpuSec {
			total += v
		}
		share := ratio(sig.StageCpuSec[stageNumOf(from.StageId)], total)
		return fmt.Sprintf("stage %s (containing node %s) holds %.0f%% of query CPU", stageLabel(from.StageId), from.NodeId, share*100)
	}},
	{CatPlanPath, CatWaitBackpressure, func(sig *Signals, from, to *CategoryResult) string {
		dir := "producer-side"
		if sig.OtherBlockedSec > sig.ProducerBlockedSec {
			dir = "consumer/buffer-side"
		}
		return fmt.Sprintf("blocked wall attributable to the fragment downstream of node %s (%s)", from.NodeId, dir)
	}},
	{CatPlanPath, CatNetworkShuffle, func(sig *Signals, from, to *CategoryResult) string {
		return fmt.Sprintf("shuffle volume dominated by fragments downstream of node %s", from.NodeId)
	}},
	{CatPlanPath, CatSkew, func(sig *Signals, from, to *CategoryResult) string {
		// Round-8 W7 rider: name the ACTUALLY-skewed stage, and claim "the
		// same stage as root node N" only when it genuinely is -- the skewed
		// stage and the A3 verdict's own stage frequently differ (verified:
		// 258_463's skew is stage 3, its verdict node 1544 is stage 5).
		if to.StageId != "" && to.StageId == from.StageId {
			return fmt.Sprintf("skew localized to stage %s, the same stage as root node %s", to.StageId, from.NodeId)
		}
		return fmt.Sprintf("skew localized to stage %s (root node %s is in stage %s)", to.StageId, from.NodeId, from.StageId)
	}},
	{CatMemorySpill, CatIOThroughput, func(sig *Signals, from, to *CategoryResult) string {
		return "spill re-read amplifies IO"
	}},
}

func stageNumOf(s string) int {
	var n int
	_, _ = fmt.Sscanf(s, "%d", &n)
	return n
}
func stageLabel(s string) string {
	if s == "" {
		return "?"
	}
	return "S" + s
}

func findAttributionRule(from, to string) *attributionRule {
	for i := range attributionRules {
		if attributionRules[i].From == from && attributionRules[i].To == to {
			return &attributionRules[i]
		}
	}
	return nil
}

// Attribute runs §5.4.2 Step 3 (causal ordering + attribution) and Step 4
// (localize is left to the caller/report renderer, which already has NodeId/
// StageId/Evidence on each CategoryResult). Returns the verdict (root, nil for
// "NO DOMINANT BOTTLENECK"), the independent findings (co-causes not explained
// by the verdict), and the attributed symptoms.
func Attribute(results []*CategoryResult, sig *Signals, th *Thresholds) (root *CategoryResult, independents []*CategoryResult, attributed []*CategoryResult) {
	byCat := map[string]*CategoryResult{}
	for _, r := range results {
		byCat[r.Category] = r
	}
	rootIdx := -1
	for i, cat := range th.CausalOrder {
		r := byCat[cat]
		if r != nil && !r.NA && r.Severity >= th.RootSeverityMin {
			root = r
			rootIdx = i
			break
		}
	}
	if root == nil {
		bestSev := 0
		for i, cat := range th.CausalOrder {
			r := byCat[cat]
			if r != nil && !r.NA && r.Severity > bestSev {
				bestSev = r.Severity
				root = r
				rootIdx = i
			}
		}
	}
	if root == nil {
		return nil, nil, nil
	}
	for i, cat := range th.CausalOrder {
		if cat == root.Category {
			continue
		}
		r := byCat[cat]
		if r == nil || r.NA || r.Severity < 1 {
			continue
		}
		if i < rootIdx {
			r.Independent = true
			independents = append(independents, r)
			continue
		}
		if rule := findAttributionRule(root.Category, cat); rule != nil {
			r.AttributedTo = root.Category
			r.AttributionReason = rule.Reason(sig, root, r)
			attributed = append(attributed, r)
		} else {
			r.Independent = true
			independents = append(independents, r)
		}
	}
	return root, independents, attributed
}

// Confidence is the verdict's explicit confidence level with printed reasons
// (§5.4.3).
type Confidence struct {
	Level   string   `json:"level"`
	Reasons []string `json:"reasons"`
}

// ComputeConfidence implements §5.4.3, including the snapshot-only MEDIUM cap:
// an upgrade to HIGH requires a measured actual (terminal file or a
// mid-run-finished stage); estimate + temporal-runaway evidence alone can only
// reach MEDIUM.
func ComputeConfidence(root *CategoryResult, independents []*CategoryResult, snapshotOnly bool) Confidence {
	if root == nil {
		return Confidence{Level: "LOW", Reasons: []string{"no category reached the elevation threshold (NO DOMINANT BOTTLENECK)"}}
	}
	hasIndependentHigh := false
	for _, r := range independents {
		if r.Severity >= 2 {
			hasIndependentHigh = true
			break
		}
	}
	if root.Severity < 2 {
		return Confidence{Level: "LOW", Reasons: []string{"root chosen by fallback (max severity below RootSeverityMin)"}}
	}
	// Category-specific "needed reference value absent" gaps force LOW even at
	// high severity (§5.4.3).
	switch root.Category {
	case CatMemorySpill:
		return Confidence{Level: "LOW", Reasons: []string{"peak user memory measured; pool/limit size is not present in the query JSON — verify against cluster config"}}
	case CatOutputResult:
		return Confidence{Level: "LOW", Reasons: []string{"client fetch rate is inferred from tail timing, not measured directly"}}
	}
	// HIGH gate (§10 item 11(d) ruling, §5.4.3): severity grades fixability, not
	// certainty, so a root severity 2 backed by a measured actual (a definitive,
	// not inferred, catastrophic fan-out) satisfies the same certainty bar as
	// severity 3 -- it substitutes for the missing-stats signature in the
	// confidence calculus even though the cause is inherent/structural rather
	// than stats-fixable. All other conditions are unchanged: two independent
	// signal families must still agree, no severity-2+ independent finding may
	// coexist, and the snapshot-only cap still applies.
	if (root.Severity == 3 || (root.Severity == 2 && root.FromMeasuredActual)) && root.SignalFamilies >= 2 && !hasIndependentHigh && (root.FromMeasuredActual || !snapshotOnly) {
		sevReason := "root severity 3"
		if root.Severity == 2 {
			sevReason = "root severity 2 with the measured-actual family present"
		}
		return Confidence{Level: "HIGH", Reasons: []string{sevReason, "at least two independent signal families agree", "no unattributed severity-2+ finding coexists"}}
	}
	reasons := []string{}
	if hasIndependentHigh {
		reasons = append(reasons, "a severity-2+ independent finding coexists, capping confidence at MEDIUM")
	}
	if root.SignalFamilies < 2 {
		reasons = append(reasons, "single evidence family backs the root")
	}
	if snapshotOnly && !root.FromMeasuredActual {
		reasons = append(reasons, "no measured actual available (snapshot-only); HIGH requires a terminal file or a finished stage")
	}
	if len(reasons) == 0 {
		reasons = append(reasons, "root severity >= 2")
	}
	return Confidence{Level: "MEDIUM", Reasons: reasons}
}
