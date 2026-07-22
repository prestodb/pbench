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
	"os"
)

// Category codes, matching the taxonomy of design.md §5.4.1.
const (
	CatPlanTime         = "A1"
	CatQueueAdmission   = "A2"
	CatPlanPath         = "A3"
	CatCPU              = "B1"
	CatMemorySpill      = "B2"
	CatIOThroughput     = "B3"
	CatStorageLatency   = "B4"
	CatNetworkShuffle   = "B5"
	CatWaitBackpressure = "C1"
	CatSkew             = "C2"
	CatScheduling       = "C3"
	CatOutputResult     = "C4"
	CatGCPauses         = "D1"
	CatLockContention   = "D2"
)

// CategoryNames gives the display name for each category code.
var CategoryNames = map[string]string{
	CatPlanTime:         "Plan time",
	CatQueueAdmission:   "Queue / admission",
	CatPlanPath:         "Plan path",
	CatCPU:              "CPU",
	CatMemorySpill:      "Memory / spill",
	CatIOThroughput:     "IO — scan throughput",
	CatStorageLatency:   "Storage / connector latency",
	CatNetworkShuffle:   "Network / exchange / shuffle",
	CatWaitBackpressure: "Wait / backpressure",
	CatSkew:             "Skew / imbalance",
	CatScheduling:       "Scheduling / parallelism",
	CatOutputResult:     "Output / result delivery",
	CatGCPauses:         "GC pauses",
	CatLockContention:   "Lock / contention",
}

// CategoryGroup gives the lifecycle group letter (A/B/C/D) for each category.
var CategoryGroup = map[string]string{
	CatPlanTime: "A", CatQueueAdmission: "A", CatPlanPath: "A",
	CatCPU: "B", CatMemorySpill: "B", CatIOThroughput: "B", CatStorageLatency: "B", CatNetworkShuffle: "B",
	CatWaitBackpressure: "C", CatSkew: "C", CatScheduling: "C", CatOutputResult: "C",
	CatGCPauses: "D", CatLockContention: "D",
}

// AllCategories lists every taxonomy category so category assessment tables are
// always printed in full (§5.4.1: "every category ... reported as mapped, never
// omitted").
var AllCategories = []string{
	CatPlanTime, CatQueueAdmission, CatPlanPath,
	CatCPU, CatMemorySpill, CatIOThroughput, CatStorageLatency, CatNetworkShuffle,
	CatWaitBackpressure, CatSkew, CatScheduling, CatOutputResult,
	CatGCPauses, CatLockContention,
}

// Thresholds are the tunable classification constants of design.md §5.4.2,
// overridable via --thresholds.
type Thresholds struct {
	PlanTimeRatioWarn float64 `json:"PlanTimeRatioWarn"`
	PlanTimeRatioHigh float64 `json:"PlanTimeRatioHigh"`

	QueueRatioWarn float64 `json:"QueueRatioWarn"`
	QueueRatioHigh float64 `json:"QueueRatioHigh"`

	FanoutModerate float64 `json:"FanoutModerate"`
	FanoutHigh     float64 `json:"FanoutHigh"`

	// EstimateErrorHigh: A3 "estimate off by orders of magnitude" booster —
	// max(actual/estimate, estimate/actual) >= this triggers when a measured
	// actual is available (wired in scoreA3).
	EstimateErrorHigh float64 `json:"EstimateErrorHigh"`

	// FallbackEstimateMin: A3 evidence LABELING only (design.md §5.4.2, §10
	// item 10) — has NO elevation effect on severity/the causal engine.
	// outputRowCount at/above this, together with confident=="LOW" and >=1
	// NaN-NDV join key, is labeled a "default-join-selectivity fallback" in
	// the rendered evidence (analyze.go's enrichA3Evidence) — i.e. the CBO's
	// crossJoin x default-selectivity guesswork, not a measured/derived
	// cardinality.
	FallbackEstimateMin float64 `json:"FallbackEstimateMin"`

	RunawayFramesMin         int     `json:"RunawayFramesMin"`
	RunawayCpuRatePerWallMin float64 `json:"RunawayCpuRatePerWallMin"`
	RunawayStageShareMin     float64 `json:"RunawayStageShareMin"`

	BroadcastLargeBytes float64 `json:"BroadcastLargeBytes"`
	// NOTE (review W2): the design's dynamic-filter-effectiveness A3 signal
	// (rows passing a dynamic filter / rows entering it) is intentionally
	// descoped for this phase — it requires correlating
	// dynamicFilterStats.producerNodeIds across producer/consumer operators, a
	// raw-JSON field not modeled yet; no dead threshold constant is kept for
	// it (a real signal to add if/when that correlation is implemented).

	CpuEfficiencyHigh float64 `json:"CpuEfficiencyHigh"`

	SpillBytesAny float64 `json:"SpillBytesAny"`

	ScanWallShareHigh float64 `json:"ScanWallShareHigh"`
	ScanWaitShareHigh float64 `json:"ScanWaitShareHigh"`

	ShuffleShareHigh         float64 `json:"ShuffleShareHigh"`
	ShuffleBytesPerInputHigh float64 `json:"ShuffleBytesPerInputHigh"`

	BlockedShareHigh float64 `json:"BlockedShareHigh"`

	SkewRatioWarn float64 `json:"SkewRatioWarn"`
	SkewRatioHigh float64 `json:"SkewRatioHigh"`

	StageRelevanceShare float64 `json:"StageRelevanceShare"`

	TinySplitRows float64 `json:"TinySplitRows"`
	// SplitsPerCoreLow: C3's under-parallelism half is informational-only by
	// design (needs the cluster core count, not in the JSON) and never
	// elevates; the constant is still surfaced in scoreC3's note/evidence so
	// it is not dead configuration, pending a --cluster-info flag.
	SplitsPerCoreLow float64 `json:"SplitsPerCoreLow"`

	FinishShareHigh float64 `json:"FinishShareHigh"`

	GcShareHigh float64 `json:"GcShareHigh"`

	// BuildKeyDupFactorMin/SmallBuildRowsMax: A3 fan-out-chain row EVIDENCE
	// LABELING only (round 5, design.md §5.4.1 A3(iii)/§10 item 14) — NO
	// elevation effect on severity/the causal engine. buildRows/buildKeyNdv
	// >= BuildKeyDupFactorMin classifies a chain row's build key as
	// non-unique; among those, buildRows <= SmallBuildRowsMax further flags
	// it dimensionNonUnique (a dimension-sized build duplicated on its own
	// key -- dedup, not pre-aggregate) rather than detail1toN (a legitimate
	// large detail expansion -- pre-aggregate).
	BuildKeyDupFactorMin float64 `json:"BuildKeyDupFactorMin"`
	SmallBuildRowsMax    float64 `json:"SmallBuildRowsMax"`

	// Round-7 healthy-query guards (design.md §5.4.2, §10 item 16): applied
	// at the Step-2 scoring stage, BEFORE Step-3 rooting/attribution, so a
	// suppressed category is never considered a root or an independent
	// finding. All four are gating/display-only — no CausalOrder/taxonomy/
	// attribution/confidence-rule change.
	//
	// SkewMinMaxTaskCpuSec: C2 near-zero-denominator guard. The max/median
	// task-CPU skew ratio is computed only when the MAX per-task CPU in the
	// relevance-gated stages is at/above this floor; below it, C2 is severity
	// 0 with an informational note. The guard is deliberately on the MAX, not
	// the median or total: a near-zero median under a genuinely heavy max IS
	// real skew and must still fire; a tiny max means no task did meaningful
	// work at all, so the ratio is measuring noise (real fixture: ratio
	// 21371 from a ~0 median with only 1.77s total query CPU — suppressed).
	SkewMinMaxTaskCpuSec float64 `json:"SkewMinMaxTaskCpuSec"`
	// IdleDriverCpuScheduledMax: the idle-driver gate. When query state ==
	// "FINISHED" AND cpu/scheduled is below this, categories C1/B3/B4/C3
	// (exactly these four) are downgraded to severity 0/informational with
	// an explicit "idle-driver artifact" note — their signals are computed
	// over aggregate scheduled/blocked/scan time that idle or short-
	// circuited driver slots accumulate while doing no real work (real
	// fixture: 3050 drivers, scheduled 755s vs CPU 1.77s → ratio 0.002).
	// FINISHED-only by design: a FAILED/timed-out query's idleness can be
	// the symptom (deadlock, external wait), so C1 must still elevate there;
	// live/RUNNING frames are never gated. B5 is deliberately excluded — its
	// primary signal is exchange busy-wall, which idle slots do not inflate
	// the same way.
	IdleDriverCpuScheduledMax float64 `json:"IdleDriverCpuScheduledMax"`
	// HealthyCpuFloorSec/HealthyMemFloorBytes: the healthy-query predicate --
	// healthy := state=="FINISHED" AND totalCpuTime <= HealthyCpuFloorSec AND
	// peakUserMemory <= HealthyMemFloorBytes. When healthy, the report leads
	// with a "no significant execution bottleneck" top-line and every
	// remaining elevated B*/C* category (a broader net than the idle-driver
	// gate alone, since a healthy-but-not-idle query is possible) is
	// downgraded to severity 0/informational; pre-execution A1/A2/A3 are
	// never touched (a fast query's planning time is exactly where A1
	// genuinely matters). Real fixture: 1.77s CPU / 384kB peak mem /
	// FINISHED → healthy; all four pathological acceptance runs are FAILED
	// with minutes-hours of CPU → two independent disqualifiers each.
	HealthyCpuFloorSec   float64 `json:"HealthyCpuFloorSec"`
	HealthyMemFloorBytes float64 `json:"HealthyMemFloorBytes"`

	// Round-8 evidence-labeling-only constants (design.md §5.4.2, §5.4.5 R8-D/
	// R8-E/R8-F) — NO elevation/severity/threshold effect on any category.
	//
	// SkewNullKeyFractionMin: R8-D. nullsFraction at/above this on a skewed
	// stage's partition-key column classifies it "degenerate/NULL partition
	// key" in the skewCause note.
	SkewNullKeyFractionMin float64 `json:"SkewNullKeyFractionMin"`
	// NdvCollapsedMax: R8-D/R8-E. distinctValuesCount at/below this (with rows
	// > 0) classifies a join/partition-key column as NDV-collapsed.
	NdvCollapsedMax float64 `json:"NdvCollapsedMax"`
	// NearMissProgressMin: R8-F. progressPercentage at/above this on a
	// non-FINISHED terminal selects the factual "likely close to finishing"
	// template over the bare number — informational only, no elevation effect.
	NearMissProgressMin float64 `json:"NearMissProgressMin"`
	// (R8-B's deadWeightFanout reuses FanoutModerate above — no new constant.)

	RootSeverityMin int      `json:"RootSeverityMin"`
	CausalOrder     []string `json:"CausalOrder"`

	// Top: evidence rows per category; the CLI's --top flag overwrites this
	// after LoadThresholds (cmd/diagnose/main.go Run), so a --thresholds file
	// value is itself just the default when --top is not passed.
	Top int `json:"Top"`
}

// DefaultThresholds returns the built-in defaults of design.md §5.4.2.
func DefaultThresholds() *Thresholds {
	return &Thresholds{
		PlanTimeRatioWarn: 0.3, PlanTimeRatioHigh: 0.5,
		QueueRatioWarn: 0.2, QueueRatioHigh: 0.5,
		FanoutModerate: 10, FanoutHigh: 100,
		EstimateErrorHigh:         1e3,
		FallbackEstimateMin:       1e15,
		RunawayFramesMin:          3,
		RunawayCpuRatePerWallMin:  4.0,
		RunawayStageShareMin:      0.5,
		BroadcastLargeBytes:       100 * 1024 * 1024,
		CpuEfficiencyHigh:         0.8,
		SpillBytesAny:             1,
		ScanWallShareHigh:         0.5,
		ScanWaitShareHigh:         0.3,
		ShuffleShareHigh:          0.4,
		ShuffleBytesPerInputHigh:  2.0,
		BlockedShareHigh:          0.5,
		SkewRatioWarn:             4,
		SkewRatioHigh:             10,
		StageRelevanceShare:       0.2,
		TinySplitRows:             10000,
		SplitsPerCoreLow:          1,
		FinishShareHigh:           0.2,
		GcShareHigh:               0.05,
		BuildKeyDupFactorMin:      2.0,
		SmallBuildRowsMax:         100000,
		SkewMinMaxTaskCpuSec:      1.0,
		IdleDriverCpuScheduledMax: 0.05,
		HealthyCpuFloorSec:        10.0,
		HealthyMemFloorBytes:      256 * 1024 * 1024,
		SkewNullKeyFractionMin:    0.9,
		NdvCollapsedMax:           1,
		NearMissProgressMin:       95.0,
		RootSeverityMin:           2,
		CausalOrder: []string{
			CatPlanTime, CatQueueAdmission, CatPlanPath, CatSkew,
			CatMemorySpill, CatNetworkShuffle, CatStorageLatency, CatIOThroughput, CatCPU,
			CatWaitBackpressure, CatScheduling, CatOutputResult,
			CatGCPauses,
		},
		Top: 5,
	}
}

// LoadThresholds returns the defaults with any fields present in the JSON file
// at path overridden (--thresholds). A nil/empty path returns the defaults
// unchanged.
func LoadThresholds(path string) (*Thresholds, error) {
	t := DefaultThresholds()
	if path == "" {
		return t, nil
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	if err := json.Unmarshal(raw, t); err != nil {
		return nil, err
	}
	return t, nil
}
