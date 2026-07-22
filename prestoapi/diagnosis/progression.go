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
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
)

// FrameFile is a discovered snapshot frame file (§5.6): files matching
// snapshot_<seq>_<epochms>.json, sorted by seq. Malformed names are skipped
// with a warning; duplicate seqs keep the later timestamp.
type FrameFile struct {
	Seq         int
	TimestampMs int64
	Path        string
}

var frameNameRe = regexp.MustCompile(`^snapshot_(\d+)_(\d+)\.json$`)

// DiscoverFrames lists and sorts the snapshot frames in dir.
func DiscoverFrames(dir string) (frames []*FrameFile, warnings []string) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, []string{fmt.Sprintf("failed to read snapshots directory %s: %v", dir, err)}
	}
	byId := map[int]*FrameFile{}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		m := frameNameRe.FindStringSubmatch(e.Name())
		if m == nil {
			warnings = append(warnings, fmt.Sprintf("skipped malformed snapshot filename: %s", e.Name()))
			continue
		}
		seq, _ := strconv.Atoi(m[1])
		ts, _ := strconv.ParseInt(m[2], 10, 64)
		if existing, ok := byId[seq]; ok && existing.TimestampMs >= ts {
			continue
		}
		byId[seq] = &FrameFile{Seq: seq, TimestampMs: ts, Path: filepath.Join(dir, e.Name())}
	}
	frames = make([]*FrameFile, 0, len(byId))
	for _, f := range byId {
		frames = append(frames, f)
	}
	sort.Slice(frames, func(i, j int) bool { return frames[i].Seq < frames[j].Seq })
	return frames, warnings
}

// ParsedFrame is one successfully parsed snapshot frame.
type ParsedFrame struct {
	Seq         int
	TimestampMs int64
	Query       *Query
	Signals     *Signals
}

// ParseFrames reads and parses every discovered frame file. Frames with
// unparsable JSON are skipped and counted in the returned warnings (§5.6).
func ParseFrames(files []*FrameFile) (parsed []*ParsedFrame, warnings []string) {
	for _, f := range files {
		raw, err := os.ReadFile(f.Path)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("frame %d (%s): read error: %v", f.Seq, f.Path, err))
			continue
		}
		q, err := ParseQuery(raw)
		if err != nil {
			warnings = append(warnings, fmt.Sprintf("frame %d (%s): parse error: %v", f.Seq, f.Path, err))
			continue
		}
		sig := ExtractSignals(q.Info, q.Extra, q.NodeMetrics, q.PlanNodes, q.Joins, nil)
		parsed = append(parsed, &ParsedFrame{Seq: f.Seq, TimestampMs: f.TimestampMs, Query: q, Signals: sig})
	}
	return parsed, warnings
}

// frameSample is the small set of query-level series values used by the
// RunawayComputation detector and the delta/rate table (§5.6).
type frameSample struct {
	timeSec         float64
	processedInput  int64
	progressPercent float64
	userMemoryBytes float64
	spilledBytes    float64
	cpuSec          float64
	stageScheduled  map[int]int64
}

func sampleFrame(pf *ParsedFrame, t0Ms int64) frameSample {
	s := frameSample{timeSec: float64(pf.TimestampMs-t0Ms) / 1000.0, cpuSec: pf.Signals.CpuSec}
	if pf.Query.Extra != nil && pf.Query.Extra.QueryStats != nil {
		eqs := pf.Query.Extra.QueryStats
		if eqs.ProcessedInputPositions != nil {
			s.processedInput = *eqs.ProcessedInputPositions
		}
		if eqs.ProgressPercentage != nil {
			s.progressPercent = *eqs.ProgressPercentage
		}
		s.stageScheduled = StageSeries(eqs.RuntimeStats, "taskScheduledTimeNanos")
	}
	if pf.Query.Info != nil && pf.Query.Info.QueryStats != nil {
		s.userMemoryBytes = float64(pf.Query.Info.QueryStats.PeakUserMemoryReservation)
	}
	s.spilledBytes = pf.Signals.SpillBytes
	return s
}

func approxFlat(delta, base float64) bool {
	if base == 0 {
		return delta == 0
	}
	return math.Abs(delta) <= 0.005*math.Abs(base)
}

// DetectRunaway implements the §5.6 RunawayComputation trigger: over >=
// RunawayFramesMin consecutive frames, throughput/progress/memory stay flat and
// spill stays zero while CPU climbs at least RunawayCpuRatePerWallMin
// CPU-seconds per wall-second. On fire, it localizes to the stage whose
// S<n>-taskScheduledTimeNanos growth accounts for >= RunawayStageShareMin of the
// query's scheduled-time growth, and names a suspect plan node inside that stage
// carrying the estimate-family A3 signature.
func DetectRunaway(frames []*ParsedFrame, th *Thresholds) *RunawaySignal {
	if len(frames) < th.RunawayFramesMin {
		return &RunawaySignal{}
	}
	sorted := append([]*ParsedFrame{}, frames...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].TimestampMs < sorted[j].TimestampMs })
	t0 := sorted[0].TimestampMs
	samples := make([]frameSample, len(sorted))
	for i, f := range sorted {
		samples[i] = sampleFrame(f, t0)
	}

	// Longest trailing run of "flat" consecutive pairs, ending at the last frame.
	runStart := len(samples) - 1
	for i := len(samples) - 1; i > 0; i-- {
		prev, cur := samples[i-1], samples[i]
		flat := approxFlat(float64(cur.processedInput-prev.processedInput), float64(prev.processedInput)) &&
			approxFlat(cur.progressPercent-prev.progressPercent, prev.progressPercent) &&
			approxFlat(cur.userMemoryBytes-prev.userMemoryBytes, prev.userMemoryBytes) &&
			cur.spilledBytes == 0 && prev.spilledBytes == 0
		if !flat {
			break
		}
		runStart = i - 1
	}
	runLen := len(samples) - runStart
	if runLen < th.RunawayFramesMin {
		return &RunawaySignal{}
	}
	first, last := samples[runStart], samples[len(samples)-1]
	wallDelta := last.timeSec - first.timeSec
	if wallDelta <= 0 {
		return &RunawaySignal{}
	}
	cpuRate := (last.cpuSec - first.cpuSec) / wallDelta
	if cpuRate < th.RunawayCpuRatePerWallMin {
		return &RunawaySignal{}
	}

	// Localize: which stage accounts for most of the scheduled-time growth.
	totalGrowth := int64(0)
	growthByStage := map[int]int64{}
	for stage, endVal := range last.stageScheduled {
		startVal := first.stageScheduled[stage]
		g := endVal - startVal
		if g < 0 {
			g = 0
		}
		growthByStage[stage] = g
		totalGrowth += g
	}
	bestStage, bestGrowth := -1, int64(-1)
	for stage, g := range growthByStage {
		if g > bestGrowth {
			bestGrowth, bestStage = g, stage
		}
	}
	sig := &RunawaySignal{Fired: true, StageNum: bestStage}
	if totalGrowth > 0 {
		sig.StageShare = float64(bestGrowth) / float64(totalGrowth)
	}
	if sig.StageShare < th.RunawayStageShareMin || bestStage < 0 {
		// Signature still fires (throughput frozen + CPU climbing) even when
		// localization is inconclusive; the caller degrades gracefully by not
		// finding a corroborating estimate-family node.
		return sig
	}
	lastFrame := sorted[len(sorted)-1]
	sig.SuspectNodeId = pickSuspectNode(lastFrame, bestStage)
	return sig
}

// pickSuspectNode names the LOW-confidence/astronomical-estimate/NaN-NDV plan
// node inside the runaway stage, cross-referencing planStatsAndCosts against the
// stage's own plan fragment (available even for a RUNNING stage, §5.6) — no
// operatorSummaries required.
func pickSuspectNode(frame *ParsedFrame, stageNum int) string {
	if frame.Query.Extra == nil || frame.Query.Extra.OutputStage == nil || frame.Query.Extra.PlanStatsAndCosts == nil {
		return ""
	}
	var nodeIds map[string]bool
	for _, s := range frame.Query.Extra.OutputStage.Flatten() {
		if n, ok := NumericStageId(s.StageId); ok && n == stageNum {
			nodeIds = s.PlanNodeIds()
			break
		}
	}
	best, bestOut := "", -1.0
	for id := range nodeIds {
		est := frame.Query.Extra.PlanStatsAndCosts.Stats[id]
		if est == nil || est.Confident != "LOW" {
			continue
		}
		m := frame.Query.NodeMetrics[id]
		joinKeyBad := nodeHasNaNJoinKeyNDV(id, frame.Query.PlanNodes, frame.Query.Extra.PlanStatsAndCosts.Stats)
		if !estimateBad(m, joinKeyBad) {
			continue
		}
		out := float64(est.OutputRowCount)
		if math.IsNaN(out) {
			out = math.MaxFloat64 // NaN/unbounded estimate is itself worst-case
		}
		if out > bestOut {
			bestOut, best = out, id
		}
	}
	return best
}

// SeriesPoint is one query-level progression sample rendered in the report.
type SeriesPoint struct {
	Seq             int     `json:"seq"`
	TimeSec         float64 `json:"timeSec"`
	OutputRows      int64   `json:"outputRows"`
	CpuSec          float64 `json:"cpuSec"`
	UserMemoryBytes float64 `json:"userMemoryBytes"`
}

// Progression is the FP-13 snapshot progression analysis result.
type Progression struct {
	FrameCount          int                `json:"frameCount"`
	Warnings            []string           `json:"warnings,omitempty"`
	Series              []SeriesPoint      `json:"series,omitempty"`
	Runaway             *RunawaySignal     `json:"runaway,omitempty"`
	EmergenceByCategory map[string]float64 `json:"emergenceByCategory,omitempty"`
	TemporalConflicts   []string           `json:"temporalConflicts,omitempty"`
	Insufficient        bool               `json:"insufficientFrames,omitempty"`
}

// AnalyzeProgression computes the full §5.6 progression section from already
// parsed frames.
func AnalyzeProgression(frames []*ParsedFrame, warnings []string, th *Thresholds) *Progression {
	p := &Progression{FrameCount: len(frames), Warnings: warnings}
	if len(frames) == 0 {
		return p
	}
	sorted := append([]*ParsedFrame{}, frames...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].TimestampMs < sorted[j].TimestampMs })
	if len(sorted) < 2 {
		p.Insufficient = true
	}
	t0 := sorted[0].TimestampMs
	for _, f := range sorted {
		outputRows := int64(0)
		if f.Query.Info != nil && f.Query.Info.QueryStats != nil {
			outputRows = f.Query.Info.QueryStats.OutputPositions
		}
		p.Series = append(p.Series, SeriesPoint{
			Seq:             f.Seq,
			TimeSec:         float64(f.TimestampMs-t0) / 1000.0,
			OutputRows:      outputRows,
			CpuSec:          f.Signals.CpuSec,
			UserMemoryBytes: f.Signals.PeakUserMemoryBytes,
		})
	}
	p.Runaway = DetectRunaway(sorted, th)

	p.EmergenceByCategory = map[string]float64{}
	for _, f := range sorted {
		f.Signals.Runaway = p.Runaway
		for _, cat := range Classify(f.Signals, th) {
			if cat.NA {
				continue
			}
			elevated := cat.Severity >= 2
			if cat.Category == CatPlanPath {
				// Round-3 fix (§5.6/§5.4.4, Issue 2): A3 emergence uses
				// live-computable signals ONLY -- cat.Severity above may already
				// include the terminal-only measured-actual family (this frame
				// could be the last live frame, or a mid-run-finished stage),
				// which must never set A3's emergence time. A category with no
				// live-computable signal at all is naturally excluded from the
				// temporal check below: it simply never gets an entry in this map.
				elevated = a3LiveEmergenceDetected(f.Signals)
			}
			if !elevated {
				continue
			}
			if _, seen := p.EmergenceByCategory[cat.Category]; !seen {
				p.EmergenceByCategory[cat.Category] = float64(f.TimestampMs-t0) / 1000.0
			}
		}
	}
	return p
}

// CheckTemporalConsistency implements §5.4.4: the root's emergence must be <=
// the emergence of every category attributed to it. When a symptom
// demonstrably precedes the claimed root, that is reported as an explicit
// temporal-conflict finding rather than silently accepted.
func CheckTemporalConsistency(root *CategoryResult, attributed []*CategoryResult, emergence map[string]float64) (consistent bool, conflicts []string) {
	consistent = true
	if root == nil || emergence == nil {
		return true, nil
	}
	rootT, ok := emergence[root.Category]
	if !ok {
		return true, nil
	}
	for _, sym := range attributed {
		t, ok := emergence[sym.Category]
		if !ok {
			continue
		}
		if t < rootT {
			consistent = false
			conflicts = append(conflicts, fmt.Sprintf(
				"%s (%s) emerged at ~t=%.0fs, BEFORE the claimed root %s (%s) at ~t=%.0fs",
				sym.Category, CategoryNames[sym.Category], t, root.Category, CategoryNames[root.Category], rootT))
		}
	}
	return consistent, conflicts
}
