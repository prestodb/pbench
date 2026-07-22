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
	"regexp"
	"strconv"
	"strings"
)

// DecodeOperatorRuntimeStats parses one OperatorSummary's raw RuntimeStats blob
// (form (a), §2: "<Op>.<node>[.<seq>].<metric>", unprefixed) into a metric map.
// Returns an empty map (never nil, never an error) on absent/malformed input —
// runtimeStats decoding is opportunistic, not required for baseline operation.
func DecodeOperatorRuntimeStats(raw *json.RawMessage) map[string]*RuntimeMetric {
	out := map[string]*RuntimeMetric{}
	if raw == nil || len(*raw) == 0 {
		return out
	}
	_ = json.Unmarshal(*raw, &out)
	return out
}

// SumRuntimeMetrics sums the `sum` field of every entry in m whose parsed
// metric-name suffix is in names, regardless of stage/operator/node scope. Used
// to total a specific wait/queue metric across an operator's own runtimeStats.
func SumRuntimeMetrics(m map[string]*RuntimeMetric, names ...string) int64 {
	want := map[string]bool{}
	for _, n := range names {
		want[n] = true
	}
	var total int64
	for key, metric := range m {
		if metric == nil {
			continue
		}
		p := ParseRuntimeStatsKey(key)
		if want[p.Metric] {
			total += metric.Sum
		}
	}
	return total
}

// ParsedRuntimeKey is the result of parsing one runtimeStats key into its scope
// (§2, §5.3 runtime_stats.go): three real forms exist and a naive
// "<Op>.<node>.<metric>" split mis-parses two of them.
type ParsedRuntimeKey struct {
	StageNum int // -1 when the key carries no "S<n>-" prefix
	HasStage bool
	Operator string // "" for stage-scoped task/driver/scheduler metrics and for
	// un-prefixed query-level planning metrics
	NodeId   string // "" when there is no operator/node scope
	Metric   string
	TaskOnly bool // true for bare task*/drivers.*/scheduler* stage-scoped keys
}

var stagePrefixRe = regexp.MustCompile(`^S(\d+)-(.*)$`)

// taskOnlyPrefixes are the bare stage-scoped, task/driver-level metric name
// prefixes that must NOT be parsed as "<Operator>.<node>.<metric>" (§2/§6.1):
// e.g. "S9-taskScheduledTimeNanos", "S9-drivers.running", "S0-schedulerWallTimeNanos".
var taskOnlyPrefixes = []string{"task", "drivers", "scheduler"}

// ParseRuntimeStatsKey parses a runtimeStats key into its scope. Parse order
// (§5.3): (1) strip an optional leading "S<digits>-" stage prefix; (2)
// special-case bare task*/drivers.*/scheduler* remainders as stage-scoped
// metrics with no operator/node id; (3) otherwise apply the operator rule:
// metric = last '.'-separated segment, operator = first segment, node id = the
// middle segments joined with '.' after dropping a literal "root" segment.
// Un-prefixed keys with no '.' at all (Java operators' own metrics, and
// query-level planning metrics like optimizerTimeNanos) are returned as a bare
// metric with no stage/operator/node scope.
func ParseRuntimeStatsKey(key string) ParsedRuntimeKey {
	rest := key
	parsed := ParsedRuntimeKey{StageNum: -1}
	if m := stagePrefixRe.FindStringSubmatch(key); m != nil {
		if n, err := strconv.Atoi(m[1]); err == nil {
			parsed.StageNum = n
			parsed.HasStage = true
		}
		rest = m[2]
	}
	for _, p := range taskOnlyPrefixes {
		if strings.HasPrefix(rest, p) {
			parsed.TaskOnly = true
			parsed.Metric = rest
			return parsed
		}
	}
	segments := strings.Split(rest, ".")
	if len(segments) == 1 {
		// Un-prefixed query-level metric, or a stage-scoped metric we don't
		// otherwise recognize: no operator/node scope, just the bare name.
		parsed.Metric = segments[0]
		return parsed
	}
	parsed.Operator = segments[0]
	middle := segments[1 : len(segments)-1]
	parsed.Metric = segments[len(segments)-1]
	if len(middle) > 0 && middle[0] == "root" {
		middle = middle[1:]
	}
	parsed.NodeId = strings.Join(middle, ".")
	return parsed
}

// DetectNativeRuntime infers whether the query ran on native (Prestissimo/Velox)
// workers directly from the JSON (§5.4.1 D1) — no cluster context needed.
//
// CORRECTED DISCRIMINATOR (deviates from design.md §2/§5.4.1 D1; flagged here
// for design sync). The design documented two native markers that turned out
// to be wrong when checked against a 191-file real Java-Presto (0.299) corpus
// alongside real native/Prestissimo corpora:
//   - The "S<n>-" stage-prefixed runtimeStats key convention is NOT
//     native-only: Java emits it too (e.g. "S0-schedulerWallTimeNanos",
//     "S1-taskUpdateRoundTripTime", "S0-driverCountPerTask", and even
//     "S0-taskScheduledTimeNanos" — the same per-stage scheduled-time series
//     §5.6 uses for RunawayComputation localization, which therefore works
//     generically on both engines with no special-casing needed).
//   - operatorSummaries[].operatorType strings overlap too (both engines
//     report e.g. "TableScanOperator", "LookupJoinOperator",
//     "HashBuilderOperator"), so operator-type names are not a clean
//     discriminator either.
//
// What actually discriminated the two real corpora: a HIERARCHICAL
// "<Operator>.<numericNodeId>[.<seq>].<metric>" key shape (whether S<n>--
// prefixed or bare) appears throughout native runtimeStats (query-level and
// per-operator), while Java's runtimeStats keys are either bare task/scheduler
// counters, "systemAccessControl.<method>" (no numeric id segment), or a
// single non-hierarchical counter on at most one operator (observed:
// "fragmentResultCacheMissCount", no dots at all) — Java operators otherwise
// report an empty runtimeStats blob. Detection therefore looks for at least
// one key whose parsed Operator and NodeId are both non-empty AND the NodeId's
// first segment is purely numeric (a real plan-node id), which the query-level
// and per-operator forms share; the caller passes both.
func DetectNativeRuntime(runtimeStats map[string]*RuntimeMetric) bool {
	for key := range runtimeStats {
		p := ParseRuntimeStatsKey(key)
		if p.Operator != "" && p.NodeId != "" && firstSegmentNumeric(p.NodeId) {
			return true
		}
	}
	return false
}

// firstSegmentNumeric reports whether the segment of nodeId before its first
// '.' (e.g. "3188" in "3188.0", or "1772") consists only of digits — i.e. it
// looks like a real Presto plan-node id, as opposed to a second word in a
// dotted method name like "systemAccessControl.checkCanAccessCatalog".
func firstSegmentNumeric(nodeId string) bool {
	seg := nodeId
	if idx := strings.IndexByte(seg, '.'); idx >= 0 {
		seg = seg[:idx]
	}
	if seg == "" {
		return false
	}
	for _, c := range seg {
		if c < '0' || c > '9' {
			return false
		}
	}
	return true
}

// StageSeries extracts, for a given metric name suffix (e.g.
// "taskScheduledTimeNanos"), the per-stage-number sum from a single frame's
// query-level runtimeStats. Used for the §5.6 per-stage scheduled/CPU-time
// growth series that localizes the RunawayComputation signal without depending
// on operatorSummaries.
func StageSeries(runtimeStats map[string]*RuntimeMetric, metric string) map[int]int64 {
	out := map[int]int64{}
	for key, m := range runtimeStats {
		if m == nil {
			continue
		}
		p := ParseRuntimeStatsKey(key)
		if !p.HasStage {
			continue
		}
		if p.TaskOnly && p.Metric == metric {
			out[p.StageNum] += m.Sum
		} else if !p.TaskOnly && p.NodeId == "" && p.Operator == "" && p.Metric == metric {
			out[p.StageNum] += m.Sum
		}
	}
	return out
}

// HasQuickStatsCounters reports whether the query-level runtimeStats contain
// quick-stats / metastore-latency counters — keys matching `QuickStatsProvider*`
// / `QuickStatsBuildTimeout*` / `ParquetQuickStatsBuilder.*FileCount*` (design.md
// §5.4.1 B4, §10 item 10). This is a key-EXISTENCE read only, used to annotate
// B4's note with a more specific sub-cause when B4 is already elevated; it
// never changes B4's (or any category's) severity. Matching is substring-based
// (not full-key-anchored) so an "S<n>-" stage prefix or an "Op.node." prefix
// ahead of these tokens — a real possibility given the three verified
// runtimeStats key forms, §2 — does not hide a real quick-stats counter.
func HasQuickStatsCounters(runtimeStats map[string]*RuntimeMetric) bool {
	for key := range runtimeStats {
		if strings.Contains(key, "QuickStatsProvider") || strings.Contains(key, "QuickStatsBuildTimeout") {
			return true
		}
		if strings.Contains(key, "ParquetQuickStatsBuilder.") && strings.Contains(key, "FileCount") {
			return true
		}
	}
	return false
}
