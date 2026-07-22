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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestParseRuntimeStatsKeyFormA covers key form (a): operator-level, unprefixed,
// "<Op>.<node>[.<seq>].<metric>".
func TestParseRuntimeStatsKeyFormA(t *testing.T) {
	p := ParseRuntimeStatsKey("HashProbe.1772.queuedWallNanos")
	assert.False(t, p.HasStage)
	assert.Equal(t, "HashProbe", p.Operator)
	assert.Equal(t, "1772", p.NodeId)
	assert.Equal(t, "queuedWallNanos", p.Metric)
	assert.False(t, p.TaskOnly)

	p2 := ParseRuntimeStatsKey("FilterProject.3188.0.runningAddInputWallNanos")
	assert.Equal(t, "FilterProject", p2.Operator)
	assert.Equal(t, "3188.0", p2.NodeId)
	assert.Equal(t, "runningAddInputWallNanos", p2.Metric)
}

// TestParseRuntimeStatsKeyFormB covers key form (b): S<n>- stage-prefixed,
// both bare task-level (no operator/node) and operator-scoped with the literal
// ".root." infix dropped.
func TestParseRuntimeStatsKeyFormB(t *testing.T) {
	p := ParseRuntimeStatsKey("S9-taskScheduledTimeNanos")
	assert.True(t, p.HasStage)
	assert.Equal(t, 9, p.StageNum)
	assert.True(t, p.TaskOnly)
	assert.Equal(t, "", p.Operator)
	assert.Equal(t, "", p.NodeId)
	assert.Equal(t, "taskScheduledTimeNanos", p.Metric)

	p2 := ParseRuntimeStatsKey("S9-drivers.running")
	assert.True(t, p2.HasStage)
	assert.True(t, p2.TaskOnly)
	assert.Equal(t, "drivers.running", p2.Metric)

	p3 := ParseRuntimeStatsKey("S10-PartitionedOutput.root.1261.driverCpuTimeNanos")
	assert.True(t, p3.HasStage)
	assert.Equal(t, 10, p3.StageNum)
	assert.Equal(t, "PartitionedOutput", p3.Operator)
	assert.Equal(t, "1261", p3.NodeId, "literal 'root' segment must be dropped, not treated as part of the node id")
	assert.Equal(t, "driverCpuTimeNanos", p3.Metric)

	p4 := ParseRuntimeStatsKey("S0-Exchange.2802.blockedWaitForProducerWallNanos")
	assert.True(t, p4.HasStage)
	assert.Equal(t, 0, p4.StageNum)
	assert.Equal(t, "Exchange", p4.Operator)
	assert.Equal(t, "2802", p4.NodeId)
	assert.Equal(t, "blockedWaitForProducerWallNanos", p4.Metric)
}

// TestParseRuntimeStatsKeyFormC covers key form (c): un-prefixed query-level
// planning metrics and Java operator keys (used as-is, no stage/operator/node
// scope inferred from a bare, dot-less name).
func TestParseRuntimeStatsKeyFormC(t *testing.T) {
	p := ParseRuntimeStatsKey("optimizerTimeNanos")
	assert.False(t, p.HasStage)
	assert.Equal(t, "", p.Operator)
	assert.Equal(t, "", p.NodeId)
	assert.Equal(t, "optimizerTimeNanos", p.Metric)

	p2 := ParseRuntimeStatsKey("logicalPlannerTimeNanos")
	assert.Equal(t, "logicalPlannerTimeNanos", p2.Metric)
}

// TestDetectNativeRuntime covers native-vs-Java inference (§5.4.1 D1,
// corrected discriminator — see DetectNativeRuntime's doc comment): a
// hierarchical Operator.numericNodeId runtimeStats key indicates native; a
// Java-style fixture with no such key must NOT be detected as native
// (guardrail: engine-agnostic, both forms exercised).
func TestDetectNativeRuntime(t *testing.T) {
	native := map[string]*RuntimeMetric{
		"S9-taskScheduledTimeNanos":      {Name: "S9-taskScheduledTimeNanos", Sum: 100},
		"HashProbe.1772.queuedWallNanos": {Sum: 5},
	}
	assert.True(t, DetectNativeRuntime(native))

	javaOnly := map[string]*RuntimeMetric{
		"optimizerTimeNanos":            {Sum: 100},
		"logicalPlannerTimeNanos":       {Sum: 5},
		"HashBuilderOperator.getOutput": {Sum: 1}, // Java-style, no short Velox token as operator
	}
	assert.False(t, DetectNativeRuntime(javaOnly))

	assert.False(t, DetectNativeRuntime(nil))
	assert.False(t, DetectNativeRuntime(map[string]*RuntimeMetric{}))
}

// TestDetectNativeRuntimeJavaSPrefixDoesNotFalsePositive is a regression test
// for a real bug (found against a 191-file real Java-Presto 0.299 corpus,
// see DetectNativeRuntime's doc comment): Java ALSO emits "S<n>-"-prefixed
// query-level runtimeStats keys (schedulerWallTimeNanos, taskUpdateRoundTripTime,
// driverCountPerTask, taskScheduledTimeNanos, ...), so the mere presence of an
// "S<n>-" prefix must NOT be treated as a native marker; only a hierarchical
// Operator.numericNodeId key shape may.
func TestDetectNativeRuntimeJavaSPrefixDoesNotFalsePositive(t *testing.T) {
	javaRealShaped := map[string]*RuntimeMetric{
		"S0-schedulerWallTimeNanos":                 {Sum: 1},
		"S1-taskUpdateRoundTripTime":                {Sum: 1},
		"S0-driverCountPerTask":                     {Sum: 1},
		"S0-taskScheduledTimeNanos":                 {Sum: 1}, // present on Java too; still not a native marker by itself
		"S1-createTime":                             {Sum: 1},
		"systemAccessControl.checkCanAccessCatalog": {Sum: 1},
		"optimizerTimeNanos":                        {Sum: 1},
		"logicalPlannerTimeNanos":                   {Sum: 1},
		"fragmentResultCacheMissCount":              {Sum: 1}, // the one stray non-hierarchical operator counter observed on Java
	}
	assert.False(t, DetectNativeRuntime(javaRealShaped), "S<n>- prefixed and non-hierarchical keys must never be treated as a native marker")

	// The per-stage taskScheduledTimeNanos series itself must still parse
	// (RunawayComputation localization works generically on both engines).
	series := StageSeries(javaRealShaped, "taskScheduledTimeNanos")
	assert.EqualValues(t, 1, series[0])
}

// TestDecodeOperatorRuntimeStats covers decoding one operator's runtimeStats
// blob and summing a specific wait metric across keys.
func TestDecodeOperatorRuntimeStats(t *testing.T) {
	raw := json.RawMessage(`{
		"TableScan.0.waitForPreloadSplitNanos": {"name":"TableScan.0.waitForPreloadSplitNanos","unit":"NANO","sum":1000,"count":1,"max":1000,"min":1000},
		"TableScan.0.ioWaitWallNanos": {"name":"TableScan.0.ioWaitWallNanos","unit":"NANO","sum":2000,"count":1,"max":2000,"min":2000},
		"TableScan.0.numSplits": {"name":"TableScan.0.numSplits","unit":"NONE","sum":5,"count":1,"max":5,"min":5}
	}`)
	decoded := DecodeOperatorRuntimeStats(&raw)
	assert.Len(t, decoded, 3)
	total := SumRuntimeMetrics(decoded, "waitForPreloadSplitNanos", "ioWaitWallNanos")
	assert.EqualValues(t, 3000, total)

	assert.Empty(t, DecodeOperatorRuntimeStats(nil))
	empty := json.RawMessage(``)
	assert.Empty(t, DecodeOperatorRuntimeStats(&empty))
}

// TestStageSeries covers the per-stage query-level rollup series used to
// localize the RunawayComputation signal (§5.6).
func TestStageSeries(t *testing.T) {
	rts := map[string]*RuntimeMetric{
		"S9-taskScheduledTimeNanos":  {Sum: 100},
		"S9-taskScheduledTimeNanos2": {Sum: 999}, // different metric name, must not match
		"S3-taskScheduledTimeNanos":  {Sum: 5},
		"taskScheduledTimeNanos":     {Sum: 42}, // unprefixed: not stage-scoped, ignored
	}
	series := StageSeries(rts, "taskScheduledTimeNanos")
	assert.EqualValues(t, 100, series[9])
	assert.EqualValues(t, 5, series[3])
	assert.NotContains(t, series, -1)
}

// TestHasQuickStatsCounters covers UT-10 (design.md §5.4.1 B4, §10 item 10):
// quick-stats / metastore-latency counter detection is a pure key-existence
// read, robust to a stage/operator prefix ahead of the token.
func TestHasQuickStatsCounters(t *testing.T) {
	cases := []struct {
		name string
		keys map[string]*RuntimeMetric
		want bool
	}{
		{"QuickStatsProvider bare", map[string]*RuntimeMetric{"QuickStatsProvider.getQuickStats": {}}, true},
		{"QuickStatsBuildTimeout bare", map[string]*RuntimeMetric{"QuickStatsBuildTimeoutCount": {}}, true},
		{"ParquetQuickStatsBuilder with FileCount", map[string]*RuntimeMetric{"ParquetQuickStatsBuilder.totalFileCount": {}}, true},
		{"ParquetQuickStatsBuilder without FileCount", map[string]*RuntimeMetric{"ParquetQuickStatsBuilder.buildTimeNanos": {}}, false},
		{"stage-prefixed QuickStatsProvider", map[string]*RuntimeMetric{"S9-QuickStatsProvider.calls": {}}, true},
		{"no quick-stats keys at all", map[string]*RuntimeMetric{"optimizerTimeNanos": {}, "S9-taskScheduledTimeNanos": {}}, false},
		{"nil map", nil, false},
		{"empty map", map[string]*RuntimeMetric{}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			assert.Equal(t, c.want, HasQuickStatsCounters(c.keys))
		})
	}
}

// TestScoreB4QuickStatsAnnotationDoesNotAffectSeverity covers UT-10's
// severity-invariance requirement: the quick-stats annotation changes the
// Note text only; severity (and every other CategoryResult field driving the
// causal engine) must be byte-identical whether or not quick-stats counters
// are present.
func TestScoreB4QuickStatsAnnotationDoesNotAffectSeverity(t *testing.T) {
	th := DefaultThresholds()
	base := baselineSignals()
	base.ScanWaitSec = 60
	base.ScanWallSec = 100
	base.ScheduledSec = 200 // relevance 0.5 >= 0.2; wait share 0.6 >= 0.3 -> elevated

	withoutQuickStats := *base
	withoutQuickStats.QuickStatsDetected = false
	resWithout := scoreB4(&withoutQuickStats, th)

	withQuickStats := *base
	withQuickStats.QuickStatsDetected = true
	resWith := scoreB4(&withQuickStats, th)

	require.Equal(t, 2, resWithout.Severity)
	assert.Equal(t, resWithout.Severity, resWith.Severity, "severity must be identical regardless of quick-stats detection")
	assert.Equal(t, resWithout.SignalFamilies, resWith.SignalFamilies)
	assert.NotContains(t, resWithout.Note, "quick-stats")
	assert.Contains(t, resWith.Note, "quick-stats / metastore latency")

	// Not elevated at all -> annotation must not appear even if quick-stats
	// counters are present (annotation is conditioned on B4 being elevated).
	quiet := baselineSignals()
	quiet.QuickStatsDetected = true
	quietRes := scoreB4(quiet, th)
	assert.Zero(t, quietRes.Severity)
	assert.NotContains(t, quietRes.Note, "quick-stats")
}
