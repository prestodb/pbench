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
	"math"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"pbench/prestoapi/plan_node"
)

// TestExtraInfoMixedNaNForms covers UT-9: planStatsAndCosts with mixed sibling
// forms (bare E-notation number next to quoted "NaN"/"Infinity" strings) in one
// node, all-NaN joinNodeStatsEstimate recognized as the "no join-key stats"
// signal, optimizerInformation, and absent fields degrading to nil (no error).
func TestExtraInfoMixedNaNForms(t *testing.T) {
	raw := []byte(`{
		"queryId": "q1", "state": "FINISHED",
		"planStatsAndCosts": {
			"stats": {
				"42": {
					"outputRowCount": 2.7948982450503094E+22,
					"totalSize": "NaN",
					"confident": "LOW",
					"variableStatistics": {
						"a": {"lowValue": "-Infinity", "highValue": "Infinity", "nullsFraction": 0.0, "averageRowSize": "NaN", "distinctValuesCount": "NaN"},
						"b": {"lowValue": 1.0, "highValue": 2.0, "nullsFraction": 0.0, "averageRowSize": 4.5, "distinctValuesCount": 56.0}
					},
					"joinNodeStatsEstimate": {
						"nullJoinBuildKeyCount": "NaN", "joinBuildKeyCount": "NaN",
						"nullJoinProbeKeyCount": "NaN", "joinProbeKeyCount": "NaN"
					}
				}
			}
		},
		"optimizerInformation": [
			{"optimizerName": "PruneJoinColumns", "optimizerTriggered": true, "isCostBased": false}
		]
	}`)
	extra := new(ExtraInfo)
	require.NoError(t, json.Unmarshal(raw, extra))
	require.NotNil(t, extra.PlanStatsAndCosts)
	node := extra.PlanStatsAndCosts.Stats["42"]
	require.NotNil(t, node)
	assert.InEpsilon(t, 2.7948982450503094e+22, float64(node.OutputRowCount), 1e-9)
	assert.True(t, math.IsNaN(float64(node.TotalSize)))
	assert.Equal(t, "LOW", node.Confident)
	assert.True(t, math.IsNaN(float64(node.VariableStatistics["a"].DistinctValuesCount)))
	assert.Equal(t, 56.0, float64(node.VariableStatistics["b"].DistinctValuesCount))
	require.NotNil(t, node.JoinNodeStatsEstimate)
	assert.True(t, node.JoinNodeStatsEstimate.AllNaN())

	require.Len(t, extra.OptimizerInformation, 1)
	assert.Equal(t, "PruneJoinColumns", extra.OptimizerInformation[0].OptimizerName)
	assert.True(t, extra.OptimizerInformation[0].OptimizerTriggered)

	minNdv, found := MinDistinctValuesCount(node.VariableStatistics)
	require.True(t, found)
	assert.Equal(t, 56.0, minNdv)

	// Absent fields degrade to nil, no error.
	assert.Nil(t, extra.QueryStats)
	assert.Nil(t, extra.OutputStage)
}

// TestJoinNodeStatsEstimateNotAllNaN ensures AllNaN is false when at least one
// field is a real number (i.e. the join DOES have key statistics).
func TestJoinNodeStatsEstimateNotAllNaN(t *testing.T) {
	nan := plan_node.JsonFloat64(math.NaN())
	j := &JoinNodeStatsEstimate{
		NullJoinBuildKeyCount: nan,
		JoinBuildKeyCount:     0,
		NullJoinProbeKeyCount: nan,
		JoinProbeKeyCount:     nan,
	}
	assert.False(t, j.AllNaN())
	assert.False(t, (*JoinNodeStatsEstimate)(nil).AllNaN())
}

// TestStageGcStatNullVsZero covers the §2/§5.4.1 D1 null-vs-zero distinction:
// a stage-level GC entry with no GC fields at all reports "no data", not zero,
// while a task-level numeric 0 is a real zero.
func TestStageGcStatNullVsZero(t *testing.T) {
	raw := []byte(`{"stageId": 3, "tasks": 4}`) // no GC fields at all -> null-like
	g := new(StageGcStat)
	require.NoError(t, json.Unmarshal(raw, g))
	assert.False(t, g.HasGcData())
	_, ok := g.FullGcTimeSeconds()
	assert.False(t, ok)

	raw2 := []byte(`{"stageId": 3, "fullGcTasks": 0, "totalFullGcSec": 0}`)
	g2 := new(StageGcStat)
	require.NoError(t, json.Unmarshal(raw2, g2))
	assert.True(t, g2.HasGcData())
	secs, ok := g2.FullGcTimeSeconds()
	assert.True(t, ok)
	assert.Equal(t, 0.0, secs)

	// Legacy field-name shape (fullGcCount/fullGcTimeInMillis).
	raw3 := []byte(`{"fullGcCount": 2, "fullGcTimeInMillis": 500}`)
	g3 := new(StageGcStat)
	require.NoError(t, json.Unmarshal(raw3, g3))
	assert.True(t, g3.HasGcData())
	secs3, ok3 := g3.FullGcTimeSeconds()
	assert.True(t, ok3)
	assert.Equal(t, 0.5, secs3)

	var nilStat *StageGcStat
	assert.False(t, nilStat.HasGcData())
	_, ok4 := nilStat.FullGcTimeSeconds()
	assert.False(t, ok4)
}

// TestTaskStatsNumericFields covers the numeric *InNanos/*InBytes task fields
// (as opposed to queryStats' human-readable string aliases), and the
// task-level GC counters being numeric (never null) at that level.
func TestTaskStatsNumericFields(t *testing.T) {
	raw := []byte(`{
		"totalScheduledTimeInNanos": 1000000000,
		"totalCpuTimeInNanos": 500000000,
		"totalBlockedTimeInNanos": 0,
		"rawInputDataSizeInBytes": 12345,
		"rawInputPositions": 100,
		"processedInputPositions": 100,
		"fullGcCount": 0,
		"fullGcTimeInMillis": 0
	}`)
	ts := new(TaskStats)
	require.NoError(t, json.Unmarshal(raw, ts))
	assert.EqualValues(t, 1000000000, ts.TotalScheduledTimeInNanos)
	assert.EqualValues(t, 500000000, ts.TotalCpuTimeInNanos)
	require.NotNil(t, ts.FullGcCount)
	assert.EqualValues(t, 0, *ts.FullGcCount)
}

// TestNumericStageId covers stage-id prefix stripping (matching
// queryjson.StageInfo.processForInsert's convention).
func TestNumericStageId(t *testing.T) {
	n, ok := NumericStageId("20260718_003937_00077_muisn.9")
	require.True(t, ok)
	assert.Equal(t, 9, n)

	_, ok = NumericStageId("not-a-stage-id")
	assert.False(t, ok)

	_, ok = NumericStageId("")
	assert.False(t, ok)
}

// TestStageNodePlanNodeIdsSkipsRemoteSource ensures PlanNodeIds() collects ids
// from the stage's own fragment but does not descend into remote-source
// children (which belong to other stages).
func TestStageNodePlanNodeIdsSkipsRemoteSource(t *testing.T) {
	s := &StageNode{
		Plan: &StagePlan{JsonRepresentation: `{
			"id": "1", "name": "Project", "identifier": "",
			"children": [
				{"id": "2", "name": "RemoteSource", "identifier": "", "remoteSources": ["9"]},
				{"id": "3", "name": "TableScan", "identifier": "", "children": []}
			]
		}`},
	}
	ids := s.PlanNodeIds()
	assert.True(t, ids["1"])
	assert.True(t, ids["3"])
	assert.False(t, ids["2"], "RemoteSource node itself should be excluded")
}

// TestStripEstimatesToleratesStringConfident covers the real native-engine
// plan-JSON quirk: an embedded estimates[].confident STRING enum, which is
// incompatible with plan_node.PlanEstimate.Confident (bool) and would
// otherwise break json.Unmarshal partway through the tree.
func TestStripEstimatesToleratesStringConfident(t *testing.T) {
	raw := []byte(`{
		"id": "1", "name": "LeftJoin", "identifier": "",
		"estimates": [{"outputRowCount": 5.0, "totalSize": "NaN", "confident": "LOW", "variableStatistics": {}}],
		"children": [
			{"id": "2", "name": "TableScan", "identifier": "", "estimates": [{"outputRowCount": 5.0, "totalSize": "NaN", "confident": "HIGH", "variableStatistics": {}}], "children": []}
		]
	}`)
	stripped := StripEstimates(raw)
	s := &StageNode{Plan: &StagePlan{JsonRepresentation: string(stripped)}}
	ids := s.PlanNodeIds()
	assert.True(t, ids["1"])
	assert.True(t, ids["2"])
}
