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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/prestodb/presto-go-client/v2/queryjson"
	"pbench/prestoapi/plan_node"
)

// TestComputeNodeMetricsJoinFanout covers UT-11: fan-out = max(output)/input of
// the operator that produced that max output, with a build-side operator (0
// output) sharing the same node id -- the probe+build join case.
func TestComputeNodeMetricsJoinFanout(t *testing.T) {
	ops := []*queryjson.OperatorSummary{
		{PlanNodeId: "10", OperatorType: "LookupJoinOperator", InputPositions: 100, OutputPositions: 5000},
		{PlanNodeId: "10", OperatorType: "HashBuilderOperator", InputPositions: 20, OutputPositions: 0},
	}
	extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
		"10": {Confident: "LOW"},
	}}}
	metrics := ComputeNodeMetrics(extra, ops)
	m := metrics["10"]
	require.NotNil(t, m)
	require.NotNil(t, m.ActualOut)
	require.NotNil(t, m.ActualIn)
	assert.EqualValues(t, 5000, *m.ActualOut)
	assert.EqualValues(t, 100, *m.ActualIn, "input must come from the operator that produced the max output, not summed across probe+build")
	require.NotNil(t, m.ActualFanout)
	assert.InEpsilon(t, 50.0, *m.ActualFanout, 1e-9)
}

// TestComputeNodeMetricsPassThroughNode covers the non-join case verified
// against real data: two operators (e.g. FilterProject + PartitionedOutput)
// sharing one node id with identical in==out must report fanout 1.0, not 0.5
// (which a naive sum(input) formula would produce).
func TestComputeNodeMetricsPassThroughNode(t *testing.T) {
	ops := []*queryjson.OperatorSummary{
		{PlanNodeId: "20", OperatorType: "FilterProject", InputPositions: 300, OutputPositions: 300},
		{PlanNodeId: "20", OperatorType: "PartitionedOutput", InputPositions: 300, OutputPositions: 300},
	}
	metrics := ComputeNodeMetrics(nil, ops)
	m := metrics["20"]
	require.NotNil(t, m)
	require.NotNil(t, m.ActualFanout)
	assert.InEpsilon(t, 1.0, *m.ActualFanout, 1e-9)
}

// TestComputeNodeMetricsEstimateOnly covers graceful degradation when the
// actual is absent (a live RUNNING-stage frame with empty operatorSummaries for
// that node, §2): actual-family metrics must be nil/"unavailable", while
// estimate-family metrics (confidence, NDV, join-stats) are still produced.
func TestComputeNodeMetricsEstimateOnly(t *testing.T) {
	nan := plan_node.JsonFloat64(nanValue())
	extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
		"30": {
			Confident:          "LOW",
			OutputRowCount:     plan_node.JsonFloat64(1e20),
			VariableStatistics: lowConfNoNdvTyped(),
			JoinNodeStatsEstimate: &JoinNodeStatsEstimate{
				NullJoinBuildKeyCount: nan, JoinBuildKeyCount: nan,
				NullJoinProbeKeyCount: nan, JoinProbeKeyCount: nan,
			},
		},
	}}}
	metrics := ComputeNodeMetrics(extra, nil) // no operator summaries at all (RUNNING stage)
	m := metrics["30"]
	require.NotNil(t, m)
	assert.Nil(t, m.ActualIn, "actual-family metrics must be unavailable, not zero")
	assert.Nil(t, m.ActualOut)
	assert.Nil(t, m.ActualFanout)
	assert.Nil(t, m.EstimateErrorRatio)
	assert.True(t, m.IsLowConfidence)
	assert.Nil(t, m.MinVariableNDV, "all-NaN NDV must be reported as missing, not a numeric value")
	assert.True(t, m.HasJoinStats)
	assert.True(t, m.AllNaNJoinStats)
}

func lowConfNoNdvTyped() map[string]plan_node.VariableStatistics {
	nan := plan_node.JsonFloat64(nanValue())
	return map[string]plan_node.VariableStatistics{
		"k1": {DistinctValuesCount: nan},
	}
}

func nanValue() float64 {
	var zero float64
	return zero / zero
}

// TestComputeNodeMetricsEstimateErrorRatio covers estimate_error_ratio
// exactness and the "estimate absent/zero -> nil" degradation.
func TestComputeNodeMetricsEstimateErrorRatio(t *testing.T) {
	ops := []*queryjson.OperatorSummary{
		{PlanNodeId: "40", OperatorType: "Agg", InputPositions: 10, OutputPositions: 200},
	}
	extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
		"40": {Confident: "HIGH", OutputRowCount: 100},
	}}}
	metrics := ComputeNodeMetrics(extra, ops)
	m := metrics["40"]
	require.NotNil(t, m.EstimateErrorRatio)
	assert.InEpsilon(t, 2.0, *m.EstimateErrorRatio, 1e-9)
	assert.False(t, m.IsLowConfidence)

	// No estimate at all for a node that has an actual -> nil ratio.
	ops2 := []*queryjson.OperatorSummary{
		{PlanNodeId: "41", OperatorType: "Agg", InputPositions: 10, OutputPositions: 200},
	}
	metrics2 := ComputeNodeMetrics(nil, ops2)
	assert.Nil(t, metrics2["41"].EstimateErrorRatio)
}

// TestComputeNodeMetricsUnionsIds covers the §6.3 grain: nodes present only in
// plan estimates (never executed) get nil actuals; nodes with actuals but no
// estimate get nil estimate fields; both are still present in the result.
func TestComputeNodeMetricsUnionsIds(t *testing.T) {
	ops := []*queryjson.OperatorSummary{
		{PlanNodeId: "actual-only", OperatorType: "TableScan", InputPositions: 5, OutputPositions: 5},
	}
	extra := &ExtraInfo{PlanStatsAndCosts: &PlanStatsAndCosts{Stats: map[string]*NodeEstimate{
		"estimate-only": {Confident: "FACT", OutputRowCount: 10},
	}}}
	metrics := ComputeNodeMetrics(extra, ops)
	require.Contains(t, metrics, "actual-only")
	require.Contains(t, metrics, "estimate-only")
	assert.Nil(t, metrics["actual-only"].EstimatedOut)
	assert.Nil(t, metrics["estimate-only"].ActualOut)
}
