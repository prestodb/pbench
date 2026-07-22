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

	"github.com/stretchr/testify/require"
)

// marshal is a small test helper: fatal on error, so tests read cleanly.
func marshal(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	require.NoError(t, err)
	return b
}

// baseQuery returns a minimal, valid, generic query JSON document (as a Go
// map so individual tests can override/extend any field) with every category
// at its "not elevated" baseline. Nothing in this baseline is query-specific:
// every synthetic test builds on top of it by injecting exactly the signal(s)
// under test, so the taxonomy/causal engine is exercised generically rather
// than against any one real sample.
func baseQuery(queryId string) map[string]any {
	return map[string]any{
		"queryId": queryId,
		"state":   "FINISHED",
		"queryStats": map[string]any{
			"createTime":                  "2026-01-01T00:00:00.000Z",
			"totalPlanningTime":           1000.0, // ms
			"analysisTime":                0.0,
			"queuedTime":                  0.0,
			"resourceWaitingTime":         0.0,
			"waitingForPrerequisitesTime": 0.0,
			"dispatchingTime":             0.0,
			"elapsedTime":                 100000.0, // 100s
			"executionTime":               99000.0,
			"totalCpuTime":                10000.0, // 10s
			"totalScheduledTime":          20000.0, // 20s
			"totalBlockedTime":            0.0,
			"fullyBlocked":                false,
			"blockedReasons":              []any{},
			"rawInputPositions":           1000000,
			"rawInputDataSize":            float64(1000000),
			"outputPositions":             1000,
			"outputDataSize":              float64(1000),
			"processedInputPositions":     1000000,
			"processedInputDataSize":      float64(1000000),
			"shuffledDataSize":            float64(0),
			"shuffledPositions":           0,
			"peakUserMemoryReservation":   float64(100000),
			"totalDrivers":                1,
			"totalSplits":                 100,
			"queuedSplits":                0,
			"peakRunningTasks":            1,
			"runningDrivers":              0,
			"blockedDrivers":              0,
			"completedDrivers":            1,
			"finishingTime":               0.0,
			"progressPercentage":          100.0,
			"stageGcStatistics":           []any{},
			"operatorSummaries":           []any{},
			"runtimeStats":                map[string]any{},
		},
		"outputStage": map[string]any{
			"stageId": queryId + ".0",
			"plan": map[string]any{
				"jsonRepresentation": `{"id":"0","name":"Output","identifier":"","children":[]}`,
			},
			"latestAttemptExecutionInfo": map[string]any{
				"state": "FINISHED",
				"tasks": []any{},
			},
			"subStages": []any{},
		},
		"planStatsAndCosts": map[string]any{
			"stats": map[string]any{},
			"costs": map[string]any{},
		},
		"optimizerInformation": []any{},
	}
}

func operatorSummary(stageId int, planNodeId, opType string, in, out int64) map[string]any {
	return map[string]any{
		"stageId":                 stageId,
		"stageExecutionId":        0,
		"pipelineId":              0,
		"operatorId":              0,
		"planNodeId":              planNodeId,
		"operatorType":            opType,
		"totalDrivers":            1,
		"addInputCalls":           1,
		"addInputWall":            "0.00ns",
		"addInputCpu":             "0.00ns",
		"rawInputDataSizeInBytes": 0,
		"rawInputPositions":       0,
		"inputDataSizeInBytes":    0,
		"inputPositions":          in,
		"getOutputCalls":          1,
		"getOutputWall":           "0.00ns",
		"getOutputCpu":            "0.00ns",
		"outputDataSizeInBytes":   0,
		"outputPositions":         out,
		"blockedWall":             "0.00ns",
		"finishCalls":             1,
		"finishWall":              "0.00ns",
		"finishCpu":               "0.00ns",
		"spilledDataSizeInBytes":  0,
	}
}

func nodeEstimate(outputRowCount any, confident string, ndv map[string]any) map[string]any {
	return map[string]any{
		"outputRowCount":     outputRowCount,
		"totalSize":          "NaN",
		"confident":          confident,
		"variableStatistics": ndv,
	}
}

func lowConfNoNdv() map[string]any {
	return map[string]any{
		"k1": map[string]any{"lowValue": "-Infinity", "highValue": "Infinity", "nullsFraction": 0.0, "averageRowSize": "NaN", "distinctValuesCount": "NaN"},
	}
}
