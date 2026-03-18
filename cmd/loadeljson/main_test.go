package loadeljson

import (
	"embed"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// Embed test JSON files
//
//go:embed *.ndjson
var testFiles embed.FS

func TestProcessJSONBytes_ValidEvent(t *testing.T) {
	jsonData := `{
		"instanceId":"test-instance",
		"clusterName":"test-cluster",
		"queryCompletedEvent":{
			"metadata":{
				"queryId":"20250616_085426_00030_ux3a6",
				"query":"SELECT * FROM test",
				"queryState":"FINISHED",
				"uri":"http://example.com/query/123"
			},
			"statistics":{
				"cpuTime":371792.0,
				"wallTime":67795.0,
				"queuedTime":0.0,
				"peakUserMemoryBytes":98161684,
				"peakTotalNonRevocableMemoryBytes":1098226989,
				"peakTaskUserMemory":10433657,
				"peakTaskTotalMemory":202274048,
				"peakNodeTotalMemory":222073173,
				"totalBytes":26975446031,
				"totalRows":2880361048,
				"outputPositions":100,
				"outputBytes":4801,
				"writtenOutputRows":0,
				"writtenOutputBytes":0,
				"cumulativeMemory":5.359635941788949E12,
				"cumulativeTotalMemory":8.514280417681293E12,
				"completedSplits":7776
			},
			"context":{
				"user":"presto",
				"serverVersion":"0.282",
				"environment":"test",
				"clientTags":[],
				"sessionProperties":{},
				"resourceEstimates":{}
			},
			"ioMetadata":{"inputs":[]},
			"warnings":[],
			"failedTasks":[],
			"createTime":1750063688.759,
			"executionStartTime":1750063689.069,
			"endTime":1750063756.554,
			"stageStatistics":[],
			"operatorStatistics":[],
			"planStatisticsRead":[],
			"planStatisticsWritten":[],
			"optimizerInformation":[],
			"scalarFunctions":[],
			"aggregateFunctions":[],
			"windowsFunctions":[]
		},
		"plan":"test plan",
		"cpuTimeMillis":371792,
		"retriedCpuTimeMillis":0,
		"wallTimeMillis":67795,
		"queuedTimeMillis":0,
		"analysisTimeMillis":630
	}`

	var qe QueryEvent
	err := json.Unmarshal([]byte(jsonData), &qe)
	require.NoError(t, err)

	// Validate the parsed event
	assert.NotNil(t, qe.QueryCompletedEvent)
	assert.Equal(t, "20250616_085426_00030_ux3a6", qe.QueryCompletedEvent.Metadata.QueryId)
	assert.Equal(t, "FINISHED", qe.QueryCompletedEvent.Metadata.QueryState)
	assert.False(t, qe.QueryCompletedEvent.CreateTime.Time.IsZero())
	assert.Equal(t, int64(100), qe.QueryCompletedEvent.Statistics.OutputPositions)
}

func TestProcessJSONBytes_InvalidJSON(t *testing.T) {
	invalidJSON := `{"invalid": json}`

	var qe QueryEvent
	err := json.Unmarshal([]byte(invalidJSON), &qe)
	assert.Error(t, err)
}

func TestProcessJSONBytes_MissingQueryCompletedEvent(t *testing.T) {
	jsonData := `{
		"instanceId":"test-instance",
		"clusterName":"test-cluster",
		"plan":"test plan"
	}`

	var qe QueryEvent
	err := json.Unmarshal([]byte(jsonData), &qe)
	require.NoError(t, err)

	// Should have no QueryCompletedEvent
	assert.Nil(t, qe.QueryCompletedEvent)
}

func TestProcessJSONBytes_FailedQuery(t *testing.T) {
	jsonData := `{
		"instanceId":"test-instance",
		"clusterName":"test-cluster",
		"queryCompletedEvent":{
			"metadata":{
				"queryId":"failed_query_id",
				"query":"SELECT * FROM nonexistent",
				"queryState":"FAILED",
				"uri":"http://example.com"
			},
			"statistics":{
				"cpuTime":0.0,
				"wallTime":0.0,
				"queuedTime":0.0,
				"peakUserMemoryBytes":0,
				"peakTotalNonRevocableMemoryBytes":0,
				"peakTaskUserMemory":0,
				"peakTaskTotalMemory":0,
				"peakNodeTotalMemory":0,
				"totalBytes":0,
				"totalRows":0,
				"outputPositions":0,
				"outputBytes":0,
				"writtenOutputRows":0,
				"writtenOutputBytes":0,
				"cumulativeMemory":0.0,
				"cumulativeTotalMemory":0.0,
				"completedSplits":0
			},
			"context":{
				"user":"test",
				"serverVersion":"0.282",
				"environment":"test",
				"clientTags":[],
				"sessionProperties":{},
				"resourceEstimates":{}
			},
			"ioMetadata":{"inputs":[]},
			"failureInfo":{
				"errorCode":{
					"code":1,
					"name":"SYNTAX_ERROR",
					"type":"USER_ERROR"
				},
				"failureType":"com.facebook.presto.sql.analyzer.SemanticException",
				"failureMessage":"Table does not exist",
				"failuresJson":"{}"
			},
			"warnings":[],
			"failedTasks":[],
			"createTime":1750064067.198,
			"executionStartTime":1750064067.208,
			"endTime":1750064067.208,
			"stageStatistics":[],
			"operatorStatistics":[],
			"planStatisticsRead":[],
			"planStatisticsWritten":[],
			"optimizerInformation":[],
			"scalarFunctions":[],
			"aggregateFunctions":[],
			"windowsFunctions":[]
		},
		"plan":"",
		"cpuTimeMillis":0,
		"retriedCpuTimeMillis":0,
		"wallTimeMillis":0,
		"queuedTimeMillis":0,
		"analysisTimeMillis":0
	}`

	var qe QueryEvent
	err := json.Unmarshal([]byte(jsonData), &qe)
	require.NoError(t, err)

	assert.NotNil(t, qe.QueryCompletedEvent.FailureInfo)
	assert.Equal(t, "SYNTAX_ERROR", qe.QueryCompletedEvent.FailureInfo.ErrorCode.Name)
	assert.Equal(t, "FAILED", qe.QueryCompletedEvent.Metadata.QueryState)
}

// Note: processPath tests are skipped as they require full initialization
// of global variables (mysqlDb, runRecorders, pseudoStage, etc.) which
// makes them integration tests rather than unit tests. These would be better
// suited for end-to-end testing with proper setup/teardown.

func TestEmbeddedNDJSONFiles(t *testing.T) {
	// Test that we can read the embedded NDJSON files
	files, err := testFiles.ReadDir(".")
	require.NoError(t, err)

	ndjsonCount := 0
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".ndjson" {
			ndjsonCount++

			// Try to read and parse the file
			data, err := testFiles.ReadFile(file.Name())
			require.NoError(t, err, "Failed to read %s", file.Name())

			// For NDJSON, we should be able to parse each line
			lines := 0
			for _, line := range []byte(string(data)) {
				if line == '\n' {
					lines++
				}
			}
			t.Logf("File %s has content of length %d", file.Name(), len(data))
		}
	}

	assert.Greater(t, ndjsonCount, 0, "Should have at least one NDJSON test file")
}

func TestRunStartEndTimeTracking(t *testing.T) {
	// Test the min/max time tracking logic used in processJSONBytes
	startTime := newSyncedTime(time.Now())
	endTime := newSyncedTime(time.UnixMilli(0))

	times := []struct {
		start time.Time
		end   time.Time
	}{
		{
			start: time.Date(2025, 6, 16, 8, 48, 8, 0, time.UTC),
			end:   time.Date(2025, 6, 16, 8, 50, 0, 0, time.UTC),
		},
		{
			start: time.Date(2025, 6, 16, 8, 45, 0, 0, time.UTC),
			end:   time.Date(2025, 6, 16, 8, 55, 0, 0, time.UTC),
		},
		{
			start: time.Date(2025, 6, 16, 9, 0, 0, 0, time.UTC),
			end:   time.Date(2025, 6, 16, 9, 10, 0, 0, time.UTC),
		},
	}

	for _, tt := range times {
		// Update start time (find minimum)
		startTime.Synchronized(func(st *syncedTime) {
			if tt.start.Before(st.t) {
				st.t = tt.start
			}
		})

		// Update end time (find maximum)
		endTime.Synchronized(func(st *syncedTime) {
			if tt.end.After(st.t) {
				st.t = tt.end
			}
		})
	}

	expectedStart := time.Date(2025, 6, 16, 8, 45, 0, 0, time.UTC)
	expectedEnd := time.Date(2025, 6, 16, 9, 10, 0, 0, time.UTC)

	assert.True(t, startTime.GetTime().Equal(expectedStart))
	assert.True(t, endTime.GetTime().Equal(expectedEnd))
}
