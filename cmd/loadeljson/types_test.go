package loadeljson

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestPrestoTime_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected time.Time
		wantErr  bool
	}{
		{
			name:     "RFC3339 format",
			input:    `"2025-06-16T08:48:08Z"`,
			expected: time.Date(2025, 6, 16, 8, 48, 8, 0, time.UTC),
			wantErr:  false,
		},
		{
			name:     "RFC3339Nano format",
			input:    `"2025-06-16T08:48:08.759Z"`,
			expected: time.Date(2025, 6, 16, 8, 48, 8, 759000000, time.UTC),
			wantErr:  false,
		},
		{
			name:     "Unix timestamp as float",
			input:    `1750063688.759`,
			expected: time.Unix(1750063688, 759000000).UTC(),
			wantErr:  false,
		},
		{
			name:     "Unix timestamp as integer",
			input:    `1750063688`,
			expected: time.Unix(1750063688, 0),
			wantErr:  false,
		},
		{
			name:    "Invalid format",
			input:   `"invalid-time"`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var pt PrestoTime
			err := json.Unmarshal([]byte(tt.input), &pt)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				// For float timestamps, allow small precision differences (< 1 microsecond)
				diff := pt.Time.Sub(tt.expected)
				if diff < 0 {
					diff = -diff
				}
				assert.True(t, diff < time.Microsecond,
					"Expected %v, got %v (diff: %v)", tt.expected, pt.Time, diff)
			}
		})
	}
}

func TestResourceGroupId_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		wantErr  bool
	}{
		{
			name:     "String format",
			input:    `"global"`,
			expected: "global",
			wantErr:  false,
		},
		{
			name:     "Array format single element",
			input:    `["global"]`,
			expected: "global",
			wantErr:  false,
		},
		{
			name:     "Array format multiple elements",
			input:    `["global", "adhoc", "user"]`,
			expected: "global.adhoc.user",
			wantErr:  false,
		},
		{
			name:     "Empty array",
			input:    `[]`,
			expected: "",
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rg ResourceGroupId
			err := json.Unmarshal([]byte(tt.input), &rg)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, rg.Value)
			}
		})
	}
}

func TestDuration_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected time.Duration
		wantErr  bool
	}{
		{
			name:     "Milliseconds as float",
			input:    `371792.0`,
			expected: 371792 * time.Millisecond,
			wantErr:  false,
		},
		{
			name:     "Days format",
			input:    `"1.5d"`,
			expected: 36 * time.Hour,
			wantErr:  false,
		},
		{
			name:     "Hours format",
			input:    `"2.5h"`,
			expected: 150 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "Minutes format",
			input:    `"30m"`,
			expected: 30 * time.Minute,
			wantErr:  false,
		},
		{
			name:     "Seconds format",
			input:    `"45s"`,
			expected: 45 * time.Second,
			wantErr:  false,
		},
		{
			name:     "Milliseconds format",
			input:    `"100ms"`,
			expected: 100 * time.Millisecond,
			wantErr:  false,
		},
		{
			name:     "Microseconds format",
			input:    `"500us"`,
			expected: 500 * time.Microsecond,
			wantErr:  false,
		},
		{
			name:     "Nanoseconds format",
			input:    `"1000ns"`,
			expected: 1000 * time.Nanosecond,
			wantErr:  false,
		},
		{
			name:    "Invalid unit",
			input:   `"10x"`,
			wantErr: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var d Duration
			err := json.Unmarshal([]byte(tt.input), &d)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, d.Duration)
			}
		})
	}
}

func TestDataSize_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected int64
		wantErr  bool
	}{
		{
			name:     "Numeric bytes",
			input:    `1024`,
			expected: 1024,
			wantErr:  false,
		},
		{
			name:     "Float bytes",
			input:    `1024.5`,
			expected: 1024,
			wantErr:  false,
		},
		{
			name:     "String with unit KB",
			input:    `"100KB"`,
			expected: 100,
			wantErr:  false,
		},
		{
			name:     "String with unit MB",
			input:    `"1.5MB"`,
			expected: 1,
			wantErr:  false,
		},
		{
			name:     "String with unit GB",
			input:    `"2.5GB"`,
			expected: 2,
			wantErr:  false,
		},
		{
			name:     "String bytes only",
			input:    `"4096B"`,
			expected: 4096,
			wantErr:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ds DataSize
			err := json.Unmarshal([]byte(tt.input), &ds)

			if tt.wantErr {
				assert.Error(t, err)
			} else {
				require.NoError(t, err)
				assert.Equal(t, tt.expected, ds.Bytes)
			}
		})
	}
}

func TestQueryEvent_UnmarshalJSON(t *testing.T) {
	// Test with the sample NDJSON data from the commit
	jsonData := `{
		"instanceId":"978ecaae-0fe1-4997-8bfc-78b04e54662a",
		"clusterName":"cluster1",
		"queryCompletedEvent":{
			"metadata":{
				"queryId":"20250616_085426_00030_ux3a6",
				"query":"SELECT * FROM test",
				"queryState":"FINISHED",
				"uri":"http://example.com"
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
			"ioMetadata":{
				"inputs":[]
			},
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

	assert.Equal(t, "978ecaae-0fe1-4997-8bfc-78b04e54662a", qe.InstanceId)
	assert.Equal(t, "cluster1", qe.ClusterName)
	assert.NotNil(t, qe.QueryCompletedEvent)
	assert.Equal(t, "20250616_085426_00030_ux3a6", qe.QueryCompletedEvent.Metadata.QueryId)
	assert.Equal(t, "FINISHED", qe.QueryCompletedEvent.Metadata.QueryState)
	assert.Equal(t, int64(371792), qe.CpuTimeMillis)
	assert.Equal(t, int64(100), qe.QueryCompletedEvent.Statistics.OutputPositions)
}

func TestQueryEvent_WithFailureInfo(t *testing.T) {
	jsonData := `{
		"instanceId":"test-instance",
		"clusterName":"test-cluster",
		"queryCompletedEvent":{
			"metadata":{
				"queryId":"test_query_id",
				"query":"SELECT 1",
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
				"failureType":"com.facebook.presto.sql.parser.ParsingException",
				"failureMessage":"Syntax error",
				"failuresJson":"{}"
			},
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
	assert.Equal(t, 1, qe.QueryCompletedEvent.FailureInfo.ErrorCode.Code)
	assert.Equal(t, "SYNTAX_ERROR", qe.QueryCompletedEvent.FailureInfo.ErrorCode.Name)
	assert.Equal(t, "USER_ERROR", qe.QueryCompletedEvent.FailureInfo.ErrorCode.Type)
	assert.NotNil(t, qe.QueryCompletedEvent.FailureInfo.FailureMessage)
	assert.Equal(t, "Syntax error", *qe.QueryCompletedEvent.FailureInfo.FailureMessage)
}
