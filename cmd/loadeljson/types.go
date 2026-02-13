package loadeljson

import (
	"encoding/json"
	"fmt"
	"time"
)

// PrestoTime is a custom time type that can unmarshal from both string and numeric formats
type PrestoTime struct {
	time.Time
}

// ResourceGroupId is a custom type that can handle both string and array formats
type ResourceGroupId struct {
	Value string
}

func (pt *PrestoTime) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case string:
		// Try parsing as RFC3339 format
		t, err := time.Parse(time.RFC3339, value)
		if err != nil {
			// Try parsing as RFC3339Nano format
			t, err = time.Parse(time.RFC3339Nano, value)
			if err != nil {
				return err
			}
		}
		pt.Time = t
		return nil
	case float64:
		// Assume it's seconds since epoch (Unix timestamp)
		pt.Time = time.Unix(int64(value), int64((value-float64(int64(value)))*1e9))
		return nil
	default:
		return fmt.Errorf("invalid time type: %T", value)
	}
}

func (rg *ResourceGroupId) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}

	switch value := v.(type) {
	case string:
		rg.Value = value
		return nil
	case []interface{}:
		// If it's an array, join the elements with "."
		parts := make([]string, 0, len(value))
		for _, item := range value {
			if str, ok := item.(string); ok {
				parts = append(parts, str)
			}
		}
		if len(parts) > 0 {
			rg.Value = parts[0]
			for i := 1; i < len(parts); i++ {
				rg.Value += "." + parts[i]
			}
		}
		return nil
	default:
		return fmt.Errorf("invalid resource group id type: %T", value)
	}
}

// QueryEvent represents the event listener JSON structure
type QueryEvent struct {
	InstanceId           string               `json:"instanceId"`
	ClusterName          string               `json:"clusterName"`
	QueryCreatedEvent    *QueryCreatedEvent   `json:"queryCreatedEvent,omitempty"`
	QueryCompletedEvent  *QueryCompletedEvent `json:"queryCompletedEvent,omitempty"`
	SplitCompletedEvent  interface{}          `json:"splitCompletedEvent,omitempty"`
	Plan                 string               `json:"plan"`
	CpuTimeMillis        int64                `json:"cpuTimeMillis"`
	RetriedCpuTimeMillis int64                `json:"retriedCpuTimeMillis"`
	WallTimeMillis       int64                `json:"wallTimeMillis"`
	QueuedTimeMillis     int64                `json:"queuedTimeMillis"`
	AnalysisTimeMillis   int64                `json:"analysisTimeMillis"`
}

type QueryCreatedEvent struct {
	CreateTime PrestoTime    `json:"createTime"`
	Context    QueryContext  `json:"context"`
	Metadata   QueryMetadata `json:"metadata"`
}

type ResourceEstimates struct {
	ExecutionTime  *Duration `json:"executionTime,omitempty"`
	CpuTime        *Duration `json:"cpuTime,omitempty"`
	PeakMemory     *int64    `json:"peakMemory,omitempty"`
	PeakTaskMemory *int64    `json:"peakTaskMemory,omitempty"`
}

type QueryCompletedEvent struct {
	Metadata              QueryMetadata        `json:"metadata"`
	Statistics            QueryStatistics      `json:"statistics"`
	Context               QueryContext         `json:"context"`
	IoMetadata            QueryIOMetadata      `json:"ioMetadata"`
	FailureInfo           *QueryFailureInfo    `json:"failureInfo,omitempty"`
	Warnings              []interface{}        `json:"warnings"`
	QueryType             *string              `json:"queryType,omitempty"`
	FailedTasks           []string             `json:"failedTasks"`
	CreateTime            PrestoTime           `json:"createTime"`
	ExecutionStartTime    PrestoTime           `json:"executionStartTime"`
	EndTime               PrestoTime           `json:"endTime"`
	StageStatistics       []StageStatistics    `json:"stageStatistics"`
	OperatorStatistics    []OperatorStatistics `json:"operatorStatistics"`
	PlanStatisticsRead    []interface{}        `json:"planStatisticsRead"`
	PlanStatisticsWritten []interface{}        `json:"planStatisticsWritten"`
	PlanNodeHash          interface{}          `json:"planNodeHash"`
	CanonicalPlan         interface{}          `json:"canonicalPlan"`
	StatsEquivalentPlan   *string              `json:"statsEquivalentPlan,omitempty"`
	ExpandedQuery         *string              `json:"expandedQuery,omitempty"`
	OptimizerInformation  []interface{}        `json:"optimizerInformation"`
	CteInformationList    []interface{}        `json:"cteInformationList"`
	ScalarFunctions       []string             `json:"scalarFunctions"`
	AggregateFunctions    []string             `json:"aggregateFunctions"`
	WindowFunctions       []string             `json:"windowFunctions"`
}

type QueryMetadata struct {
	QueryId       string  `json:"queryId"`
	TransactionId *string `json:"transactionId,omitempty"`
	Query         string  `json:"query"`
	QueryState    string  `json:"queryState"`
	Uri           string  `json:"uri"`
	Plan          *string `json:"plan,omitempty"`
	JsonPlan      *string `json:"jsonPlan,omitempty"`
	QueryHash     *string `json:"queryHash,omitempty"`
}

type QueryStatistics struct {
	CpuTime                          Duration  `json:"cpuTime"`
	WallTime                         Duration  `json:"wallTime"`
	QueuedTime                       Duration  `json:"queuedTime"`
	AnalysisTime                     *Duration `json:"analysisTime,omitempty"`
	PeakUserMemoryBytes              int64     `json:"peakUserMemoryBytes"`
	PeakTotalNonRevocableMemoryBytes int64     `json:"peakTotalNonRevocableMemoryBytes"`
	PeakTaskUserMemory               int64     `json:"peakTaskUserMemory"`
	PeakTaskTotalMemory              int64     `json:"peakTaskTotalMemory"`
	PeakNodeTotalMemory              int64     `json:"peakNodeTotalMemory"`
	TotalBytes                       int64     `json:"totalBytes"`
	TotalRows                        int64     `json:"totalRows"`
	OutputPositions                  int64     `json:"outputPositions"`
	OutputBytes                      int64     `json:"outputBytes"`
	WrittenOutputRows                int64     `json:"writtenOutputRows"`
	WrittenOutputBytes               int64     `json:"writtenOutputBytes"`
	CumulativeMemory                 float64   `json:"cumulativeMemory"`
	CumulativeTotalMemory            float64   `json:"cumulativeTotalMemory"`
	CompletedSplits                  int       `json:"completedSplits"`
}

type QueryContext struct {
	User                string            `json:"user"`
	Principal           *string           `json:"principal,omitempty"`
	RemoteClientAddress *string           `json:"remoteClientAddress,omitempty"`
	UserAgent           *string           `json:"userAgent,omitempty"`
	ClientInfo          *string           `json:"clientInfo,omitempty"`
	Source              *string           `json:"source,omitempty"`
	Catalog             *string           `json:"catalog,omitempty"`
	Schema              *string           `json:"schema,omitempty"`
	ResourceGroupId     *ResourceGroupId  `json:"resourceGroupId,omitempty"`
	SessionProperties   map[string]string `json:"sessionProperties"`
	ServerVersion       string            `json:"serverVersion"`
	Environment         string            `json:"environment"`
	ClientTags          []string          `json:"clientTags"`
	ResourceEstimates   ResourceEstimates `json:"resourceEstimates"`
}

type QueryIOMetadata struct {
	Inputs []interface{} `json:"inputs"`
	Output interface{}   `json:"output,omitempty"`
}

type QueryFailureInfo struct {
	ErrorCode      ErrorCode `json:"errorCode"`
	FailureType    *string   `json:"failureType,omitempty"`
	FailureMessage *string   `json:"failureMessage,omitempty"`
	FailureTask    *string   `json:"failureTask,omitempty"`
	FailureHost    *string   `json:"failureHost,omitempty"`
	FailuresJson   string    `json:"failuresJson"`
}

type ErrorCode struct {
	Code int    `json:"code"`
	Name string `json:"name"`
	Type string `json:"type"`
}

type StageStatistics struct {
	StageId                 int      `json:"stageId"`
	StageExecutionId        int      `json:"stageExecutionId"`
	Tasks                   int      `json:"tasks"`
	TotalScheduledTime      Duration `json:"totalScheduledTime"`
	TotalCpuTime            Duration `json:"totalCpuTime"`
	RetriedCpuTime          Duration `json:"retriedCpuTime"`
	TotalBlockedTime        Duration `json:"totalBlockedTime"`
	RawInputDataSize        DataSize `json:"rawInputDataSize"`
	ProcessedInputDataSize  DataSize `json:"processedInputDataSize"`
	PhysicalWrittenDataSize DataSize `json:"physicalWrittenDataSize"`
}

type OperatorStatistics struct {
	StageId                     int      `json:"stageId"`
	StageExecutionId            int      `json:"stageExecutionId"`
	PipelineId                  int      `json:"pipelineId"`
	OperatorId                  int      `json:"operatorId"`
	PlanNodeId                  string   `json:"planNodeId"`
	OperatorType                string   `json:"operatorType"`
	TotalDrivers                int64    `json:"totalDrivers"`
	AddInputCalls               int64    `json:"addInputCalls"`
	AddInputWall                Duration `json:"addInputWall"`
	AddInputCpu                 Duration `json:"addInputCpu"`
	AddInputAllocation          DataSize `json:"addInputAllocation"`
	RawInputDataSize            DataSize `json:"rawInputDataSize"`
	RawInputPositions           int64    `json:"rawInputPositions"`
	InputDataSize               DataSize `json:"inputDataSize"`
	InputPositions              int64    `json:"inputPositions"`
	GetOutputCalls              int64    `json:"getOutputCalls"`
	GetOutputWall               Duration `json:"getOutputWall"`
	GetOutputCpu                Duration `json:"getOutputCpu"`
	GetOutputAllocation         DataSize `json:"getOutputAllocation"`
	OutputDataSize              DataSize `json:"outputDataSize"`
	OutputPositions             int64    `json:"outputPositions"`
	PhysicalWrittenDataSize     DataSize `json:"physicalWrittenDataSize"`
	BlockedWall                 Duration `json:"blockedWall"`
	FinishCalls                 int64    `json:"finishCalls"`
	FinishWall                  Duration `json:"finishWall"`
	FinishCpu                   Duration `json:"finishCpu"`
	FinishAllocation            DataSize `json:"finishAllocation"`
	UserMemoryReservation       DataSize `json:"userMemoryReservation"`
	RevocableMemoryReservation  DataSize `json:"revocableMemoryReservation"`
	SystemMemoryReservation     DataSize `json:"systemMemoryReservation"`
	PeakUserMemoryReservation   DataSize `json:"peakUserMemoryReservation"`
	PeakSystemMemoryReservation DataSize `json:"peakSystemMemoryReservation"`
	PeakTotalMemoryReservation  DataSize `json:"peakTotalMemoryReservation"`
	SpilledDataSize             DataSize `json:"spilledDataSize"`
	Info                        *string  `json:"info,omitempty"`
}

// Duration represents a time duration in the format used by Presto
type Duration struct {
	time.Duration
}

func (d *Duration) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		d.Duration = time.Duration(value) * time.Millisecond
		return nil
	case string:
		// Parse Presto duration format: "43.99d", "1.5h", "30m", "45s", "100ms", etc.
		var amount float64
		var unit string
		n, err := fmt.Sscanf(value, "%f%s", &amount, &unit)
		if err != nil || n != 2 {
			// Try standard Go duration format as fallback
			d.Duration, err = time.ParseDuration(value)
			return err
		}

		// Convert Presto units to Go duration
		switch unit {
		case "d":
			d.Duration = time.Duration(amount * float64(24*time.Hour))
		case "h":
			d.Duration = time.Duration(amount * float64(time.Hour))
		case "m":
			d.Duration = time.Duration(amount * float64(time.Minute))
		case "s":
			d.Duration = time.Duration(amount * float64(time.Second))
		case "ms":
			d.Duration = time.Duration(amount * float64(time.Millisecond))
		case "us":
			d.Duration = time.Duration(amount * float64(time.Microsecond))
		case "ns":
			d.Duration = time.Duration(amount * float64(time.Nanosecond))
		default:
			return fmt.Errorf("unknown duration unit: %s", unit)
		}
		return nil
	default:
		return fmt.Errorf("invalid duration type")
	}
}

// DataSize represents a data size in bytes
type DataSize struct {
	Bytes int64
}

func (ds *DataSize) UnmarshalJSON(b []byte) error {
	var v interface{}
	if err := json.Unmarshal(b, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		ds.Bytes = int64(value)
		return nil
	case string:
		// Parse string like "1.5MB", "100KB", etc.
		// For simplicity, we'll just try to extract the number
		var size float64
		var unit string
		fmt.Sscanf(value, "%f%s", &size, &unit)
		ds.Bytes = int64(size)
		return nil
	default:
		return fmt.Errorf("invalid data size type")
	}
}
