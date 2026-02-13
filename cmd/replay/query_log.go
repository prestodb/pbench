package replay

import (
	"encoding/json"
	"fmt"
	"github.com/ethanyzhang/presto-go/query_json"
	"time"
)

// QueryLog was generated using https://mholt.github.io/json-to-go/
type QueryLog struct {
	InstanceID          string `json:"instanceId,omitempty"`
	ClusterName         string `json:"clusterName,omitempty"`
	QueryCompletedEvent struct {
		Metadata struct {
			QueryID string `json:"queryId,omitempty"`
			//TransactionID          string `json:"transactionId,omitempty"`
			//TracingID              string `json:"tracingId,omitempty"`
			Query string `json:"query,omitempty"`
			//QueryHash              string `json:"queryHash,omitempty"`
			//PreparedQuery          any    `json:"preparedQuery,omitempty"`
			//QueryState string `json:"queryState,omitempty"`
			//URI                    string `json:"uri,omitempty"`
			//Plan                   string `json:"plan,omitempty"`
			//JSONPlan string `json:"jsonPlan,omitempty"`
			//GraphvizPlan           string `json:"graphvizPlan,omitempty"`
			//Payload                string `json:"payload,omitempty"`
			//RuntimeOptimizedStages []any  `json:"runtimeOptimizedStages,omitempty"`
			//PlanNodeRuntimeStats   string `json:"planNodeRuntimeStats,omitempty"`
		} `json:"metadata,omitempty"`
		//Statistics struct {
		//	CPUTime                            float64             `json:"cpuTime,omitempty"`
		//	RetriedCPUTime                     float64             `json:"retriedCpuTime,omitempty"`
		//	WallTime                           float64             `json:"wallTime,omitempty"`
		//	WaitingForPrerequisitesTime        float64             `json:"waitingForPrerequisitesTime,omitempty"`
		//	QueuedTime                         float64             `json:"queuedTime,omitempty"`
		//	WaitingForResourcesTime            float64             `json:"waitingForResourcesTime,omitempty"`
		//	SemanticAnalyzingTime              float64             `json:"semanticAnalyzingTime,omitempty"`
		//	ColumnAccessPermissionCheckingTime float64             `json:"columnAccessPermissionCheckingTime,omitempty"`
		//	DispatchingTime                    float64             `json:"dispatchingTime,omitempty"`
		//	PlanningTime                       float64             `json:"planningTime,omitempty"`
		//	AnalysisTime                       float64             `json:"analysisTime,omitempty"`
		//	ExecutionTime                      float64             `json:"executionTime,omitempty"`
		//	PeakRunningTasks                   int                 `json:"peakRunningTasks,omitempty"`
		//	PeakUserMemoryBytes                int                 `json:"peakUserMemoryBytes,omitempty"`
		//	PeakTotalNonRevocableMemoryBytes   int                 `json:"peakTotalNonRevocableMemoryBytes,omitempty"`
		//	PeakTaskUserMemory                 int                 `json:"peakTaskUserMemory,omitempty"`
		//	PeakTaskTotalMemory                int                 `json:"peakTaskTotalMemory,omitempty"`
		//	PeakNodeTotalMemory                int                 `json:"peakNodeTotalMemory,omitempty"`
		//	TotalBytes                         int                 `json:"totalBytes,omitempty"`
		//	TotalRows                          int                 `json:"totalRows,omitempty"`
		//	OutputBytes                        int                 `json:"outputBytes,omitempty"`
		//	OutputRows                         int                 `json:"outputRows,omitempty"`
		//	WrittenOutputBytes                 int                 `json:"writtenOutputBytes,omitempty"`
		//	WrittenOutputRows                  int                 `json:"writtenOutputRows,omitempty"`
		//	WrittenIntermediateBytes           int                 `json:"writtenIntermediateBytes,omitempty"`
		//	SpilledBytes                       int                 `json:"spilledBytes,omitempty"`
		//	CumulativeMemory                   float64             `json:"cumulativeMemory,omitempty"`
		//	CumulativeTotalMemory              float64             `json:"cumulativeTotalMemory,omitempty"`
		//	CompletedSplits                    int                 `json:"completedSplits,omitempty"`
		//	Complete                           bool                `json:"complete,omitempty"`
		//	RuntimeStats                       presto.RuntimeStats `json:"runtimeStats,omitempty"`
		//} `json:"statistics,omitempty"`
		Context struct {
			//User                string   `json:"user,omitempty"`
			//Principal           string   `json:"principal,omitempty"`
			//RemoteClientAddress string   `json:"remoteClientAddress,omitempty"`
			//UserAgent           string   `json:"userAgent,omitempty"`
			//ClientInfo          any      `json:"clientInfo,omitempty"`
			//ClientTags          []any    `json:"clientTags,omitempty"`
			//Source              string   `json:"source,omitempty"`
			//QueryActionType     any      `json:"queryActionType,omitempty"`
			Catalog           string            `json:"catalog,omitempty"`
			Schema            string            `json:"schema,omitempty"`
			ResourceGroupID   []string          `json:"resourceGroupId,omitempty"`
			SessionProperties map[string]string `json:"sessionProperties,omitempty"`
			//ResourceEstimates struct {
			//	ExecutionTime  any `json:"executionTime,omitempty"`
			//	CPUTime        any `json:"cpuTime,omitempty"`
			//	PeakMemory     any `json:"peakMemory,omitempty"`
			//	PeakTaskMemory any `json:"peakTaskMemory,omitempty"`
			//} `json:"resourceEstimates,omitempty"`
			//ServerAddress string `json:"serverAddress,omitempty"`
			//ServerVersion string `json:"serverVersion,omitempty"`
			Environment string `json:"environment,omitempty"`
		} `json:"context,omitempty"`
		IoMetadata struct {
			Inputs []struct {
				CatalogName   string   `json:"catalogName,omitempty"`
				Schema        string   `json:"schema,omitempty"`
				Table         string   `json:"table,omitempty"`
				Columns       []string `json:"columns,omitempty"`
				ConnectorInfo struct {
					PartitionIds []string `json:"partitionIds,omitempty"`
					Truncated    bool     `json:"truncated,omitempty"`
				} `json:"connectorInfo,omitempty"`
				Statistics struct {
					RowCount         Value `json:"rowCount,omitempty"`
					TotalSize        Value `json:"totalSize,omitempty"`
					ColumnStatistics struct {
						NullsFraction       Value `json:"nullsFraction,omitempty"`
						DistinctValuesCount Value `json:"distinctValuesCount,omitempty"`
						DataSize            Value `json:"dataSize,omitempty"`
						Range               Range `json:"range,omitempty"`
					} `json:"columnStatistics,omitempty"`
				} `json:"statistics,omitempty"`
				//SerializedCommitOutput string `json:"serializedCommitOutput,omitempty"`
			} `json:"inputs,omitempty"`
			//Output any `json:"output,omitempty"`
		} `json:"ioMetadata,omitempty"`
		FailureInfo any `json:"failureInfo,omitempty"`
		//Warnings    []struct {
		//	WarningCode struct {
		//		Code int    `json:"code,omitempty"`
		//		Name string `json:"name,omitempty"`
		//	} `json:"warningCode,omitempty"`
		//	Message string `json:"message,omitempty"`
		//} `json:"warnings,omitempty"`
		QueryType string `json:"queryType,omitempty"`
		//FailedTasks []any  `json:"failedTasks,omitempty"`
		//StageStatistics []struct {
		//	StageID                 int    `json:"stageId,omitempty"`
		//	StageExecutionID        int    `json:"stageExecutionId,omitempty"`
		//	Tasks                   int    `json:"tasks,omitempty"`
		//	TotalScheduledTime      string `json:"totalScheduledTime,omitempty"`
		//	TotalCPUTime            string `json:"totalCpuTime,omitempty"`
		//	RetriedCPUTime          string `json:"retriedCpuTime,omitempty"`
		//	TotalBlockedTime        string `json:"totalBlockedTime,omitempty"`
		//	RawInputDataSize        string `json:"rawInputDataSize,omitempty"`
		//	ProcessedInputDataSize  string `json:"processedInputDataSize,omitempty"`
		//	PhysicalWrittenDataSize string `json:"physicalWrittenDataSize,omitempty"`
		//	GcStatistics            struct {
		//		StageID          int `json:"stageId,omitempty"`
		//		StageExecutionID int `json:"stageExecutionId,omitempty"`
		//		Tasks            int `json:"tasks,omitempty"`
		//		FullGcTasks      int `json:"fullGcTasks,omitempty"`
		//		MinFullGcSec     int `json:"minFullGcSec,omitempty"`
		//		MaxFullGcSec     int `json:"maxFullGcSec,omitempty"`
		//		TotalFullGcSec   int `json:"totalFullGcSec,omitempty"`
		//		AverageFullGcSec int `json:"averageFullGcSec,omitempty"`
		//	} `json:"gcStatistics,omitempty"`
		//	CPUDistribution struct {
		//		P25     int     `json:"p25,omitempty"`
		//		P50     int     `json:"p50,omitempty"`
		//		P75     int     `json:"p75,omitempty"`
		//		P90     int     `json:"p90,omitempty"`
		//		P95     int     `json:"p95,omitempty"`
		//		P99     int     `json:"p99,omitempty"`
		//		Min     int     `json:"min,omitempty"`
		//		Max     int     `json:"max,omitempty"`
		//		Total   int     `json:"total,omitempty"`
		//		Average float64 `json:"average,omitempty"`
		//	} `json:"cpuDistribution,omitempty"`
		//	MemoryDistribution struct {
		//		P25     int     `json:"p25,omitempty"`
		//		P50     int     `json:"p50,omitempty"`
		//		P75     int     `json:"p75,omitempty"`
		//		P90     int     `json:"p90,omitempty"`
		//		P95     int     `json:"p95,omitempty"`
		//		P99     int     `json:"p99,omitempty"`
		//		Min     int     `json:"min,omitempty"`
		//		Max     int     `json:"max,omitempty"`
		//		Total   int     `json:"total,omitempty"`
		//		Average float64 `json:"average,omitempty"`
		//	} `json:"memoryDistribution,omitempty"`
		//} `json:"stageStatistics,omitempty"`
		//OperatorStatistics []struct {
		//	StageID                     int     `json:"stageId,omitempty"`
		//	StageExecutionID            int     `json:"stageExecutionId,omitempty"`
		//	PipelineID                  int     `json:"pipelineId,omitempty"`
		//	OperatorID                  int     `json:"operatorId,omitempty"`
		//	PlanNodeID                  string  `json:"planNodeId,omitempty"`
		//	OperatorType                string  `json:"operatorType,omitempty"`
		//	TotalDrivers                int     `json:"totalDrivers,omitempty"`
		//	AddInputCalls               int     `json:"addInputCalls,omitempty"`
		//	AddInputWall                string  `json:"addInputWall,omitempty"`
		//	AddInputCPU                 string  `json:"addInputCpu,omitempty"`
		//	AddInputAllocation          string  `json:"addInputAllocation,omitempty"`
		//	RawInputDataSize            string  `json:"rawInputDataSize,omitempty"`
		//	RawInputPositions           int     `json:"rawInputPositions,omitempty"`
		//	InputDataSize               string  `json:"inputDataSize,omitempty"`
		//	InputPositions              int     `json:"inputPositions,omitempty"`
		//	SumSquaredInputPositions    float64 `json:"sumSquaredInputPositions,omitempty"`
		//	GetOutputCalls              int     `json:"getOutputCalls,omitempty"`
		//	GetOutputWall               string  `json:"getOutputWall,omitempty"`
		//	GetOutputCPU                string  `json:"getOutputCpu,omitempty"`
		//	GetOutputAllocation         string  `json:"getOutputAllocation,omitempty"`
		//	OutputDataSize              string  `json:"outputDataSize,omitempty"`
		//	OutputPositions             int     `json:"outputPositions,omitempty"`
		//	PhysicalWrittenDataSize     string  `json:"physicalWrittenDataSize,omitempty"`
		//	BlockedWall                 string  `json:"blockedWall,omitempty"`
		//	FinishCalls                 int     `json:"finishCalls,omitempty"`
		//	FinishWall                  string  `json:"finishWall,omitempty"`
		//	FinishCPU                   string  `json:"finishCpu,omitempty"`
		//	FinishAllocation            string  `json:"finishAllocation,omitempty"`
		//	UserMemoryReservation       string  `json:"userMemoryReservation,omitempty"`
		//	RevocableMemoryReservation  string  `json:"revocableMemoryReservation,omitempty"`
		//	SystemMemoryReservation     string  `json:"systemMemoryReservation,omitempty"`
		//	PeakUserMemoryReservation   string  `json:"peakUserMemoryReservation,omitempty"`
		//	PeakSystemMemoryReservation string  `json:"peakSystemMemoryReservation,omitempty"`
		//	PeakTotalMemoryReservation  string  `json:"peakTotalMemoryReservation,omitempty"`
		//	SpilledDataSize             string  `json:"spilledDataSize,omitempty"`
		//	Info                        any     `json:"info,omitempty"`
		//	RuntimeStats                struct {
		//	} `json:"runtimeStats,omitempty"`
		//} `json:"operatorStatistics,omitempty"`
		//PlanStatisticsRead    []any   `json:"planStatisticsRead,omitempty"`
		//PlanStatisticsWritten []any   `json:"planStatisticsWritten,omitempty"`
		CreateTime         Float64Time `json:"createTime,omitempty"`
		ExecutionStartTime Float64Time `json:"executionStartTime,omitempty"`
		EndTime            Float64Time `json:"endTime,omitempty"`
		//ExpandedQuery        any     `json:"expandedQuery,omitempty"`
		//OptimizerInformation []struct {
		//	OptimizerName       string `json:"optimizerName,omitempty"`
		//	OptimizerTriggered  bool   `json:"optimizerTriggered,omitempty"`
		//	OptimizerApplicable any    `json:"optimizerApplicable,omitempty"`
		//} `json:"optimizerInformation,omitempty"`
		//ScalarFunctions    []any `json:"scalarFunctions,omitempty"`
		//AggregateFunctions []any `json:"aggregateFunctions,omitempty"`
		//WindowsFunctions   []any `json:"windowsFunctions,omitempty"`
	} `json:"queryCompletedEvent,omitempty"`
	//QueryOptimiserEvent  any    `json:"queryOptimiserEvent,omitempty"`
	//SplitCompletedEvent  any    `json:"splitCompletedEvent,omitempty"`
	//Plan                 string `json:"plan,omitempty"`
	CPUTimeMillis        query_json.Duration `json:"cpuTimeMillis,omitempty"`
	RetriedCPUTimeMillis query_json.Duration `json:"retriedCpuTimeMillis,omitempty"`
	WallTimeMillis       query_json.Duration `json:"wallTimeMillis,omitempty"`
	QueuedTimeMillis     query_json.Duration `json:"queuedTimeMillis,omitempty"`
	AnalysisTimeMillis   query_json.Duration `json:"analysisTimeMillis,omitempty"`
}

type Value struct {
	Value   any  `json:"value,omitempty"`
	Unknown bool `json:"unknown,omitempty"`
}

type Range struct {
	Min float64 `json:"min,omitempty"`
	Max float64 `json:"max,omitempty"`
}

type Float64Time struct {
	time.Time
}

func (t *Float64Time) UnmarshalJSON(bytes []byte) error {
	var v any
	if err := json.Unmarshal(bytes, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		t.Time = time.UnixMilli(int64(value * 1000))
		return nil
	default:
		return fmt.Errorf("invalid Float64Time")
	}
}

func (t *Float64Time) MarshalJSON() ([]byte, error) {
	return json.Marshal(t.Time)
}
