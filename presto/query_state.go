package presto

import (
	"context"
	"fmt"
	"net/http"
	"time"
)

// QueryStateInfo is the Go translation of the QueryStateInfo class in Presto Java:
// https://github.com/prestodb/presto/blob/master/presto-main/src/main/java/com/facebook/presto/server/QueryStateInfo.java
// Unused fields are commented out for now.
type QueryStateInfo struct {
	QueryId string `json:"queryId"`
	//QueryState string `json:"queryState"`
	//ResourceGroupId []string  `json:"resourceGroupId"`
	//Query          string    `json:"query"`
	//QueryTruncated bool      `json:"queryTruncated"`
	CreateTime time.Time `json:"createTime"`
	//User           string    `json:"user"`
	//Authenticated  bool      `json:"authenticated"`
	//Source         string    `json:"source,omitempty"`
	//Catalog        string    `json:"catalog"`
	//Progress        struct {
	//	ElapsedTimeMillis        int  `json:"elapsedTimeMillis"`
	//	QueuedTimeMillis         int  `json:"queuedTimeMillis"`
	//	ExecutionTimeMillis      int  `json:"executionTimeMillis"`
	//	CpuTimeMillis            int  `json:"cpuTimeMillis"`
	//	ScheduledTimeMillis      int  `json:"scheduledTimeMillis"`
	//	CurrentMemoryBytes       int  `json:"currentMemoryBytes"`
	//	PeakMemoryBytes          int  `json:"peakMemoryBytes"`
	//	PeakTotalMemoryBytes     int  `json:"peakTotalMemoryBytes"`
	//	PeakTaskTotalMemoryBytes int  `json:"peakTaskTotalMemoryBytes"`
	//	CumulativeUserMemory     int  `json:"cumulativeUserMemory"`
	//	CumulativeTotalMemory    int  `json:"cumulativeTotalMemory"`
	//	InputRows                int  `json:"inputRows"`
	//	InputBytes               int  `json:"inputBytes"`
	//	Blocked                  bool `json:"blocked"`
	//	QueuedDrivers            int  `json:"queuedDrivers"`
	//	RunningDrivers           int  `json:"runningDrivers"`
	//	CompletedDrivers         int  `json:"completedDrivers"`
	//} `json:"progress"`
	//WarningCodes []interface{} `json:"warningCodes"`
}

// GetQueryStatsOptions includes parameters in https://github.com/prestodb/presto/blob/a7af002182098ba5a61248edfcaaa66e5d50e489/presto-main/src/main/java/com/facebook/presto/server/QueryStateInfoResource.java#L89-L95
type GetQueryStatsOptions struct {
	User                         *string `query:"user"`
	IncludeLocalQueryOnly        *bool   `query:"includeLocalQueryOnly"`
	IncludeAllQueries            *bool   `query:"includeAllQueries"`
	IncludeAllQueryProgressStats *bool   `query:"includeAllQueryProgressStats"`
	ExcludeResourceGroupPathInfo *bool   `query:"excludeResourceGroupPathInfo"`
	QueryTextSizeLimit           *int    `query:"queryTextSizeLimit"`
}

func (c *Client) GetQueryState(ctx context.Context, reqOpt *GetQueryStatsOptions, opts ...RequestOption) ([]QueryStateInfo, *http.Response, error) {
	req, err := c.NewRequest("GET",
		fmt.Sprintf("v1/queryState?%s", GenerateHttpQueryParameter(reqOpt)), nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	infoArray := make([]QueryStateInfo, 0, 16)
	resp, err := c.Do(ctx, req, &infoArray)
	if err != nil {
		return nil, resp, err
	}
	return infoArray, resp, nil
}
