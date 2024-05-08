package replay

import "time"

type QueryFrame struct {
	StartTime              time.Time         `json:"start_time"`
	Query                  string            `json:"query"`
	OriginalQueryId        string            `json:"original_query_id"`
	OriginalWallTimeMillis int64             `json:"original_wall_time_millis"`
	Catalog                string            `json:"catalog"`
	Schema                 string            `json:"schema"`
	SessionProperties      map[string]string `json:"session_properties,omitempty"`
}

type QueryFrameQueue []*QueryFrame

func (q *QueryFrameQueue) Len() int {
	return len(*q)
}

func (q *QueryFrameQueue) Less(i, j int) bool {
	return (*q)[i].StartTime.Before((*q)[j].StartTime)
}

func (q *QueryFrameQueue) Swap(i, j int) {
	(*q)[i], (*q)[j] = (*q)[j], (*q)[i]
}

func (q *QueryFrameQueue) Push(x any) {
	*q = append(*q, x.(*QueryFrame))
}

func (q *QueryFrameQueue) Pop() any {
	ret := (*q)[0]
	*q = (*q)[1:]
	return ret
}
