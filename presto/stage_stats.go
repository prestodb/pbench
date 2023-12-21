package presto

type StageStats struct {
	StageId         string       `json:"stageId"`
	State           string       `json:"state"`
	Done            bool         `json:"done"`
	Nodes           int          `json:"nodes"`
	TotalSplits     int          `json:"totalSplits"`
	QueuesSplits    int          `json:"queuedSplits"`
	RunningSplits   int          `json:"runningSplits"`
	CompletedSplits int          `json:"completedSplits"`
	CPUTimeMillis   int64        `json:"cpuTimeMillis"`
	WallTimeMillis  int64        `json:"wallTimeMillis"`
	ProcessedRows   int64        `json:"processedRows"`
	ProcessedBytes  int64        `json:"processedBytes"`
	SubStages       []StageStats `json:"subStages"`
}
