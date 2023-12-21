package presto

type StatementStats struct {
	State                             string        `json:"state"`
	WaitingForPrerequisites           bool          `json:"waitingForPrerequisites"`
	Queued                            bool          `json:"queued"`
	Scheduled                         bool          `json:"scheduled"`
	Nodes                             int           `json:"nodes"`
	TotalSplits                       int           `json:"totalSplits"`
	QueuesSplits                      int           `json:"queuedSplits"`
	RunningSplits                     int           `json:"runningSplits"`
	CompletedSplits                   int           `json:"completedSplits"`
	CPUTimeMillis                     int64         `json:"cpuTimeMillis"`
	WallTimeMillis                    int64         `json:"wallTimeMillis"`
	WaitingForPrerequisitesTimeMillis int64         `json:"waitingForPrerequisitesTimeMillis"`
	QueuedTimeMillis                  int64         `json:"queuedTimeMillis"`
	ElapsedTimeMillis                 int64         `json:"elapsedTimeMillis"`
	ProcessedRows                     int64         `json:"processedRows"`
	ProcessedBytes                    int64         `json:"processedBytes"`
	PeakMemoryBytes                   int64         `json:"peakMemoryBytes"`
	PeakTotalMemoryBytes              int64         `json:"peakTotalMemoryBytes"`
	PeakTaskTotalMemoryBytes          int64         `json:"peakTaskTotalMemoryBytes"`
	SpilledBytes                      int64         `json:"spilledBytes"`
	RootStage                         *StageStats   `json:"rootStage,omitempty"`
	RuntimeStats                      *RuntimeStats `json:"runtimeStats,omitempty"`
}
