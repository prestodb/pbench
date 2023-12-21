package presto

type Warning struct {
	WarningCode WarningCode `json:"warningCode"`
	Message     string      `json:"message"`
}

type WarningCode struct {
	Code int    `json:"code"`
	Name string `json:"name"`
}
