package presto

type RuntimeStats map[string]RuntimeMetric

type RuntimeMetric struct {
	Name  string      `json:"name"`
	Unit  RuntimeUnit `json:"unit"`
	Sum   int64       `json:"sum"`
	Count int64       `json:"count"`
	Max   int64       `json:"max"`
	Min   int64       `json:"min"`
}
