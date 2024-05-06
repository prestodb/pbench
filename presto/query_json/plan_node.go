package query_json

import (
	"encoding/json"
	"fmt"
	"math"
)

type PlanNode struct {
	Id            string         `json:"id"`
	Name          string         `json:"name"`
	Identifier    string         `json:"identifier"`
	Details       string         `json:"details"`
	Children      []PlanNode     `json:"children"`
	RemoteSources []string       `json:"remoteSources"`
	Estimates     []PlanEstimate `json:"estimates"`
}

type PlanTree map[string]struct {
	Plan PlanNode `json:"plan"`
}

type PlanEstimate struct {
	OutputRowCount     JsonFloat64                   `json:"outputRowCount"`
	TotalSize          JsonFloat64                   `json:"totalSize"`
	Confident          bool                          `json:"confident"`
	VariableStatistics map[string]VariableStatistics `json:"variableStatistics"`
}

type VariableStatistics struct {
	LowValue            JsonFloat64 `json:"lowValue"`
	HighValue           JsonFloat64 `json:"highValue"`
	NullsFraction       JsonFloat64 `json:"nullsFraction"`
	AverageRowSize      JsonFloat64 `json:"averageRowSize"`
	DistinctValuesCount JsonFloat64 `json:"distinctValuesCount"`
}

type JsonFloat64 float64

func (f *JsonFloat64) MarshalJSON() ([]byte, error) {
	value := float64(*f)
	if math.IsNaN(value) {
		return []byte(`"NaN"`), nil
	} else if math.IsInf(value, 0) {
		if math.IsInf(value, -1) {
			return []byte(`"-Infinity"`), nil
		}
		return []byte(`"Infinity"`), nil
	} else {
		return json.Marshal(value)
	}
}

func (f *JsonFloat64) UnmarshalJSON(data []byte) error {
	var v any
	if err := json.Unmarshal(data, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		*f = JsonFloat64(value)
		return nil
	case string:
		switch value {
		case "NaN":
			*f = JsonFloat64(math.NaN())
		case "Infinity":
			*f = JsonFloat64(math.Inf(1))
		case "-Infinity":
			*f = JsonFloat64(math.Inf(-1))
		default:
			return fmt.Errorf("invalid JsonFloat64 %s", value)
		}
		return nil
	default:
		return fmt.Errorf("invalid JsonFloat64")
	}
}
