package query_json

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"testing"
)

func TestPlanTree(t *testing.T) {
	bytes, err := os.ReadFile("sample_plan.json")
	assert.Nil(t, err)
	planTree := make(PlanTree)
	assert.Nil(t, json.Unmarshal(bytes, &planTree))
	fmt.Println(planTree)
}

func TestJsonFloat64(t *testing.T) {
	content := `{
              "lowValue": "-Infinity",
              "highValue": "Infinity",
              "nullsFraction": 0.0,
              "averageRowSize": 2.0,
              "distinctValuesCount": "NaN"
            }`
	expected := VariableStatistics{
		LowValue:            JsonFloat64(math.Inf(-1)),
		HighValue:           JsonFloat64(math.Inf(1)),
		NullsFraction:       JsonFloat64(0),
		AverageRowSize:      JsonFloat64(2.0),
		DistinctValuesCount: JsonFloat64(math.NaN()),
	}
	b, err := json.Marshal(&expected)
	assert.Nil(t, err)
	assert.Equal(t, `{"lowValue":"-Infinity","highValue":"Infinity","nullsFraction":0,"averageRowSize":2,"distinctValuesCount":"NaN"}`, string(b))
	actual := VariableStatistics{}
	err = json.Unmarshal([]byte(content), &actual)
	assert.Nil(t, err)
	assert.True(t, math.IsInf(float64(actual.LowValue), -1))
	assert.True(t, math.IsInf(float64(actual.HighValue), 1))
	assert.True(t, math.IsNaN(float64(actual.DistinctValuesCount)), actual.DistinctValuesCount)
}
