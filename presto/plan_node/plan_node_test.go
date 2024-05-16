package plan_node_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"pbench/presto/plan_node"
	"testing"
)

func TestPlanTree(t *testing.T) {
	bytes, err := os.ReadFile("sample_plan.json")
	assert.Nil(t, err)
	planTree := make(plan_node.PlanTree)
	assert.Nil(t, json.Unmarshal(bytes, &planTree))
	details := make(map[string]*plan_node.PlanNodeDetail)
	count := 0
	assert.Nil(t, planTree.Traverse(context.Background(), func(ctx context.Context, node *plan_node.PlanNode) error {
		fmt.Println(node.Identifier)
		if node.Details != "" {
			count++
			id := fmt.Sprintf("%s_%s", node.Id, node.Name)
			//fmt.Printf("%s\n%s-----\n", id, node.Details)
			ast, parseErr := plan_node.PlanNodeDetailParser.ParseString(id, node.Details)
			if assert.Nil(t, parseErr) {
				details[id] = ast
			}
		}
		return nil
	}, plan_node.PlanTreeDFSTraverse))
	fmt.Println(count, len(details))
}

func TestJsonFloat64(t *testing.T) {
	content := `{
              "lowValue": "-Infinity",
              "highValue": "Infinity",
              "nullsFraction": 0.0,
              "averageRowSize": 2.0,
              "distinctValuesCount": "NaN"
            }`
	expected := plan_node.VariableStatistics{
		LowValue:            plan_node.JsonFloat64(math.Inf(-1)),
		HighValue:           plan_node.JsonFloat64(math.Inf(1)),
		NullsFraction:       plan_node.JsonFloat64(0),
		AverageRowSize:      plan_node.JsonFloat64(2.0),
		DistinctValuesCount: plan_node.JsonFloat64(math.NaN()),
	}
	b, err := json.Marshal(&expected)
	assert.Nil(t, err)
	assert.Equal(t, `{"lowValue":"-Infinity","highValue":"Infinity","nullsFraction":0,"averageRowSize":2,"distinctValuesCount":"NaN"}`, string(b))
	actual := plan_node.VariableStatistics{}
	err = json.Unmarshal([]byte(content), &actual)
	assert.Nil(t, err)
	assert.True(t, math.IsInf(float64(actual.LowValue), -1))
	assert.True(t, math.IsInf(float64(actual.HighValue), 1))
	assert.True(t, math.IsNaN(float64(actual.DistinctValuesCount)), actual.DistinctValuesCount)
}
