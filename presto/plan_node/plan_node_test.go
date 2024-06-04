package plan_node_test

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"pbench/presto/plan_node"
	"strings"
	"testing"
)

func traceValue(assignmentMap map[string]plan_node.Value, tableHandle *plan_node.HiveTableHandle, value plan_node.Value) plan_node.Value {
	switch typed := value.(type) {
	case *plan_node.HiveColumnHandle:
		if typed.Table == nil {
			typed.Table = tableHandle
		}
	case *plan_node.TypeCastedValue:
		typed.OriginalValue = traceValue(assignmentMap, tableHandle, typed.OriginalValue)
	case *plan_node.IdentRef:
		return traceValue(assignmentMap, tableHandle, assignmentMap[typed.Ident])
	case *plan_node.FunctionCall:
		for i := 0; i < len(typed.Parameters); i++ {
			typed.Parameters[i] = traceValue(assignmentMap, tableHandle, typed.Parameters[i])
		}
	}
	return value
}

func TestPlanTree(t *testing.T) {
	bytes, err := os.ReadFile("sample_plan.json")
	if !assert.Nil(t, err) {
		t.FailNow()
	}
	planTree := make(plan_node.PlanTree)
	if !assert.Nil(t, json.Unmarshal(bytes, &planTree)) {
		t.FailNow()
	}
	assignmentMap := make(map[string]plan_node.Value)
	assert.Nil(t, planTree.Traverse(context.Background(), func(ctx context.Context, node *plan_node.PlanNode) error {
		tableHandle := plan_node.ParseHiveTableHandle(node.Identifier)
		id := fmt.Sprintf("%s_%s", node.Id, node.Name)
		if node.Details != "" {
			ast, parseErr := plan_node.PlanNodeDetailParser.ParseString(id, node.Details)
			if !assert.Nil(t, parseErr) {
				t.FailNow()
			}
			for i := len(ast.Stmts) - 1; i >= 0; i-- {
				if assignment, ok := ast.Stmts[i].(*plan_node.Assignment); ok {
					assignmentMap[assignment.Identifier.Ident] = traceValue(assignmentMap, tableHandle, assignment.AssignedValue)
				}
			}
		}
		if plan_node.IsJoin[node.Name] {
			preds, parseErr := plan_node.PlanNodeJoinPredicatesParser.ParseString(id, node.Identifier)
			if !assert.Nil(t, parseErr) {
				t.FailNow()
			}
			b := strings.Builder{}
			for _, pred := range preds.Predicates {
				b.WriteString(assignmentMap[pred.LeftColumn].String())
				b.WriteString(" x ")
				b.WriteString(assignmentMap[pred.RightColumn].String())
				b.WriteString("\n")
			}
			fmt.Printf("%s\n%s\n", id, b.String())
		}
		return nil
	}, plan_node.PlanTreeDFSTraverse))
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
