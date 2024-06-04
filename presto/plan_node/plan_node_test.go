package plan_node_test

import (
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"math"
	"os"
	"pbench/presto/plan_node"
	"testing"
)

func testParseJoin(t *testing.T, fileName, expected string) {
	t.Helper()
	t.Run(fileName, func(t *testing.T) {
		bytes, err := os.ReadFile(fileName)
		if !assert.Nil(t, err) {
			t.FailNow()
		}
		planTree := make(plan_node.PlanTree)
		if !assert.Nil(t, json.Unmarshal(bytes, &planTree)) {
			t.FailNow()
		}
		joins, parseErr := planTree.ParseJoins()
		if assert.Nil(t, parseErr) {
			assert.Equal(t, expected, fmt.Sprint(joins))
		}
	})
}

func TestParseJoin(t *testing.T) {
	testParseJoin(t, "sample.plan.json",
		`[{RightJoin CAST(glue.ng_public.admin_system_city.id AS bigint) glue.lks.LR_branded_car_enrollment.city_id} {LeftJoin glue.lks.LR_branded_car_enrollment.country glue.lks.LR_admin_system_country.code} {InnerJoin CAST(glue.ng_public.fleet_car.id AS bigint) glue.ng_public.fleet_car_tag_binding.car_id} {LeftJoin glue.ng_public.fleet_car_tag_binding.car_tag_id CAST(glue.ng_public.fleet_car_tag.id AS bigint)} {LeftJoin glue.lks.LR_branded_car_enrollment.car_id CAST(glue.ng_public.fleet_car.id AS bigint)}]`)
	testParseJoin(t, "arithmetics.plan.json", `[{InnerJoin (hive.test_join.t1.a) + (INTEGER '2') ((hive.test_join.t2.b) + (INTEGER '1')) - ((INTEGER '2') * (abs(hive.test_join.t2.c)))}]`)
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
