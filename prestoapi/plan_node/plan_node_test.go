package plan_node_test

import (
	"encoding/json"
	"fmt"
	"math"
	"os"
	"pbench/prestoapi/plan_node"
	"testing"

	"github.com/stretchr/testify/assert"
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
		`[JoinType:RightJoin, Left:CAST(glue.ng_public.admin_system_city.id AS bigint), Right:glue.lks.LR_branded_car_enrollment.city_id JoinType:LeftJoin, Left:glue.lks.LR_branded_car_enrollment.country, Right:glue.lks.LR_admin_system_country.code JoinType:InnerJoin, Left:CAST(glue.ng_public.fleet_car.id AS bigint), Right:glue.ng_public.fleet_car_tag_binding.car_id JoinType:LeftJoin, Left:glue.ng_public.fleet_car_tag_binding.car_tag_id, Right:CAST(glue.ng_public.fleet_car_tag.id AS bigint) JoinType:LeftJoin, Left:glue.lks.LR_branded_car_enrollment.car_id, Right:CAST(glue.ng_public.fleet_car.id AS bigint)]`)
	testParseJoin(t, "arithmetics.plan.json", `[JoinType:InnerJoin, Left:(hive.test_join.t1.a) + (INTEGER '2'), Right:((hive.test_join.t2.b) + (INTEGER '1')) - ((INTEGER '2') * (abs(hive.test_join.t2.c)))]`)
	testParseJoin(t, "sample1.json",
		`[JoinType:InnerJoin, Left:CAST(glue.ng_public.etl_city_kpi_hourly.city_id AS varchar), Right:CAST(glue.looker_scratch.lr_sh3yd1718099588916_city_kpi_full.city_id AS varchar) JoinType:InnerJoin, Left:date_format(CAST(CAST(glue.ng_public.etl_city_kpi_hourly.period_hour_local_date AS date) AS timestamp), VARCHAR '%Y-%m-%d'), Right:date_format(CAST(glue.looker_scratch.lr_sh3yd1718099588916_city_kpi_full.date AS timestamp), VARCHAR '%Y-%m-%d')]`)
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
