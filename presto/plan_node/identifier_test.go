package plan_node_test

import (
	"pbench/presto/plan_node"
	"testing"
)

func TestParseIdentifiers(t *testing.T) {
	testParsing[plan_node.PlanNodeIdentifiers](t,
		[]string{
			`[table = TableHandle {connectorId='glue', connectorHandle='HiveTableHandle{schemaName=looker_scratch, tableName=LR_SH2E81713104009327_branded_car_enrollment, analyzePartitionValues=Optional.empty}', layout='Optional[looker_scratch.LR_SH2E81713104009327_branded_car_enrollment{domains={car_id=[ [["1", <max>)] ], country=[ [["gh"]] ], cohort=[ [["Branded_Verified"]] ]}}]'}, projectLocality = LOCAL]`,
		},
		[]plan_node.PlanNodeIdentifiers{
			plan_node.PlanNodeIdentifiers{},
		})
}
