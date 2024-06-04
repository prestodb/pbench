package plan_node_test

import (
	"github.com/stretchr/testify/assert"
	"pbench/presto/plan_node"
	"testing"
)

func TestParseHiveTableHandle(t *testing.T) {
	testParsing[plan_node.HiveTableHandle](t, plan_node.PlanNodeIdentifierParserOptions,
		[]string{
			`HiveTableHandle{schemaName=test_schema, tableName=city, analyzePartitionValues=Optional.empty}`,
		},
		[]plan_node.HiveTableHandle{{
			Schema: "test_schema",
			Table:  "city",
		}})
}

func TestParseScanIdentifier(t *testing.T) {
	literal := `[table = TableHandle {connectorId='glue', connectorHandle='HiveTableHandle{schemaName=test_schema, tableName=city, analyzePartitionValues=Optional.empty}', layout='Optional[ng_public.admin_system_city{domains={id=[ [["706"]] ]}}]'}, filterPredicate = ((id) = (INTEGER'706')) AND ((BIGINT'706') = (CAST(id AS bigint))), projectLocality = LOCAL]`
	handle := plan_node.ParseHiveTableHandle(literal)
	assert.Equal(t, &plan_node.HiveTableHandle{
		Schema:  "test_schema",
		Table:   "city",
		Catalog: "glue",
	}, handle)
}

func TestParseJoinPredicates(t *testing.T) {
	testParsing[plan_node.JoinPredicates](t, plan_node.PlanNodeIdentifierParserOptions,
		[]string{`[("ws_item_sk_252" = "wr_item_sk") AND ("ws_order_number_266" = "wr_order_number")][$hashvalue_528, $hashvalue_530]`},
		[]plan_node.JoinPredicates{
			{Predicates: []plan_node.JoinPredicate{
				{
					Left:  "ws_item_sk_252",
					Right: "wr_item_sk",
				},
				{
					Left:  "ws_order_number_266",
					Right: "wr_order_number",
				},
			}},
		})
}
