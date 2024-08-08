package plan_node_test

import (
	"pbench/presto/plan_node"
	"testing"

	"github.com/stretchr/testify/assert"
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
	testParsing[plan_node.JoinIdentifier](t, plan_node.PlanNodeIdentifierParserOptions,
		[]string{
			`[("company_id" = "expr_24") AND not(IS_NULL(COALESCE(CAST(city_id AS bigint), city_id_6)))][$hashvalue_154, $hashvalue_161]`,
			`[("ws_item_sk_252" = "wr_item_sk") AND ("ws_order_number_266" = "wr_order_number") AND ("date_format_5" = "date_format")][$hashvalue_528, $hashvalue_530]`,
			`[("ws_item_sk_252" = "wr_item_sk") OR ("ws_order_number_266" = "wr_order_number") OR ("date_format_5" = "date_format")][$hashvalue_528, $hashvalue_530]`,
			"[((CAST(start_date AS timestamp with time zone)) <= (date_trunc_559)) AND (((CAST(lead AS timestamp with time zone)) > (date_trunc_559)) OR (IS_NULL(lead)))]",
			"[(((replace(lower(name_44), VARCHAR' ')) = (replace(lower(model_52), VARCHAR' '))) OR ((replace(lower(model), VARCHAR' ')) = (replace(lower(model_52), VARCHAR' ')))) AND ((COALESCE(CAST(year AS integer), INTEGER'3000')) >= (COALESCE(start_year, INTEGER'1900')))]",
		},
		[]plan_node.JoinIdentifier{
			{
				FirstExp: &plan_node.JoinPredicate{
					Left:  &plan_node.Column{Name: "company_id"},
					Op:    "=",
					Right: &plan_node.Column{Name: "expr_24"},
				},
				AndExp: []plan_node.Expression{
					&plan_node.FunctionCall{
						FunctionName: "IS_NULL",
						Parameters: []plan_node.Value{
							&plan_node.FunctionCall{
								FunctionName: "COALESCE",
								Parameters: []plan_node.Value{
									&plan_node.TypeCastedValue{
										OriginalValue: &plan_node.IdentRef{Ident: "city_id"},
										CastedType:    plan_node.DataType{Name: "bigint"},
									},
									&plan_node.IdentRef{Ident: "city_id_6"},
								},
							},
						},
					},
				},
			},
			{
				FirstExp: &plan_node.JoinPredicate{
					Left:  &plan_node.Column{Name: "ws_item_sk_252"},
					Op:    "=",
					Right: &plan_node.Column{Name: "wr_item_sk"},
				},
				AndExp: []plan_node.Expression{
					&plan_node.JoinPredicate{
						Left:  &plan_node.Column{Name: "ws_order_number_266"},
						Op:    "=",
						Right: &plan_node.Column{Name: "wr_order_number"},
					},
					&plan_node.JoinPredicate{
						Left:  &plan_node.Column{Name: "date_format_5"},
						Op:    "=",
						Right: &plan_node.Column{Name: "date_format"},
					},
				},
			},
			{
				FirstExp: &plan_node.JoinPredicate{
					Left:  &plan_node.Column{Name: "ws_item_sk_252"},
					Op:    "=",
					Right: &plan_node.Column{Name: "wr_item_sk"},
				},
				OrExp: []plan_node.Expression{

					&plan_node.JoinPredicate{
						Left:  &plan_node.Column{Name: "ws_order_number_266"},
						Op:    "=",
						Right: &plan_node.Column{Name: "wr_order_number"},
					},
					&plan_node.JoinPredicate{
						Left:  &plan_node.Column{Name: "date_format_5"},
						Op:    "=",
						Right: &plan_node.Column{Name: "date_format"},
					},
				},
			},
			{
				FirstExp: &plan_node.JoinPredicate{ //((CAST(start_date AS timestamp with time zone)) <= (date_trunc_559))
					Left: &plan_node.ParansColumn{
						Value: &plan_node.TypeCastedValue{
							OriginalValue: &plan_node.IdentRef{Ident: "start_date"},
							CastedType: plan_node.DataType{
								Name: "timestampwithtimezone",
							},
						},
					},
					Op: "<=",
					Right: &plan_node.ParansColumn{
						Value: &plan_node.IdentRef{Ident: "date_trunc_559"},
					},
				},
				AndExp: []plan_node.Expression{
					&plan_node.LogicalExpression{ //((CAST(lead AS timestamp with time zone)) > (date_trunc_559)) OR (IS_NULL(lead))
						Left: &plan_node.JoinPredicate{
							Left: &plan_node.ParansColumn{ //((CAST(lead AS timestamp with time zone)) > (date_trunc_559))
								Value: &plan_node.TypeCastedValue{
									OriginalValue: &plan_node.IdentRef{Ident: "lead"},
									CastedType: plan_node.DataType{
										Name: "timestampwithtimezone",
									},
								},
							},
							Op: ">",
							Right: &plan_node.ParansColumn{
								Value: &plan_node.IdentRef{Ident: "date_trunc_559"},
							},
						},
						Op: "OR",
						Right: &plan_node.FunctionCall{ //(IS_NULL(lead))
							FunctionName: "IS_NULL",
							Parameters: []plan_node.Value{
								&plan_node.IdentRef{Ident: "lead"},
							},
						},
					},
				},
			},
			{
				FirstExp: &plan_node.LogicalExpression{
					Left: &plan_node.JoinPredicate{ // ((replace(lower(name_44), VARCHAR' ')) = (replace(lower(model_52), VARCHAR' ')))
						Left: &plan_node.ParansColumn{
							Value: &plan_node.FunctionCall{
								FunctionName: "replace",
								Parameters: []plan_node.Value{
									&plan_node.FunctionCall{
										FunctionName: "lower",
										Parameters: []plan_node.Value{
											&plan_node.IdentRef{Ident: "name_44"},
										},
									},
									&plan_node.TypedValue{
										DataType:     "VARCHAR",
										ValueLiteral: " ",
									},
								},
							},
						},
						Op: "=",
						Right: &plan_node.ParansColumn{
							Value: &plan_node.FunctionCall{
								FunctionName: "replace",
								Parameters: []plan_node.Value{
									&plan_node.FunctionCall{
										FunctionName: "lower",
										Parameters: []plan_node.Value{
											&plan_node.IdentRef{Ident: "model_52"},
										},
									},
									&plan_node.TypedValue{
										DataType:     "VARCHAR",
										ValueLiteral: " ",
									},
								},
							},
						},
					},
					Op: "OR",
					Right: &plan_node.JoinPredicate{ // ((replace(lower(model), VARCHAR' ')) = (replace(lower(model_52), VARCHAR' ')))
						Left: &plan_node.ParansColumn{
							Value: &plan_node.FunctionCall{
								FunctionName: "replace",
								Parameters: []plan_node.Value{
									&plan_node.FunctionCall{
										FunctionName: "lower",
										Parameters: []plan_node.Value{
											&plan_node.IdentRef{Ident: "model"},
										},
									},
									&plan_node.TypedValue{
										DataType:     "VARCHAR",
										ValueLiteral: " ",
									},
								},
							},
						},
						Op: "=",
						Right: &plan_node.ParansColumn{
							Value: &plan_node.FunctionCall{
								FunctionName: "replace",
								Parameters: []plan_node.Value{
									&plan_node.FunctionCall{
										FunctionName: "lower",
										Parameters: []plan_node.Value{
											&plan_node.IdentRef{Ident: "model_52"},
										},
									},
									&plan_node.TypedValue{
										DataType:     "VARCHAR",
										ValueLiteral: " ",
									},
								},
							},
						},
					},
				},
				AndExp: []plan_node.Expression{ //((COALESCE(CAST(year AS integer), INTEGER'3000')) >= (COALESCE(start_year, INTEGER'1900')))
					&plan_node.JoinPredicate{
						Left: &plan_node.ParansColumn{
							Value: &plan_node.FunctionCall{
								FunctionName: "COALESCE",
								Parameters: []plan_node.Value{
									&plan_node.TypeCastedValue{
										OriginalValue: &plan_node.IdentRef{Ident: "year"},
										CastedType:    plan_node.DataType{Name: "integer"},
									},
									&plan_node.TypedValue{
										DataType:     "INTEGER",
										ValueLiteral: "3000",
									},
								},
							},
						},
						Op: ">=",
						Right: &plan_node.ParansColumn{
							Value: &plan_node.FunctionCall{
								FunctionName: "COALESCE",
								Parameters: []plan_node.Value{
									&plan_node.IdentRef{Ident: "start_year"},
									&plan_node.TypedValue{
										DataType:     "INTEGER",
										ValueLiteral: "1900",
									},
								},
							},
						},
					},
				},
			},
		})
}
