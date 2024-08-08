package plan_node_test

import (
	"fmt"
	"pbench/presto/plan_node"
	"testing"

	"github.com/alecthomas/participle/v2"
	"github.com/stretchr/testify/assert"
)

func testParsing[T any](t *testing.T, options []participle.Option, testLiterals []string, expected []T) {
	t.Helper()
	if !assert.Equalf(t, len(testLiterals), len(expected), "length of testLiterals and expected should be same") {
		t.FailNow()
	}
	parser := participle.MustBuild[T](options...)
	for i, literal := range testLiterals {
		testName := fmt.Sprintf("case_%d", i)
		t.Run(testName, func(t *testing.T) {
			ast, parseErr := parser.ParseString(testName, literal)
			if assert.Nil(t, parseErr) {
				assert.Equal(t, expected[i], *ast)
			}
		})
	}
}

func TestParseLayout(t *testing.T) {
	testParsing[plan_node.Layout](t, plan_node.PlanNodeDetailParserOptions,
		[]string{
			`LAYOUT: schema.table{domains={id=[ [["706"]] ]}}`,
			`LAYOUT: schema.SH2E81713104009327{domains={car_id=[ [["1", <max>)] ], country=[ [["gh"]] ], cohort=[ [["Random Text"]] ]}}`,
		},
		[]plan_node.Layout{
			{`schema.table{domains={id=[[["706"]]]}}`},
			{`schema.SH2E81713104009327{domains={car_id=[[["1",<max>)]],country=[[["gh"]]],cohort=[[["Random Text"]]]}}`},
		})
}

func TestParseHiveColumnHandle(t *testing.T) {
	testParsing[plan_node.HiveColumnHandle](t, plan_node.PlanNodeDetailParserOptions,
		[]string{
			"cohort:varchar(16):2:REGULAR",
			"city_id:bigint:-13:PARTITION_KEY (23:6)",
			`car_id:bigint:0:REGULAR
    :: [["1", <max>)]`,
			`city_id:bigint:-13:PARTITION_KEY (23:6)
    :: [["706"], ["1024", "2048")]`,
		},
		[]plan_node.HiveColumnHandle{
			{ // case 0
				ColumnName:  plan_node.IdentRef{Ident: "cohort"},
				DataType:    "varchar(16)",
				ColumnIndex: 2,
				ColumnType:  "REGULAR",
			},
			{ // case 1
				ColumnName:  plan_node.IdentRef{Ident: "city_id"},
				DataType:    "bigint",
				ColumnIndex: -13,
				ColumnType:  "PARTITION_KEY",
				Loc: &plan_node.SourceLocation{
					RowNumber:    23,
					ColumnNumber: 6,
				},
			},
			{ // case 2
				ColumnName:  plan_node.IdentRef{Ident: "car_id"},
				DataType:    "bigint",
				ColumnIndex: 0,
				ColumnType:  "REGULAR",
				Ranges: []plan_node.Range{
					{
						LowValue: &plan_node.Marker{
							Bound: plan_node.EXACTLY,
							Value: "1",
						},
						HighValue: &plan_node.Marker{
							Bound: plan_node.BELOW,
						},
					},
				},
			},
			{ // case 3
				ColumnName:  plan_node.IdentRef{Ident: "city_id"},
				DataType:    "bigint",
				ColumnIndex: -13,
				ColumnType:  "PARTITION_KEY",
				Loc: &plan_node.SourceLocation{
					RowNumber:    23,
					ColumnNumber: 6,
				},
				Ranges: []plan_node.Range{
					{
						LowValue: &plan_node.Marker{
							Bound: plan_node.EXACTLY,
							Value: "706",
						},
					},
					{
						LowValue: &plan_node.Marker{
							Bound: plan_node.EXACTLY,
							Value: "1024",
						},
						HighValue: &plan_node.Marker{
							Bound: plan_node.BELOW,
							Value: "2048",
						},
					},
				},
			},
		})
}

func TestParseTypedValue(t *testing.T) {
	testParsing[plan_node.TypedValue](t, plan_node.PlanNodeDetailParserOptions,
		[]string{
			`BIGINT'0'`,
			`REAL'3.14'`,
		},
		[]plan_node.TypedValue{
			{"BIGINT", "0"},
			{"REAL", "3.14"},
		})
}

func TestParseRange(t *testing.T) {
	testParsing[plan_node.Range](t, plan_node.PlanNodeDetailParserOptions,
		[]string{
			`["1", <max>)`,
			`(<min>, "2024"]`,
			`["706"]`,
			`["706", "1024"]`,
		},
		[]plan_node.Range{
			{ // case 0
				LowValue: &plan_node.Marker{
					Bound: plan_node.EXACTLY,
					Value: "1",
				},
				HighValue: &plan_node.Marker{Bound: plan_node.BELOW},
			},
			{ // case 1
				LowValue: &plan_node.Marker{
					Bound: plan_node.ABOVE,
				},
				HighValue: &plan_node.Marker{
					Bound: plan_node.EXACTLY,
					Value: "2024",
				},
			},
			{ // case 2
				LowValue: &plan_node.Marker{
					Bound: plan_node.EXACTLY,
					Value: "706",
				},
			},
			{ // case 3
				LowValue: &plan_node.Marker{
					Bound: plan_node.EXACTLY,
					Value: "706",
				},
				HighValue: &plan_node.Marker{
					Bound: plan_node.EXACTLY,
					Value: "1024",
				},
			},
		})
}

func TestParseAssignment(t *testing.T) {
	testParsing[plan_node.Assignment](t, plan_node.PlanNodeDetailParserOptions,
		[]string{
			`$hashvalue_108 := $hashvalue_109`,
			`country := country:string:1:REGULAR (23:6)
			:: [["gh"]]`,
			`$hashvalue_109 := combine_hash(BIGINT'0', COALESCE($operator$hash_code(car_id), BIGINT'0')) (22:5)`,
			"expr_3 := CAST(id AS bigint) (24:12)",
			`array_agg_51 := "presto.default.array_agg"((name_35)) ORDER BY OrderingScheme {orderBy='[Ordering {variable='name_35', sortOrder='ASC_NULLS_LAST'}]', orderings='{name_35=ASC_NULLS_LAST}'} (6:21)`,
			`branded_car_enrollment.target_id := car_id (22:5)`,
			`expr_5 := ((b) + (INTEGER'1')) - ((INTEGER'2') * (abs(c))) (10:6)`,
			`expr_555 := CAST(valid_on_539 AS timestamp with time zone) (33:7)`,
			"date_trunc := TIMESTAMP WITH TIME ZONE'7037190144000000'",
			"lead := lead(start_date) RANGE UNBOUNDED_PRECEDING CURRENT_ROW (48:6)",
			"lead := COALESCE($operator$hash_code(cast), BIGINT'0') (48:6)",
			"expr_75 := (date_trunc_63) - (INTERVAL YEAR TO MONTH'0-3') (19:64)",
		},
		[]plan_node.Assignment{
			{ // case 0
				Identifier:    plan_node.IdentRef{Ident: "$hashvalue_108"},
				AssignedValue: &plan_node.IdentRef{Ident: "$hashvalue_109"},
			},
			{ // case 1
				Identifier: plan_node.IdentRef{Ident: "country"},
				AssignedValue: &plan_node.HiveColumnHandle{
					ColumnName:  plan_node.IdentRef{Ident: "country"},
					DataType:    "string",
					ColumnIndex: 1,
					ColumnType:  "REGULAR",
					Loc: &plan_node.SourceLocation{
						RowNumber:    23,
						ColumnNumber: 6,
					},
					Ranges: []plan_node.Range{
						{LowValue: &plan_node.Marker{
							Bound: plan_node.EXACTLY,
							Value: "gh",
						}},
					},
				},
			},
			{ // case 2
				Identifier: plan_node.IdentRef{Ident: "$hashvalue_109"},
				AssignedValue: &plan_node.FunctionCall{
					FunctionName: "combine_hash",
					Parameters: []plan_node.Value{
						&plan_node.TypedValue{
							DataType:     "BIGINT",
							ValueLiteral: "0",
						},
						&plan_node.FunctionCall{
							FunctionName: "COALESCE",
							Parameters: []plan_node.Value{
								&plan_node.FunctionCall{
									FunctionName: "$operator$hash_code",
									Parameters: []plan_node.Value{
										&plan_node.IdentRef{Ident: "car_id"},
									},
								},
								&plan_node.TypedValue{
									DataType:     "BIGINT",
									ValueLiteral: "0",
								},
							},
						},
					},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    22,
					ColumnNumber: 5,
				},
			},
			{ // case 3
				Identifier: plan_node.IdentRef{Ident: "expr_3"},
				AssignedValue: &plan_node.TypeCastedValue{
					OriginalValue: &plan_node.IdentRef{Ident: "id"},
					CastedType:    plan_node.DataType{Name: "bigint"},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    24,
					ColumnNumber: 12,
				},
			},
			{ // case 4
				Identifier: plan_node.IdentRef{Ident: "array_agg_51"},
				AssignedValue: &plan_node.CatchAllValue{
					Value: "\"presto.default.array_agg\"((name_35))ORDERBYOrderingScheme{orderBy='[Ordering {variable='name_35', sortOrder='ASC_NULLS_LAST'}]',orderings='{name_35=ASC_NULLS_LAST}'}(6:21)"},
				Loc: nil,
			},
			{ // case 5
				Identifier:    plan_node.IdentRef{Ident: "branded_car_enrollment.target_id"},
				AssignedValue: &plan_node.IdentRef{Ident: "car_id"},
				Loc: &plan_node.SourceLocation{
					RowNumber:    22,
					ColumnNumber: 5,
				},
			},
			{ // case 6
				Identifier: plan_node.IdentRef{Ident: "expr_5"},
				AssignedValue: &plan_node.MathExpr{
					Left: &plan_node.MathExpr{
						Left: &plan_node.IdentRef{Ident: "b"},
						Op:   "+",
						Right: &plan_node.TypedValue{
							DataType:     "INTEGER",
							ValueLiteral: "1",
						},
					},
					Op: "-",
					Right: &plan_node.MathExpr{
						Left: &plan_node.TypedValue{
							DataType:     "INTEGER",
							ValueLiteral: "2",
						},
						Op: "*",
						Right: &plan_node.FunctionCall{
							FunctionName: "abs",
							Parameters: []plan_node.Value{
								&plan_node.IdentRef{Ident: "c"},
							},
						},
					},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    10,
					ColumnNumber: 6,
				},
			},
			{ // case 7
				Identifier: plan_node.IdentRef{Ident: "expr_555"},
				AssignedValue: &plan_node.TypeCastedValue{
					OriginalValue: &plan_node.IdentRef{
						Ident: "valid_on_539",
					},
					CastedType: plan_node.DataType{
						Name: "timestampwithtimezone",
					}},
				Loc: &plan_node.SourceLocation{
					RowNumber:    33,
					ColumnNumber: 7,
				},
			},
			{ // case 8
				Identifier: plan_node.IdentRef{Ident: "date_trunc"},
				AssignedValue: &plan_node.TypedValue{
					DataType:     "TIMESTAMPWITHTIMEZONE",
					ValueLiteral: "7037190144000000",
				},
			},
			{ // function options: "lead := lead(start_date) RANGE UNBOUNDED_PRECEDING CURRENT_ROW (48:6)
				Identifier: plan_node.IdentRef{Ident: "lead"},
				AssignedValue: &plan_node.FunctionCall{
					FunctionName: "lead",
					Parameters: []plan_node.Value{
						&plan_node.IdentRef{
							Ident: "start_date",
						},
					},
					Options: []string{
						"RANGE", "UNBOUNDED_PRECEDING", "CURRENT_ROW",
					},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    48,
					ColumnNumber: 6,
				},
			},
			{ //lead := COALESCE($operator$hash_code(city_id_151), BIGINT'0') (48:6)"
				Identifier: plan_node.IdentRef{Ident: "lead"},
				AssignedValue: &plan_node.FunctionCall{
					FunctionName: "COALESCE",
					Parameters: []plan_node.Value{
						&plan_node.FunctionCall{
							FunctionName: "$operator$hash_code",
							Parameters: []plan_node.Value{
								&plan_node.IdentRef{
									Ident: "cast",
								},
							},
						},
						&plan_node.TypedValue{
							DataType:     "BIGINT",
							ValueLiteral: "0",
						},
					},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    48,
					ColumnNumber: 6,
				},
			},
			{ //"expr_75 := (date_trunc_63) - (INTERVAL YEAR TO MONTH'0-3') (19:64)"
				Identifier: plan_node.IdentRef{Ident: "expr_75"},
				AssignedValue: &plan_node.MathExpr{
					Left: &plan_node.IdentRef{Ident: "date_trunc_63"},
					Op:   "-",
					Right: &plan_node.TypedValue{
						DataType:     "INTERVALYEARTOMONTH",
						ValueLiteral: "0-3",
					},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    19,
					ColumnNumber: 64,
				},
			},
		})
}

func TestParseSwitch(t *testing.T) {
	testParsing[plan_node.Assignment](t, plan_node.PlanNodeDetailParserOptions,
		[]string{
			"expr_106 := SWITCH(BOOLEAN'true', WHEN((min) = (rank), INTEGER'1'), INTEGER'0') (3:215)",
			"expr_95 := SWITCH(BOOLEAN'true', WHEN(((state) = (VARCHAR'finished')) AND (IS_NULL(retry_to_try)), concat(CAST(city_id AS varchar), CAST(order_id AS varchar))), null) (5:8)",
			"expr_41 := SWITCH(BOOLEAN'true', WHEN(is_deleted, VARCHAR'Yes'), VARCHAR'No') (15:70)",
			"expr_41 := SWITCH(BOOLEAN'true', WHEN(lower(name) LIKE VARCHAR'%birmingham%', INTEGER'409'), VARCHAR'No') (15:70)",
			"expr_103 := SWITCH(BOOLEAN'true', WHEN(((CAST(has_order AS decimal(12,2))) > (DECIMAL'0.00')) OR ((CAST(waiting_orders AS decimal(12,2))) > (DECIMAL'0.00')), driver_root_id ), null) (16:8)",
		},
		[]plan_node.Assignment{
			{ // switch 1 "expr_106 := SWITCH(BOOLEAN'true', WHEN((min) = (rank), INTEGER'1'), INTEGER'0') (3:215)"
				Identifier: plan_node.IdentRef{Ident: "expr_106"},
				AssignedValue: &plan_node.Switch{
					DataType: &plan_node.TypedValue{
						DataType:     "BOOLEAN",
						ValueLiteral: "true",
					},
					When: []plan_node.SwitchWhen{
						{
							Exp: &plan_node.CompareWhenExp{
								Left: &plan_node.IdentRef{
									Ident: "min",
								},
								Op: "=",
								Right: &plan_node.IdentRef{
									Ident: "rank",
								},
							},
							Value: &plan_node.TypedValue{
								DataType:     "INTEGER",
								ValueLiteral: "1",
							},
						},
					},
					DefaultValue: &plan_node.TypedValue{
						DataType:     "INTEGER",
						ValueLiteral: "0",
					},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    3,
					ColumnNumber: 215,
				},
			},
			{ // switch 2 "expr_95 := SWITCH(BOOLEAN'true', WHEN(((state) = (VARCHAR'finished')) AND (IS_NULL(retry_to_try)), concat(CAST(city_id AS varchar), CAST(order_id AS varchar))), null) (5:8)"
				Identifier: plan_node.IdentRef{Ident: "expr_95"},
				AssignedValue: &plan_node.Switch{
					DataType: &plan_node.TypedValue{
						DataType:     "BOOLEAN",
						ValueLiteral: "true",
					},
					When: []plan_node.SwitchWhen{
						{
							Exp: &plan_node.AndOrWhenExp{
								Left: &plan_node.CompareWhenExp{
									Left: &plan_node.IdentRef{
										Ident: "state",
									},
									Op: "=",
									Right: &plan_node.TypedValue{
										DataType:     "VARCHAR",
										ValueLiteral: "finished",
									},
								},
								Op: "AND",
								Right: &plan_node.BooleanWhenExp{
									Eval: &plan_node.FunctionCall{
										FunctionName: "IS_NULL",
										Parameters: []plan_node.Value{
											&plan_node.IdentRef{
												Ident: "retry_to_try",
											},
										},
									},
								},
							},
							Value: &plan_node.FunctionCall{
								FunctionName: "concat",
								Parameters: []plan_node.Value{
									&plan_node.TypeCastedValue{
										OriginalValue: &plan_node.IdentRef{
											Ident: "city_id",
										},
										CastedType: plan_node.DataType{
											Name: "varchar",
										},
									},
									&plan_node.TypeCastedValue{
										OriginalValue: &plan_node.IdentRef{
											Ident: "order_id",
										},
										CastedType: plan_node.DataType{
											Name: "varchar",
										},
									},
								},
							},
						},
					},
					DefaultValue: &plan_node.IdentRef{
						Ident: "null",
					},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    5,
					ColumnNumber: 8,
				},
			},
			{ // expr_41 := SWITCH(BOOLEAN'true', WHEN(is_deleted, VARCHAR'Yes'), VARCHAR'No') (15:70)
				Identifier: plan_node.IdentRef{Ident: "expr_41"},
				AssignedValue: &plan_node.Switch{
					DataType: &plan_node.TypedValue{
						DataType:     "BOOLEAN",
						ValueLiteral: "true",
					},
					When: []plan_node.SwitchWhen{
						{
							Exp: &plan_node.BooleanWhenExp{
								Eval: &plan_node.IdentRef{
									Ident: "is_deleted",
								},
							},
							Value: &plan_node.TypedValue{
								DataType:     "VARCHAR",
								ValueLiteral: "Yes",
							},
						},
					},
					DefaultValue: &plan_node.TypedValue{
						DataType:     "VARCHAR",
						ValueLiteral: "No",
					},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    15,
					ColumnNumber: 70,
				},
			},
			{ // expr_41 := SWITCH(BOOLEAN'true', WHEN(lower(name) LIKE VARCHAR'%birmingham%', INTEGER'409'), VARCHAR'No') (15:70)
				Identifier: plan_node.IdentRef{Ident: "expr_41"},
				AssignedValue: &plan_node.Switch{
					DataType: &plan_node.TypedValue{
						DataType:     "BOOLEAN",
						ValueLiteral: "true",
					},
					When: []plan_node.SwitchWhen{
						{
							Exp: &plan_node.LikeWhenExp{
								Left: &plan_node.FunctionCall{
									FunctionName: "lower",
									Parameters: []plan_node.Value{
										&plan_node.IdentRef{
											Ident: "name",
										},
									},
								},
								Right: plan_node.TypedValue{
									DataType:     "VARCHAR",
									ValueLiteral: "%birmingham%",
								},
							},
							Value: &plan_node.TypedValue{
								DataType:     "INTEGER",
								ValueLiteral: "409",
							},
						},
					},
					DefaultValue: &plan_node.TypedValue{
						DataType:     "VARCHAR",
						ValueLiteral: "No",
					},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    15,
					ColumnNumber: 70,
				},
			},
			{ // "expr_103 := SWITCH(BOOLEAN'true', WHEN(((CAST(has_order AS decimal(12,2))) > (DECIMAL'0.00')) OR ((CAST(waiting_orders AS decimal(12,2))) > (DECIMAL'0.00')), driver_root_id ), null) (16:8)"
				Identifier: plan_node.IdentRef{Ident: "expr_103"},
				AssignedValue: &plan_node.Switch{
					DataType: &plan_node.TypedValue{
						DataType:     "BOOLEAN",
						ValueLiteral: "true",
					},
					When: []plan_node.SwitchWhen{
						{
							Exp: &plan_node.AndOrWhenExp{
								Left: &plan_node.CompareWhenExp{
									Left: &plan_node.TypeCastedValue{
										OriginalValue: &plan_node.IdentRef{Ident: "has_order"},
										CastedType:    plan_node.DataType{Name: "decimal", Option: "12,2"},
									},
									Op:    ">",
									Right: &plan_node.TypedValue{DataType: "DECIMAL", ValueLiteral: "0.00"},
								},
								Op: "OR",
								Right: &plan_node.CompareWhenExp{
									Left: &plan_node.TypeCastedValue{
										OriginalValue: &plan_node.IdentRef{Ident: "waiting_orders"},
										CastedType:    plan_node.DataType{Name: "decimal", Option: "12,2"},
									},
									Op:    ">",
									Right: &plan_node.TypedValue{DataType: "DECIMAL", ValueLiteral: "0.00"},
								},
							},
							Value: &plan_node.IdentRef{
								Ident: "driver_root_id",
							},
						},
					},
					DefaultValue: &plan_node.IdentRef{
						Ident: "null",
					},
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    16,
					ColumnNumber: 8,
				},
			},
		})
}

func TestIdent(t *testing.T) {
	testParsing[plan_node.IdentRef](t, plan_node.PlanNodeDetailParserOptions,
		[]string{
			"branded_car_enrollment.target_id",
		},
		[]plan_node.IdentRef{
			{Ident: "branded_car_enrollment.target_id"},
		})
}

func TestParseFunctionCall(t *testing.T) {
	testParsing[plan_node.FunctionCall](t, plan_node.PlanNodeDetailParserOptions,
		[]string{
			//`combine_hash(BIGINT'0', COALESCE($operator$hash_code(country), BIGINT'0'))`,
			`array_join(array_agg_51, VARCHAR';')`,
			//`CAST(id_17 AS bigint)`,
			//`combine_hash(combine_hash(BIGINT'0', COALESCE($operator$hash_code(id_17), BIGINT'0')), COALESCE($operator$hash_code(name_35), BIGINT'0'))`,
		},
		[]plan_node.FunctionCall{
			{
				FunctionName: "array_join",
				Parameters: []plan_node.Value{
					&plan_node.IdentRef{Ident: "array_agg_51"},
					&plan_node.TypedValue{DataType: "VARCHAR", ValueLiteral: ";"},
				},
			},
		})
}

func TestParsePlanNodeDetail(t *testing.T) {
	testParsing[plan_node.PlanNodeDetails](t, plan_node.PlanNodeDetailParserOptions,
		[]string{`expr_4 := CAST(city_id_0 AS varchar) (8:12)
date_format_5 := date_format(CAST(CAST(period_hour_local_date AS date) AS timestamp), VARCHAR'%Y-%m-%d') (8:12)
LAYOUT: ng_public.etl_city_kpi_hourly{}
city_id_0 := city_id:bigint:1:REGULAR (8:11)
period_hour_local_date := period_hour_local_date:string:-13:PARTITION_KEY (8:11)
    :: [["2023-03-01", "2024-05-31"]]
has_order_h := has_order_h:decimal(10,2):37:REGULAR (8:11)
`},
		[]plan_node.PlanNodeDetails{
			{
				Stmts: []plan_node.PlanNodeDetailStmt{
					&plan_node.Assignment{
						Identifier: plan_node.IdentRef{Ident: "expr_4"},
						AssignedValue: &plan_node.TypeCastedValue{
							OriginalValue: &plan_node.IdentRef{Ident: "city_id_0"},
							CastedType:    plan_node.DataType{Name: "varchar"},
						},
						Loc: &plan_node.SourceLocation{
							RowNumber:    8,
							ColumnNumber: 12,
						},
					},
					&plan_node.Assignment{
						Identifier: plan_node.IdentRef{Ident: "date_format_5"},
						AssignedValue: &plan_node.FunctionCall{
							FunctionName: "date_format",
							Parameters: []plan_node.Value{
								&plan_node.TypeCastedValue{
									OriginalValue: &plan_node.TypeCastedValue{
										OriginalValue: &plan_node.IdentRef{Ident: "period_hour_local_date"},
										CastedType:    plan_node.DataType{Name: "date"},
									},
									CastedType: plan_node.DataType{Name: "timestamp"},
								},
								&plan_node.TypedValue{
									DataType:     "VARCHAR",
									ValueLiteral: "%Y-%m-%d",
								},
							},
						},
						Loc: &plan_node.SourceLocation{
							RowNumber:    8,
							ColumnNumber: 12,
						},
					},
					&plan_node.Layout{
						LayoutString: "ng_public.etl_city_kpi_hourly{}",
					},
					&plan_node.Assignment{
						Identifier: plan_node.IdentRef{Ident: "city_id_0"},
						AssignedValue: &plan_node.HiveColumnHandle{
							ColumnName:  plan_node.IdentRef{Ident: "city_id"},
							DataType:    "bigint",
							ColumnIndex: 1,
							ColumnType:  "REGULAR",
							Loc: &plan_node.SourceLocation{
								RowNumber:    8,
								ColumnNumber: 11,
							},
						},
					},
					&plan_node.Assignment{
						Identifier: plan_node.IdentRef{Ident: "period_hour_local_date"},
						AssignedValue: &plan_node.HiveColumnHandle{
							ColumnName:  plan_node.IdentRef{Ident: "period_hour_local_date"},
							DataType:    "string",
							ColumnIndex: -13,
							ColumnType:  "PARTITION_KEY",
							Loc: &plan_node.SourceLocation{
								RowNumber:    8,
								ColumnNumber: 11,
							},
							Ranges: []plan_node.Range{
								{
									LowValue: &plan_node.Marker{
										Bound: plan_node.EXACTLY,
										Value: "2023-03-01",
									},
									HighValue: &plan_node.Marker{
										Bound: plan_node.EXACTLY,
										Value: "2024-05-31",
									},
								},
							},
						},
					},
					&plan_node.Assignment{
						Identifier: plan_node.IdentRef{Ident: "has_order_h"},
						AssignedValue: &plan_node.HiveColumnHandle{
							ColumnName:  plan_node.IdentRef{Ident: "has_order_h"},
							DataType:    "decimal(10,2)",
							ColumnIndex: 37,
							ColumnType:  "REGULAR",
							Loc: &plan_node.SourceLocation{
								RowNumber:    8,
								ColumnNumber: 11,
							},
						},
					},
				},
			},
		})
}
