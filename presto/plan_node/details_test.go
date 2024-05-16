package plan_node_test

import (
	"fmt"
	"github.com/alecthomas/participle/v2"
	"github.com/stretchr/testify/assert"
	"pbench/presto/plan_node"
	"testing"
)

func testParsing[T any](t *testing.T, testLiterals []string, expected []T) {
	t.Helper()
	if !assert.Equalf(t, len(testLiterals), len(expected), "length of testLiterals and expected should be same") {
		t.FailNow()
	}
	parser := participle.MustBuild[T](plan_node.PlanNodeDetailParserOptions...)
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
	testParsing[plan_node.Layout](t,
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
	testParsing[plan_node.HiveColumnHandle](t,
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
	testParsing[plan_node.TypedValue](t,
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
	testParsing[plan_node.Range](t,
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
	testParsing[plan_node.Assignment](t,
		[]string{
			`$hashvalue_108 := $hashvalue_109`,
			`country := country:string:1:REGULAR (23:6)
    :: [["gh"]]`,
			"$hashvalue_109 := combine_hash(BIGINT'0', COALESCE($operator$hash_code(car_id), BIGINT'0')) (22:5)",
			"expr_3 := CAST(id AS bigint) (24:12)",
			`array_agg_51 := "presto.default.array_agg"((name_35)) ORDER BY OrderingScheme {orderBy='[Ordering {variable='name_35', sortOrder='ASC_NULLS_LAST'}]', orderings='{name_35=ASC_NULLS_LAST}'} (6:21)`,
			`branded_car_enrollment.target_id := car_id (22:5)`,
		},
		[]plan_node.Assignment{
			{ // case 0
				Identifier:    plan_node.IdentRef{Ident: "$hashvalue_108"},
				AssignedValue: plan_node.IdentRef{Ident: "$hashvalue_109"},
			},
			{ // case 1
				Identifier: plan_node.IdentRef{Ident: "country"},
				AssignedValue: plan_node.HiveColumnHandle{
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
				AssignedValue: plan_node.FunctionCall{
					FunctionName: "combine_hash",
					Parameters: []plan_node.Value{
						plan_node.TypedValue{
							DataType:     "BIGINT",
							ValueLiteral: "0",
						},
						plan_node.FunctionCall{
							FunctionName: "COALESCE",
							Parameters: []plan_node.Value{
								plan_node.FunctionCall{
									FunctionName: "$operator$hash_code",
									Parameters: []plan_node.Value{
										plan_node.IdentRef{Ident: "car_id"},
									},
								},
								plan_node.TypedValue{
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
				AssignedValue: plan_node.TypeCastedValue{
					OriginalValue: plan_node.IdentRef{Ident: "id"},
					CastedType:    "bigint",
				},
				Loc: &plan_node.SourceLocation{
					RowNumber:    24,
					ColumnNumber: 12,
				},
			},
			{ // case 4
				Identifier: plan_node.IdentRef{Ident: "array_agg_51"},
				AssignedValue: plan_node.CatchAllValue{
					Value: "\"presto.default.array_agg\"((name_35))ORDERBYOrderingScheme{orderBy='[Ordering {variable='name_35', sortOrder='ASC_NULLS_LAST'}]',orderings='{name_35=ASC_NULLS_LAST}'}(6:21)"},
				Loc: nil,
			},
			{ // case 5
				Identifier:    plan_node.IdentRef{Ident: "branded_car_enrollment.target_id"},
				AssignedValue: plan_node.IdentRef{Ident: "car_id"},
				Loc: &plan_node.SourceLocation{
					RowNumber:    22,
					ColumnNumber: 5,
				},
			},
		})
}

func TestIdent(t *testing.T) {
	testParsing[plan_node.IdentRef](t,
		[]string{
			"branded_car_enrollment.target_id",
		},
		[]plan_node.IdentRef{
			{Ident: "branded_car_enrollment.target_id"},
		})
}

func TestParseFunctionCall(t *testing.T) {
	testParsing[plan_node.FunctionCall](t,
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
					plan_node.IdentRef{Ident: "array_agg_51"},
					plan_node.TypedValue{DataType: "VARCHAR", ValueLiteral: ";"},
				},
			},
		})
}
