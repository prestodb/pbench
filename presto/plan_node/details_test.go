package plan_node_test

import (
	"fmt"
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"github.com/stretchr/testify/assert"
	"pbench/presto/plan_node"
	"testing"
)

var basicLexer = lexer.MustStateful(lexer.Rules{
	"Root": {
		{"Ident", `[a-zA-Z_$][\w$]*`, nil},
		{"String", `"(\\"|[^"])*"|'(\\'|[^'])*'`, nil}, // single-quoted or double-quoted.
		{"EOL", `[\n\r]+`, nil},
		{"Int", `-?\d+`, nil},
		{"RangeStart", `::`, nil},
		{"Assign", `:=`, nil},
		{"Min", `<min>`, nil},
		{"Max", `<max>`, nil},
		{"Punctuation", `[-[!@#$%^&*()+_={}\|:;"'<,>.?/]|]`, nil},
		{"Whitespace", `[ \t]+`, nil},
	}})

func testParsing[T any](t *testing.T, testLiterals []string, expected []T) {
	t.Helper()
	if !assert.Equalf(t, len(testLiterals), len(expected), "length of testLiterals and expected should be same") {
		t.FailNow()
	}
	parser, err := participle.Build[T](
		participle.Lexer(basicLexer),
		participle.Union[plan_node.Value](plan_node.HiveColumnHandle{}, plan_node.CatchAllValue{}),
		participle.Elide("Whitespace"))
	if !assert.Nil(t, err) {
		t.FailNow()
	}
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
			`LAYOUT: schema.SH2E81713104009327{domains={car_id=[ [["1", <max>)] ], country=[ [["gh"]] ], cohort=[ [["Random Text"]] ]}}` + "\n",
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
			{
				ColumnName:  "cohort",
				DataType:    "varchar(16)",
				ColumnIndex: 2,
				ColumnType:  "REGULAR",
			},
			{
				ColumnName:  "city_id",
				DataType:    "bigint",
				ColumnIndex: -13,
				ColumnType:  "PARTITION_KEY",
				Loc: &plan_node.SourceLocation{
					RowNumber:    23,
					ColumnNumber: 6,
				},
			},
			{
				ColumnName:  "car_id",
				DataType:    "bigint",
				ColumnIndex: 0,
				ColumnType:  "REGULAR",
				Ranges: []plan_node.Range{
					{LowValue: &plan_node.Marker{Bound: plan_node.EXACTLY, Value: "1"}, HighValue: &plan_node.Marker{Bound: plan_node.BELOW}},
				},
			},
			{
				ColumnName:  "city_id",
				DataType:    "bigint",
				ColumnIndex: -13,
				ColumnType:  "PARTITION_KEY",
				Loc: &plan_node.SourceLocation{
					RowNumber:    23,
					ColumnNumber: 6,
				},
				Ranges: []plan_node.Range{
					{LowValue: &plan_node.Marker{Bound: plan_node.EXACTLY, Value: "706"}},
					{LowValue: &plan_node.Marker{Bound: plan_node.EXACTLY, Value: "1024"}, HighValue: &plan_node.Marker{Bound: plan_node.BELOW, Value: "2048"}},
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
			{LowValue: &plan_node.Marker{Bound: plan_node.EXACTLY, Value: "1"}, HighValue: &plan_node.Marker{Bound: plan_node.BELOW}},
			{LowValue: &plan_node.Marker{Bound: plan_node.ABOVE}, HighValue: &plan_node.Marker{Bound: plan_node.EXACTLY, Value: "2024"}},
			{LowValue: &plan_node.Marker{Bound: plan_node.EXACTLY, Value: "706"}},
			{LowValue: &plan_node.Marker{Bound: plan_node.EXACTLY, Value: "706"}, HighValue: &plan_node.Marker{Bound: plan_node.EXACTLY, Value: "1024"}},
		})
}

func TestParseAssignment(t *testing.T) {
	testParsing[plan_node.Assignment](t,
		[]string{
			`country := country:string:1:REGULAR (23:6)
    :: [["gh"]]`,
			"$hashvalue_109 := combine_hash(BIGINT'0', COALESCE($operator$hash_code(car_id), BIGINT'0')) (22:5)",
			"expr_3 := CAST(id AS bigint) (24:12)",
			`array_agg_51 := "presto.default.array_agg"((name_35)) ORDER BY OrderingScheme {orderBy='[Ordering {variable='name_35', sortOrder='ASC_NULLS_LAST'}]', orderings='{name_35=ASC_NULLS_LAST}'} (6:21)`,
		},
		[]plan_node.Assignment{
			{"country", plan_node.HiveColumnHandle{
				ColumnName:  "country",
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
			}},
			{"$hashvalue_109", plan_node.CatchAllValue{
				Value: "combine_hash(BIGINT'0',COALESCE($operator$hash_code(car_id),BIGINT'0'))(22:5)"}},
			{"expr_3", plan_node.CatchAllValue{
				Value: "CAST(idASbigint)(24:12)"}},
			{"array_agg_51", plan_node.CatchAllValue{
				Value: "\"presto.default.array_agg\"((name_35))ORDERBYOrderingScheme{orderBy='[Ordering {variable='name_35', sortOrder='ASC_NULLS_LAST'}]',orderings='{name_35=ASC_NULLS_LAST}'}(6:21)"}},
		})
}
