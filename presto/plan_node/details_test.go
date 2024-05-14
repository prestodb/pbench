package plan_node_test

import (
	"github.com/alecthomas/participle/v2"
	"github.com/stretchr/testify/assert"
	"pbench/presto/plan_node"
	"strconv"
	"testing"
)

func TestParseLayout(t *testing.T) {
	testLiteral := `LAYOUT: schema.table{domains={id=[ [["706"]] ]}}`
	parser, err := participle.Build[plan_node.Layout]()
	assert.Nil(t, err)
	ast, parseErr := parser.ParseString("", testLiteral)
	assert.Nil(t, parseErr)
	assert.Equal(t, `schema.table{domains={id=[[["706"]]]}}`, ast.LayoutString)
}

func TestParseHiveColumnHandle(t *testing.T) {
	testLiterals := []string{
		"cohort:varchar(16):2:REGULAR",
		"city_id:bigint:-13:PARTITION_KEY (23:6)",
	}
	expectedHandles := []plan_node.HiveColumnHandle{
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
	}
	parser, err := participle.Build[plan_node.HiveColumnHandle]()
	assert.Nil(t, err)
	for i, literal := range testLiterals {
		ast, parseErr := parser.ParseString(strconv.Itoa(i), literal)
		assert.Nil(t, parseErr)
		assert.Equal(t, expectedHandles[i], *ast)
	}
}
