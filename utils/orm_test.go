package utils

import (
	"encoding/json"
	"reflect"
	"testing"

	"github.com/ethanyzhang/presto-go/query_json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type simpleOrmStruct struct {
	ID   string `table_a:"id"`
	Name string `table_a:"name" table_b:"full_name"`
	Age  int    `table_b:"age"`
}

func TestCollectRowsForEachTable_Simple(t *testing.T) {
	obj := simpleOrmStruct{ID: "1", Name: "Alice", Age: 30}
	v := reflect.ValueOf(obj)
	result, _ := collectRowsForEachTable(v, "table_a", "table_b")

	// table_a should have 1 row with 2 columns (id, name)
	assert.Equal(t, 1, len(result["table_a"]))
	assert.Equal(t, 2, result["table_a"][0].ColumnCount())
	assert.Equal(t, []string{"id", "name"}, result["table_a"][0].ColumnNames)
	assert.Equal(t, []any{"1", "Alice"}, result["table_a"][0].Values)

	// table_b should have 1 row with 2 columns (full_name, age)
	assert.Equal(t, 1, len(result["table_b"]))
	assert.Equal(t, 2, result["table_b"][0].ColumnCount())
	assert.Equal(t, []string{"full_name", "age"}, result["table_b"][0].ColumnNames)
	assert.Equal(t, []any{"Alice", 30}, result["table_b"][0].Values)
}

type nestedOrmStruct struct {
	ID    string `table_x:"id"`
	Inner simpleOrmStruct
}

func TestCollectRowsForEachTable_Nested(t *testing.T) {
	obj := nestedOrmStruct{
		ID:    "outer-1",
		Inner: simpleOrmStruct{ID: "inner-1", Name: "Bob", Age: 25},
	}
	v := reflect.ValueOf(obj)
	result, _ := collectRowsForEachTable(v, "table_x", "table_a")

	// table_x has only the outer ID
	assert.Equal(t, 1, len(result["table_x"]))
	assert.Equal(t, 1, result["table_x"][0].ColumnCount())
	assert.Equal(t, "outer-1", result["table_x"][0].Values[0])

	// table_a has inner ID and Name via nested struct traversal
	assert.Equal(t, 1, len(result["table_a"]))
	assert.Contains(t, result["table_a"][0].ColumnNames, "id")
	assert.Contains(t, result["table_a"][0].ColumnNames, "name")
}

type sliceOrmStruct struct {
	Name  string `table_s:"name"`
	Items []simpleOrmStruct
}

type sliceOrmStructSharedTag struct {
	ParentID string `table_a:"parent_id"`
	Items    []simpleOrmStruct
}

func TestCollectRowsForEachTable_Slice(t *testing.T) {
	obj := sliceOrmStruct{
		Name: "parent",
		Items: []simpleOrmStruct{
			{ID: "1", Name: "A", Age: 10},
			{ID: "2", Name: "B", Age: 20},
		},
	}
	v := reflect.ValueOf(obj)
	result, _ := collectRowsForEachTable(v, "table_s", "table_a")

	// table_s has 1 row (only Name field), inner Items don't have table_s tags
	assert.Equal(t, 1, len(result["table_s"]))
	assert.Equal(t, "parent", result["table_s"][0].Values[0])

	// table_a has 2 rows from the slice
	assert.Equal(t, 2, len(result["table_a"]))
}

func TestCollectRowsForEachTable_SliceCartesianProduct(t *testing.T) {
	// When parent and children both contribute to the same table, it's a cartesian product
	obj := sliceOrmStructSharedTag{
		ParentID: "p1",
		Items: []simpleOrmStruct{
			{ID: "1", Name: "A", Age: 10},
			{ID: "2", Name: "B", Age: 20},
		},
	}
	v := reflect.ValueOf(obj)
	result, _ := collectRowsForEachTable(v, "table_a")

	// table_a: parent_id="p1" × 2 items = 2 rows, each with parent_id + id + name columns
	assert.Equal(t, 2, len(result["table_a"]))
	for _, row := range result["table_a"] {
		assert.Contains(t, row.ColumnNames, "parent_id")
		assert.Contains(t, row.ColumnNames, "id")
	}
}

func TestCollectRowsForEachTable_JsonRawMessage(t *testing.T) {
	type withJson struct {
		Data json.RawMessage `table_j:"data"`
	}
	obj := withJson{Data: json.RawMessage(`{ "key" :  "value" }`)}
	v := reflect.ValueOf(obj)
	result, _ := collectRowsForEachTable(v, "table_j")

	assert.Equal(t, 1, len(result["table_j"]))
	// JSON should be compacted
	assert.Equal(t, `{"key":"value"}`, result["table_j"][0].Values[0])
}

func TestCollectRowsForEachTable_Duration(t *testing.T) {
	type withDuration struct {
		Elapsed query_json.Duration `table_d:"elapsed_ms"`
	}
	var dur query_json.Duration
	err := json.Unmarshal([]byte(`"5.00s"`), &dur)
	require.NoError(t, err)
	obj := withDuration{Elapsed: dur}
	v := reflect.ValueOf(obj)
	result, _ := collectRowsForEachTable(v, "table_d")

	assert.Equal(t, 1, len(result["table_d"]))
	// Duration should be converted to milliseconds
	assert.Equal(t, int64(5000), result["table_d"][0].Values[0])
}

func TestCollectRowsForEachTable_NilPointer(t *testing.T) {
	type withPtr struct {
		Name *string `table_p:"name"`
	}
	obj := withPtr{Name: nil}
	v := reflect.ValueOf(obj)
	result, _ := collectRowsForEachTable(v, "table_p")
	// Nil pointer fields are skipped
	assert.Equal(t, 0, len(result["table_p"]))
}

func TestCollectRowsForEachTable_NoMatchingTables(t *testing.T) {
	obj := simpleOrmStruct{ID: "1", Name: "Alice", Age: 30}
	v := reflect.ValueOf(obj)
	result, _ := collectRowsForEachTable(v, "nonexistent_table")
	assert.Equal(t, 0, len(result))
}

func TestMergeRowsMap(t *testing.T) {
	a := map[TableName][]*Row{
		"t1": {func() *Row {
			r := NewRowWithColumnCapacity(1)
			r.AddColumn("a", 1)
			return r
		}()},
	}
	b := map[TableName][]*Row{
		"t1": {func() *Row {
			r := NewRowWithColumnCapacity(1)
			r.AddColumn("b", "x")
			return r
		}(), func() *Row {
			r := NewRowWithColumnCapacity(1)
			r.AddColumn("b", "y")
			return r
		}()},
	}
	result, _ := MergeRowsMap(a, b)
	// t1: 1 row * 2 rows = 2 rows (cartesian product)
	assert.Equal(t, 2, len(result["t1"]))
}

func TestMergeRowsMap_CartesianProductLimit(t *testing.T) {
	// Build a map where the product exceeds MaxCartesianProductSize
	largeA := make([]*Row, 1000)
	for i := range largeA {
		r := NewRowWithColumnCapacity(1)
		r.AddColumn("a", i)
		largeA[i] = r
	}
	largeB := make([]*Row, 1000)
	for i := range largeB {
		r := NewRowWithColumnCapacity(1)
		r.AddColumn("b", i)
		largeB[i] = r
	}
	a := map[TableName][]*Row{"t": largeA}
	b := map[TableName][]*Row{"t": largeB}
	// 1000 * 1000 = 1_000_000 > MaxCartesianProductSize (100_000)
	_, err := MergeRowsMap(a, b)
	assert.Error(t, err)
	assert.Contains(t, err.Error(), "exceeds limit")
}

func TestMergeColumns_NoAliasing(t *testing.T) {
	// When one row is empty, MergeColumns must return a new copy, not a shared reference.
	// Otherwise AddColumn on the result would mutate the original row.
	empty := NewRowWithColumnCapacity(0)
	b := NewRowWithColumnCapacity(1)
	b.AddColumn("col", "val")

	merged := MergeColumns(empty, b)
	// Mutating merged must not affect b
	merged.AddColumn("extra", "extra_val")
	assert.Equal(t, 1, b.ColumnCount(), "original row should not be mutated")
	assert.Equal(t, 2, merged.ColumnCount())
}

func TestMergeColumns_BothEmpty(t *testing.T) {
	a := NewRowWithColumnCapacity(0)
	b := NewRowWithColumnCapacity(0)
	merged := MergeColumns(a, b)
	assert.Equal(t, 0, merged.ColumnCount())
}

func TestMergeRowsMap_NewTable(t *testing.T) {
	a := map[TableName][]*Row{}
	b := map[TableName][]*Row{
		"new_table": {func() *Row {
			r := NewRowWithColumnCapacity(1)
			r.AddColumn("col", "val")
			return r
		}()},
	}
	result, _ := MergeRowsMap(a, b)
	// When a has no rows for a table, MergeRowsMap creates a default empty row and multiplies
	assert.Equal(t, 1, len(result["new_table"]))
}
