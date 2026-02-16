package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestNewRowWithColumnCapacity(t *testing.T) {
	row := NewRowWithColumnCapacity(4)
	assert.NotNil(t, row)
	assert.Equal(t, 0, len(row.ColumnNames))
	assert.Equal(t, 0, len(row.Values))
	assert.Equal(t, 4, cap(row.ColumnNames))
	assert.Equal(t, 4, cap(row.Values))
}

func TestRowAddColumnAndColumnCount(t *testing.T) {
	row := NewRowWithColumnCapacity(2)
	row.AddColumn("name", "alice")
	row.AddColumn("age", 30)
	assert.Equal(t, 2, row.ColumnCount())
	assert.Equal(t, []string{"name", "age"}, row.ColumnNames)
	assert.Equal(t, []any{"alice", 30}, row.Values)
}

func TestRowColumnCountPanicsOnMismatch(t *testing.T) {
	row := &Row{
		ColumnNames: []string{"a", "b"},
		Values:      []any{1},
	}
	assert.Panics(t, func() { row.ColumnCount() })
}

func TestMergeColumns(t *testing.T) {
	t.Run("both non-empty", func(t *testing.T) {
		a := NewRowWithColumnCapacity(2)
		a.AddColumn("x", 1)
		a.AddColumn("y", 2)
		b := NewRowWithColumnCapacity(1)
		b.AddColumn("z", 3)

		merged := MergeColumns(a, b)
		assert.Equal(t, 3, merged.ColumnCount())
		assert.Equal(t, []string{"x", "y", "z"}, merged.ColumnNames)
		assert.Equal(t, []any{1, 2, 3}, merged.Values)
	})

	t.Run("a is empty", func(t *testing.T) {
		a := NewRowWithColumnCapacity(0)
		b := NewRowWithColumnCapacity(1)
		b.AddColumn("z", 3)

		merged := MergeColumns(a, b)
		assert.Equal(t, b, merged)
	})

	t.Run("b is empty", func(t *testing.T) {
		a := NewRowWithColumnCapacity(1)
		a.AddColumn("x", 1)
		b := NewRowWithColumnCapacity(0)

		merged := MergeColumns(a, b)
		assert.Equal(t, a, merged)
	})
}

func TestMultiplyRows(t *testing.T) {
	t.Run("cartesian product", func(t *testing.T) {
		a1 := NewRowWithColumnCapacity(1)
		a1.AddColumn("a", 1)
		a2 := NewRowWithColumnCapacity(1)
		a2.AddColumn("a", 2)

		b1 := NewRowWithColumnCapacity(1)
		b1.AddColumn("b", "x")
		b2 := NewRowWithColumnCapacity(1)
		b2.AddColumn("b", "y")
		b3 := NewRowWithColumnCapacity(1)
		b3.AddColumn("b", "z")

		result := MultiplyRows([]*Row{a1, a2}, []*Row{b1, b2, b3})
		assert.Equal(t, 6, len(result))
		// Verify the first and last combinations
		assert.Equal(t, []string{"a", "b"}, result[0].ColumnNames)
		assert.Equal(t, []any{1, "x"}, result[0].Values)
		assert.Equal(t, []any{2, "z"}, result[5].Values)
	})

	t.Run("single row each", func(t *testing.T) {
		a := NewRowWithColumnCapacity(1)
		a.AddColumn("id", 1)
		b := NewRowWithColumnCapacity(1)
		b.AddColumn("val", "hello")

		result := MultiplyRows([]*Row{a}, []*Row{b})
		assert.Equal(t, 1, len(result))
		assert.Equal(t, 2, result[0].ColumnCount())
	})

	t.Run("empty slices", func(t *testing.T) {
		result := MultiplyRows([]*Row{}, []*Row{})
		assert.Equal(t, 0, len(result))
	})
}
