package utils

type Row struct {
	ColumnNames []string
	Values      []any
}

func (r *Row) ColumnCount() int {
	if len(r.ColumnNames) != len(r.Values) {
		panic("invalid state")
	}
	return len(r.ColumnNames)
}

func (r *Row) AddColumn(name string, value any) {
	r.ColumnNames = append(r.ColumnNames, name)
	r.Values = append(r.Values, value)
}

func NewRowWithColumnCapacity(numColumns int) *Row {
	return &Row{
		ColumnNames: make([]string, 0, numColumns),
		Values:      make([]any, 0, numColumns),
	}
}

// MergeColumns concatenates the columns of two rows into a single new row.
// Used by MultiplyRows to build each row of a cartesian product.
// Always returns a new Row to avoid aliasing mutations between callers.
func MergeColumns(a, b *Row) (ret *Row) {
	la, lb := a.ColumnCount(), b.ColumnCount()
	l := la + lb
	if l == 0 {
		return NewRowWithColumnCapacity(0)
	}
	ret = &Row{
		ColumnNames: make([]string, l),
		Values:      make([]any, l),
	}
	copy(ret.ColumnNames, a.ColumnNames)
	copy(ret.Values, a.Values)
	copy(ret.ColumnNames[la:], b.ColumnNames)
	copy(ret.Values[la:], b.Values)
	return
}

// MultiplyRows computes the cartesian product of two row sets. Every row in a is paired with every
// row in b, and each pair is merged into a single row containing columns from both. This models SQL
// denormalization: parent columns (a) are repeated for each child row (b).
//
// Example: a has 1 row [parent_id=p1], b has 2 rows [id=1, id=2]
//
//	→ result: [{parent_id=p1, id=1}, {parent_id=p1, id=2}]
func MultiplyRows(a, b []*Row) []*Row {
	multipliedRows := make([]*Row, 0, len(a)*len(b))
	for _, x := range a {
		for _, y := range b {
			multipliedRows = append(multipliedRows, MergeColumns(x, y))
		}
	}
	return multipliedRows
}
