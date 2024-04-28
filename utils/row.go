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

func MergeColumns(a, b *Row) (ret *Row) {
	la, lb := a.ColumnCount(), b.ColumnCount()
	if la == 0 {
		return b
	}
	if lb == 0 {
		return a
	}
	l := la + b.ColumnCount()
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

func MultiplyRows(a, b []*Row) []*Row {
	multipliedRows := make([]*Row, 0, len(a)*len(b))
	for _, x := range a {
		for _, y := range b {
			multipliedRows = append(multipliedRows, MergeColumns(x, y))
		}
	}
	return multipliedRows
}
