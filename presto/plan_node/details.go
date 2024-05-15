package plan_node

type (
	// Value is a union type interface for all possible value types in an assignment
	Value interface {
		value()
	}
	Bound uint8
	// UnquotedString will trim the surrounded quotation marks when we convert the string token to the actual value.
	UnquotedString string
)

// See presto-common/src/main/java/com/facebook/presto/common/predicate/Marker.java
type Marker struct {
	Bound Bound
	Value UnquotedString
}

type Range struct {
	LowValue  *Marker `parser:"@(('[' | '(') (String | Min))"`
	HighValue *Marker `parser:"(']' | (',' @((String | Max) (']' | ')'))))"`
}

type SourceLocation struct {
	RowNumber    int `parser:"'(' @Int ':'"`
	ColumnNumber int `parser:"@Int ')'"`
}

type HiveColumnHandle struct {
	ColumnName  string          `parser:"@Ident ':'"`
	DataType    string          `parser:"@(~':')+ ':'"`
	ColumnIndex int             `parser:"@Int ':'"`
	ColumnType  string          `parser:"@('REGULAR' | 'PARTITION_KEY' | 'SYNTHESIZED' | 'AGGREGATED')"`
	Loc         *SourceLocation `parser:"@@?"`
	Ranges      []Range         `parser:"(EOL RangeStart '[' (@@ ','?)+ ']')?"`
}

func (h HiveColumnHandle) value() {}

type CatchAllValue struct {
	Value string `parser:"@(~EOL)+ EOL?"`
}

func (c CatchAllValue) value() {}

type TypedValue struct {
	DataType     string         `parser:"@('BIGINT' | 'INTEGER' | 'SMALLINT' | 'TINYINT' | 'REAL' | 'DECIMAL')"`
	ValueLiteral UnquotedString `parser:"@String"`
}

type Layout struct {
	LayoutString string `parser:"'LAYOUT' ':' @(~EOL)+ EOL?"`
}

type Assignment struct {
	Identifier    string `parser:"@Ident Assign"`
	AssignedValue Value  `parser:"@@"`
}
