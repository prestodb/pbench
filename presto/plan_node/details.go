package plan_node

import (
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type (
	// Value is a union type interface for all possible value types in an assignment
	Value interface {
		value()
	}
	PlanNodeDetailStmt interface {
		stmt()
	}
	Bound uint8
	// UnquotedString will trim the surrounded quotation marks when we convert the string token to the actual value.
	UnquotedString string
)

var (
	planNodeDetailLexer = lexer.MustStateful(lexer.Rules{
		"Root": {
			{"DataType", `(?i)BIGINT|INTEGER|SMALLINT|TINYINT|REAL|DECIMAL|VARCHAR|DATE`, nil},
			{"Cast", `(?i)CAST`, nil},
			{"As", `(?i)AS`, nil},
			{"Ident", `[a-zA-Z_$][\w.$]*`, nil},
			{"Int", `-?\d+`, nil},
			{"Assign", `:=`, nil},
			{"RangeStart", `::`, nil},
			{"Min", `<min>`, nil},
			{"Max", `<max>`, nil},
			{"String", `"(\\"|[^"])*"|'(\\'|[^'])*'`, nil}, // single-quoted or double-quoted.
			{"EOL", `[\n\r]`, nil},
			{"Punctuation", `[-[!@#$%^&*()+_={}\|:;"'<,>.?/]|]`, nil},
			{"Whitespace", `[ \t]`, nil},
		}})
	PlanNodeDetailParserOptions = []participle.Option{
		participle.Lexer(planNodeDetailLexer),
		participle.CaseInsensitive("DataType", "Cast", "As"),
		participle.Elide("Whitespace"),
		participle.Union[Value](
			HiveColumnHandle{},
			FunctionCall{},
			IdentRef{},
			TypedValue{},
			TypeCastedValue{},
			CatchAllValue{},
		),
		participle.Union[PlanNodeDetailStmt](
			Layout{},
			Distribution{},
			HiveColumnHandle{},
			Assignment{},
		),
	}
	PlanNodeDetailParser = participle.MustBuild[PlanNodeDetails](PlanNodeDetailParserOptions...)
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
	ColumnName  IdentRef        `parser:"@@ ':'"`
	DataType    string          `parser:"@(~':')+ ':'"`
	ColumnIndex int             `parser:"@Int ':'"`
	ColumnType  string          `parser:"@('REGULAR' | 'PARTITION_KEY' | 'SYNTHESIZED' | 'AGGREGATED')"`
	Loc         *SourceLocation `parser:"@@?"`
	Ranges      []Range         `parser:"(EOL RangeStart '[' (@@ ','?)+ ']')?"`
}

func (h HiveColumnHandle) value() {}
func (h HiveColumnHandle) stmt()  {}

type CatchAllValue struct {
	Value string `parser:"@(~EOL)+"`
}

func (c CatchAllValue) value() {}

type IdentRef struct {
	Ident string `parser:"@Ident"`
}

func (r IdentRef) value() {}

type TypedValue struct {
	DataType     string         `parser:"@DataType"`
	ValueLiteral UnquotedString `parser:"@String"`
}

func (v TypedValue) value() {}

type Layout struct {
	LayoutString string `parser:"'LAYOUT' ':' @(~EOL)+"`
}

func (l Layout) stmt() {}

type Distribution struct {
	DistributionString string `parser:"'Distribution' ':' @(~EOL)+"`
}

func (d Distribution) stmt() {}

type FunctionCall struct {
	FunctionName string  `parser:"(?! Ident '(' Int) @Ident '('"`
	Parameters   []Value `parser:"(')' | @@ (',' @@)* ')')"`
}

func (f FunctionCall) value() {}

type TypeCastedValue struct {
	OriginalValue Value  `parser:"Cast '(' @@ 'AS'"`
	CastedType    string `parser:"@DataType ')'"`
}

func (v TypeCastedValue) value() {}

type Assignment struct {
	Identifier    IdentRef        `parser:"@@ Assign"`
	AssignedValue Value           `parser:"@@"`
	Loc           *SourceLocation `parser:"@@?"`
}

func (a Assignment) stmt() {}

type PlanNodeDetails struct {
	Stmts []PlanNodeDetailStmt `parser:"(@@ EOL)+"`
}
