package plan_node

import (
	"fmt"
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"strings"
)

type (
	// Value is a union type interface for all possible value types in an assignment
	Value interface {
		value()
		String() string
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
			&HiveColumnHandle{},
			&FunctionCall{},
			&IdentRef{},
			&TypedValue{},
			&TypeCastedValue{},
			&MathExpr{},
			&CatchAllValue{},
		),
		participle.Union[PlanNodeDetailStmt](
			&Layout{},
			&Distribution{},
			&HiveColumnHandle{},
			&Assignment{},
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
	Table       *HiveTableHandle
}

func (h *HiveColumnHandle) value() {}
func (h *HiveColumnHandle) stmt()  {}
func (h *HiveColumnHandle) String() string {
	return fmt.Sprintf("%s.%s", h.Table.String(), h.ColumnName.Ident)
}

type CatchAllValue struct {
	Value string `parser:"@(~EOL)+"`
}

func (c *CatchAllValue) value() {}
func (c *CatchAllValue) String() string {
	return c.Value
}

type IdentRef struct {
	Ident string `parser:"@Ident"`
}

func (r *IdentRef) value() {}
func (r *IdentRef) String() string {
	return r.Ident
}

type TypedValue struct {
	DataType     string         `parser:"@DataType"`
	ValueLiteral UnquotedString `parser:"@String"`
}

func (v *TypedValue) value() {}
func (v *TypedValue) String() string {
	return fmt.Sprintf("%s '%s'", v.DataType, v.ValueLiteral)
}

type Layout struct {
	LayoutString string `parser:"'LAYOUT' ':' @(~EOL)+"`
}

func (l *Layout) stmt() {}

type Distribution struct {
	DistributionString string `parser:"'Distribution' ':' @(~EOL)+"`
}

func (d *Distribution) stmt() {}

type FunctionCall struct {
	FunctionName string  `parser:"(?! Ident '(' Int) @Ident '('"`
	Parameters   []Value `parser:"(')' | @@ (',' @@)* ')')"`
}

func (f *FunctionCall) value() {}
func (f *FunctionCall) String() string {
	b := strings.Builder{}
	for i, param := range f.Parameters {
		if i > 0 {
			b.WriteString(", ")
		}
		b.WriteString(param.String())
	}
	return fmt.Sprintf("%s(%s)", f.FunctionName, b.String())
}

type TypeCastedValue struct {
	OriginalValue Value  `parser:"Cast '(' @@ 'AS'"`
	CastedType    string `parser:"@DataType ')'"`
}

func (v *TypeCastedValue) value() {}
func (v *TypeCastedValue) String() string {
	return fmt.Sprintf("CAST(%s AS %s)", v.OriginalValue.String(), v.CastedType)
}

type Assignment struct {
	Identifier    IdentRef        `parser:"@@ Assign"`
	AssignedValue Value           `parser:"@@"`
	Loc           *SourceLocation `parser:"@@?"`
}

func (a *Assignment) stmt() {}

type PlanNodeDetails struct {
	Stmts []PlanNodeDetailStmt `parser:"(@@ EOL)+"`
}

type MathExpr struct {
	LeftOp  Value  `parser:"'(' @@ ')'"`
	Op      string `parser:"@('+' | '-' | '*' | '/' | '%')"`
	RightOp Value  `parser:"'(' @@ ')'"`
}

func (m *MathExpr) value() {}
func (m *MathExpr) String() string {
	return fmt.Sprintf("(%s) %s (%s)", m.LeftOp.String(), m.Op, m.RightOp.String())
}
