package plan_node

import (
	"fmt"
	"slices"
	"strings"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

type (
	// Value is a union type interface for all possible value types in an assignment
	Value interface {
		value()
		String() string
		GetAssignments() []string
	}
	PlanNodeDetailStmt interface {
		stmt()
	}
	Bound uint8
	// UnquotedString will trim the surrounded quotation marks when we convert the string token to the actual value.
	UnquotedString string

	WhenCond interface {
		cond()
	}
	WhenExp interface {
		when()
	}
)

var (
	planNodeDetailLexer = lexer.MustStateful(lexer.Rules{
		"Root": {
			{Name: "Keyword", Pattern: `\b(CAST|SWITCH|WHEN)\b`, Action: nil},
			{Name: "Ident", Pattern: `[a-zA-Z_$][\w.$]*`, Action: nil},
			{Name: "String", Pattern: `"(\\"|[^"])*"|'(\\'|[^'])*'`, Action: nil}, // single-quoted or double-quoted.
			{Name: "Int", Pattern: `-?\d+`, Action: nil},
			{Name: "Assign", Pattern: `:=`, Action: nil},
			{Name: "RangeStart", Pattern: `::`, Action: nil},
			{Name: "Min", Pattern: `<min>`, Action: nil},
			{Name: "Max", Pattern: `<max>`, Action: nil},
			{Name: "EOL", Pattern: `[\n\r]`, Action: nil},
			{Name: `Comparison`, Pattern: `<>|!=|<=|>=|=|>|<`, Action: nil},
			{Name: `Operators`, Pattern: `[-+*/%.()]`, Action: nil},
			{Name: "Punctuation", Pattern: `[-[!@#$%^&*()+_={}\|:;"'<,>.?/]|]`, Action: nil},
			{Name: "whitespace", Pattern: `[ \t]`, Action: nil},
		}})
	PlanNodeDetailParserOptions = []participle.Option{
		participle.Lexer(planNodeDetailLexer),
		// participle.CaseInsensitive("Keyword"),
		participle.Union[Value](
			&HiveColumnHandle{},
			&FunctionCall{},
			&IdentRef{},
			&TypedValue{},
			&TypeCastedValue{},
			&Switch{},
			&MathExpr{},
			&CatchAllValue{},
		),
		participle.Union[PlanNodeDetailStmt](
			&Layout{},
			&Distribution{},
			&HiveColumnHandle{},
			&Assignment{},
			&UnKnownRange{},
			&CatchAllValue{},
		),
		participle.Union[WhenExp](&CompareWhenExp{}, &AndOrWhenExp{}, &LikeWhenExp{}, &BooleanWhenExp{}),
	}
	PlanNodeDetailParser = participle.MustBuild[PlanNodeDetails](PlanNodeDetailParserOptions...)
)

// See presto-common/src/main/java/com/facebook/presto/common/predicate/Marker.java
type Marker struct {
	Bound Bound          `json:"bound,omitempty"`
	Value UnquotedString `json:"value,omitempty"`
}

type Range struct {
	Null      string  `parser:"( 'NULL'" json:"null,omitempty"`
	LowValue  *Marker `parser:"| @( ('[' | '(') (String | Min) )" json:"lowValue,omitempty"`
	HighValue *Marker `parser:"( ']' | (',' @((String | Max) (']' | ')'))) ) )" json:"highValue,omitempty"`
}

type SourceLocation struct {
	RowNumber    int `parser:"'(' @Int ':'" json:"rowNumber"`
	ColumnNumber int `parser:"@Int ')'" json:"columnNumber"`
}

type HiveColumnHandle struct {
	ColumnName  IdentRef         `parser:"@@ ':'" json:"columnName"`
	DataType    string           `parser:"@(~':')+ ':'" json:"dataType"`
	ColumnIndex int              `parser:"@Int ':'" json:"columnIndex"`
	ColumnType  string           `parser:"@('REGULAR' | 'PARTITION_KEY' | 'SYNTHESIZED' | 'AGGREGATED')" json:"columnType"`
	Loc         *SourceLocation  `parser:"@@?" json:"loc,omitempty"`
	Ranges      []Range          `parser:"(EOL RangeStart '[' @@ (',' @@)* ']')?" json:"range,omitempty"`
	Table       *HiveTableHandle `parser:"" json:"table,omitempty"`
}

func (h *HiveColumnHandle) value() {}

func (h *HiveColumnHandle) stmt() {}

func (h *HiveColumnHandle) String() string {
	return fmt.Sprintf("%s.%s", h.Table.String(), h.ColumnName.Ident)
}

func (*HiveColumnHandle) GetAssignments() []string {
	return nil
}

type UnKnownRange struct {
	Value string `parser:"RangeStart '[' @(~EOL)+ ']'"`
}

func (*UnKnownRange) stmt() {}

type CatchAllValue struct {
	Value string `parser:"@(~EOL)+" json:"value"`
}

func (c *CatchAllValue) value() {}

func (*CatchAllValue) stmt() {}

func (c *CatchAllValue) String() string {
	return c.Value
}

func (*CatchAllValue) GetAssignments() []string {
	return nil
}

type IdentRef struct {
	Ident string `parser:"(?! (Ident+ String | Ident 'WITH')) @Ident" json:"ident"`
}

func (r *IdentRef) value() {}

func (r *IdentRef) String() string {
	return r.Ident
}

func (r *IdentRef) GetAssignments() []string {
	return []string{r.Ident}
}

type TypedValue struct {
	DataType     string         `parser:"(?! Ident '(') @(Ident)+" json:"dataType"`
	ValueLiteral UnquotedString `parser:"@String" json:"valueLiteral"`
}

func (v *TypedValue) value() {}

func (v *TypedValue) String() string {
	return fmt.Sprintf("%s '%s'", v.DataType, v.ValueLiteral)
}

func (v *TypedValue) GetAssignments() []string {
	return nil
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
	FunctionName string   `parser:"(?! (Ident '(' Int | Ident ',')) @Ident '('" json:"functionName"`
	Parameters   []Value  `parser:"(')' | @@ (',' @@)* ')')" json:"parameters,omitempty"`
	Options      []string `parser:"(@'RANGE' @Ident*)?" json:"options,omitempty"`
}

func (*FunctionCall) value() {}

func (*FunctionCall) exp() {}

func (*FunctionCall) when() {}

func (f *FunctionCall) GetJoinPredicates() []JoinPredicate {
	return nil
}

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

func (f *FunctionCall) GetAssignments() []string {
	var rev []string
	for _, param := range f.Parameters {
		rev = append(rev, param.GetAssignments()...)
	}
	return rev
}

type TypeCastedValue struct {
	OriginalValue Value    `parser:"'CAST' '(' @@ 'AS'" json:"originalValue"`
	CastedType    DataType `parser:"@@ ')'" json:"castedType"`
}

func (v *TypeCastedValue) value() {}

func (v *TypeCastedValue) String() string {
	return fmt.Sprintf("CAST(%s AS %v)", v.OriginalValue.String(), v.CastedType)
}

func (v *TypeCastedValue) GetAssignments() []string {
	return v.OriginalValue.GetAssignments()
}

type CompareWhenExp struct {
	Left  Value  `parser:"(?= '(' @@ ')' Comparison) '(' @@ ')'"`
	Op    string `parser:"@Comparison"`
	Right Value  `parser:"'(' @@ ')'"`
}

func (*CompareWhenExp) when() {}

type LikeWhenExp struct {
	Left  Value      `parser:"(?= @@ 'LIKE') @@ 'LIKE'"`
	Right TypedValue `parser:" @@"`
}

func (*LikeWhenExp) when() {}

type BooleanWhenExp struct {
	Eval Value `parser:"@@"`
}

func (*BooleanWhenExp) when() {}

type AndOrWhenExp struct {
	Left  WhenExp `parser:"(?= '(' @@ ')' ('AND'|'OR')) '(' @@ ')'"`
	Op    string  `parser:"@Ident"`
	Right WhenExp `parser:"'(' @@ ')'"`
}

func (*AndOrWhenExp) when() {}

type SwitchWhen struct {
	Exp   WhenExp `parser:"@@ ','"`
	Value Value   `parser:"@@"`
}

type Switch struct {
	DataType     Value        `parser:"'SWITCH' '(' @@"`
	When         []SwitchWhen `parser:"( ','  'WHEN' '(' @@ ')' )+"`
	DefaultValue Value        `parser:"',' @@ ')'"`
}

func (*Switch) value() {}

func (v *Switch) String() string {
	return fmt.Sprintf("SWITCH(%v, %v, %v)", v.DataType, v.When, v.DefaultValue)
}

func (*Switch) GetAssignments() []string {
	return nil
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
	Left  Value  `parser:"(?= '(' @@ ')' Operators) '(' @@ ')'"`
	Op    string `parser:"@Operators"`
	Right Value  `parser:"'(' @@ ')'"`
}

func (m *MathExpr) value() {}

func (m *MathExpr) String() string {
	return fmt.Sprintf("(%s) %s (%s)", m.Left.String(), m.Op, m.Right.String())
}

func (m *MathExpr) GetAssignments() []string {
	return slices.Concat(m.Left.GetAssignments(), m.Right.GetAssignments())
}

type DataType struct {
	Name   string `parser:"@(Ident)+" json:"name"`
	Option string `parser:"( '(' @Int (@',' @Int)? ')' )?" json:"option,omitempty"`
}

func (d DataType) String() string {
	if d.Option == "" {
		return d.Name
	} else {
		return fmt.Sprintf("%s(%s)", d.Name, d.Option)
	}
}
