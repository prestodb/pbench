package plan_node

import (
	"fmt"
	"regexp"
	"slices"

	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
)

var (
	hiveTableHandleParserRegex = regexp.MustCompile(`.*connectorId='(.*)', connectorHandle='HiveTableHandle\{schemaName=(.*?), tableName=(.*?),.*`)
	planNodeIdentifierLexer    = lexer.MustStateful(lexer.Rules{
		"Root": {
			{Name: "Keywords", Pattern: `(?i)\b(CAST|AND|OR|AS)\b`, Action: nil},
			{Name: "Ident", Pattern: `[a-zA-Z_$][\w.$]*`, Action: nil},
			{Name: "Int", Pattern: `-?\d+`, Action: nil},
			{Name: "String", Pattern: `"(\\"|[^"])*"|'(\\'|[^'])*'`, Action: nil}, // single-quoted or double-quoted.
			{Name: `Operators`, Pattern: `<>|!=|<=|>=|[-+*/%,.()=<>]`, Action: nil},
			{Name: "EOL", Pattern: `[\n\r]`, Action: nil},
			{Name: "Punctuation", Pattern: `[-[!@#$%^&*()+_={}\|:;"'<,>.?/]|]`, Action: nil},
			{Name: "whitespace", Pattern: `[ \t]`, Action: nil},
		}})
	PlanNodeIdentifierParserOptions = []participle.Option{
		participle.Lexer(planNodeIdentifierLexer),
		participle.CaseInsensitive("Keywords"),
		participle.Unquote("String"),
		participle.Union[Value](
			&FunctionCall{},
			&IdentRef{},
			&TypedValue{},
			&TypeCastedValue{},
			&CatchAllValue{},
		),
		participle.Union[ColumnExp](
			&Column{},
			&ParansColumn{},
		),
		participle.Union[Expression](&LogicalExpression{}, &JoinPredicate{}, &FunctionCall{}),
	}
	PlanNodeJoinPredicatesParser = participle.MustBuild[JoinIdentifier](PlanNodeIdentifierParserOptions...)
)

type HiveTableHandle struct {
	Schema  string `parser:"'HiveTableHandle' '{' 'schemaName' '=' @Ident" json:"schema"`
	Table   string `parser:"',' 'tableName' '=' @Ident (~'}')* '}'" json:"table"`
	Catalog string `parser:"" json:"catalog"`
}

func (h *HiveTableHandle) String() string {
	if h == nil {
		return "<nil table>"
	}
	return fmt.Sprintf("%s.%s.%s", h.Catalog, h.Schema, h.Table)
}

func ParseHiveTableHandle(literal string) *HiveTableHandle {
	if match := hiveTableHandleParserRegex.FindStringSubmatch(literal); match != nil {
		return &HiveTableHandle{
			Schema:  match[2],
			Table:   match[3],
			Catalog: match[1],
		}
	}
	return nil
}

type ColumnExp interface {
	col()
	String() string
	GetAssignments() []string
}

type Column struct {
	Name string `parser:"@String"`
}

func (*Column) col() {}

func (c *Column) String() string {
	return c.Name
}

func (c *Column) GetAssignments() []string {
	return []string{c.Name}
}

type ParansColumn struct {
	Value Value `parser:" '(' @@ ')'"`
}

func (*ParansColumn) col() {}

func (c *ParansColumn) String() string {
	return c.Value.String()
}

func (c *ParansColumn) GetAssignments() []string {
	return c.Value.GetAssignments()
}

type Expression interface {
	exp()
	GetJoinPredicates() []JoinPredicate
}

type LogicalExpression struct {
	Left  Expression `parser:"(?! '(' @@ ')' Operators)'(' @@ ')'"`
	Op    string     `parser:" ( @('AND' | 'OR') "`
	Right Expression `parser:" '(' @@  ')')?"`
}

func (*LogicalExpression) exp() {}

func (j *LogicalExpression) GetJoinPredicates() []JoinPredicate {
	return slices.Concat(j.Left.GetJoinPredicates(), j.Right.GetJoinPredicates())
}

type JoinPredicate struct {
	Left  ColumnExp `parser:"@@"`
	Op    string    `parser:"@Operators"`
	Right ColumnExp `parser:"@@?"`
}

func (*JoinPredicate) exp() {}

func (j *JoinPredicate) GetJoinPredicates() []JoinPredicate {
	return []JoinPredicate{*j}
}

func (j *JoinPredicate) GetAssignments() ([]string, []string) {
	return j.Left.GetAssignments(), j.Right.GetAssignments()
}

type JoinIdentifier struct {
	// either multiple AND or multiple OR, can't mix
	FirstExp Expression   `parser:"'[' '(' @@ ')'"`
	AndExp   []Expression `parser:"(( 'AND' ('not')?'(' @@ ')' ) |"`
	OrExp    []Expression `parser:"( 'OR' ('not')?'(' @@ ')' ))* ']' (~EOL)*"`
}

func (j *JoinIdentifier) GetJoins() []Expression {
	return slices.Concat([]Expression{j.FirstExp}, j.AndExp, j.OrExp)
}
