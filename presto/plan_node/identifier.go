package plan_node

import (
	"fmt"
	"github.com/alecthomas/participle/v2"
	"github.com/alecthomas/participle/v2/lexer"
	"regexp"
)

var (
	hiveTableHandleParserRegex = regexp.MustCompile(`.*connectorId='(.*)', connectorHandle='HiveTableHandle\{schemaName=(.*?), tableName=(.*?),.*`)
	planNodeIdentifierLexer    = lexer.MustStateful(lexer.Rules{
		"Root": {
			{"Ident", `[a-zA-Z_$][\w.$]*`, nil},
			{"String", `"(\\"|[^"])*"|'(\\'|[^'])*'`, nil}, // single-quoted or double-quoted.
			{"EOL", `[\n\r]`, nil},
			{"Punctuation", `[-[!@#$%^&*()+_={}\|:;"'<,>.?/]|]`, nil},
			{"Whitespace", `[ \t]`, nil},
		}})
	PlanNodeIdentifierParserOptions = []participle.Option{
		participle.Lexer(planNodeIdentifierLexer),
		participle.Unquote("String"),
		participle.Elide("Whitespace"),
	}
	PlanNodeJoinPredicatesParser = participle.MustBuild[JoinPredicates](PlanNodeIdentifierParserOptions...)
)

type HiveTableHandle struct {
	Schema  string `parser:"'HiveTableHandle' '{' 'schemaName' '=' @Ident"`
	Table   string `parser:"',' 'tableName' '=' @Ident (~'}')* '}'"`
	Catalog string
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

type JoinPredicate struct {
	Left  string `parser:"'(' @String"`
	Right string `parser:"'=' @String ')'"`
}

type JoinPredicates struct {
	Predicates []JoinPredicate `parser:"'[' @@ ('AND' @@)* ']' (~EOL)*"`
}
