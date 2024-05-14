package plan_node

type HiveColumnHandle struct {
	ColumnName  string          `parser:"@Ident ':'"`
	DataType    string          `parser:"@(~':')+ ':'"`
	ColumnIndex int             `parser:"@('-'? Int) ':'"`
	ColumnType  string          `parser:"@('REGULAR' | 'PARTITION_KEY' | 'SYNTHESIZED' | 'AGGREGATED')"`
	Loc         *SourceLocation `parser:"@@?"`
}

type SourceLocation struct {
	RowNumber    int `parser:"'(' @Int ':'"`
	ColumnNumber int `parser:"@Int ')'"`
}

type Layout struct {
	LayoutString string `parser:"'LAYOUT' ':' @(~'$')+"`
}

type Assignment struct {
}
