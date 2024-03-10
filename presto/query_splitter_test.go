package presto_test

import (
	_ "embed"
	"github.com/stretchr/testify/assert"
	"pbench/presto"
	"strings"
	"testing"
)

//go:embed btq_outer2inner.sql
var fileWithTrailingComment string

func TestQuerySplitter(t *testing.T) {
	files := []string{
		`select * from table1 where a = '\'--;'; --comment
select * from table3;-;;
`,
		fileWithTrailingComment,
	}
	expectedQueries := [][]string{
		{
			"select * from table1 where a = '\\'--;'",
			"--comment\nselect * from table3",
			"-",
		}, {fileWithTrailingComment[:663]},
	}
	for i, file := range files {
		if queries, err := presto.SplitQueries(strings.NewReader(file)); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, expectedQueries[i], queries)
		}
	}
}
