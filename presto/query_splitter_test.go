package presto_test

import (
	"github.com/stretchr/testify/assert"
	"presto-benchmark/presto"
	"strings"
	"testing"
)

func TestQuerySplitter(t *testing.T) {
	files := []string{
		`select * from table1 where a = '\'--;'; --comment
select * from table3;-`,
	}
	expectedQueries := [][]string{
		{
			"select * from table1 where a = '\\'--;'",
			"--comment\nselect * from table3",
			"-",
		},
	}
	for i, file := range files {
		if queries, err := presto.SplitQueries(strings.NewReader(file)); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, expectedQueries[i], queries)
		}
	}
}
