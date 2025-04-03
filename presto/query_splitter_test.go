package presto_test

import (
	_ "embed"
	"github.com/stretchr/testify/assert"
	"pbench/presto"
	"strings"
	"testing"
)

//go:embed query_splitter_test.sql
var fileWithTrailingComment string

func TestQuerySplitter(t *testing.T) {
	files := []string{
		`select * from table1 where a = '\'/**/--;'; --comment
select * from table3;-;/*
select * from **/
another query;;missing semicolon, should be discarded
`,
		fileWithTrailingComment,
	}
	expectedQueries := [][]string{
		{
			"select * from table1 where a = '\\'/**/--;'",
			"--comment\nselect * from table3",
			"-",
			"/*\nselect * from **/\nanother query",
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

func TestSplitQueriesWithSession(t *testing.T) {
	input := `/* header comment */
--SET SESSION join_reordering_strategy = 'NONE';
--session query_max_memory = '1GB'
--session max_splits_per_node = 1234
--session optimize_hash_generation = true
-- normal comment
SELECT 
    *  -- inline comment
FROM 
    table1
WHERE
    id > 0;`

	expected := []presto.QueryWithSession{
		{
			Query: "SELECT * FROM table1 WHERE id > 0",
			SessionParams: map[string]any{
				"join_reordering_strategy": "NONE",
				"query_max_memory":         "1GB",
				"max_splits_per_node":      int64(1234),
				"optimize_hash_generation": true,
			},
		},
	}

	queries, err := presto.SplitQueriesWithSession(strings.NewReader(input))
	assert.NoError(t, err)
	assert.Equal(t, expected, queries)
}
