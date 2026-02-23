package prestoapi_test

import (
	_ "embed"
	"pbench/prestoapi"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
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
		if queries, err := prestoapi.SplitQueries(strings.NewReader(file)); err != nil {
			t.Fatal(err)
		} else {
			assert.Equal(t, expectedQueries[i], queries)
		}
	}
}

func TestQuerySplitterEscapedBackslash(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected []string
	}{
		{
			name:     "escaped backslash before closing quote",
			input:    `SELECT '\\'; SELECT 1;`,
			expected: []string{`SELECT '\\'`, `SELECT 1`},
		},
		{
			name:     "single escaped quote stays in string",
			input:    `SELECT 'it\'s'; SELECT 2;`,
			expected: []string{`SELECT 'it\'s'`, `SELECT 2`},
		},
		{
			name:     "triple backslash: escaped backslash then escaped quote, next quote closes",
			input:    `SELECT '\\\''; SELECT 3;`,
			expected: []string{`SELECT '\\\''`, `SELECT 3`},
		},
		{
			name:     "four backslashes are two escaped backslashes then closing quote",
			input:    `SELECT '\\\\'; SELECT 4;`,
			expected: []string{`SELECT '\\\\'`, `SELECT 4`},
		},
		{
			name:     "double quoted with escaped backslash",
			input:    `SELECT "col\\"; SELECT 5;`,
			expected: []string{`SELECT "col\\"`, `SELECT 5`},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			queries, err := prestoapi.SplitQueries(strings.NewReader(tt.input))
			assert.NoError(t, err)
			assert.Equal(t, tt.expected, queries)
		})
	}
}
