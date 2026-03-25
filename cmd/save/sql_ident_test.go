package save

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSqlIdent(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
	}{
		{"simple name", "my_table", `"my_table"`},
		{"name with double quote", `my"table`, `"my""table"`},
		{"empty string", "", `""`},
		{"name with spaces", "my table", `"my table"`},
		{"name with special chars", "x); DROP TABLE --", `"x); DROP TABLE --"`},
		{"multiple double quotes", `a""b`, `"a""""b"`},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert.Equal(t, tt.expected, sqlIdent(tt.input))
		})
	}
}
