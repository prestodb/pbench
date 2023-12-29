package presto_benchmark

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestParseTaskDirectory(t *testing.T) {
	/** from top to bottom
	         root
	          |
	       query_01
	       /      \
	  query_02   query_03
	        \     /
	       query_04
	*/
	tm, err := ParseDirectory("benchmarks/test")
	assert.Nil(t, err)
	root := tm.Root()
	query01 := tm.Get("query_01")
	query02 := tm.Get("query_02")
	query03 := tm.Get("query_03")
	query04 := tm.Get("query_04")
	assert.Equal(t, 0, len(root.Prerequisites))
	assert.Equal(t, 1, len(root.Next))
	assert.Equal(t, query01, root.Next[0])

	assert.Equal(t, 1, len(query01.Prerequisites))
	assert.Equal(t, root, query01.Prerequisites[0])
	assert.Equal(t, 2, len(query01.Next))
	assert.Equal(t, query02, query01.Next[0])
	assert.Equal(t, query03, query01.Next[1])

	assert.Equal(t, 1, len(query02.Prerequisites))
	assert.Equal(t, query01, query02.Prerequisites[0])
	assert.Equal(t, 1, len(query02.Next))
	assert.Equal(t, query04, query02.Next[0])

	assert.Equal(t, 1, len(query03.Prerequisites))
	assert.Equal(t, query01, query03.Prerequisites[0])
	assert.Equal(t, 1, len(query03.Next))
	assert.Equal(t, query04, query03.Next[0])

	assert.Equal(t, 2, len(query04.Prerequisites))
	assert.Equal(t, query02, query04.Prerequisites[0])
	assert.Equal(t, query03, query04.Prerequisites[1])
	assert.Equal(t, 0, len(query04.Next))
	assert.Equal(t, 1, len(query04.QueryFiles))
	assert.Equal(t, "query_04.sql", query04.QueryFiles[0])
}
