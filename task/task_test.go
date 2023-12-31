package task

import (
	"github.com/stretchr/testify/assert"
	"strings"
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
	query01, tm, err := ParseTaskChain("../benchmarks/test/query_01.json")
	assert.Nil(t, err)
	query02 := tm.Get("query_02")
	query03 := tm.Get("query_03")
	query04 := tm.Get("query_04")

	assert.Equal(t, 0, len(query01.Prerequisites))
	assert.Equal(t, 2, len(query01.NextTasks))
	assert.Equal(t, query02, query01.NextTasks[0])
	assert.Equal(t, query03, query01.NextTasks[1])

	assert.Equal(t, 1, len(query02.Prerequisites))
	assert.Equal(t, query01, query02.Prerequisites[0])
	assert.Equal(t, 1, len(query02.NextTasks))
	assert.Equal(t, query04, query02.NextTasks[0])

	assert.Equal(t, 1, len(query03.Prerequisites))
	assert.Equal(t, query01, query03.Prerequisites[0])
	assert.Equal(t, 1, len(query03.NextTasks))
	assert.Equal(t, query04, query03.NextTasks[0])

	assert.Equal(t, 2, len(query04.Prerequisites))
	assert.Equal(t, query02, query04.Prerequisites[0])
	assert.Equal(t, query03, query04.Prerequisites[1])
	assert.Equal(t, 0, len(query04.NextTasks))
	assert.Equal(t, 1, len(query04.QueryFiles))
	assert.True(t, strings.HasSuffix(query04.QueryFiles[0], "query_04.sql"))
}
