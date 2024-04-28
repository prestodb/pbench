package loadjson

import (
	_ "embed"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"pbench/presto/query_json"
	"testing"
)

//go:embed 20240422_013209_00111_k6ve9_shows_schema.json
var testJson []byte

func TestParseQueryInfo(t *testing.T) {
	queryInfo := new(query_json.QueryInfo)
	assert.Nil(t, json.Unmarshal(testJson, queryInfo))
	assert.Nil(t, queryInfo.PrepareForInsert())
	assert.Nil(t, queryInfo.OutputStage)
	assert.Equal(t, 4, len(queryInfo.FlattenedStageList))
}
