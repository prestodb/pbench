package loadjson

import (
	"embed"
	_ "embed"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"pbench/presto/query_json"
	"testing"
)

// Embed multiple JSON files
//
//go:embed *.json
var testFiles embed.FS

type testCase struct {
	name     string
	filePath string
	expected int
}

func TestParseQueryInfo(t *testing.T) {
	tests := []testCase{
		{
			name:     "Presto query info JSON",
			filePath: "20240422_013209_00111_k6ve9_shows_schema.json",
			expected: 4,
		},
		{
			name:     "Trino query info JSON",
			filePath: "trino_query_info.json",
			expected: 5,
		},
		// Add more test cases as needed
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			jsonBytes, err := testFiles.ReadFile(tc.filePath)
			assert.Nil(t, err)

			queryInfo := new(query_json.QueryInfo)
			assert.Nil(t, json.Unmarshal(jsonBytes, queryInfo))
			assert.Nil(t, queryInfo.PrepareForInsert())
			assert.Nil(t, queryInfo.OutputStage)
			assert.Equal(t, tc.expected, len(queryInfo.FlattenedStageList))
		})
	}
}
