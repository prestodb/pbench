package load

import (
	_ "embed"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"pbench/presto"
	"pbench/utils"
	"testing"
)

//go:embed 20240422_013209_00111_k6ve9.json
var testJson []byte

func TestParseQueryInfo(t *testing.T) {
	queryInfo := new(presto.QueryInfo)
	assert.Nil(t, json.Unmarshal(testJson, queryInfo))
	assert.Nil(t, queryInfo.FlattenAndPrepareForInsert())
	assert.Nil(t, queryInfo.OutputStage)
	assert.Equal(t, 4, len(queryInfo.FlattenedStageList))
}

func testInsertQueryInfo(t *testing.T) {
	db := utils.InitMySQLConnFromCfg("../mysql.json")
	qi := presto.QueryInfo{QueryId: "a1",
		Session: &presto.Session{
			TransactionId:     "5f840560-6676-4686-94e3-387ed0d0c8a0",
			Schema:            nil,
			Catalog:           nil,
			SystemProperties:  nil,
			User:              "pbench",
			RemoteUserAddress: "",
			Source:            nil,
			UserAgent:         "",
			ClientTags:        nil,
		}}
	e := utils.SqlInsertObject(db, qi, "presto_query_creation_info")
	assert.Nil(t, e)
}
