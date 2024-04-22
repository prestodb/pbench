package utils

import (
	"encoding/json"
	_ "github.com/go-sql-driver/mysql"
	"github.com/stretchr/testify/assert"
	"pbench/presto"
	"testing"
)

func testInsertQueryInfo(t *testing.T) {
	db := InitMySQLConnFromCfg("../mysql.json")
	qi := presto.QueryInfo{QueryID: "a1",
		Session: presto.Session{
			TransactionId:     "5f840560-6676-4686-94e3-387ed0d0c8a0",
			Schema:            nil,
			Catalog:           nil,
			SystemProperties:  nil,
			User:              "pbench",
			RemoteUserAddress: "",
			Source:            nil,
			UserAgent:         "",
			ClientTags:        json.RawMessage(""),
		}}
	r, e := SqlInsertObject(db, qi, "presto_query_creation_info")
	assert.Nil(t, e)
	assert.NotNil(t, r)
}
