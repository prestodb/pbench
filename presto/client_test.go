package presto_test

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"pbench/presto"
	"pbench/presto/query_json"
	"strings"
	"syscall"
	"testing"
)

func TestQuery(t *testing.T) {
	// This test requires Presto hive query runner.
	client, err := presto.NewClient("http://127.0.0.1:8080", false)
	assert.Nil(t, err)
	if _, _, err = client.GetClusterInfo(context.Background()); err != nil {
		t.Skip("local cluster is not ready")
	}
	qr, _, err := client.
		User("ethan").
		Catalog("tpch").
		Schema("sf1").
		SessionParam("query_max_memory_per_node", "55GB").
		SessionParam("hive.parquet_pushdown_filter_enabled", true).
		ClientInfo("TestQuery client").
		ClientTags("test", "client_1").
		Query(context.Background(), `select * from customer`)
	if err != nil {
		if errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("%v: this test requires Presto Hive query runner to run.", err)
		} else {
			t.Fatal(err)
		}
	}
	rowCount := 0
	err = qr.Drain(context.Background(), func(qr *presto.QueryResults) error {
		rowCount += len(qr.Data)
		return nil
	})
	assert.Nil(t, err)
	assert.Equal(t, 150000, rowCount)

	buf := &strings.Builder{}
	var queryInfo *query_json.QueryInfo
	queryInfo, _, err = client.GetQueryInfo(context.Background(), qr.Id, false, buf)
	assert.Nil(t, err)
	assert.Nil(t, queryInfo)
	assert.Greater(t, buf.Len(), 0)
	queryInfo, _, err = client.GetQueryInfo(context.Background(), qr.Id, true, nil)
	assert.Nil(t, err)
	assert.Equal(t, qr.Id, queryInfo.QueryId)
}

func TestGenerateQueryParameter(t *testing.T) {
	stringValue := "was it clear (already)?"
	serializedQuery := presto.GenerateHttpQueryParameter(&struct {
		StringField *string `query:"stringField"`
		BoolField   bool    `query:"boolField"`
		IntField    int     `query:"intField"`
		BoolPtr     *bool   `query:"boolPtr"`
		StringPtr   *string `query:"stringPtr"`
	}{
		StringField: &stringValue,
		BoolField:   true,
		IntField:    123,
	})
	assert.Equal(t, `stringField=was+it+clear+%28already%29%3F&boolField=true&intField=123`, serializedQuery)
}
