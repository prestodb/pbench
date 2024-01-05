package presto_test

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"presto-benchmark/presto"
	"strings"
	"syscall"
	"testing"
)

func TestQuery(t *testing.T) {
	// This test requires Presto hive query runner.
	client, err := presto.NewClient("http://127.0.0.1:8080")
	assert.Nil(t, err)
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
	_, err = client.GetQueryInfo(context.Background(), qr.Id, false, buf)
	assert.Nil(t, err)
	assert.Greater(t, buf.Len(), 0)
}
