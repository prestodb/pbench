package presto_test

import (
	"context"
	"github.com/stretchr/testify/assert"
	"presto-benchmark/presto"
	"testing"
)

func TestQuery(t *testing.T) {
	client, err := presto.NewClient("http://127.0.0.1:8080")
	if err != nil {
		t.Fatal(err)
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
		t.Fatal(err)
	}
	count, err := qr.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 150000, count)
}
