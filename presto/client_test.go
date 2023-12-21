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
	client.User("ethan").Catalog("tpch").Schema("sf1")
	client.SessionParam("query_max_memory_per_node", "55GB")
	client.SessionParam("hive.parquet_pushdown_filter_enabled", true)
	qr, _, err := client.Query(context.Background(), `select * from customer`)
	if err != nil {
		t.Fatal(err)
	}
	count, err := qr.Drain(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	assert.Equal(t, 150000, count)
}
