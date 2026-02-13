package save

import (
	"context"
	"fmt"
	presto "github.com/ethanyzhang/presto-go"
	"testing"
)

func TestTableSummary_QueryTableSummary(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test - requires live Presto server")
	}
	client, _ := presto.NewClient("http://localhost:8080")
	if _, _, err := client.GetClusterInfo(context.Background()); err != nil {
		t.Skip("local cluster is not ready")
	}
	session := client.Catalog("tpch").Schema("sf1")
	ts := &TableSummary{Catalog: "tpch", Schema: "sf1", Name: "lineitem"}
	ts.QueryTableSummary(context.Background(), session, true)
	fmt.Println(ts)
}
