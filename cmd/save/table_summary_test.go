package save

import (
	"context"
	"fmt"
	"pbench/presto"
	"testing"
)

func TestTableSummary_QueryTableSummary(t *testing.T) {
	client, _ := presto.NewClient("http://localhost:8080", false)
	if _, _, err := client.GetClusterInfo(context.Background()); err != nil {
		t.Skip("local cluster is not ready")
	}
	client.Catalog("tpch").Schema("sf1")
	ts := &TableSummary{Catalog: "tpch", Schema: "sf1", Name: "lineitem"}
	ts.QueryTableSummary(context.Background(), client, true)
	fmt.Println(ts)
}
