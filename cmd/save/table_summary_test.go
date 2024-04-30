package save

import (
	"context"
	"fmt"
	"github.com/stretchr/testify/assert"
	"pbench/presto"
	"testing"
)

func TestTableSummary_QueryTableSummary(t *testing.T) {
	t.SkipNow()
	client, _ := presto.NewClient("http://localhost:8080", false)
	client.Catalog("tpch").Schema("sf1")
	ts := &TableSummary{Name: "lineitem"}
	assert.Nil(t, ts.QueryTableSummary(context.Background(), client))
	fmt.Println(ts)
}
