package prestoapi

import (
	"context"
	"testing"

	presto "github.com/ethanyzhang/presto-go"
	"github.com/ethanyzhang/presto-go/prestotest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type testRow struct {
	Name    string  `presto:"name"`
	Value   float64 `presto:"value"`
	Active  bool    `presto:"active"`
	Count   int     `presto:"count"`
	Missing string  `presto:"missing"` // column not in query
}

func TestQueryAndUnmarshal_Struct(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SELECT name, value, active, count FROM test_table LIMIT 1",
		Columns: []presto.Column{
			{Name: "name", Type: "varchar"},
			{Name: "value", Type: "double"},
			{Name: "active", Type: "boolean"},
			{Name: "count", Type: "bigint"},
		},
		Data: [][]any{{"alice", 3.14, true, 42}},
	})

	client, err := presto.NewClient(mock.URL())
	require.NoError(t, err)

	var result []testRow
	err = QueryAndUnmarshal(context.Background(), &client.Session, "SELECT name, value, active, count FROM test_table LIMIT 1", &result)
	require.NoError(t, err)
	require.Equal(t, 1, len(result))
	assert.Equal(t, "alice", result[0].Name)
	assert.InDelta(t, 3.14, result[0].Value, 0.001)
	assert.True(t, result[0].Active)
	assert.Equal(t, 42, result[0].Count)
	assert.Equal(t, "", result[0].Missing)
}

func TestQueryAndUnmarshal_MultiBatch(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SELECT id FROM multi_batch",
		Columns: []presto.Column{
			{Name: "id", Type: "bigint"},
		},
		Data: [][]any{
			{1}, {2}, {3}, {4}, {5}, {6},
		},
		DataBatches: 3,
	})

	client, err := presto.NewClient(mock.URL())
	require.NoError(t, err)

	type idRow struct {
		ID int `presto:"id"`
	}
	var results []idRow
	err = QueryAndUnmarshal(context.Background(), &client.Session, "SELECT id FROM multi_batch", &results)
	require.NoError(t, err)
	assert.Equal(t, 6, len(results))
	for i, row := range results {
		assert.Equal(t, i+1, row.ID)
	}
}

func TestQueryAndUnmarshal_Scalar(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:     "SELECT COUNT(*) FROM some_table",
		Columns: []presto.Column{{Name: "_col0", Type: "bigint"}},
		Data:    [][]any{{12345}},
	})

	client, err := presto.NewClient(mock.URL())
	require.NoError(t, err)

	var count int
	err = QueryAndUnmarshal(context.Background(), &client.Session, "SELECT COUNT(*) FROM some_table", &count)
	require.NoError(t, err)
	assert.Equal(t, 12345, count)
}

func TestQueryAndUnmarshal_EmptyResult(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:     "SELECT id FROM empty_table",
		Columns: []presto.Column{{Name: "id", Type: "bigint"}},
		Data:    [][]any{},
	})

	client, err := presto.NewClient(mock.URL())
	require.NoError(t, err)

	type idRow struct {
		ID int `presto:"id"`
	}
	var results []idRow
	err = QueryAndUnmarshal(context.Background(), &client.Session, "SELECT id FROM empty_table", &results)
	require.NoError(t, err)
	assert.Equal(t, 0, len(results))
}

func TestQueryAndUnmarshal_QueryError(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:          "SELECT * FROM nonexistent",
		QueueBatches: 1,
		Error: &presto.QueryError{
			ErrorName: "TABLE_NOT_FOUND",
			Message:   "Table nonexistent does not exist",
			ErrorCode: 1,
			ErrorType: "USER_ERROR",
		},
	})

	client, err := presto.NewClient(mock.URL())
	require.NoError(t, err)

	var results []testRow
	err = QueryAndUnmarshal(context.Background(), &client.Session, "SELECT * FROM nonexistent", &results)
	assert.Error(t, err)
}
