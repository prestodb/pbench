package save

import (
	"context"
	"testing"

	presto "github.com/ethanyzhang/presto-go"
	"github.com/ethanyzhang/presto-go/prestotest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestQueryTableSummary_Basic(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	// SHOW CREATE TABLE
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:     "SHOW CREATE TABLE test_catalog.test_schema.test_table",
		Columns: []presto.Column{{Name: "Create Table", Type: "varchar"}},
		Data:    [][]any{{"CREATE TABLE test_catalog.test_schema.test_table (\n   id bigint,\n   name varchar\n)"}},
	})

	// SHOW STATS FOR
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SHOW STATS FOR test_catalog.test_schema.test_table",
		Columns: []presto.Column{
			{Name: "column_name", Type: "varchar"},
			{Name: "data_size", Type: "double"},
			{Name: "distinct_values_count", Type: "double"},
			{Name: "nulls_fraction", Type: "double"},
			{Name: "row_count", Type: "double"},
			{Name: "low_value", Type: "varchar"},
			{Name: "high_value", Type: "varchar"},
		},
		Data: [][]any{
			{"id", nil, 100.0, 0.0, nil, "1", "1000"},
			{"name", 5000.0, 90.0, 0.05, nil, nil, nil},
			{nil, nil, nil, nil, 500.0, nil, nil},
		},
	})

	// DESCRIBE — returns column metadata, not statistics
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "DESCRIBE test_catalog.test_schema.test_table",
		Columns: []presto.Column{
			{Name: "Column", Type: "varchar"},
			{Name: "Type", Type: "varchar"},
			{Name: "Extra", Type: "varchar"},
			{Name: "Comment", Type: "varchar"},
			{Name: "Precision", Type: "bigint"},
			{Name: "Scale", Type: "bigint"},
			{Name: "Length", Type: "bigint"},
		},
		Data: [][]any{
			{"id", "bigint", "", "", 64.0, 0.0, nil},
			{"name", "varchar", "", "", nil, nil, 256.0},
		},
	})

	client, err := presto.NewClient(mock.URL())
	require.NoError(t, err)

	ts := &TableSummary{
		Name:    "test_table",
		Catalog: "test_catalog",
		Schema:  "test_schema",
	}

	ts.QueryTableSummary(context.Background(), &client.Session, false)

	assert.Contains(t, ts.Ddl, "CREATE TABLE")
	assert.NotNil(t, ts.RowCount)
	assert.Equal(t, 500, *ts.RowCount)
	// SHOW STATS has 3 rows (2 columns + summary), DESCRIBE has 2 rows (columns only).
	// DESCRIBE overwrites first 2 entries, adding DataType; summary row is preserved.
	assert.Equal(t, 3, len(ts.ColumnStats))
	// DESCRIBE populates DataType, Precision, Scale, Length fields
	assert.NotNil(t, ts.ColumnStats[0].DataType)
	assert.Equal(t, "bigint", *ts.ColumnStats[0].DataType)
	assert.NotNil(t, ts.ColumnStats[0].Precision)
	assert.Equal(t, 64.0, *ts.ColumnStats[0].Precision)
	assert.NotNil(t, ts.ColumnStats[0].Scale)
	assert.Equal(t, 0.0, *ts.ColumnStats[0].Scale)
	assert.Nil(t, ts.ColumnStats[0].Length)

	assert.NotNil(t, ts.ColumnStats[1].DataType)
	assert.Equal(t, "varchar", *ts.ColumnStats[1].DataType)
	assert.Nil(t, ts.ColumnStats[1].Precision)
	assert.Nil(t, ts.ColumnStats[1].Scale)
	assert.NotNil(t, ts.ColumnStats[1].Length)
	assert.Equal(t, 256.0, *ts.ColumnStats[1].Length)
}

func TestQueryTableSummary_FallbackToCount(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	// SHOW CREATE TABLE
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:     "SHOW CREATE TABLE cat.sch.tbl",
		Columns: []presto.Column{{Name: "Create Table", Type: "varchar"}},
		Data:    [][]any{{"CREATE TABLE cat.sch.tbl (id bigint)"}},
	})

	// SHOW STATS FOR - no row count in summary row
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "SHOW STATS FOR cat.sch.tbl",
		Columns: []presto.Column{
			{Name: "column_name", Type: "varchar"},
			{Name: "data_size", Type: "double"},
			{Name: "distinct_values_count", Type: "double"},
			{Name: "nulls_fraction", Type: "double"},
			{Name: "row_count", Type: "double"},
			{Name: "low_value", Type: "varchar"},
			{Name: "high_value", Type: "varchar"},
		},
		Data: [][]any{
			{"id", nil, 50.0, 0.0, nil, "1", "100"},
			{nil, nil, nil, nil, nil, nil, nil}, // no row count
		},
	})

	// DESCRIBE — returns column metadata, not statistics
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "DESCRIBE cat.sch.tbl",
		Columns: []presto.Column{
			{Name: "Column", Type: "varchar"},
			{Name: "Type", Type: "varchar"},
			{Name: "Extra", Type: "varchar"},
			{Name: "Comment", Type: "varchar"},
			{Name: "Precision", Type: "bigint"},
			{Name: "Scale", Type: "bigint"},
			{Name: "Length", Type: "bigint"},
		},
		Data: [][]any{
			{"id", "bigint", "", "", 64.0, 0.0, nil},
		},
	})

	// SELECT COUNT(*) fallback
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:     "SELECT COUNT(*) FROM cat.sch.tbl",
		Columns: []presto.Column{{Name: "_col0", Type: "bigint"}},
		Data:    [][]any{{42}},
	})

	client, err := presto.NewClient(mock.URL())
	require.NoError(t, err)

	ts := &TableSummary{
		Name:    "tbl",
		Catalog: "cat",
		Schema:  "sch",
	}

	ts.QueryTableSummary(context.Background(), &client.Session, false)

	require.NotNil(t, ts.RowCount)
	assert.Equal(t, 42, *ts.RowCount)
}

func TestQueryTableSummary_ErrorRecovery(t *testing.T) {
	mock := prestotest.NewMockPrestoServer()
	defer mock.Close()

	// SHOW CREATE TABLE returns an error
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:          "SHOW CREATE TABLE err_cat.err_sch.err_tbl",
		QueueBatches: 1,
		Error: &presto.QueryError{
			ErrorName: "TABLE_NOT_FOUND",
			Message:   "Table err_cat.err_sch.err_tbl does not exist",
			ErrorCode: 1,
			ErrorType: "USER_ERROR",
		},
	})

	client, err := presto.NewClient(mock.URL())
	require.NoError(t, err)

	ts := &TableSummary{
		Name:    "err_tbl",
		Catalog: "err_cat",
		Schema:  "err_sch",
	}

	// Should not panic — the defer/recover in QueryTableSummary handles errors
	ts.QueryTableSummary(context.Background(), &client.Session, false)

	// DDL should be empty since query failed
	assert.Equal(t, "", ts.Ddl)
}
