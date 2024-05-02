package presto

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestBuiltinRows(t *testing.T) {
	t.SkipNow()
	pRows, pCols := getRowsFromPresto(t)
	bRows, bCols := getBuiltinRows(t)
	assert.Equal(t, pRows, bRows)
	assert.Equal(t, pCols, bCols)
}

func TestPrestoUnmarshalScalar(t *testing.T) {
	client, _ := NewClient("http://localhost:8080", false)
	client.Catalog("tpch").Schema("sf1")
	ctx := context.Background()
	var ddl string
	expectedDdl := "CREATE TABLE tpch.sf1.lineitem (\n   \"orderkey\" bigint NOT NULL,\n   \"partkey\" bigint NOT NULL,\n   \"suppkey\" bigint NOT NULL,\n   \"linenumber\" integer NOT NULL,\n   \"quantity\" double NOT NULL,\n   \"extendedprice\" double NOT NULL,\n   \"discount\" double NOT NULL,\n   \"tax\" double NOT NULL,\n   \"returnflag\" varchar(1) NOT NULL,\n   \"linestatus\" varchar(1) NOT NULL,\n   \"shipdate\" date NOT NULL,\n   \"commitdate\" date NOT NULL,\n   \"receiptdate\" date NOT NULL,\n   \"shipinstruct\" varchar(25) NOT NULL,\n   \"shipmode\" varchar(10) NOT NULL,\n   \"comment\" varchar(44) NOT NULL\n)"

	if err := QueryAndUnmarshal(ctx, client, "SHOW CREATE TABLE lineitem", &ddl); err != nil {
		fmt.Println(err)
		columnHeaders := []Column{{Name: "Create Table"}}
		rows := []json.RawMessage{json.RawMessage(`["CREATE TABLE tpch.sf1.lineitem (\n   \"orderkey\" bigint NOT NULL,\n   \"partkey\" bigint NOT NULL,\n   \"suppkey\" bigint NOT NULL,\n   \"linenumber\" integer NOT NULL,\n   \"quantity\" double NOT NULL,\n   \"extendedprice\" double NOT NULL,\n   \"discount\" double NOT NULL,\n   \"tax\" double NOT NULL,\n   \"returnflag\" varchar(1) NOT NULL,\n   \"linestatus\" varchar(1) NOT NULL,\n   \"shipdate\" date NOT NULL,\n   \"commitdate\" date NOT NULL,\n   \"receiptdate\" date NOT NULL,\n   \"shipinstruct\" varchar(25) NOT NULL,\n   \"shipmode\" varchar(10) NOT NULL,\n   \"comment\" varchar(44) NOT NULL\n)"]`)}
		assert.Nil(t, UnmarshalQueryData(rows, columnHeaders, &ddl))
	}
	assert.Equal(t, expectedDdl, ddl)
}

func TestPrestoUnmarshal(t *testing.T) {
	rows, columnHeaders := getBuiltinRows(t)
	var nilPtr *[]string
	err := UnmarshalQueryData(rows, columnHeaders, nilPtr)
	assert.ErrorIs(t, err, InvalidUnmarshalError) // nil pointer

	columnsStats := make([]ColumnStats, 8, 17)
	err = UnmarshalQueryData(rows, columnHeaders, columnsStats)
	assert.ErrorIs(t, err, InvalidUnmarshalError) // not a pointer

	err = UnmarshalQueryData(rows, columnHeaders, &columnsStats[0])
	assert.ErrorIs(t, err, InvalidUnmarshalError) // not an array/slice pointer

	// UnmarshalQueryData into a []json.RawMessage
	var decodedRows []json.RawMessage
	err = UnmarshalQueryData(rows, columnHeaders, &decodedRows)
	assert.Nil(t, err)
	assert.Equal(t, rows, decodedRows)

	rowStrings := make([]string, 8)
	err = UnmarshalQueryData(rows, columnHeaders, &rowStrings)
	assert.Nil(t, err)
	assert.Equal(t, len(rows), len(rowStrings))
	for i, row := range rowStrings {
		assert.Equal(t, row, string(rows[i]))
	}

	rowBytes := make([][]byte, 8)
	err = UnmarshalQueryData(rows, columnHeaders, &rowBytes)
	assert.Nil(t, err)
	assert.Equal(t, len(rows), len(rowBytes))
	for i, row := range rowBytes {
		assert.Equal(t, row, []byte(rows[i]))
	}

	// UnmarshalQueryData into TableStats.Columns
	err = UnmarshalQueryData(rows, columnHeaders, &columnsStats)
	assert.Nil(t, err)
	assert.Equal(t, len(rows), len(columnsStats))
	newFloat64 := func(f float64) *float64 {
		return &f
	}
	newString := func(v string) *string {
		return &v
	}
	zero := newFloat64(0)
	assert.Equal(t, []ColumnStats{
		{"orderkey", nil, newFloat64(1500254), zero, nil, newString("1"), newString("6000000"), nil, nil, nil},
		{"partkey", nil, newFloat64(200044), zero, nil, newString("1"), newString("200000"), nil, nil, nil},
		{"suppkey", nil, newFloat64(10000.0), zero, nil, newString("1"), newString("10000"), nil, nil, nil},
		{"linenumber", nil, newFloat64(7.0), zero, nil, newString("1"), newString("7"), nil, nil, nil},
		{"quantity", nil, newFloat64(50.0), zero, nil, newString("1.0"), newString("50.0"), nil, nil, nil},
		{"extendedprice", nil, newFloat64(933985.0), zero, nil, newString("901.0"), newString("104949.5"), nil, nil, nil},
		{"discount", nil, newFloat64(11.0), zero, nil, newString("0.0"), newString("0.1"), nil, nil, nil},
		{"tax", nil, newFloat64(9.0), zero, nil, newString("0.0"), newString("0.08"), nil, nil, nil},
		{"returnflag", newFloat64(6001215.0), newFloat64(3.0), zero, nil, nil, nil, nil, nil, nil},
		{"linestatus", newFloat64(6001215.0), newFloat64(2.0), zero, nil, nil, nil, nil, nil, nil},
		{"shipdate", nil, newFloat64(2526.0), zero, nil, newString("1992-01-02"), newString("1998-12-01"), nil, nil, nil},
		{"commitdate", nil, newFloat64(2466.0), zero, nil, newString("1992-01-31"), newString("1998-10-31"), nil, nil, nil},
		{"receiptdate", nil, newFloat64(2554.0), zero, nil, newString("1992-01-04"), newString("1998-12-31"), nil, nil, nil},
		{"shipinstruct", newFloat64(7.2006409e7), newFloat64(4.0), zero, nil, nil, nil, nil, nil, nil},
		{"shipmode", newFloat64(2.5717034e7), newFloat64(7.0), zero, nil, nil, nil, nil, nil, nil},
		{"comment", newFloat64(1.58997209e8), newFloat64(4580252.0), zero, nil, nil, nil, nil, nil, nil},
		{"", nil, nil, nil, newFloat64(6001215.0), nil, nil, nil, nil, nil},
	}, columnsStats)
}

func getRowsFromPresto(t *testing.T) ([]json.RawMessage, []Column) {
	client, _ := NewClient("http://localhost:8080", false)
	client.Catalog("tpch").Schema("sf1")
	ctx := context.Background()

	clientResult, _, err := client.Query(ctx, "SHOW STATS FOR lineitem")
	assert.Nil(t, err)

	rows := make([]json.RawMessage, 0, 17)
	err = clientResult.Drain(ctx, func(qr *QueryResults) error {
		rows = append(rows, qr.Data...)
		return nil
	})
	if assert.Nil(t, err) {
		return rows, clientResult.Columns
	} else {
		return rows, nil
	}
}

func getBuiltinRows(t *testing.T) ([]json.RawMessage, []Column) {
	rows := make([]json.RawMessage, 0, 17)
	assert.Nil(t, json.Unmarshal(rowsBytes, &rows))
	columnHeaders := make([]Column, 0, 8)
	assert.Nil(t, json.Unmarshal(columnHeaderBytes, &columnHeaders))
	return rows, columnHeaders
}

var rowsBytes = []byte(`[["orderkey",null,1500254.0,0.0,null,"1","6000000",null],
["partkey",null,200044.0,0.0,null,"1","200000",null],
["suppkey",null,10000.0,0.0,null,"1","10000",null],
["linenumber",null,7.0,0.0,null,"1","7",null],
["quantity",null,50.0,0.0,null,"1.0","50.0",null],
["extendedprice",null,933985.0,0.0,null,"901.0","104949.5",null],
["discount",null,11.0,0.0,null,"0.0","0.1",null],
["tax",null,9.0,0.0,null,"0.0","0.08",null],
["returnflag",6001215.0,3.0,0.0,null,null,null,null],
["linestatus",6001215.0,2.0,0.0,null,null,null,null],
["shipdate",null,2526.0,0.0,null,"1992-01-02","1998-12-01",null],
["commitdate",null,2466.0,0.0,null,"1992-01-31","1998-10-31",null],
["receiptdate",null,2554.0,0.0,null,"1992-01-04","1998-12-31",null],
["shipinstruct",7.2006409E7,4.0,0.0,null,null,null,null],
["shipmode",2.5717034E7,7.0,0.0,null,null,null,null],
["comment",1.58997209E8,4580252.0,0.0,null,null,null,null],
[null,null,null,null,6001215.0,null,null,null]]`)

var columnHeaderBytes = []byte(`[
  {
    "name": "column_name",
    "type": "varchar",
    "typeSignature": {
      "rawType": "varchar"
    }
  },
  {
    "name": "data_size",
    "type": "double",
    "typeSignature": {
      "rawType": "double"
    }
  },
  {
    "name": "distinct_values_count",
    "type": "double",
    "typeSignature": {
      "rawType": "double"
    }
  },
  {
    "name": "nulls_fraction",
    "type": "double",
    "typeSignature": {
      "rawType": "double"
    }
  },
  {
    "name": "row_count",
    "type": "double",
    "typeSignature": {
      "rawType": "double"
    }
  },
  {
    "name": "low_value",
    "type": "varchar",
    "typeSignature": {
      "rawType": "varchar"
    }
  },
  {
    "name": "high_value",
    "type": "varchar",
    "typeSignature": {
      "rawType": "varchar"
    }
  },
  {
    "name": "histogram",
    "type": "varchar",
    "typeSignature": {
      "rawType": "varchar"
    }
  }
]`)
