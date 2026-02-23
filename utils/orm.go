package utils

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"

	"github.com/ethanyzhang/presto-go/query_json"
)

type TableName string

// MergeRowsMap merges two table→rows maps by computing a cartesian product per table. For each table
// in b, its rows are cross-joined with the existing rows in a for that table (via MultiplyRows). If a
// has no rows yet for a table, a single empty row is used as the seed so that b's columns are preserved.
//
// This is the core mechanism for denormalizing nested structs: parent-level scalar fields produce 1 row
// in a, and a nested slice of N structs produces N rows in b. The merge yields N rows, each carrying
// both the parent's columns and one child's columns — equivalent to a SQL cross join.
func MergeRowsMap(a, b map[TableName][]*Row) map[TableName][]*Row {
	for tableName, rows2 := range b {
		rows1 := a[tableName]
		if len(rows1) == 0 {
			rows1 = []*Row{NewRowWithColumnCapacity(16)}
		}
		a[tableName] = MultiplyRows(rows1, rows2)
	}
	return a
}

func SqlInsertObject(ctx context.Context, db *sql.DB, obj any, tableNames ...TableName) (returnedErr error) {
	tx, beginTxErr := db.BeginTx(ctx, nil)
	if beginTxErr != nil {
		return beginTxErr
	}
	defer func() {
		if returnedErr != nil {
			_ = tx.Rollback()
		} else {
			returnedErr = tx.Commit()
		}
	}()
	v, ok := obj.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(obj)
	}
	if k := DerefValue(&v); k != reflect.Struct {
		return fmt.Errorf("obj must be a struct, got %v", k)
	}
	rowsMap := collectRowsForEachTable(v, tableNames...)

	for table, rows := range rowsMap {
		if len(rows) == 0 {
			continue
		}
		if len(rows) == 1 && rows[0].ColumnCount() <= 1 {
			continue
		}

		for _, row := range rows {
			placeholders := strings.Repeat("?,", row.ColumnCount())
			// Get rid of the trailing comma.
			placeholders = placeholders[:len(placeholders)-1]
			sqlStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
				table, strings.Join(row.ColumnNames, ","), placeholders)
			//log.Info().Str("sql", sqlStmt).Array("values", log.NewMarshaller(row.Values)).Msg("execute sql")
			_, err := tx.Exec(sqlStmt, row.Values...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// collectRowsForEachTable uses reflection to extract SQL rows from a struct, using struct field tags
// as column mappings. Each tableNames entry corresponds to a tag key (e.g., "table_a"); a field tagged
// `table_a:"col_name"` contributes its value as column "col_name" to the "table_a" row.
//
// Nested structs are traversed recursively and their columns are merged via cartesian product with the
// parent's columns (so if a parent contributes 1 row and a child slice contributes 3, the result is 3
// rows each containing both parent and child columns). Slice/array fields of structs produce one row
// per element. json.RawMessage values are compacted, and query_json.Duration values are converted to
// milliseconds.
func collectRowsForEachTable(v reflect.Value, tableNames ...TableName) (rowsMap map[TableName][]*Row) {
	rowsMap = make(map[TableName][]*Row)
	if k := DerefValue(&v); k != reflect.Struct {
		return
	}
	t, fieldCount := v.Type(), v.NumField()
	for i := 0; i < fieldCount; i++ {
		f, fv := t.Field(i), v.Field(i)
		fvk := DerefValue(&fv)
		if fvk == reflect.Invalid {
			continue
		}
		var fieldValue any
		for _, tableName := range tableNames {
			columnName := f.Tag.Get(string(tableName))
			if columnName == "" {
				continue
			}
			if fieldValue == nil {
				switch typed := fv.Interface().(type) {
				case json.RawMessage:
					compactedJson := &bytes.Buffer{}
					if err := json.Compact(compactedJson, typed); err == nil {
						typed = compactedJson.Bytes()
					}
					fieldValue = string(typed)
				case query_json.Duration:
					// TODO: Add a tag for precision. EventListener only uses ms.
					fieldValue = typed.Milliseconds()
				default:
					fieldValue = fv.Interface()
				}
			}
			if len(rowsMap[tableName]) == 0 {
				rowsMap[tableName] = []*Row{NewRowWithColumnCapacity(16)}
			}
			for _, row := range rowsMap[tableName] {
				row.AddColumn(columnName, fieldValue)
			}
		}
		if fvk == reflect.Struct {
			rowsMapFromStruct := collectRowsForEachTable(fv, tableNames...)
			rowsMap = MergeRowsMap(rowsMap, rowsMapFromStruct)
		} else if fvk == reflect.Array || fvk == reflect.Slice {
			if fv.Len() == 0 {
				continue
			}
			elem := fv.Index(0)
			if k := DerefValue(&elem); k != reflect.Struct {
				continue
			}
			rowsMapFromArray := make(map[TableName][]*Row)
			for j := 0; j < fv.Len(); j++ {
				rowsMapFromElement := collectRowsForEachTable(fv.Index(j), tableNames...)
				for table, rmap := range rowsMapFromElement {
					rowsMapFromArray[table] = append(rowsMapFromArray[table], rmap...)
				}
			}
			rowsMap = MergeRowsMap(rowsMap, rowsMapFromArray)
		}
	}
	return
}
