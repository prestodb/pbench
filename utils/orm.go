package utils

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"pbench/presto/query_json"
	"reflect"
	"strings"
)

type TableName string

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
		placeholders := strings.Repeat("?,", rows[0].ColumnCount())
		// Get rid of the trailing comma.
		placeholders = placeholders[:len(placeholders)-1]
		sqlStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
			table, strings.Join(rows[0].ColumnNames, ","), placeholders)

		for _, row := range rows {
			//log.Info().Str("sql", sqlStmt).Array("values", log.NewMarshaller(row.Values)).Msg("execute sql")
			_, err := tx.Exec(sqlStmt, row.Values...)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

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
