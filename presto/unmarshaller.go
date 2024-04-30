package presto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var (
	RawJsonMessageType    = reflect.TypeOf((*json.RawMessage)(nil)).Elem()
	InvalidUnmarshalError = errors.New("unmarshall: receiving value")
	structColumnMapCache  = make(map[reflect.Type]map[string]int)
)

// Get fields with presto field tag (column name) from v, map the column name to field index.
func buildColumnMap(t reflect.Type) map[string]int {
	k := t.Kind()
	if k == reflect.Interface || k == reflect.Pointer {
		t = t.Elem()
		k = t.Kind()
	}
	if t.Kind() != reflect.Struct {
		return nil
	}
	if columnMap, cached := structColumnMapCache[t]; cached {
		return columnMap
	}
	columnMap := make(map[string]int)
	for i := 0; i < t.NumField(); i++ {
		if columnName := t.Field(i).Tag.Get("presto"); columnName != "" {
			columnMap[columnName] = i
		}
	}
	structColumnMapCache[t] = columnMap
	return columnMap
}

func unmarshalScalar(data any, v reflect.Value) {
	vt := v.Type()
	for v.Kind() == reflect.Pointer {
		if v.IsNil() {
			v.Set(reflect.New(vt.Elem()))
		}
		v = v.Elem()
		vt = v.Type()
		continue
	}
	if !v.CanSet() {
		return
	}

	dv := reflect.ValueOf(data)
	if dv.CanConvert(vt) {
		v.Set(dv.Convert(vt))
	}
}

func UnmarshalQueryData(data []json.RawMessage, columns []Column, v any) error {
	if len(data) == 0 {
		return nil
	}
	vPtr := reflect.ValueOf(v)
	if vPtr.Kind() != reflect.Pointer {
		return fmt.Errorf("%w must be a pointer, but it is %T", InvalidUnmarshalError, v)
	} else if vPtr.IsNil() {
		return fmt.Errorf("%w must not be nil", InvalidUnmarshalError)
	}

	vArray := vPtr.Elem()
	vArrayKind := vArray.Kind()
	if vArrayKind != reflect.Slice && vArrayKind != reflect.Array {
		if len(data) > 1 {
			return fmt.Errorf("%w must be a pointer to an array or slice, but it is a pointer to %v", InvalidUnmarshalError, vArray.Type())
		} else {
			var cols []any
			if err := json.Unmarshal(data[0], &cols); err != nil {
				return err
			}
			if len(cols) == 0 {
				return nil
			}
			unmarshalScalar(cols[0], reflect.ValueOf(v))
			return nil
		}
	}

	dataArray := reflect.ValueOf(data)
	vElementType := vArray.Type().Elem()
	// If dest is also []json.RawMessage, just return
	if vElementType == RawJsonMessageType {
		vArray.Set(dataArray)
		return nil
	}

	columnMap := buildColumnMap(vElementType)
	columnCount := len(columns)
	columnFieldIndexes := make([]int, 0, columnCount)
	for _, column := range columns {
		if fieldIndex, ok := columnMap[column.Name]; ok {
			columnFieldIndexes = append(columnFieldIndexes, fieldIndex)
		} else {
			columnFieldIndexes = append(columnFieldIndexes, -1)
		}
	}
	for i := 0; i < dataArray.Len(); i++ {
		if vArrayKind == reflect.Slice {
			if i >= vArray.Cap() {
				vArray.Grow(1)
			}
			if i >= vArray.Len() {
				vArray.SetLen(i + 1)
			}
		}

		vElement := vArray.Index(i)
		dataElement := dataArray.Index(i)
		if dataElement.CanConvert(vElementType) {
			vElement.Set(dataElement.Convert(vElementType))
			continue
		}

		row := make([]any, columnCount)
		// deserialize column values.
		if err := json.Unmarshal(data[i], &row); err != nil {
			return err
		}
		for j, colValue := range row {
			if colValue == nil {
				continue
			}
			fieldIndex := columnFieldIndexes[j]
			if fieldIndex < 0 {
				continue
			}
			field := vElement.Field(fieldIndex)
			unmarshalScalar(colValue, field)
		}
	}
	return nil
}

func QueryAndUnmarshal(ctx context.Context, client *Client, query string, v any) error {
	clientResult, _, err := client.Query(ctx, query)
	if err != nil {
		return err
	}
	rows := make([]json.RawMessage, 0)
	err = clientResult.Drain(ctx, func(qr *QueryResults) error {
		if len(qr.Data) > 0 {
			rows = append(rows, qr.Data...)
		}
		return nil
	})
	if err != nil {
		return err
	}
	return UnmarshalQueryData(rows, clientResult.Columns, v)
}
