package presto

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
)

var (
	RawJsonMessageType   = reflect.TypeOf((*json.RawMessage)(nil)).Elem()
	UnmarshalError       = errors.New("unmarshall: receiving value")
	structColumnMapCache = make(map[reflect.Type]map[string]int)
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
	} else if v.Kind() == reflect.String {
		v.SetString(fmt.Sprint(dv.Interface()))
	}
}

func unmarshalRow(rawRowData json.RawMessage, v reflect.Value, columnFieldIndexes []int) error {
	rawRowDataValue := reflect.ValueOf(rawRowData)
	vType := v.Type()
	if rawRowDataValue.CanConvert(vType) {
		if v.CanSet() {
			v.Set(rawRowDataValue.Convert(vType))
			return nil
		}
		return fmt.Errorf("%w cannot be set", UnmarshalError)
	}

	row := make([]any, len(columnFieldIndexes))
	// deserialize column values.
	if err := json.Unmarshal(rawRowData, &row); err != nil {
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
		field := v.Field(fieldIndex)
		unmarshalScalar(colValue, field)
	}
	return nil
}

func UnmarshalQueryData(data []json.RawMessage, columns []Column, v any) error {
	if len(data) == 0 {
		return nil
	}
	vPtr := reflect.ValueOf(v)
	if vPtr.Kind() != reflect.Pointer {
		return fmt.Errorf("%w must be a pointer, but it is %T", UnmarshalError, v)
	} else if vPtr.IsNil() {
		if vPtr.CanAddr() {
			vPtr.Set(reflect.New(vPtr.Type().Elem()))
		} else {
			return fmt.Errorf("%w non-addressable value", UnmarshalError)
		}
	}

	vArrayOrStruct := vPtr.Elem()
	vKind, vType := vArrayOrStruct.Kind(), vArrayOrStruct.Type()
	dataArray := reflect.ValueOf(data)
	// map from column name to field index
	var columnMap map[string]int
	if vKind == reflect.Slice || vKind == reflect.Array {
		vElemType := vType.Elem()
		// If dest is also []json.RawMessage, just return
		if vElemType == RawJsonMessageType {
			vArrayOrStruct.Set(dataArray)
			return nil
		}
		columnMap = buildColumnMap(vElemType)
	} else if vKind == reflect.Struct {
		columnMap = buildColumnMap(vType)
	} else {
		// Then this is a scalar value!
		if len(data) > 1 {
			return fmt.Errorf("%w must be a pointer to an array, slice, or struct. But it is a pointer to %v", UnmarshalError, vArrayOrStruct.Type())
		} else {
			var cols []any
			if err := json.Unmarshal(data[0], &cols); err != nil {
				return err
			}
			if len(cols) == 0 {
				return nil
			}
			unmarshalScalar(cols[0], vPtr)
			return nil
		}
	}

	columnFieldIndexes := make([]int, 0, len(columns))
	for _, column := range columns {
		if fieldIndex, ok := columnMap[column.Name]; ok {
			columnFieldIndexes = append(columnFieldIndexes, fieldIndex)
		} else {
			columnFieldIndexes = append(columnFieldIndexes, -1)
		}
	}

	if vKind == reflect.Slice || vKind == reflect.Array {
		for i := 0; i < len(data); i++ {
			if vKind == reflect.Slice {
				if i >= vArrayOrStruct.Cap() {
					vArrayOrStruct.Grow(1)
				}
				if i >= vArrayOrStruct.Len() {
					vArrayOrStruct.SetLen(i + 1)
				}
			}

			vElem := vArrayOrStruct.Index(i)
			if err := unmarshalRow(data[i], vElem, columnFieldIndexes); err != nil {
				return err
			}
		}
	} else if err := unmarshalRow(data[0], vArrayOrStruct, columnFieldIndexes); err != nil { // struct
		return err
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
