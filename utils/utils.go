package utils

import (
	"bytes"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"golang.org/x/sys/unix"
	"os"
	"pbench/log"
	"reflect"
	"strings"
)

func PrepareOutputDirectory(path string) {
	if stat, statErr := os.Stat(path); statErr != nil {
		if errors.Is(statErr, unix.ENOENT) {
			if mkdirErr := os.MkdirAll(path, 0755); mkdirErr != nil {
				log.Fatal().Err(mkdirErr).Msg("failed to create output directory")
			} else {
				log.Info().Str("output_path", path).Msg("output directory created")
			}
		} else {
			log.Fatal().Err(statErr).Msg("output path not valid")
		}
	} else if !stat.IsDir() {
		log.Fatal().Str("output_path", path).Msg("output path is not a directory")
	} else {
		log.Info().Str("output_path", path).Msg("output directory")
	}
}

func InitMySQLConnFromCfg(cfgPath string) *sql.DB {
	if cfgPath == "" {
		return nil
	}
	if cfgBytes, ioErr := os.ReadFile(cfgPath); ioErr != nil {
		log.Error().Err(ioErr).Msg("failed to read MySQL connection config")
		return nil
	} else {
		mySQLCfg := &struct {
			Username string `json:"username"`
			Password string `json:"password"`
			Server   string `json:"server"`
			Database string `json:"database"`
		}{}
		if err := json.Unmarshal(cfgBytes, mySQLCfg); err != nil {
			log.Error().Err(err).Msg("failed to unmarshal MySQL connection config for the run recorder")
			return nil
		}
		if db, err := sql.Open("mysql", fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true",
			mySQLCfg.Username, mySQLCfg.Password, mySQLCfg.Server, mySQLCfg.Database)); err != nil {
			log.Error().Err(err).Msg("failed to initialize MySQL connection for the run recorder")
			return nil
		} else {
			return db
		}
	}
}

type Row struct {
	ColumnNames []string
	Values      []any
}

func (r *Row) ColumnCount() int {
	if len(r.ColumnNames) != len(r.Values) {
		panic("invalid state")
	}
	return len(r.ColumnNames)
}

func (r *Row) AddColumn(name string, value any) {
	r.ColumnNames = append(r.ColumnNames, name)
	r.Values = append(r.Values, value)
}

func newRowWithColumnCapacity(numColumns int) *Row {
	return &Row{
		ColumnNames: make([]string, 0, numColumns),
		Values:      make([]any, 0, numColumns),
	}
}

func mergeRow(a, b *Row) (ret *Row) {
	la, lb := a.ColumnCount(), b.ColumnCount()
	if la == 0 {
		return b
	}
	if lb == 0 {
		return a
	}
	l := la + b.ColumnCount()
	ret = &Row{
		ColumnNames: make([]string, l),
		Values:      make([]any, l),
	}
	copy(ret.ColumnNames, a.ColumnNames)
	copy(ret.Values, a.Values)
	copy(ret.ColumnNames[la:], b.ColumnNames)
	copy(ret.Values[la:], b.Values)
	return
}

func multiplyRows(a, b []*Row) []*Row {
	mergedRows := make([]*Row, 0, len(a)*len(b))
	for _, x := range a {
		for _, y := range b {
			mergedRows = append(mergedRows, mergeRow(x, y))
		}
	}
	return mergedRows
}

func derefValue(v *reflect.Value) reflect.Kind {
	k := v.Kind()
	for k == reflect.Pointer || k == reflect.Interface {
		*v = v.Elem()
		k = v.Kind()
	}
	return k
}

func collectRows(v reflect.Value, tableName string) (rows []*Row) {
	if k := derefValue(&v); k != reflect.Struct {
		return
	}
	t, fieldCount := v.Type(), v.NumField()
	for i := 0; i < fieldCount; i++ {
		f, fv := t.Field(i), v.Field(i)
		fvk := derefValue(&fv)
		if columnName := f.Tag.Get(tableName); columnName != "" {
			var newValue any
			if fvk == reflect.Invalid {
				newValue = nil
			} else if jsonMsg, ok := fv.Interface().(json.RawMessage); ok {
				compactedJson := &bytes.Buffer{}
				if err := json.Compact(compactedJson, jsonMsg); err == nil {
					jsonMsg = compactedJson.Bytes()
				}
				newValue = string(jsonMsg)
			} else {
				newValue = fv.Interface()
			}
			if len(rows) == 0 {
				rows = []*Row{newRowWithColumnCapacity(16)}
			}
			for _, row := range rows {
				row.AddColumn(columnName, newValue)
			}
		} else if fvk == reflect.Struct {
			rowsFromStruct := collectRows(fv, tableName)
			if len(rowsFromStruct) > 0 {
				rows = multiplyRows(rows, rowsFromStruct)
			}
		} else if fvk == reflect.Array {
			if fv.Len() == 0 {
				continue
			}
			elem := fv.Index(0)
			if k := derefValue(&elem); k != reflect.Struct {
				continue
			}
			rowsFromArray := make([]*Row, 0, 8)
			for j := 0; j < fv.Len(); j++ {
				rowsFromElement := collectRows(fv.Index(j), tableName)
				rowsFromArray = append(rowsFromArray, rowsFromElement...)
			}
			if len(rowsFromArray) > 0 {
				rows = multiplyRows(rows, rowsFromArray)
			}
		}
	}
	return
}

func SqlInsertObject(db *sql.DB, obj any, tableName string) ([]sql.Result, error) {
	v, ok := obj.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(obj)
	}
	if k := derefValue(&v); k != reflect.Struct {
		return nil, fmt.Errorf("obj must be a struct, got %v", k)
	}
	rows := collectRows(v, tableName)
	if len(rows) == 0 {
		return nil, fmt.Errorf("did not find a column to insert")
	}
	placeholders := strings.Repeat("?,", rows[0].ColumnCount())
	placeholders = placeholders[:len(placeholders)-1]
	sqlStmt := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName, strings.Join(rows[0].ColumnNames, ","), placeholders)

	results := make([]sql.Result, 0, len(rows))
	for _, row := range rows {
		log.Info().Str("sql", sqlStmt).Array("values", log.NewMarshaller(row.Values)).Msg("execute sql")
		result, err := db.Exec(sqlStmt, row.Values...)
		results = append(results, result)
		if err != nil {
			return results, err
		}
	}
	return results, nil
}
