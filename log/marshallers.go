package log

import (
	"fmt"
	"github.com/rs/zerolog"
	"log"
	"reflect"
	"sort"
	"strings"
)

var MaskPointerValueForTesting bool
var logObjectMarshallerInterface = reflect.TypeOf((*zerolog.LogObjectMarshaler)(nil)).Elem()
var logArrayMarshallerInterface = reflect.TypeOf((*zerolog.LogArrayMarshaler)(nil)).Elem()
var NestedLevelLimit = 2
var FieldOrElementLimit = 12

type ObjectMarshaller struct {
	objValue    reflect.Value
	nestedLevel int
}

type MapMarshaller struct {
	mapValue    reflect.Value
	nestedLevel int
}

type ArrayMarshaller struct {
	arrValue    reflect.Value
	nestedLevel int
}

type UnsupportedTypeMarshaller struct {
	value reflect.Value
}

func NewObjectMarshaller(obj any) zerolog.LogObjectMarshaler {
	return newObjectMarshallerWithNestedLevel(obj, 1)
}

func newObjectMarshallerWithNestedLevel(obj any, nestedLevel int) zerolog.LogObjectMarshaler {
	v, ok := obj.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(obj)
	}
	k := v.Kind()
	for k == reflect.Pointer || k == reflect.Interface {
		if v.Type().Implements(logObjectMarshallerInterface) {
			return v.Interface().(zerolog.LogObjectMarshaler)
		}
		v = v.Elem()
		k = v.Kind()
	}
	if k == reflect.Map {
		return &MapMarshaller{mapValue: v}
	}
	if k != reflect.Struct && k != reflect.Array && k != reflect.Slice {
		log.Panicf("the supplied arr parameter is %v, not a struct, array, or slice.", k)
	}
	return &ObjectMarshaller{objValue: v, nestedLevel: nestedLevel}
}

func NewMapMarshaller(m any) zerolog.LogObjectMarshaler {
	return newMapMarshallerWithNestedLevel(m, 1)
}

func newMapMarshallerWithNestedLevel(m any, nestedLevel int) zerolog.LogObjectMarshaler {
	v, ok := m.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(m)
	}
	k := v.Kind()
	for k == reflect.Pointer || k == reflect.Interface {
		if v.Type().Implements(logObjectMarshallerInterface) {
			return v.Interface().(zerolog.LogObjectMarshaler)
		}
		v = v.Elem()
		k = v.Kind()
	}
	if k == reflect.Struct {
		return &ObjectMarshaller{objValue: v}
	}
	if k != reflect.Map {
		log.Panicf("the supplied m parameter is %v, not a map.", k)
	}
	return &MapMarshaller{mapValue: v, nestedLevel: nestedLevel}
}

func NewArrayMarshaller(arr any) zerolog.LogArrayMarshaler {
	return newArrayMarshallerWithNestedLevel(arr, 1)
}

func newArrayMarshallerWithNestedLevel(arr any, nestedLevel int) zerolog.LogArrayMarshaler {
	v, ok := arr.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(arr)
	}
	k := v.Kind()
	for k == reflect.Pointer || k == reflect.Interface {
		if v.Type().Implements(logArrayMarshallerInterface) {
			return v.Interface().(zerolog.LogArrayMarshaler)
		}
		v = v.Elem()
		k = v.Kind()
	}
	if k != reflect.Array && k != reflect.Slice {
		log.Panicf("the supplied arr parameter is %v, not an array or slice.", k)
	}
	return &ArrayMarshaller{arrValue: v, nestedLevel: nestedLevel}
}

func NewUnsupportedTypeMarshaller(value any) zerolog.LogObjectMarshaler {
	v, ok := value.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(value)
	}
	k := v.Kind()
	for k == reflect.Pointer || k == reflect.Interface {
		if v.Type().Implements(logObjectMarshallerInterface) {
			return v.Interface().(zerolog.LogObjectMarshaler)
		}
		v = v.Elem()
		k = v.Kind()
	}
	return &UnsupportedTypeMarshaller{value: v}
}

func (obj *ObjectMarshaller) MarshalZerologObject(e *zerolog.Event) {
	if k := obj.objValue.Kind(); k == reflect.Array || k == reflect.Slice {
		e.Array("array", NewArrayMarshaller(obj.objValue))
		return
	}
	typ := obj.objValue.Type()
	for i := 0; i < obj.objValue.NumField(); i++ {
		if i >= FieldOrElementLimit {
			e.Str("...", "<field truncated>")
			break
		}
		logField(e, toSnakeCase(typ.Field(i).Name), obj.objValue.Field(i), obj.nestedLevel)
	}
}

func (m *MapMarshaller) MarshalZerologObject(e *zerolog.Event) {
	fieldNameMap := make(map[string]reflect.Value)
	for _, mapKey := range m.mapValue.MapKeys() {
		var fieldName string
		k := mapKey.Kind()
		for k == reflect.Pointer || k == reflect.Interface {
			mapKey = mapKey.Elem()
			k = mapKey.Kind()
		}
		switch k {
		case reflect.String:
			fieldName = mapKey.String()
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
			fieldName = fmt.Sprint(mapKey.Int())
		case reflect.Uint, reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64:
			fieldName = fmt.Sprint(mapKey.Uint())
		case reflect.Uintptr:
			if MaskPointerValueForTesting {
				fieldName = "0x0"
			} else {
				fieldName = fmt.Sprint(mapKey.Uint())
			}
		case reflect.Bool:
			fieldName = fmt.Sprint(mapKey.Bool())
		case reflect.Float32, reflect.Float64:
			fieldName = fmt.Sprint(mapKey.Float())
		default:
			fieldName = mapKey.String()
		}
		fieldNameMap[fieldName] = mapKey
	}
	fieldNames := make([]string, 0, len(fieldNameMap))
	for k := range fieldNameMap {
		fieldNames = append(fieldNames, k)
	}
	sort.Strings(fieldNames)
	for i := 0; i < len(fieldNames); i++ {
		if i >= FieldOrElementLimit {
			e.Str("...", "<map truncated>")
			break
		}
		logField(e, toSnakeCase(fieldNames[i]), m.mapValue.MapIndex(fieldNameMap[fieldNames[i]]), m.nestedLevel)
	}
}

func (arr *ArrayMarshaller) MarshalZerologArray(a *zerolog.Array) {
	for i := 0; i < arr.arrValue.Len(); i++ {
		if i >= FieldOrElementLimit {
			a.Str("...")
			break
		}
		v := arr.arrValue.Index(i)
		k := v.Kind()
		for k == reflect.Interface || k == reflect.Pointer {
			if v.Type().Implements(logObjectMarshallerInterface) {
				a.Object(v.Interface().(zerolog.LogObjectMarshaler))
				break
			} else {
				v = v.Elem()
				k = v.Kind()
			}
		}
		switch k {
		case reflect.String:
			a.Str(v.String())
		case reflect.Int:
			a.Int(int(v.Int()))
		case reflect.Int8:
			a.Int8(int8(v.Int()))
		case reflect.Int16:
			a.Int16(int16(v.Int()))
		case reflect.Int32:
			a.Int32(int32(v.Int()))
		case reflect.Int64:
			a.Int64(v.Int())
		case reflect.Uint:
			a.Uint(uint(v.Uint()))
		case reflect.Uint8:
			a.Uint8(uint8(v.Uint()))
		case reflect.Uint16:
			a.Uint16(uint16(v.Uint()))
		case reflect.Uint32:
			a.Uint32(uint32(v.Uint()))
		case reflect.Uint64:
			a.Uint64(v.Uint())
		case reflect.Uintptr:
			if MaskPointerValueForTesting {
				a.Uint64(0)
			} else {
				a.Uint64(v.Uint())
			}
		case reflect.Bool:
			a.Bool(v.Bool())
		case reflect.Float32:
			a.Float32(float32(v.Float()))
		case reflect.Float64:
			a.Float64(v.Float())
		case reflect.Struct:
			if arr.nestedLevel+1 > NestedLevelLimit {
				a.Str(v.String())
			} else {
				a.Object(newObjectMarshallerWithNestedLevel(v, arr.nestedLevel+1))
			}
		case reflect.Map:
			if arr.nestedLevel+1 > NestedLevelLimit {
				a.Str(v.String())
			} else {
				a.Object(newMapMarshallerWithNestedLevel(v, arr.nestedLevel+1))
			}
		default:
			a.Object(NewUnsupportedTypeMarshaller(v))
		}
	}
}

func (m *UnsupportedTypeMarshaller) MarshalZerologObject(e *zerolog.Event) {
	e.Str("type", fmt.Sprint(m.value.Kind()))
	e.Str("value", fmt.Sprint(m.value))
}

func toSnakeCase(in string) string {
	b := strings.Builder{}
	for i := 0; i < len(in); i++ {
		if in[i] >= 'A' && in[i] <= 'Z' {
			if i > 0 {
				b.WriteByte('_')
			}
			b.WriteByte(in[i] - 'A' + 'a')
		} else {
			b.WriteByte(in[i])
		}
	}
	return b.String()
}

func logField(e *zerolog.Event, fieldName string, field reflect.Value, nestedLevel int) {
	k := field.Kind()
	for k == reflect.Interface || k == reflect.Pointer {
		if field.Type().Implements(logObjectMarshallerInterface) {
			e.Object(fieldName, field.Interface().(zerolog.LogObjectMarshaler))
			break
		} else if field.Type().Implements(logArrayMarshallerInterface) {
			e.Array(fieldName, field.Interface().(zerolog.LogArrayMarshaler))
			break
		} else {
			field = field.Elem()
			k = field.Kind()
		}
	}
	switch k {
	case reflect.String:
		e.Str(fieldName, field.String())
	case reflect.Int:
		e.Int(fieldName, int(field.Int()))
	case reflect.Int8:
		e.Int8(fieldName, int8(field.Int()))
	case reflect.Int16:
		e.Int16(fieldName, int16(field.Int()))
	case reflect.Int32:
		e.Int32(fieldName, int32(field.Int()))
	case reflect.Int64:
		e.Int64(fieldName, field.Int())
	case reflect.Uint:
		e.Uint(fieldName, uint(field.Uint()))
	case reflect.Uint8:
		e.Uint8(fieldName, uint8(field.Uint()))
	case reflect.Uint16:
		e.Uint16(fieldName, uint16(field.Uint()))
	case reflect.Uint32:
		e.Uint32(fieldName, uint32(field.Uint()))
	case reflect.Uint64:
		e.Uint64(fieldName, field.Uint())
	case reflect.Uintptr:
		if MaskPointerValueForTesting {
			e.Uint64(fieldName, 0)
		} else {
			e.Uint64(fieldName, field.Uint())
		}
	case reflect.Bool:
		e.Bool(fieldName, field.Bool())
	case reflect.Float32:
		e.Float32(fieldName, float32(field.Float()))
	case reflect.Float64:
		e.Float64(fieldName, field.Float())
	case reflect.Array, reflect.Slice:
		if nestedLevel+1 > NestedLevelLimit {
			e.Str(fieldName, field.String())
		} else {
			e.Array(fieldName, newArrayMarshallerWithNestedLevel(field, nestedLevel+1))
		}
	case reflect.Struct:
		if nestedLevel+1 > NestedLevelLimit {
			e.Str(fieldName, field.String())
		} else {
			e.Object(fieldName, newObjectMarshallerWithNestedLevel(field, nestedLevel+1))
		}
	case reflect.Map:
		if nestedLevel+1 > NestedLevelLimit {
			e.Str(fieldName, field.String())
		} else {
			e.Object(fieldName, newMapMarshallerWithNestedLevel(field, nestedLevel+1))
		}
	case reflect.Invalid:
	default:
		e.Object(fieldName, NewUnsupportedTypeMarshaller(field))
	}
}
