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

type StringMapMarshaller map[string]string

func (m StringMapMarshaller) MarshalZerologObject(e *zerolog.Event) {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, key := range keys {
		e.Str(key, m[key])
	}
}

var logObjectMarshallerInterface = reflect.TypeOf((*zerolog.LogObjectMarshaler)(nil)).Elem()
var logArrayMarshallerInterface = reflect.TypeOf((*zerolog.LogArrayMarshaler)(nil)).Elem()

type ArrayMarshaller struct {
	arrValue reflect.Value
}

func NewArrayMarshaller(arr any) zerolog.LogArrayMarshaler {
	if marshaller, ok := arr.(zerolog.LogArrayMarshaler); ok {
		return marshaller
	}
	var (
		v  reflect.Value
		ok bool
	)
	if v, ok = arr.(reflect.Value); !ok {
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
	return &ArrayMarshaller{arrValue: v}
}

func (arr *ArrayMarshaller) MarshalZerologArray(a *zerolog.Array) {
	for i := 0; i < arr.arrValue.Len(); i++ {
		v := arr.arrValue.Index(i)
		for v.Kind() == reflect.Interface || v.Kind() == reflect.Pointer {
			if v.Type().Implements(logObjectMarshallerInterface) {
				a.Object(v.Interface().(zerolog.LogObjectMarshaler))
				break
			} else {
				v = v.Elem()
			}
		}
		switch v.Kind() {
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
			a.Object(NewObjectMarshaller(v))
		case reflect.Interface, reflect.Pointer:
		default:
			a.Object(NewUnsupportedTypeMarshaller(v))
		}
	}
}

type ObjectMarshaller struct {
	objValue reflect.Value
}

func NewObjectMarshaller(obj any) zerolog.LogObjectMarshaler {
	if marshaller, ok := obj.(zerolog.LogObjectMarshaler); ok {
		return marshaller
	}
	var (
		v       reflect.Value
		success bool
	)
	if v, success = obj.(reflect.Value); !success {
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
	if k != reflect.Struct && k != reflect.Array && k != reflect.Slice {
		log.Panicf("the supplied arr parameter is %v, not a struct, array, or slice.", k)
	}
	return &ObjectMarshaller{objValue: v}
}

func (obj *ObjectMarshaller) MarshalZerologObject(e *zerolog.Event) {
	if k := obj.objValue.Kind(); k == reflect.Array || k == reflect.Slice {
		e.Array("array", NewArrayMarshaller(obj.objValue))
		return
	}
	typ := obj.objValue.Type()
	for i := 0; i < obj.objValue.NumField(); i++ {
		field := obj.objValue.Field(i)
		fieldName := strings.ToLower(typ.Field(i).Name)
		ft := field.Type()
		for field.Kind() == reflect.Interface || field.Kind() == reflect.Pointer {
			if ft.Implements(logObjectMarshallerInterface) {
				e.Object(fieldName, field.Interface().(zerolog.LogObjectMarshaler))
				break
			} else if ft.Implements(logArrayMarshallerInterface) {
				e.Array(fieldName, field.Interface().(zerolog.LogArrayMarshaler))
				break
			} else {
				field = field.Elem()
			}
		}
		switch field.Kind() {
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
			e.Array(fieldName, NewArrayMarshaller(field))
		case reflect.Struct:
			e.Object(fieldName, NewObjectMarshaller(field))
		case reflect.Invalid, reflect.Pointer, reflect.Interface:
		default:
			e.Object(fieldName, NewUnsupportedTypeMarshaller(field))
		}
	}
}

type UnsupportedTypeMarshaller struct {
	value reflect.Value
}

func NewUnsupportedTypeMarshaller(value any) *UnsupportedTypeMarshaller {
	var (
		v       reflect.Value
		success bool
	)
	if v, success = value.(reflect.Value); !success {
		v = reflect.ValueOf(value)
	}
	return &UnsupportedTypeMarshaller{value: v}
}

func (m *UnsupportedTypeMarshaller) MarshalZerologObject(e *zerolog.Event) {
	e.Str("type", fmt.Sprint(m.value.Kind()))
	e.Str("value", fmt.Sprint(m.value))
}
