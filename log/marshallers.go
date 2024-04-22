package log

import (
	"fmt"
	"github.com/rs/zerolog"
	"reflect"
	"sort"
	"strings"
	"time"
)

var (
	MaskPointerValueForTesting bool
	DefaultNestedLevelLimit    = 3
	DefaultFieldOrElementLimit = 15

	durationType      = reflect.TypeOf((*time.Duration)(nil)).Elem()
	stringerInterface = reflect.TypeOf((*fmt.Stringer)(nil)).Elem()
	errorInterface    = reflect.TypeOf((*error)(nil)).Elem()
)

type Marshaller struct {
	value               reflect.Value
	nestedLevel         int
	nestedLevelLimit    int
	fieldOrElementLimit int
}

func (ms *Marshaller) SetNestedLevelLimit(nestedLevelLimit int) *Marshaller {
	ms.nestedLevelLimit = nestedLevelLimit
	return ms
}

func (ms *Marshaller) SetFieldOrElementLimit(fieldOrElementLimit int) *Marshaller {
	ms.fieldOrElementLimit = fieldOrElementLimit
	return ms
}

func (ms *Marshaller) SetNestedLevel(nestedLevel int) *Marshaller {
	ms.nestedLevel = nestedLevel
	return ms
}

func (ms *Marshaller) Nest() *Marshaller {
	ms.nestedLevel++
	return ms
}

func derefValue(v *reflect.Value) reflect.Kind {
	k := v.Kind()
	for k == reflect.Pointer || k == reflect.Interface {
		*v = v.Elem()
		k = v.Kind()
	}
	return k
}

// NewMarshaller creates a new marshaller to serialize objects to log. If another marshaller
// is provided in "other", its internal state will be copied to the newly created marshaller.
func NewMarshaller(obj any, other ...*Marshaller) *Marshaller {
	v, ok := obj.(reflect.Value)
	if !ok {
		v = reflect.ValueOf(obj)
	}
	derefValue(&v)
	if len(other) > 0 {
		return &Marshaller{
			value:               v,
			nestedLevel:         other[0].nestedLevel,
			nestedLevelLimit:    other[0].nestedLevelLimit,
			fieldOrElementLimit: other[0].fieldOrElementLimit,
		}
	}
	return &Marshaller{
		value:               v,
		nestedLevel:         1,
		nestedLevelLimit:    DefaultNestedLevelLimit,
		fieldOrElementLimit: DefaultFieldOrElementLimit,
	}
}

func (ms *Marshaller) MarshalZerologObject(e *zerolog.Event) {
	switch k := ms.value.Kind(); k {
	case reflect.Map:
		ms.marshalZerologMap(e)
	case reflect.Struct:
		typ := ms.value.Type()
		for i := 0; i < ms.value.NumField(); i++ {
			if i >= ms.fieldOrElementLimit {
				e.Str("...", "<field truncated>")
				break
			}
			ms.logField(e, toSnakeCase(typ.Field(i).Name), ms.value.Field(i))
		}
	case reflect.Slice, reflect.Array:
		e.Array("array", ms)
	default:
		e.Str("kind", fmt.Sprint(k)).
			Str("type", fmt.Sprint(ms.value.Type()))
		if ms.value.Type().Implements(stringerInterface) {
			e.Str("value", fmt.Sprint(ms.value))
		} else {
			e.Str("value", fmt.Sprintf("%#v", ms.value))
		}
	}
}

func (ms *Marshaller) marshalZerologMap(e *zerolog.Event) {
	// We first get all the keys from the map then sort those keys.
	fieldNameMap := make(map[string]reflect.Value)
	for _, mapKey := range ms.value.MapKeys() {
		for k := mapKey.Kind(); k == reflect.Pointer || k == reflect.Interface; k = mapKey.Kind() {
			mapKey = mapKey.Elem()
		}
		fieldNameMap[fmt.Sprint(mapKey)] = mapKey
	}
	fieldNames := make([]string, 0, len(fieldNameMap))
	for fieldName := range fieldNameMap {
		fieldNames = append(fieldNames, fieldName)
	}
	sort.Strings(fieldNames)
	for i := 0; i < len(fieldNames); i++ {
		if i >= ms.fieldOrElementLimit {
			e.Str("...", "<map truncated>")
			break
		}
		ms.logField(e, toSnakeCase(fieldNames[i]), ms.value.MapIndex(fieldNameMap[fieldNames[i]]))
	}
}

func tryCastToTime(v reflect.Value) *time.Time {
	if !v.CanInterface() {
		return nil
	}
	if t, ok := v.Interface().(time.Time); ok {
		return &t
	}
	return nil
}

func (ms *Marshaller) MarshalZerologArray(a *zerolog.Array) {
	if k := ms.value.Kind(); k != reflect.Array && k != reflect.Slice {
		return
	}
	for i := 0; i < ms.value.Len(); i++ {
		if i >= ms.fieldOrElementLimit {
			a.Str("...")
			break
		}
		v := ms.value.Index(i)
		k := derefValue(&v)
		if k == reflect.Invalid {
			continue
		}
		if v.Type().Implements(errorInterface) {
			a.Err(v.Interface().(error))
			continue
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
			if v.Type() == durationType {
				a.Dur(v.Interface().(time.Duration))
			} else {
				a.Int64(v.Int())
			}
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
		case reflect.Struct, reflect.Map:
			if t := tryCastToTime(v); t != nil {
				a.Time(*t)
			} else if ms.nestedLevel+1 > ms.nestedLevelLimit {
				a.Str(v.String())
			} else {
				a.Object(NewMarshaller(v, ms).Nest())
			}
		default:
			if v.Type().Implements(stringerInterface) {
				a.Str(fmt.Sprint(v))
			} else {
				a.Object(NewMarshaller(v, ms))
			}
		}
	}
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

func (ms *Marshaller) logField(e *zerolog.Event, fieldName string, field reflect.Value) {
	k := derefValue(&field)
	if k == reflect.Invalid {
		return
	}
	if field.Type().Implements(errorInterface) {
		e.AnErr(fieldName, field.Interface().(error))
		return
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
		if field.Type() == durationType {
			e.Dur(fieldName, field.Interface().(time.Duration))
		} else {
			e.Int64(fieldName, field.Int())
		}
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
		if ms.nestedLevel+1 > ms.nestedLevelLimit {
			e.Str(fieldName, field.String())
		} else {
			e.Array(fieldName, NewMarshaller(field, ms).Nest())
		}
	case reflect.Struct, reflect.Map:
		if t := tryCastToTime(field); t != nil {
			e.Time(fieldName, *t)
		} else if ms.nestedLevel+1 > ms.nestedLevelLimit {
			e.Str(fieldName, field.String())
		} else {
			e.Object(fieldName, NewMarshaller(field, ms).Nest())
		}
	default:
		if field.Type().Implements(stringerInterface) {
			e.Str(fieldName, fmt.Sprint(field))
		} else {
			e.Object(fieldName, NewMarshaller(field))
		}
	}
}
