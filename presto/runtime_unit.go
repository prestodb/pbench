package presto

import (
	"fmt"
	"strconv"
)

type RuntimeUnit int8

const (
	RuntimeUnitNone RuntimeUnit = iota
	RuntimeUnitNano
	RuntimeUnitByte
)

var runtimeUnitMap = NewBiMap(map[RuntimeUnit]string{
	RuntimeUnitNone: "NONE",
	RuntimeUnitNano: "NANO",
	RuntimeUnitByte: "BYTE",
})

func (u *RuntimeUnit) String() (string, error) {
	if value, ok := runtimeUnitMap.Lookup(*u); ok {
		return value, nil
	}
	return strconv.Itoa(int(*u)), fmt.Errorf("unknown RuntimeUnit %d", int(*u))
}

func ParseRuntimeUnit(str string) (RuntimeUnit, error) {
	if key, ok := runtimeUnitMap.RLookup(str); ok {
		return key, nil
	}
	return RuntimeUnit(0), fmt.Errorf("unknown RuntimeUnit string %s, defaulting to %s",
		str, runtimeUnitMap.DirectLookup(RuntimeUnit(0)))
}
func (u *RuntimeUnit) MarshalText() (text []byte, err error) {
	str, err := u.String()
	return []byte(str), err
}

func (u *RuntimeUnit) UnmarshalText(text []byte) error {
	var err error
	*u, err = ParseRuntimeUnit(string(text))
	return err
}
