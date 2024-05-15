package plan_node

import (
	"errors"
	"fmt"
	"strings"
)

const (
	BELOW Bound = iota
	EXACTLY
	ABOVE
)

const (
	MinValue = "<min>"
	MaxValue = "<max>"
)

var (
	boundMap = map[string]Bound{
		"[": EXACTLY, "]": EXACTLY, "(": ABOVE, ")": BELOW, MinValue: ABOVE, MaxValue: BELOW}
	InvalidMarkerError = errors.New("invalid Marker")
)

func (b *Bound) Capture(values []string) error {
	*b = boundMap[values[0]]
	return nil
}

func (s *UnquotedString) Capture(values []string) error {
	*s = UnquotedString(strings.Trim(values[0], `"'`))
	return nil
}

func (m *Marker) Capture(values []string) error {
	if len(values) != 2 {
		return fmt.Errorf("%w %s", InvalidMarkerError, strings.Join(values, " "))
	}
	for _, v := range values {
		if bound, found := boundMap[v]; found {
			m.Bound = bound
		} else if err := m.Value.Capture([]string{v}); err != nil {
			return err
		}
	}
	return nil
}

func (r *Range) String() string {
	if r.LowValue == nil {
		return "<error>"
	}
	builder := strings.Builder{}
	if r.LowValue.Value == "" {
		builder.WriteString("(<min>")
	} else {
		if r.LowValue.Bound == EXACTLY {
			builder.WriteString("[")
		} else {
			builder.WriteString("(")
		}
		builder.WriteString(`"` + string(r.LowValue.Value) + `"`)
	}
	if r.HighValue == nil {
		builder.WriteString("]")
	} else if r.HighValue.Value == "" {
		builder.WriteString(", <max>)")
	} else {
		builder.WriteString(", \"" + string(r.HighValue.Value) + "\"")
		if r.HighValue.Bound == EXACTLY {
			builder.WriteString("]")
		} else {
			builder.WriteString(")")
		}
	}
	return builder.String()
}
