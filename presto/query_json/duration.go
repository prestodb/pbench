package query_json

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/xhit/go-str2duration/v2"
)

type Duration struct {
	time.Duration
}

func (d *Duration) String() string {
	return d.Duration.String()
}

func (d *Duration) MarshalJSON() ([]byte, error) {
	return json.Marshal(d.String())
}

func (d *Duration) UnmarshalJSON(bytes []byte) error {
	var v any
	if err := json.Unmarshal(bytes, &v); err != nil {
		return err
	}
	switch value := v.(type) {
	case float64:
		// Milliseconds
		d.Duration = time.Duration(value * 1e6)
		return nil
	case string:
		var err error
		d.Duration, err = str2duration.ParseDuration(value)
		if err != nil {
			return err
		}
		return nil
	default:
		return fmt.Errorf("invalid duration")
	}
}
