package replay

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestFloat64Time_UnmarshalJSON(t *testing.T) {
	tests := []struct {
		name    string
		input   string
		wantErr bool
		check   func(t *testing.T, ft Float64Time)
	}{
		{
			name:  "valid float64 timestamp",
			input: `1700000000.0`,
			check: func(t *testing.T, ft Float64Time) {
				expected := time.UnixMilli(int64(1700000000.0 * 1000))
				assert.Equal(t, expected, ft.Time)
			},
		},
		{
			name:  "null value",
			input: `null`,
			check: func(t *testing.T, ft Float64Time) {
				assert.True(t, ft.Time.IsZero(), "null should produce zero time")
			},
		},
		{
			name:    "string value",
			input:   `"not a number"`,
			wantErr: true,
		},
		{
			name:    "boolean value",
			input:   `true`,
			wantErr: true,
		},
		{
			name:  "zero value",
			input: `0.0`,
			check: func(t *testing.T, ft Float64Time) {
				assert.Equal(t, time.UnixMilli(0), ft.Time)
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var ft Float64Time
			err := json.Unmarshal([]byte(tt.input), &ft)
			if tt.wantErr {
				require.Error(t, err)
				return
			}
			require.NoError(t, err)
			tt.check(t, ft)
		})
	}
}

func TestFloat64Time_UnmarshalJSON_InStruct(t *testing.T) {
	// Simulate a query log entry where timestamps can be null
	type Entry struct {
		CreateTime Float64Time `json:"createTime,omitempty"`
		EndTime    Float64Time `json:"endTime,omitempty"`
	}

	input := `{"createTime": 1700000000.0, "endTime": null}`
	var entry Entry
	require.NoError(t, json.Unmarshal([]byte(input), &entry))

	assert.False(t, entry.CreateTime.Time.IsZero())
	assert.True(t, entry.EndTime.Time.IsZero())
}
