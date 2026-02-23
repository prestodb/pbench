package utils

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestPivotEqualLengthArrays(t *testing.T) {
	a := &PrestoFlagsArray{
		ServerUrl:  []string{"http://host1:8080", "http://host2:8080"},
		IsTrino:    []bool{false, true},
		ForceHttps: []bool{false, true},
		UserName:   []string{"user1", "user2"},
		Password:   []string{"pass1", "pass2"},
	}
	result := a.Pivot()
	assert.Equal(t, 2, len(result))
	assert.Equal(t, PrestoFlags{
		ServerUrl:  "http://host1:8080",
		IsTrino:    false,
		ForceHttps: false,
		UserName:   "user1",
		Password:   "pass1",
	}, result[0])
	assert.Equal(t, PrestoFlags{
		ServerUrl:  "http://host2:8080",
		IsTrino:    true,
		ForceHttps: true,
		UserName:   "user2",
		Password:   "pass2",
	}, result[1])
}

func TestPivotSingleServer(t *testing.T) {
	a := &PrestoFlagsArray{
		ServerUrl:  []string{"http://localhost:8080"},
		IsTrino:    []bool{true},
		ForceHttps: []bool{false},
		UserName:   []string{"admin"},
		Password:   []string{"secret"},
	}
	result := a.Pivot()
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "http://localhost:8080", result[0].ServerUrl)
	assert.True(t, result[0].IsTrino)
	assert.Equal(t, "admin", result[0].UserName)
}

func TestPivotMismatchedLengths(t *testing.T) {
	a := &PrestoFlagsArray{
		ServerUrl:  []string{"http://host1:8080", "http://host2:8080", "http://host3:8080"},
		IsTrino:    []bool{true},
		ForceHttps: []bool{},
		UserName:   []string{"user1", "user2"},
		Password:   []string{"pass1"},
	}
	result := a.Pivot()
	assert.Equal(t, 3, len(result))
	// Only the first server gets IsTrino=true
	assert.True(t, result[0].IsTrino)
	assert.False(t, result[1].IsTrino)
	assert.False(t, result[2].IsTrino)
	// Only the first two servers get usernames
	assert.Equal(t, "user1", result[0].UserName)
	assert.Equal(t, "user2", result[1].UserName)
	assert.Equal(t, "", result[2].UserName)
	// Only the first server gets a password
	assert.Equal(t, "pass1", result[0].Password)
	assert.Equal(t, "", result[1].Password)
}

func TestPivotEmptyArrays(t *testing.T) {
	a := &PrestoFlagsArray{
		ServerUrl: []string{"http://host1:8080"},
	}
	result := a.Pivot()
	assert.Equal(t, 1, len(result))
	assert.Equal(t, "http://host1:8080", result[0].ServerUrl)
	assert.False(t, result[0].IsTrino)
	assert.Equal(t, "", result[0].UserName)
}

func TestNewPrestoClient(t *testing.T) {
	pf := &PrestoFlags{
		ServerUrl: "http://localhost:8080",
		UserName:  "testuser",
	}
	client := pf.NewPrestoClient()
	assert.NotNil(t, client)
}
