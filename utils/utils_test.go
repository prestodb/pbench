package utils

import (
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"testing"
)

func TestExpandHomeDirectory(t *testing.T) {
	path := "~/Downloads"
	ExpandHomeDirectory(&path)
	assert.Equal(t, filepath.Join(os.Getenv("HOME"), "Downloads"), path)
}
