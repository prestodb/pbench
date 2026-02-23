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

func TestExpandHomeDirectory_TildeUsername(t *testing.T) {
	// ~otheruser paths should NOT be expanded (only ~ and ~/ are supported)
	path := "~otheruser/files"
	ExpandHomeDirectory(&path)
	assert.Equal(t, "~otheruser/files", path, "should not expand ~username paths")
}

func TestExpandHomeDirectory_JustTilde(t *testing.T) {
	path := "~"
	ExpandHomeDirectory(&path)
	assert.Equal(t, os.Getenv("HOME"), path)
}
