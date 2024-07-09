package cmp

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/spf13/cobra"
)

func TestRun(t *testing.T) {
	// Paths to existing directories
	buildDir := "./test_1"
	probeDir := "./test_2"

	// Set up test output directory
	outputDir, err := os.MkdirTemp("", "output")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(outputDir)

	// Set up test parameters
	OutputPath = outputDir
	FileIdRegexStr = `.*(query_\d{2})(?:_c0)?\.output`

	// Run the function
	Run(&cobra.Command{}, []string{buildDir, probeDir})

	// Check if diff files were created as expected
	expectedDiffs := []string{"query_02.diff", "query_03.diff"}
	for _, diffFile := range expectedDiffs {
		diffPath := filepath.Join(outputDir, diffFile)
		if _, err := os.Stat(diffPath); os.IsNotExist(err) {
			t.Errorf("Expected diff file %s to be created, but it doesn't exist", diffPath)
		}
	}
	/*
		// Check if no diff file was created for matching files
		noDiffFile := filepath.Join(outputDir, "query_01.diff")
		if _, err := os.Stat(noDiffFile); !os.IsNotExist(err) {
			t.Errorf("Expected no diff file for %s, but it exists", noDiffFile)
		}

		// Check the content of a diff file
		query02DiffPath := filepath.Join(outputDir, "query_02.diff")
		content, err := os.ReadFile(query02DiffPath)
		if err != nil {
			t.Fatalf("Failed to read diff file %s: %v", query02DiffPath, err)
		}
		if len(content) == 0 {
			t.Errorf("Diff file %s is empty", query02DiffPath)
		}
	*/
	// You can add more specific checks on the content of the diff files here
}
