package cmp

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRun(t *testing.T) {
	// Paths to existing directories
	//buildDir := "tests/test_1_build"
	//probeDir := "tests/test_1_probe"

	// Set up test output directory
	outputDir, err := os.MkdirTemp("", "output")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(outputDir)

	// Set up test parameters
	OutputPath = outputDir
	FileIdRegexStr = `.*(query_\d{2})(?:_c0)?\.output`

	expectedReturns := []int{2, 1, 0, 0, 0, 0}
	for i := 0; i <= 5; i++ {
		buildDir := fmt.Sprintf("tests/test_%d_build", i)
		probeDir := fmt.Sprintf("tests/test_%d_probe", i)
		returnCode := CompareRun([]string{buildDir, probeDir})
		assert.Equal(t, expectedReturns[i], returnCode, "test_%d failed", i)
	}

	//returnCode := Compare_Run([]string{buildDir, probeDir})

	//assert.Equal(t, expectedReturns, returnCode)

}
