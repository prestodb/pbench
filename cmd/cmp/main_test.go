package cmp

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestRun(t *testing.T) {

	FileIdRegexStr = `.*(query_\d{2})(?:_c0)?\.output`

	expectedReturns := []int{0, 1, 1, 1, 0, 0}
	for i := 0; i <= 5; i++ {
		outputDir, err := os.MkdirTemp("", fmt.Sprintf("test_%d_output_", i))
		if err != nil {
			t.Fatal(err)
		}
		defer os.RemoveAll(outputDir)
		OutputPath = outputDir

		buildDir := fmt.Sprintf("tests/test_%d_build", i)
		probeDir := fmt.Sprintf("tests/test_%d_probe", i)
		returnCode := CompareRun([]string{buildDir, probeDir})
		assert.Equal(t, expectedReturns[i], returnCode, "test_%d failed", i)
	}

	//returnCode := Compare_Run([]string{buildDir, probeDir})

	//assert.Equal(t, expectedReturns, returnCode)

}
