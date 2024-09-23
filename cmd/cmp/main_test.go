package cmp

import (
	"fmt"
	"github.com/stretchr/testify/assert"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRun(t *testing.T) {

	FileIdRegexStr = `.*(query_\d{2})(?:_c0)?\.output`

	for i := 0; i <= 5; i++ {
		outputDir, err := os.MkdirTemp("tests", fmt.Sprintf("test_%d_output_", i))
		if !assert.Nil(t, err) {
			t.FailNow()
		}
		OutputPath = outputDir

		buildDir := fmt.Sprintf("tests/test_%d_build", i)
		probeDir := fmt.Sprintf("tests/test_%d_probe", i)
		compareRun([]string{buildDir, probeDir})

		err = compareResults(fmt.Sprintf("baseline/test_%d", i), OutputPath)
		if !assert.Nil(t, err) {
			t.FailNow()
		}

		fmt.Println(os.TempDir())
		err = os.RemoveAll(outputDir)
		if err != nil {
			t.Errorf("Failed to remove output directory %s: %v", outputDir, err)
		}

	}

}

func compareResults(currentDir string, baselineDir string) error {
	// Read directories
	currentFiles, err := os.ReadDir(currentDir)
	if err != nil {
		return fmt.Errorf("error reading current directory %s: %v", currentDir, err)
	}

	baselineFiles, err := os.ReadDir(baselineDir)
	if err != nil {
		return fmt.Errorf("error reading baseline directory %s: %v", baselineDir, err)
	}

	if len(currentFiles) != len(baselineFiles) {
		return fmt.Errorf("incorrect number of diff files produced for %s", currentDir)
	}

	baselineFileMap := make(map[string]bool)
	for _, file := range baselineFiles {
		baselineFileMap[file.Name()] = true
	}

	// Compare diff files
	for _, currentFile := range currentFiles {
		fileName := currentFile.Name()

		if !baselineFileMap[fileName] {
			return fmt.Errorf("diff file %s exists in current directory but not in baseline directory", fileName)
		}

		// Read and store file contents
		currentContent, err := os.ReadFile(filepath.Join(currentDir, fileName))
		if err != nil {
			return fmt.Errorf("error reading current file %s: %v", fileName, err)
		}

		baselineContent, err := os.ReadFile(filepath.Join(baselineDir, fileName))
		if err != nil {
			return fmt.Errorf("error reading baseline file %s: %v", fileName, err)
		}

		// creates slices based on the lines in the diff file
		currentLines := strings.Split(string(currentContent), "\n")
		baselineLines := strings.Split(string(baselineContent), "\n")

		// Ensure there are at least 3 lines in each file, if not, there is an error
		if len(currentLines) < 3 || len(baselineLines) < 3 {
			return fmt.Errorf("file %s has fewer than 3 lines", fileName)
		}

		// Compare contents, ignoring the first two lines
		if !reflect.DeepEqual(currentLines[2:], baselineLines[2:]) {
			return fmt.Errorf("difference found in %s", fileName)
		}

	}

	fmt.Println("All diff files match between current and baseline directories")
	return nil
}
