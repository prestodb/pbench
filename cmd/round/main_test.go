package round

import (
	"bufio"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func split(in string) ([]string, error) {
	scanner := bufio.NewScanner(strings.NewReader(in))
	scanner.Split(scanCommaSeparatedField)
	out := make([]string, 0, 10)
	for scanner.Scan() {
		out = append(out, scanner.Text())
	}
	if err := scanner.Err(); err != nil {
		return nil, err
	} else {
		return out, nil
	}
}

func TestIsFileExtAccepted(t *testing.T) {
	// No extensions filter — accept everything.
	FileExtensions = nil
	assert.True(t, isFileExtAccepted("file.csv"))
	assert.True(t, isFileExtAccepted("file.txt"))

	// With extensions filter.
	FileExtensions = []string{".csv", ".tsv"}
	assert.True(t, isFileExtAccepted("data.csv"))
	assert.True(t, isFileExtAccepted("data.tsv"))
	assert.False(t, isFileExtAccepted("data.json"))
	assert.False(t, isFileExtAccepted("data.txt"))

	// Reset.
	FileExtensions = nil
}

func TestProcessRoundDecimalFile(t *testing.T) {
	tmpDir := t.TempDir()
	inPath := filepath.Join(tmpDir, "input.csv")

	content := "1.123456789,hello,2.987654321\n42,world,3.14159265\n"
	require.NoError(t, os.WriteFile(inPath, []byte(content), 0644))

	// Initialize package state.
	DecimalPrecision = 2
	FileFormat = "csv"
	InPlaceRewrite = false
	decimalRegExp = regexp.MustCompile(fmt.Sprintf(`"?(\d+\.\d{%d})\d+"?`, DecimalPrecision))

	err := processRoundDecimalFile(inPath)
	require.NoError(t, err)

	outPath := filepath.Join(tmpDir, "input.rewrite.csv")
	result, err := os.ReadFile(outPath)
	require.NoError(t, err)
	assert.Contains(t, string(result), "1.12")
	assert.Contains(t, string(result), "2.98")
	assert.Contains(t, string(result), "3.14")
	assert.Contains(t, string(result), "hello")
	assert.Contains(t, string(result), "42")
}

func TestColumnSplitterEscapedBackslash(t *testing.T) {
	// Escaped backslash before closing quote: '\\' should close the string
	out, err := split(`'\\',next`)
	assert.NoError(t, err)
	assert.Equal(t, []string{`'\\'`, "next"}, out)

	// Single escaped quote stays in string: 'it\'s' is one token
	out, err = split(`'it\'s',next`)
	assert.NoError(t, err)
	assert.Equal(t, []string{`'it\'s'`, "next"}, out)
}

func TestColumnSplitter(t *testing.T) {
	inputs := []string{
		`"abc,d",1.22332,true,"'def'"def   `,
		`11,23,'f,f` + "\n  ",
		`1,931,1,354.0,1.071598667572002,1,931,2,201.75,1.11804961109744`}
	expectedOutput := [][]string{
		{`"abc,d"`, "1.22332", "true", `"'def'"def`},
		nil,
		{"1", "931", "1", "354.0", "1.071598667572002", "1", "931", "2", "201.75", "1.11804961109744"},
	}
	expectedError := []error{
		nil,
		fmt.Errorf("expecting ' but got EOF"),
		nil,
	}
	for i, input := range inputs {
		out, err := split(input)
		assert.Equal(t, expectedOutput[i], out)
		assert.Equal(t, expectedError[i], err)
	}
}
