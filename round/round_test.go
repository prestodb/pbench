package round

import (
	"bufio"
	"fmt"
	"github.com/stretchr/testify/assert"
	"strings"
	"testing"
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
