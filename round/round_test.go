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
	inputs := []string{`"abc,d",1.22332,true,"'def'"def   `, `11,23,'f,f` + "\n  "}
	expectedOutput := [][]string{
		{`"abc,d"`, "1.22332", "true", `"'def'"def`},
		nil,
	}
	expectedError := []error{
		nil,
		fmt.Errorf("expecting ' but got EOF"),
	}
	for i, input := range inputs {
		out, err := split(input)
		assert.Equal(t, expectedOutput[i], out)
		assert.Equal(t, expectedError[i], err)
	}
}
