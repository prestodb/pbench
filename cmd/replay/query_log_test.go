package replay

import (
	"bufio"
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"os"
	"testing"
)

func TestQueryLog(t *testing.T) {
	t.SkipNow()
	inputFile, oErr := os.Open("/Users/ezhang/IBM/202404150000_0")
	assert.Nil(t, oErr)
	defer inputFile.Close()
	scanner := bufio.NewScanner(inputFile)
	scanner.Buffer(make([]byte, 1024), 8*bufio.MaxScanTokenSize)
	for scanner.Scan() {
		line := scanner.Text()
		logEntry := &QueryLog{}
		err := json.Unmarshal([]byte(line), logEntry)
		assert.Nil(t, err)
	}
}
