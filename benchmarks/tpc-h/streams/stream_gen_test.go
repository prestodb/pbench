package streams

import (
	"bufio"
	"encoding/json"
	"fmt"
	"os"
	"pbench/utils"
	"strconv"
	"strings"
	"testing"

	"pbench/stage"

	"github.com/stretchr/testify/assert"
)

func TestGenerateStreams(t *testing.T) {
	// The csv is extracted from the TPC-DS specification
	// https://www.tpc.org/tpc_documents_current_versions/pdf/tpc-ds_v2.1.0.pdf
	// Page 124, Appendix D: Query Ordering
	reader, err := os.Open("query_streams.csv")
	assert.Nil(t, err)
	scanner := bufio.NewScanner(reader)
	var streams [][]int
	streams = make([][]int, 40)
	for row := 0; scanner.Scan(); row++ {
		line := scanner.Text()
		if row != 0 {
			queryIds := strings.Split(line, ",")
			streams[row-1] = make([]int, 22)
			for i := 0; i < len(queryIds); i++ {
				id, _ := strconv.Atoi(queryIds[i])
				streams[row-1][i] = id
			}
		}
	}
	for streamId := 0; streamId < len(streams); streamId++ {
		file, err := os.OpenFile(fmt.Sprintf("stream_%02d.json", streamId+1), utils.OpenNewFileFlags, 0644)
		assert.Nil(t, err)
		st := &stage.Stage{
			QueryFiles:       make([]string, 0, 99),
			StartOnNewClient: true,
		}
		for _, queryId := range streams[streamId] {
			st.QueryFiles = append(st.QueryFiles, fmt.Sprintf("../queries/query_%02d.sql", queryId))
		}
		bytes, err := json.MarshalIndent(st, "", "  ")
		assert.Nil(t, err)
		_, err = file.Write(bytes)
		assert.Nil(t, err)
		_ = file.Close()
	}
}
