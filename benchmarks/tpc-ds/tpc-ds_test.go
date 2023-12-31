package tpc_ds

import (
	"encoding/json"
	"fmt"
	"os"
	"presto-benchmark/stage"
	"testing"
)

func TestGenOneFile(t *testing.T) {
	queryFiles := make([]string, 0, 99)
	for i := 1; i < 100; i++ {
		queryFiles = append(queryFiles, fmt.Sprintf("query_%02d.sql", i))
	}
	stage := &stage.Stage{
		Id:         "serial",
		Queries:    nil,
		QueryFiles: queryFiles,
	}
	bytes, err := json.MarshalIndent(stage, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	err = os.WriteFile("serial.json", append(bytes, '\n'), 0644)
	if err != nil {
		t.Fatal(err)
	}
}
