package tpc_ds

import (
	"encoding/json"
	"fmt"
	"os"
	"presto-benchmark/task"
	"testing"
)

func TestGen(t *testing.T) {
	for i := 1; i < 100; i++ {
		queryName := fmt.Sprintf("query_%02d", i)
		task := &task.Task{
			Id:            queryName,
			Queries:       nil,
			QueryFiles:    []string{queryName + ".sql"},
			NextTaskPaths: []string{fmt.Sprintf("query_%02d", i+1)},
		}
		bytes, err := json.MarshalIndent(task, "", "    ")
		if err != nil {
			t.Fatal(err)
		}
		err = os.WriteFile(queryName+".json", bytes, 0644)
		if err != nil {
			t.Fatal(err)
		}
	}
}
