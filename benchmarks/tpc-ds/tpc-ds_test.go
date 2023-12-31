package tpc_ds

import (
	"encoding/json"
	"fmt"
	"os"
	"presto-benchmark/stage"
	"testing"
)

func TestGen(t *testing.T) {
	for i := 1; i < 100; i++ {
		queryName := fmt.Sprintf("query_%02d", i)
		stage := &stage.Stage{
			Id:             queryName,
			Queries:        nil,
			QueryFiles:     []string{queryName + ".sql"},
			NextStagePaths: []string{fmt.Sprintf("query_%02d%s", i+1, stage.DefaultStageFileExt)},
		}
		if i == 99 {
			stage.NextStagePaths = nil
		}
		bytes, err := json.MarshalIndent(stage, "", "    ")
		if err != nil {
			t.Fatal(err)
		}
		err = os.WriteFile(queryName+".json", append(bytes, '\n'), 0644)
		if err != nil {
			t.Fatal(err)
		}
	}
}
