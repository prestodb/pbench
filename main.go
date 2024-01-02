package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"presto-benchmark/log"
	"presto-benchmark/stage"
	"strings"
	"time"
)

func main() {
	argLen := len(os.Args)
	if argLen < 2 {
		log.Fatal().Msg("expecting directory paths or file paths.")
	}
	startingStage := new(stage.Stage)

	for i := 1; i < argLen; i++ {
		if st, err := processPath(os.Args[i]); err == nil {
			startingStage.MergeWith(st)
		}
	}
	_, _, err := stage.ParseStageGraph(startingStage)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to parse benchmark")
	}

	b := strings.Builder{}
	startingStage.OnQueryCompletion = func(qr *stage.QueryResult) {
		b.WriteString(qr.StageId + ",")
		if qr.QueryFile != nil {
			b.WriteString(*qr.QueryFile)
		} else {
			b.WriteString("inline")
		}
		b.WriteString(fmt.Sprintf(",%d,%d,%s,%s,%s\n",
			qr.QueryIndex, qr.RowCount,
			qr.StartTime.Format(time.RFC3339), qr.EndTime.Format(time.RFC3339), *qr.Duration))
	}
	results := startingStage.Run(context.Background())
	byt, _ := json.Marshal(results)
	_ = os.WriteFile(startingStage.Id+"_result.json", byt, 0644)
	_ = os.WriteFile(startingStage.Id+"_time.csv", []byte(b.String()), 0644)
}

func processPath(path string) (st *stage.Stage, err error) {
	defer func() {
		if err != nil {
			log.Error().Err(err).Str("path", path).Send()
		}
	}()
	stat, err := os.Stat(path)
	if err != nil {
		return nil, err
	}
	if stat.IsDir() {
		st = new(stage.Stage)
		entries, err := os.ReadDir(path)
		if err != nil {
			return nil, err
		}
		for _, entry := range entries {
			if entry.IsDir() || !strings.HasSuffix(entry.Name(), stage.DefaultStageFileExt) {
				continue
			}
			fullPath := filepath.Join(path, entry.Name())
			newStage, err := processPath(fullPath)
			if err == nil {
				st.MergeWith(newStage)
			}
		}
		return st, nil
	} else {
		if !strings.HasSuffix(path, stage.DefaultStageFileExt) {
			return nil, fmt.Errorf("%s is not a %s file", path, stage.DefaultStageFileExt)
		}
		return stage.ReadStageFromFile(path)
	}
}
