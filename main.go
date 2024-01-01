package main

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"presto-benchmark/log"
	"presto-benchmark/stage"
	"strings"
)

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

func main() {
	argLen := len(os.Args)
	if argLen < 2 {
		log.Fatal().Msg("expecting directory paths or file paths.")
	}
	startingStage := new(stage.Stage)

	for i := 1; i < argLen; i++ {
		path := os.Args[i]
		if st, err := processPath(path); err == nil {
			startingStage.MergeWith(st)
		}
	}
	_, _, err := stage.ParseStageGraph(startingStage)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to parse benchmark")
	}
	errs := startingStage.Run(context.Background())
	if len(errs) > 0 {
		log.Error().Array("errors", log.NewMarshaller(errs)).Send()
	}
}
