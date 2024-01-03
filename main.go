package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"presto-benchmark/log"
	"presto-benchmark/presto"
	"presto-benchmark/stage"
	"strings"
)

func main() {
	serverUrl := flag.String("server", stage.DefaultServerUrl, "Presto server address")
	wd, _ := os.Getwd()
	outputPath := flag.String("output-path", wd, "Output path")
	getClientFn := func() *presto.Client {
		client, _ := presto.NewClient(*serverUrl)
		return client
	}
	flag.Parse()
	startingStage := new(stage.Stage)
	startingStage.GetClient = getClientFn
	if outputPath != nil {
		startingStage.OutputPath = *outputPath
	}

	if len(flag.Args()) == 0 {
		return
	}
	for _, path := range flag.Args() {
		if st, err := processPath(path); err == nil {
			startingStage.MergeWith(st)
		}
	}
	_, _, err := stage.ParseStageGraph(startingStage)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to parse benchmark")
	}

	startingStage.Run(context.Background())
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
