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
	wd, _ := os.Getwd()
	serverUrl := flag.String("s", stage.DefaultServerUrl, "Presto server address")
	outputPath := flag.String("o", wd, "Output path")
	flag.Parse()
	flag.Usage = func() {
		_, _ = fmt.Fprintf(flag.CommandLine.Output(), "Usage of %s:\n", os.Args[0])
		flag.PrintDefaults()
		fmt.Println(`Provide a list of root-level benchmark stage JSON files.`)
	}
	if flag.NArg() == 0 {
		flag.Usage()
		return
	}

	mainStage := new(stage.Stage)
	mainStage.GetClient = func() *presto.Client {
		client, _ := presto.NewClient(*serverUrl)
		return client
	}
	if outputPath != nil {
		mainStage.OutputPath = *outputPath
	}

	for _, path := range flag.Args() {
		if st, err := processPath(path); err == nil {
			mainStage.MergeWith(st)
		}
	}
	_, _, err := stage.ParseStageGraph(mainStage)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to parse benchmark")
	}

	mainStage.Run(context.Background())
}

func processPath(path string) (st *stage.Stage, returnErr error) {
	defer func() {
		if returnErr != nil {
			log.Error().Err(returnErr).Str("path", path).Send()
		}
	}()
	stat, statErr := os.Stat(path)
	if statErr != nil {
		return nil, statErr
	}
	if stat.IsDir() {
		st = new(stage.Stage)
		entries, ioErr := os.ReadDir(path)
		if ioErr != nil {
			return nil, ioErr
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
