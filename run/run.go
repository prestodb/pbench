package run

import (
	"context"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"presto-benchmark/log"
	"presto-benchmark/stage"
	"strings"
)

var OutputPath string

func Run(cmd *cobra.Command, args []string) {
	mainStage := new(stage.Stage)
	mainStage.OutputPath = OutputPath

	for _, path := range args {
		if st, err := processStagePath(path); err == nil {
			mainStage.MergeWith(st)
		}
	}
	_, _, err := stage.ParseStageGraph(mainStage)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to parse benchmark")
	}

	mainStage.Run(context.Background())
}

func processStagePath(path string) (st *stage.Stage, returnErr error) {
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
			newStage, err := processStagePath(fullPath)
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
