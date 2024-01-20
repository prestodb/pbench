package stage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"presto-benchmark/log"
)

type Map map[string]*Stage

func (m *Map) Get(stageId string) *Stage {
	if m == nil {
		return nil
	}
	return (*m)[stageId]
}

func ParseStageGraph(startingStage *Stage) (*Stage, Map, error) {
	stages := make(Map)
	startingStage, err := ParseStage(startingStage, stages)
	if err == nil {
		err = checkStageLinks(startingStage)
	}
	if err != nil {
		return nil, nil, err
	}
	return startingStage, stages, nil
}

func ParseStageGraphFromFile(startingFile string) (*Stage, Map, error) {
	stages := make(Map)
	startingStage, err := ParseStageFromFile(startingFile, stages)
	if err == nil {
		err = checkStageLinks(startingStage)
	}
	if err != nil {
		return nil, nil, err
	}
	return startingStage, stages, nil
}

func ReadStageFromFile(filePath string) (*Stage, error) {
	filePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, err
	}
	stage := &Stage{
		Id:      fileNameWithoutPathAndExt(filePath),
		BaseDir: filepath.Dir(filePath),
	}
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", filePath, err)
	}
	if err = json.Unmarshal(bytes, stage); err != nil {
		return nil, fmt.Errorf("failed to parse json %s: %w", filePath, err)
	}
	log.Debug().Str("id", stage.Id).Str("path", filePath).Msg("read stage file")
	return stage, nil
}

func ParseStageFromFile(filePath string, stages Map) (*Stage, error) {
	stage, ok := stages[fileNameWithoutPathAndExt(filePath)]
	if ok {
		log.Debug().Msgf("%s already parsed, returned", stage.Id)
		return stage, nil
	}
	stage, err := ReadStageFromFile(filePath)
	if err != nil {
		return nil, err
	}
	return ParseStage(stage, stages)
}

func ParseStage(stage *Stage, stages Map) (*Stage, error) {
	stageFound, ok := stages[stage.Id]
	if ok {
		log.Debug().Msgf("%s already parsed, returned", stage.Id)
		return stageFound, nil
	}
	for _, queryFile := range stage.QueryFiles {
		if !filepath.IsAbs(queryFile) {
			queryFile = filepath.Join(stage.BaseDir, queryFile)
		}
		if _, err := os.Stat(queryFile); err != nil {
			return nil, fmt.Errorf("%s links to an invalid query file %s: %w", stage.Id, queryFile, err)
		}
	}
	for i, nextStagePath := range stage.NextStagePaths {
		if !filepath.IsAbs(nextStagePath) {
			nextStagePath = filepath.Join(stage.BaseDir, nextStagePath)
			stage.NextStagePaths[i] = nextStagePath
		}
		if _, err := os.Stat(nextStagePath); err != nil {
			return nil, fmt.Errorf("%s links to an invalid next stage file %s: %w", stage.Id, nextStagePath, err)
		}
	}
	stages[stage.Id] = stage
	for _, nextStagePath := range stage.NextStagePaths {
		if nextStage, err := ParseStageFromFile(nextStagePath, stages); err != nil {
			return nil, err
		} else {
			stage.NextStages = append(stage.NextStages, nextStage)
			nextStage.wgPrerequisites.Add(1)
		}
	}
	return stage, nil
}

func fileNameWithoutPathAndExt(filePath string) string {
	// The stage ID is the file name without directory path and extension.
	// It will be filePath[lastPathSeparator+1 : lastDot], so we have the following default values.
	lastPathSeparator, lastDot := -1, len(filePath)
	for i := 0; i < len(filePath); i++ {
		switch filePath[i] {
		case os.PathSeparator:
			lastPathSeparator = i
		case '.':
			lastDot = i
		}
	}
	if lastDot <= lastPathSeparator+1 || lastPathSeparator+1 >= len(filePath) {
		return filePath
	}
	return filePath[lastPathSeparator+1 : lastDot]
}

func checkStageLinks(stage *Stage) error {
	nextStageMap := make(map[string]bool)
	for _, nextStage := range stage.NextStages {
		if nextStageMap[nextStage.Id] {
			return fmt.Errorf("stage %s got duplicated next stages %s", stage.Id, nextStage.Id)
		}
		nextStageMap[nextStage.Id] = true
		if err := checkStageLinks(nextStage); err != nil {
			return err
		}
	}
	return nil
}
