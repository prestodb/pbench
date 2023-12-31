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

const DefaultStageFileExt = ".json"

func ParseStageChain(startingFile string) (*Stage, Map, error) {
	stages := make(Map)
	startingStage, err := ParseStage(startingFile, stages)
	if err == nil {
		err = checkStageLinks(startingStage)
	}
	if err != nil {
		return nil, nil, err
	}
	return startingStage, stages, nil
}

func ParseStage(filePath string, stages Map) (*Stage, error) {
	stageId := stageIdFromFilePath(filePath)
	stage, ok := stages[stageId]
	if ok {
		log.Debug().Msgf("%s already parsed, returned", stageId)
		return stage, nil
	}
	log.Debug().Msgf("parsing stage %s", stageId)
	stage = &Stage{
		Id: stageId,
	}
	filePath, err := filepath.Abs(filePath)
	if err != nil {
		return nil, err
	}
	bytes, err := os.ReadFile(filePath)
	if err != nil {
		return nil, fmt.Errorf("failed to read %s: %w", filePath, err)
	}
	if err = json.Unmarshal(bytes, stage); err != nil {
		return nil, fmt.Errorf("failed to parse json %s: %w", filePath, err)
	}
	dirPath := filepath.Dir(filePath)
	for i, queryFile := range stage.QueryFiles {
		if !filepath.IsAbs(queryFile) {
			queryFile = filepath.Join(dirPath, queryFile)
			stage.QueryFiles[i] = queryFile
		}
		if _, err = os.Stat(queryFile); err != nil {
			return nil, fmt.Errorf("%s links to an invalid query file %s: %w", stageId, queryFile, err)
		}
	}
	for i, nextStagePath := range stage.NextStagePaths {
		if !filepath.IsAbs(nextStagePath) {
			nextStagePath = filepath.Join(dirPath, nextStagePath)
			stage.NextStagePaths[i] = nextStagePath
		}
		if _, err = os.Stat(nextStagePath); err != nil {
			return nil, fmt.Errorf("%s links to an invalid next stage file %s: %w", stageId, nextStagePath, err)
		}
	}
	stages[stageId] = stage
	for _, nextStagePath := range stage.NextStagePaths {
		if nextStage, err := ParseStage(nextStagePath, stages); err != nil {
			return nil, err
		} else {
			stage.NextStages = append(stage.NextStages, nextStage)
			nextStage.Prerequisites = append(nextStage.Prerequisites, stage)
			nextStage.wgPrerequisites.Add(1)
		}
	}
	return stage, nil
}

func stageIdFromFilePath(filePath string) string {
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
		foundMyselfInNextStage := false
		prerequisiteMap := make(map[string]bool)
		for _, prerequisite := range nextStage.Prerequisites {
			if prerequisite == stage {
				foundMyselfInNextStage = true
			}
			if prerequisiteMap[prerequisite.Id] {
				return fmt.Errorf("stage %s got duplicated prerequisite stages %s", nextStage.Id, prerequisite.Id)
			}
			prerequisiteMap[prerequisite.Id] = true
		}
		if !foundMyselfInNextStage {
			return fmt.Errorf("stage %s is not found in its next stage %s's prerequisites", stage.Id, nextStage.Id)
		}
		if err := checkStageLinks(nextStage); err != nil {
			return err
		}
	}
	return nil
}
