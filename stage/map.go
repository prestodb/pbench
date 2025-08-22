package stage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"pbench/log"

	"github.com/go-playground/validator/v10"
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
	validate := validator.New()
	err = validate.Struct(stage)
	if err != nil {
		return nil, fmt.Errorf("failed to parse json %s: %w", filePath, err)
	}
	for i, queryFile := range stage.QueryFiles {
		if !filepath.IsAbs(queryFile) {
			queryFile = filepath.Join(stage.BaseDir, queryFile)
			stage.QueryFiles[i] = queryFile
		}
		if _, err = os.Stat(queryFile); err != nil {
			return nil, fmt.Errorf("%s links to an invalid query file %s: %w", stage.Id, queryFile, err)
		}
	}
	log.Debug().Str("id", stage.Id).Str("path", filePath).Msg("read stage file")
	return stage, nil
}

func ParseStageFromFile(filePath string, stages Map) (*Stage, error) {
	// For regular files, just use the filename as stage ID
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

	// Process Streams to create multiple instances directly
	err := processStreams(stage, stages)
	if err != nil {
		return nil, fmt.Errorf("failed to process streams for stage %s: %w", stage.Id, err)
	}

	// Process regular NextStagePaths
	for i, nextStagePath := range stage.NextStagePaths {
		if !filepath.IsAbs(nextStagePath) {
			nextStagePath = filepath.Join(stage.BaseDir, nextStagePath)
			stage.NextStagePaths[i] = nextStagePath
		}

		if fileInfo, err := os.Stat(nextStagePath); err != nil {
			return nil, fmt.Errorf("%s links to an invalid next stage file %s: %w", stage.Id, nextStagePath, err)
		} else if fileInfo.IsDir() {
			return nil, fmt.Errorf("%s links to a directory as next stage: %s", stage.Id, nextStagePath)
		}
	}

	stages[stage.Id] = stage

	// Parse regular next stages
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

// processStreams expands StreamSpecs by directly creating stage instances
func processStreams(stage *Stage, stages Map) error {
	if len(stage.Streams) == 0 {
		return nil
	}

	// For each stream spec, create multiple stage instances
	for _, spec := range stage.Streams {
		if spec.StreamCount <= 0 {
			return fmt.Errorf("stream_count must be positive, got %d for stream %s", spec.StreamCount, spec.StreamName)
		}

		// Validate seeds if provided
		if len(spec.Seeds) > 0 {
			if len(spec.Seeds) != 1 && len(spec.Seeds) != spec.StreamCount {
				return fmt.Errorf("seeds array length (%d) must be either 1 or equal to stream_count (%d) for stream %s",
					len(spec.Seeds), spec.StreamCount, spec.StreamName)
			}
		}

		// Resolve relative paths
		streamPath := spec.StreamName
		if !filepath.IsAbs(streamPath) {
			streamPath = filepath.Join(stage.BaseDir, streamPath)
		}

		// Verify the base stream file exists
		if _, err := os.Stat(streamPath); err != nil {
			return fmt.Errorf("stream file %s does not exist: %w", streamPath, err)
		}

		// Create multiple stream instances directly
		for i := 0; i < spec.StreamCount; i++ {
			instanceNumber := i + 1

			// Read the base stream file
			streamStage, err := ReadStageFromFile(streamPath)
			if err != nil {
				return fmt.Errorf("failed to read stream file %s: %w", streamPath, err)
			}

			// Set unique ID for this stream instance
			baseId := fileNameWithoutPathAndExt(streamPath)
			streamStage.Id = fmt.Sprintf("%s_stream_%d", baseId, instanceNumber)

			// Set stream instance information directly
			streamStage.StreamInstanceNumber = instanceNumber

			// Calculate and set custom seed if provided
			if len(spec.Seeds) > 0 {
				var seed int64
				if len(spec.Seeds) == 1 {
					// Single base seed - use deterministic offset
					seed = spec.Seeds[0] + int64(i*1000)
				} else {
					// Individual seeds provided
					seed = spec.Seeds[i]
				}
				streamStage.CustomSeed = &seed
			}

			// Add to stages map and NextStages
			stages[streamStage.Id] = streamStage
			stage.NextStages = append(stage.NextStages, streamStage)
			streamStage.wgPrerequisites.Add(1)
		}

		log.Info().Str("stream_name", spec.StreamName).Int("stream_count", spec.StreamCount).
			Int("custom_seeds", len(spec.Seeds)).Msg("created stream instances")
	}

	// Clear Streams since they've been processed
	stage.Streams = nil

	return nil
}
