package stage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"pbench/log"
	"strings"

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
	// Generate unique stage ID that handles virtual paths
	stageId := generateUniqueStageId(filePath)
	stage, ok := stages[stageId]
	if ok {
		log.Debug().Msgf("%s already parsed, returned", stage.Id)
		return stage, nil
	}

	// Extract real file path for reading
	realPath, _ := parseVirtualPath(filePath)
	stage, err := ReadStageFromFile(realPath)
	if err != nil {
		return nil, err
	}

	// Override the stage ID to make it unique
	stage.Id = stageId

	return ParseStage(stage, stages)
}

func ParseStage(stage *Stage, stages Map) (*Stage, error) {
	stageFound, ok := stages[stage.Id]
	if ok {
		log.Debug().Msgf("%s already parsed, returned", stage.Id)
		return stageFound, nil
	}

	// Process StreamSpecs to generate multiple instances of base streams
	err := processStreamSpecs(stage)
	if err != nil {
		return nil, fmt.Errorf("failed to process stream specs for stage %s: %w", stage.Id, err)
	}

	for i, nextStagePath := range stage.NextStagePaths {
		// Extract real path for validation and path resolution
		realPath, _ := parseVirtualPath(nextStagePath)

		if !filepath.IsAbs(realPath) {
			realPath = filepath.Join(stage.BaseDir, realPath)
			// Update the virtual path with the absolute real path
			if idx := strings.Index(nextStagePath, "#"); idx != -1 {
				stage.NextStagePaths[i] = realPath + nextStagePath[idx:]
			} else {
				stage.NextStagePaths[i] = realPath
			}
		}

		if fileInfo, err := os.Stat(realPath); err != nil {
			return nil, fmt.Errorf("%s links to an invalid next stage file %s: %w", stage.Id, realPath, err)
		} else if fileInfo.IsDir() {
			return nil, fmt.Errorf("%s links to a directory as next stage: %s", stage.Id, realPath)
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

// parseVirtualPath extracts the real file path and virtual suffix from a virtual path
// Virtual paths have format: "/path/to/file.json#stream_1"
func parseVirtualPath(virtualPath string) (realPath, suffix string) {
	if idx := strings.Index(virtualPath, "#"); idx != -1 {
		return virtualPath[:idx], virtualPath[idx+1:]
	}
	return virtualPath, ""
}

// generateUniqueStageId creates a unique stage ID for virtual paths
func generateUniqueStageId(filePath string) string {
	realPath, suffix := parseVirtualPath(filePath)
	baseId := fileNameWithoutPathAndExt(realPath)
	if suffix != "" {
		return fmt.Sprintf("%s_%s", baseId, suffix)
	}
	return baseId
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

// processStreamSpecs expands StreamSpecs into NextStagePaths by generating multiple instances
func processStreamSpecs(stage *Stage) error {
	if len(stage.StreamSpecs) == 0 {
		return nil
	}

	// For each stream spec, expand it to multiple stage paths
	for _, spec := range stage.StreamSpecs {
		if spec.StreamCount <= 0 {
			return fmt.Errorf("stream_count must be positive, got %d for stream %s", spec.StreamCount, spec.StreamName)
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

		// Generate multiple copies by adding each instance to NextStagePaths
		// Each instance gets a unique virtual path to ensure unique stage IDs
		for i := 0; i < spec.StreamCount; i++ {
			// Create a unique virtual path by appending instance number
			virtualPath := fmt.Sprintf("%s#stream_%d", streamPath, i+1)
			stage.NextStagePaths = append(stage.NextStagePaths, virtualPath)
		}

		log.Debug().Str("stream_name", spec.StreamName).Int("stream_count", spec.StreamCount).
			Msg("expanded stream spec into next stage paths")
	}

	// Clear StreamSpecs since they've been processed into NextStagePaths
	stage.StreamSpecs = nil

	return nil
}
