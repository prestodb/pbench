package cmp

import (
	"github.com/spf13/cobra"
	"os"
	"os/exec"
	"path/filepath"
	"pbench/log"
	"pbench/utils"
	"regexp"
)

var (
	OutputPath     string
	FileIdRegexStr string
	fileIdRegex    *regexp.Regexp
)

func Run(_ *cobra.Command, args []string) {
	returnCode := compareRun(args)
	os.Exit(returnCode)
}

func compareRun(args []string) int {
	var (
		err       error
		fileIdMap map[string]string
	)
	fileIdRegex, err = regexp.Compile(FileIdRegexStr)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to compile file ID regex")
	}

	buildSidePath, probeSidePath := args[0], args[1]
	utils.ExpandHomeDirectory(&buildSidePath)
	utils.ExpandHomeDirectory(&probeSidePath)
	// Build side
	if fileIdMap, err = buildFileIdMap(buildSidePath); err != nil {
		log.Fatal().Err(err).Str("build_side_path", buildSidePath).Msg("failed to build file ID map")
	}

	buildSideFileCount := len(fileIdMap)

	utils.ExpandHomeDirectory(&OutputPath)
	utils.PrepareOutputDirectory(OutputPath)

	probeSideMatched, fileCompared, diffWritten, errorFileCount := 0, 0, 0, 0
	entries, readDirErr := os.ReadDir(probeSidePath)

	if readDirErr != nil {
		log.Fatal().Err(readDirErr).Str("probe_side_path", probeSidePath).Msg("failed to read the probe side directory")
	}

	// Scan the probe side
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if match := fileIdRegex.FindStringSubmatch(entry.Name()); len(match) > 0 {
			fileId := match[1]
			buildSideFilePath, exists := fileIdMap[fileId]
			if !exists {
				continue
			}
			probeSideMatched++
			probeSideFilePath := filepath.Join(probeSidePath, entry.Name())
			diffs, diffErr := generateDiff(buildSideFilePath, probeSideFilePath)

			if diffErr != nil {
				errorFileCount++
				continue
			}

			fileCompared++
			if len(diffs) > 0 {
				diffFilePath := filepath.Join(OutputPath, fileId+".diff")
				// Print out the diffs
				err = os.WriteFile(diffFilePath, []byte(diffs), 0644)
				if err != nil {
					log.Error().Err(err).Str("output_file", diffFilePath).Msg("failed to write diff file")
					continue
				}

				log.Info().Str("build_side", buildSideFilePath).Str("probe_side", probeSideFilePath).Msg("diff result written")
				diffWritten++
			}

			delete(fileIdMap, fileId)
		}
	}

	log.Info().Int("build_side_count", buildSideFileCount).Int("probe_side_matched", probeSideMatched).
		Int("files_with_errors", errorFileCount).Int("files_compared", fileCompared).Int("diff_written", diffWritten).Send()

	if errorFileCount > 0 || diffWritten > 0 {
		return 1
	} else {
		return 0
	}

}

func buildFileIdMap(path string) (map[string]string, error) {
	fileIdMap := make(map[string]string)
	entries, err := os.ReadDir(path)
	if err != nil {
		return nil, err
	}
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		if match := fileIdRegex.FindStringSubmatch(entry.Name()); len(match) > 0 {
			fileIdMap[match[1]] = filepath.Join(path, entry.Name())
		}
	}
	return fileIdMap, nil
}

func readFileIntoString(filePath string) string {
	if bytes, err := os.ReadFile(filePath); err != nil {
		log.Error().Err(err).Str("path", filePath).Msg("failed to read file")
		return ""
	} else {
		return string(bytes)
	}
}

func generateDiff(buildSideFilePath, probeSideFilePath string) (string, error) {
	cmd := exec.Command("diff", "-u", buildSideFilePath, probeSideFilePath)
	output, err := cmd.CombinedOutput()

	switch cmd.ProcessState.ExitCode() {
	case 0, 1:
		return string(output), nil
	default:
		// some error running diff
		log.Error().Err(err).
			Str("build_side", buildSideFilePath).
			Str("probe_side", probeSideFilePath).
			Str("error_message", string(output)).
			Msg("Error while performing diff")
		return "", err

	}
}
