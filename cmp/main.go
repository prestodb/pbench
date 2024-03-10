package cmp

import (
	"errors"
	"fmt"
	"github.com/hexops/gotextdiff"
	"github.com/hexops/gotextdiff/myers"
	"github.com/hexops/gotextdiff/span"
	"github.com/spf13/cobra"
	"golang.org/x/sys/unix"
	"os"
	"path/filepath"
	"pbench/log"
	"regexp"
)

var (
	OutputPath     string
	FileIdRegexStr string
	fileIdRegex    *regexp.Regexp
)

func Run(_ *cobra.Command, args []string) {
	var (
		err       error
		fileIdMap map[string]string
	)

	fileIdRegex, err = regexp.Compile(FileIdRegexStr)
	if err != nil {
		log.Fatal().Err(err).Msg("failed to compile file ID regex")
	}

	buildSidePath, probeSidePath := args[0], args[1]
	// Build side
	if fileIdMap, err = buildFileIdMap(buildSidePath); err != nil {
		log.Fatal().Err(err).Str("build_side_path", buildSidePath).Msg("failed to build file ID map")
	}

	prepareOutputDirectory()

	probeSideFileCount, fileCompared, diffWritten := 0, 0, 0
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
			probeSideFileCount++
			buildSideFilePath, exists := fileIdMap[fileId]
			if !exists {
				continue
			}
			probeSideFilePath := filepath.Join(probeSidePath, entry.Name())
			buildSideString, probeSideString := readFileIntoString(buildSideFilePath), readFileIntoString(probeSideFilePath)
			edits := myers.ComputeEdits(span.URIFromPath(buildSideString), buildSideString, probeSideString)
			fileCompared++
			if len(edits) > 0 {
				diffFilePath := filepath.Join(OutputPath, fileId+".diff")
				diffFile, ioErr := os.OpenFile(diffFilePath, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
				if ioErr != nil {
					log.Error().Err(ioErr).Str("output_file", diffFilePath).Msg("failed to open output file")
					continue
				}
				_, ioErr = fmt.Fprintln(diffFile, gotextdiff.ToUnified(buildSideFilePath, probeSideFilePath, buildSideString, edits))
				if ioErr != nil {
					log.Error().Err(ioErr).Str("output_file", diffFilePath).Msg("failed to write to output file")
					_ = diffFile.Close()
					continue
				}
				_ = diffFile.Close()
				log.Info().Str("build_side", buildSideFilePath).Str("probe_side", probeSideFilePath).Msg("diff result written")
				diffWritten++
			}
			delete(fileIdMap, fileId)
		}
	}
	log.Info().Int("build_side_count", len(fileIdMap)).Int("probe_side_count", probeSideFileCount).
		Int("file_compared", fileCompared).Int("diff_written", diffWritten).Send()
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

func prepareOutputDirectory() {
	if stat, statErr := os.Stat(OutputPath); statErr != nil {
		if errors.Is(statErr, unix.ENOENT) {
			if mkdirErr := os.MkdirAll(OutputPath, 0755); mkdirErr != nil {
				log.Fatal().Err(mkdirErr).Msg("failed to create output directory")
			} else {
				log.Info().Str("output_path", OutputPath).Msg("output directory created")
			}
		} else {
			log.Fatal().Err(statErr).Msg("output path not valid")
		}
	} else if !stat.IsDir() {
		log.Fatal().Str("output_path", OutputPath).Msg("output path is not a directory")
	} else {
		log.Info().Str("output_path", OutputPath).Msg("output directory")
	}
}

func readFileIntoString(filePath string) string {
	if bytes, err := os.ReadFile(filePath); err != nil {
		log.Error().Err(err).Str("path", filePath).Msg("failed to read file")
		return ""
	} else {
		return string(bytes)
	}
}
