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
	"presto-benchmark/log"
	"regexp"
)

var (
	OutputPath string
	IdRegexStr string
	idRegex    *regexp.Regexp
)

func Run(_ *cobra.Command, args []string) {
	var (
		err                    error
		fileIdMapA, fileIdMapB map[string]string
	)
	idRegex, err = regexp.Compile(IdRegexStr)
	if err != nil {
		log.Error().Err(err).Msg("failed to compile regex")
		return
	}
	if fileIdMapA, err = buildFileIdMap(args[0]); err != nil {
		log.Error().Err(err).Str("path", args[0]).Msg("failed to build file ID map")
		return
	}
	if fileIdMapB, err = buildFileIdMap(args[1]); err != nil {
		log.Error().Err(err).Str("path", args[1]).Msg("failed to build file ID map")
		return
	}
	if _, statErr := os.Stat(OutputPath); statErr != nil {
		if errors.Is(statErr, unix.ENOENT) {
			if mkdirErr := os.MkdirAll(OutputPath, 0755); mkdirErr != nil {
				log.Error().Err(mkdirErr).Msg("failed to create output directory")
				return
			} else {
				log.Info().Str("output_path", OutputPath).Msg("output directory created")
			}
		} else {
			log.Error().Err(statErr).Msg("output path not valid")
			return
		}
	} else {
		log.Info().Str("output_path", OutputPath).Msg("output directory")
	}

	fileCountA, fileCountB := len(fileIdMapA), len(fileIdMapB)
	fileCompared, diffWritten := 0, 0
	for fileId, filePathInA := range fileIdMapA {
		if filePathInB, exists := fileIdMapB[fileId]; exists {
			a := readFileIntoString(filePathInA)
			b := readFileIntoString(filePathInB)
			edits := myers.ComputeEdits(span.URIFromPath(filePathInA), a, b)
			fileCompared++
			if len(edits) > 0 {
				diffFilePath := filepath.Join(OutputPath, fileId+".diff")
				diffFile, ioErr := os.OpenFile(diffFilePath, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
				if ioErr != nil {
					log.Error().Err(ioErr).Str("output_file", diffFilePath).Msg("failed to open output file")
					continue
				}
				_, ioErr = fmt.Fprintln(diffFile, gotextdiff.ToUnified(filePathInA, filePathInB, a, edits))
				if ioErr != nil {
					log.Error().Err(ioErr).Str("output_file", diffFilePath).Msg("failed to write to output file")
					continue
				}
				_ = diffFile.Close()
				log.Info().Str("file A", filePathInA).Str("file B", filePathInB).Msg("diff result written")
				diffWritten++
			}
			delete(fileIdMapA, fileId)
			delete(fileIdMapB, fileId)
		}
	}
	log.Info().Int("file_count_A", fileCountA).Int("file_count_B", fileCountB).
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
		if match := idRegex.FindStringSubmatch(entry.Name()); len(match) > 0 {
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
