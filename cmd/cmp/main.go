package cmp

import (
	"fmt"
	"github.com/spf13/cobra"
	"os"
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

	utils.ExpandHomeDirectory(&OutputPath)
	utils.PrepareOutputDirectory(OutputPath)

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

			diffs := Do(probeSideString, buildSideString)

			if len(diffs) > 0 {
				diffFilePath := filepath.Join(OutputPath, fileId+".diff")
				diffFile, ioErr := os.OpenFile(diffFilePath, utils.OpenNewFileFlags, 0644)
				if ioErr != nil {
					log.Error().Err(ioErr).Str("output_file", diffFilePath).Msg("failed to open output file")
					continue
				}

				// Print out the diffs
				for _, diff := range diffs {
					_, ioErr = fmt.Fprintf(diffFile, "%s: %s\n", diff.Type, diff.Text)

				}

				/*
					// Get and print the destination text
					dstText := Dst(diffs)
					fmt.Printf("\nDestination text:\n%s\n", dstText)

					// Get and print the source text
					srcText := Src(diffs)
					fmt.Printf("\nSource text:\n%s\n", srcText)
				*/

				//_, ioErr = fmt.Fprintln(diffFile, gotextdiff.ToUnified(buildSideFilePath, probeSideFilePath, buildSideString, diffText))
				if ioErr != nil {
					log.Error().Err(ioErr).Str("output_file", diffFilePath).Msg("failed to write to output file")
					_ = diffFile.Close()
					continue
				}
				_ = diffFile.Close()
				log.Info().Str("build_side", buildSideFilePath).Str("probe_side", probeSideFilePath).Msg("diff result written")
				diffWritten++
			}

			/*
				edits := myers.ComputeEdits(span.URIFromPath(buildSideString), buildSideString, probeSideString)
				fileCompared++
				if len(edits) > 0 {
					diffFilePath := filepath.Join(OutputPath, fileId+".diff")
					diffFile, ioErr := os.OpenFile(diffFilePath, utils.OpenNewFileFlags, 0644)
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

			*/
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

func readFileIntoString(filePath string) string {
	if bytes, err := os.ReadFile(filePath); err != nil {
		log.Error().Err(err).Str("path", filePath).Msg("failed to read file")
		return ""
	} else {
		return string(bytes)
	}
}
