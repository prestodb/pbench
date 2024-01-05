package round

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"presto-benchmark/log"
	"regexp"
	"strings"
)

var (
	DecimalRegExp  = regexp.MustCompile(`"?(\d+\.\d{11})\d*"?`)
	FileExtensions []string
	FileFormat     string
	InPlaceRewrite bool
	Recursive      bool
)

const InProgressExt = ".InProgress"

func Args(cmd *cobra.Command, args []string) error {
	for _, ext := range FileExtensions {
		if ext[0] != '.' {
			return fmt.Errorf("file extension %s not accepted, it should start with a dot (.)", ext)
		}
	}
	if FileFormat != "csv" && FileFormat != "json" {
		return fmt.Errorf(`file format %s is not an accepted value, only "json" or "csv" are accepted`, FileFormat)
	}
	if err := cobra.MinimumNArgs(1)(cmd, args); err != nil {
		return err
	}
	return nil
}

func Run(cmd *cobra.Command, args []string) {
	for _, path := range args {
		if err := processRoundDecimalPath(path); err != nil {
			log.Error().Str("path", path).Err(err).Send()
		}
	}
}

func processRoundDecimalPath(path string) error {
	stat, err := os.Stat(path)
	if err != nil {
		return err
	}
	if !stat.IsDir() {
		return processRoundDecimalFile(path)
	}
	entries, err := os.ReadDir(path)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		fullPath := filepath.Join(path, entry.Name())
		if entry.IsDir() {
			if Recursive {
				if err := processRoundDecimalPath(fullPath); err != nil {
					return err
				}
			} else {
				continue
			}
		} else {
			if err := processRoundDecimalFile(fullPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func processRoundDecimalFile(inputPath string) (err error) {
	inputFile, ioErr := os.Open(inputPath)
	if ioErr != nil {
		return ioErr
	}

	var outputPath string
	if InPlaceRewrite {
		outputPath = inputPath + InProgressExt
	} else {
		ext := filepath.Ext(inputPath) // this includes the dot
		outputPath = inputPath[0:len(inputPath)-len(ext)] + ".rewrite" + ext
	}

	var (
		outputFile *os.File
		bufWriter  *bufio.Writer
	)
	defer func() {
		_ = inputFile.Close()
		if outputFile == nil {
			// This means we didn't even find a decimal column.
			return
		}
		if bufWriter != nil {
			if ioErr := bufWriter.Flush(); ioErr != nil {
				log.Error().Str("path", outputPath).Err(ioErr).Msg("failed to flush the output file")
			}
		}
		_ = outputFile.Close()
		if err != nil {
			// no need to overwrite the original file, delete the .InProgress file.
			if ioErr := os.Remove(outputPath); ioErr != nil {
				log.Error().Str("path", outputPath).Err(ioErr).Msg("failed to remove the temporary file")
			}
			return
		}
		if !InPlaceRewrite {
			return
		}
		// need to overwrite the original file, delete the original file first.
		if ioErr := os.Remove(inputPath); ioErr != nil {
			log.Error().Str("path", inputPath).Err(ioErr).Msg("failed to remove the original file")
			return
		}
		// then rename the .wip file to have the original file's name.
		if ioErr := os.Rename(outputPath, inputPath); ioErr != nil {
			log.Error().Str("from", outputPath).Str("to", inputPath).Err(err).Msg("failed to rename file")
			return
		}
		log.Info().Str("path", inputPath).Msg("file updated")
	}()
	scanner, colCount, lineNum := bufio.NewScanner(inputFile), 0, 1
	var decimalColIndexes []int
	for ; scanner.Scan(); lineNum++ {
		rowContent := scanner.Text()
		if FileFormat == "json" {
			rowContent = strings.Trim(scanner.Text(), "[]")
		}
		colScanner := bufio.NewScanner(strings.NewReader(rowContent))
		cols := make([]string, 0, colCount)
		for colScanner.Scan() {
			cols = append(cols, colScanner.Text())
		}
		if scanErr := colScanner.Err(); scanErr != nil {
			return scanErr
		}
		if decimalColIndexes == nil {
			decimalColIndexes = make([]int, 0, 2)
			for i, col := range cols {
				if match := DecimalRegExp.FindStringSubmatch(col); len(match) > 0 {
					log.Info().Msgf("%s column %d seems to be a decimal: %s", inputPath, i, col)
					decimalColIndexes = append(decimalColIndexes, i)
					if FileFormat == "csv" {
						cols[i] = fmt.Sprintf(`"%s"`, match[1])
					} else {
						cols[i] = fmt.Sprintf(`%s`, match[1])
					}
				}
			}
			if len(decimalColIndexes) > 0 {
				// found decimal columns, need to create a .wip file
				outputFile, err = os.OpenFile(outputPath, os.O_TRUNC|os.O_WRONLY|os.O_CREATE, 0644)
				if err != nil {
					return err
				}
				bufWriter = bufio.NewWriter(outputFile)
				colCount = len(cols)
			} else {
				// no decimal column, quick exit
				return nil
			}
		} else {
			if len(cols) != colCount {
				return fmt.Errorf("%s: the first line had %d columns but line %d had %d columns", inputPath, colCount, lineNum, len(cols))
			}
			for _, idx := range decimalColIndexes {
				if match := DecimalRegExp.FindStringSubmatch(cols[idx]); len(match) > 0 {
					if FileFormat == "csv" {
						cols[idx] = fmt.Sprintf(`"%s"`, match[1])
					} else {
						cols[idx] = fmt.Sprintf(`%s`, match[1])
					}
				}
			}
		}
		_, err = bufWriter.WriteString(strings.Join(cols, ",") + "\n")
		if err != nil {
			return err
		}
	}
	if scanner.Err() != nil {
		return fmt.Errorf("error at line %d: %w", lineNum, scanner.Err())
	}
	return nil
}

func isFileExtAccepted(path string) bool {
	if len(FileExtensions) == 0 {
		return true
	}
	for _, ext := range FileExtensions {
		if strings.HasSuffix(path, ext) {
			return true
		}
	}
	return false
}

func scanCommaSeparatedField(data []byte, atEOF bool) (int, []byte, error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	inQuote := byte(0)
	for pos := 0; pos < len(data); pos++ {
		if inQuote > 0 {
			if i := bytes.IndexByte(data[pos:], inQuote); i >= 0 {
				pos += i
				if i == 0 || data[pos-1] != '\\' {
					inQuote = 0
				}
			} else {
				break
			}
		} else if i := bytes.IndexAny(data[pos:], `'",\n`); i >= 0 {
			pos += i
			switch data[pos] {
			case '"', '\'':
				inQuote = data[pos]
			case ',', '\n':
				if inQuote > 0 {
					return 0, nil, fmt.Errorf("expecting %c but got %c", inQuote, data[pos])
				}
				token := strings.TrimSpace(string(data[:pos]))
				if len(token) > 0 {
					return pos + 1, []byte(token), nil
				} else {
					return pos + 1, nil, nil
				}
			}
		} else {
			break
		}
	}
	// If we're at EOF, we have a final, non-terminated line. Return it.
	if atEOF {
		if inQuote > 0 {
			return 0, nil, fmt.Errorf("expecting %c but got EOF", inQuote)
		}
		token := strings.TrimSpace(string(data))
		if len(token) > 0 {
			return len(data), []byte(token), nil
		} else {
			return len(data), nil, nil
		}
	}
	// Request more data.
	return 0, nil, nil
}
