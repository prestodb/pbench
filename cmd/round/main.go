package round

import (
	"bufio"
	"bytes"
	"fmt"
	"github.com/spf13/cobra"
	"os"
	"path/filepath"
	"pbench/log"
	"pbench/utils"
	"regexp"
	"strings"
)

var (
	DecimalPrecision int
	FileExtensions   []string
	FileFormat       string
	InPlaceRewrite   bool
	Recursive        bool

	decimalRegExp *regexp.Regexp
	fileScanned   int
	fileWritten   int
)

const InProgressExt = ".InProgress"

func Args(cmd *cobra.Command, args []string) error {
	for _, ext := range FileExtensions {
		if ext[0] != '.' {
			return fmt.Errorf(`file extension "%s" not accepted, it should start with a dot (.)`, ext)
		}
	}
	if FileFormat != "csv" && FileFormat != "json" {
		return fmt.Errorf(`file format "%s" is not an accepted value, only "json" or "csv" is accepted`, FileFormat)
	}
	if err := cobra.MinimumNArgs(1)(cmd, args); err != nil {
		return err
	}
	return nil
}

func Run(_ *cobra.Command, args []string) {
	decimalRegExp = regexp.MustCompile(fmt.Sprintf(`"?(\d+\.\d{%d})\d+"?`, DecimalPrecision))
	for _, path := range args {
		if err := processRoundDecimalPath(path); err != nil {
			log.Error().Str("path", path).Err(err).Send()
		}
	}
	log.Info().Int("file_scanned", fileScanned).Int("file_written", fileWritten).Send()
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
				if err = processRoundDecimalPath(fullPath); err != nil {
					return err
				}
			} else {
				continue
			}
		} else {
			if err = processRoundDecimalFile(fullPath); err != nil {
				return err
			}
		}
	}
	return nil
}

func processRoundDecimalFile(inputPath string) (err error) {
	if !isFileExtAccepted(inputPath) {
		return nil
	}
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
		if bufWriter != nil && err == nil {
			if ioErr := bufWriter.Flush(); ioErr != nil {
				wrappedErr := fmt.Errorf("failed to flush the output file %s: %w", outputPath, ioErr)
				if err == nil {
					err = wrappedErr
				}
				log.Error().Err(wrappedErr).Send()
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
			// for in-place rewrite, we do not need to do anything.
			log.Info().Str("path", outputPath).Msg("file written")
			fileWritten++
			return
		}
		// need to overwrite the original file, delete the original file first.
		if ioErr := os.Remove(inputPath); ioErr != nil {
			err = fmt.Errorf("failed to remove the original file %s: %w", inputPath, ioErr)
			return
		}
		// then rename the .wip file to have the original file's name.
		if ioErr := os.Rename(outputPath, inputPath); ioErr != nil {
			err = fmt.Errorf("failed to rename file %s to %s: %w", outputPath, inputPath, ioErr)
			return
		}
		fileWritten++
		log.Info().Str("path", inputPath).Msg("file updated")
	}()

	scanner, colCount, lineNum := bufio.NewScanner(inputFile), 0, 1
	var decimalColIndexes []int
	fileScanned++
	for ; scanner.Scan(); lineNum++ {
		rowContent := scanner.Text()
		if FileFormat == "json" {
			rowContent = strings.Trim(scanner.Text(), "[]")
		}
		colScanner := bufio.NewScanner(strings.NewReader(rowContent))
		colScanner.Split(scanCommaSeparatedField)
		cols := make([]string, 0, colCount)
		for colScanner.Scan() {
			cols = append(cols, colScanner.Text())
		}
		if scanErr := colScanner.Err(); scanErr != nil {
			return fmt.Errorf("%s line %d: %w", inputPath, lineNum, scanErr)
		}
		if decimalColIndexes == nil {
			decimalColIndexes = make([]int, 0, 2)
			for i, col := range cols {
				if match := decimalRegExp.FindStringSubmatch(col); len(match) > 0 {
					log.Info().Msgf("%s column %d seems to be a decimal: %s", inputPath, i, col)
					decimalColIndexes = append(decimalColIndexes, i)
					if FileFormat == "csv" {
						cols[i] = fmt.Sprintf(`"%s"`, match[1])
					} else {
						cols[i] = match[1]
					}
				}
			}
			if len(decimalColIndexes) > 0 {
				// found decimal columns, need to create a .wip file
				outputFile, err = os.OpenFile(outputPath, utils.OpenNewFileFlags, 0644)
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
				if match := decimalRegExp.FindStringSubmatch(cols[idx]); len(match) > 0 {
					if FileFormat == "csv" {
						cols[idx] = fmt.Sprintf(`"%s"`, match[1])
					} else {
						cols[idx] = match[1]
					}
				}
			}
		}
		if FileFormat == "json" {
			_, err = bufWriter.WriteString(fmt.Sprintf("[%s]\n", strings.Join(cols, ",")))
		} else {
			_, err = bufWriter.WriteString(strings.Join(cols, ",") + "\n")
		}
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
