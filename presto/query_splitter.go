package presto

import (
	"bufio"
	"bytes"
	"io"
	"strings"
)

func SplitQueries(in io.Reader) ([]string, error) {
	scanner := bufio.NewScanner(in)
	scanner.Split(ScanSqlStmt)
	stmts := make([]string, 0, 2)
	for scanner.Scan() {
		stmts = append(stmts, scanner.Text())
	}
	return stmts, scanner.Err()
}

func ScanSqlStmt(data []byte, atEOF bool) (int, []byte, error) {
	if atEOF && len(data) == 0 {
		return 0, nil, nil
	}
	pos, inQuote, inComment, inMultilineComment := 0, byte(0), false, false
	for ; pos < len(data); pos++ {
		if inComment {
			if i := bytes.IndexByte(data[pos:], '\n'); i >= 0 {
				pos += i
				inComment = false
			} else {
				break
			}
		} else if inMultilineComment {
			if i := bytes.IndexByte(data[pos:], '*'); i >= 0 {
				pos += i
				if pos+1 >= len(data) { // this batch ends with this '*'
					break
				} else if data[pos+1] == '/' {
					pos++
					inMultilineComment = false
				}
			} else {
				break
			}
		} else if inQuote > 0 {
			if i := bytes.IndexByte(data[pos:], inQuote); i >= 0 {
				pos += i
				if i == 0 || data[pos-1] != '\\' {
					inQuote = 0
				}
			} else {
				break
			}
		} else {
			if i := bytes.IndexAny(data[pos:], `-'";/`); i >= 0 {
				pos += i
				switch data[pos] {
				case '-':
					if pos+1 >= len(data) { // this batch ends with this '-'
						break
					} else if data[pos+1] == '-' {
						pos++
						inComment = true
					}
				case '/':
					if pos+1 >= len(data) { // this batch ends with this '/'
						break
					} else if data[pos+1] == '*' {
						pos++
						inMultilineComment = true
					}
				case '"', '\'':
					inQuote = data[pos]
				case ';':
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
	}
	if atEOF {
		// If we're at EOF, there is a partial statement without a semicolon, we will have to discard it.
		return len(data), nil, nil
	}
	// Request more data.
	return 0, nil, nil
}
