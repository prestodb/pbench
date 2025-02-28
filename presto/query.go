package presto

import (
	"bufio"
	"context"
	"io"
	"net/http"
	"pbench/presto/query_json"
	"strconv"
	"strings"
)

// QueryWithSession represents a query and its additional session parameters
type QueryWithSession struct {
	Query         string
	SessionParams map[string]any
}

func (c *Client) requestQueryResults(ctx context.Context, req *http.Request) (*QueryResults, *http.Response, error) {
	qr := new(QueryResults)
	resp, err := c.Do(ctx, req, qr)
	if err != nil {
		return nil, resp, err
	}
	qr.client = c
	if qr.Error != nil {
		return qr, resp, qr.Error
	}
	return qr, resp, nil
}

func (c *Client) Query(ctx context.Context, query string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := c.NewRequest("POST",
		"v1/statement", query, opts...)
	if err != nil {
		return nil, nil, err
	}

	return c.requestQueryResults(ctx, req)
}

func (c *Client) FetchNextBatch(ctx context.Context, nextUri string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := c.NewRequest("GET",
		nextUri, nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	return c.requestQueryResults(ctx, req)
}

func (c *Client) CancelQuery(ctx context.Context, nextUri string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := c.NewRequest("DELETE",
		nextUri, nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	return c.requestQueryResults(ctx, req)
}

// GetQueryInfo retrieves the query JSON for the given query ID.
// If writer is nil, we return deserialized QueryInfo. Otherwise, we just return the raw buffer.
func (c *Client) GetQueryInfo(ctx context.Context, queryId string, pretty bool, writer io.Writer, opts ...RequestOption) (*query_json.QueryInfo, *http.Response, error) {
	urlStr := "v1/query/" + queryId
	if pretty {
		urlStr += "?pretty"
	}
	req, err := c.NewRequest("GET",
		urlStr, nil, opts...)
	if err != nil {
		return nil, nil, err
	}
	var (
		resp      *http.Response
		queryInfo *query_json.QueryInfo
	)
	if writer != nil {
		resp, err = c.Do(ctx, req, writer)
	} else {
		queryInfo = new(query_json.QueryInfo)
		resp, err = c.Do(ctx, req, queryInfo)
	}
	if err != nil {
		return nil, resp, err
	}
	return queryInfo, resp, nil
}

// ParseSessionCommand checks if a query is a session parameter command and returns the parameter and value
// Format: --session parameter_name=parameter_value or --SET SESSION parameter_name=parameter_value
func ParseSessionCommand(query string) (paramName string, paramValue string, isSession bool) {
	query = strings.TrimSpace(query)
	
	// Check if query starts with --session or --set session (case insensitive)
	queryLower := strings.ToLower(query)
	if !strings.HasPrefix(queryLower, "--session") && !strings.HasPrefix(queryLower, "--set session") {
		return "", "", false
	}

	// Remove the prefix
	if strings.HasPrefix(queryLower, "--set session") {
		query = strings.TrimSpace(query[13:])  // len("--set session") = 13
	} else {
		query = strings.TrimSpace(query[9:])   // len("--session") = 9
	}

	// Split on equals sign and handle spaces
	parts := strings.SplitN(query, "=", 2)
	if len(parts) != 2 {
		return "", "", false
	}

	paramName = strings.ToLower(strings.TrimSpace(parts[0]))
	paramValue = strings.TrimSpace(parts[1])

	// Remove quotes if present
	if strings.HasPrefix(paramValue, "'") && strings.HasSuffix(paramValue, "'") {
		paramValue = paramValue[1:len(paramValue)-1]
	}
	if strings.HasPrefix(paramValue, "\"") && strings.HasSuffix(paramValue, "\"") {
		paramValue = paramValue[1:len(paramValue)-1]
	}

	// Remove trailing semicolon if present
	if strings.HasSuffix(paramValue, ";") {
		paramValue = strings.TrimSuffix(paramValue, ";")
	}

	// Convert value to uppercase for enum values
	paramValue = strings.ToUpper(paramValue)

	return paramName, paramValue, true
}

// cleanQuery removes unnecessary whitespace, newlines, comments and trailing semicolon from a query
func cleanQuery(query string) string {
	// Split into lines and handle each line
	lines := strings.Split(query, "\n")
	cleanLines := make([]string, 0, len(lines))
	
	for _, line := range lines {
		// Remove inline comments
		if idx := strings.Index(line, "--"); idx >= 0 {
			line = line[:idx]
		}
		
		trimmed := strings.TrimSpace(line)
		if trimmed != "" {
			cleanLines = append(cleanLines, trimmed)
		}
	}
	
	// Join with single spaces
	query = strings.Join(cleanLines, " ")
	
	// Remove trailing semicolon
	if strings.HasSuffix(query, ";") {
		query = strings.TrimSuffix(query, ";")
	}
	
	return query
}

// SplitQueriesWithSession splits a SQL file into individual queries and their associated session parameters
func SplitQueriesWithSession(r io.Reader) ([]QueryWithSession, error) {
	queries := make([]QueryWithSession, 0)
	currentSessionParams := make(map[string]any)
	
	scanner := bufio.NewScanner(r)
	var currentQuery strings.Builder
	inMultilineComment := false
	
	for scanner.Scan() {
		line := scanner.Text()
		trimmedLine := strings.TrimSpace(line)

		// Skip empty lines
		if len(trimmedLine) == 0 {
			continue
		}

		// Handle multiline comments
		if strings.HasPrefix(trimmedLine, "/*") {
			inMultilineComment = true
		}
		if inMultilineComment {
			if strings.HasSuffix(trimmedLine, "*/") {
				inMultilineComment = false
			}
			continue
		}

		// Handle single line comments and session parameters
		if strings.HasPrefix(trimmedLine, "--") {
			paramName, paramValue, isSession := ParseSessionCommand(trimmedLine)
			if isSession {
				// Try to parse value as number or boolean first
				if val, err := strconv.ParseInt(paramValue, 10, 64); err == nil {
					currentSessionParams[paramName] = val
				} else if val, err := strconv.ParseFloat(paramValue, 64); err == nil {
					currentSessionParams[paramName] = val
				} else if val, err := strconv.ParseBool(paramValue); err == nil {
					currentSessionParams[paramName] = val
				} else {
					// Remove any remaining quotes from string values
					if strings.HasPrefix(paramValue, "'") && strings.HasSuffix(paramValue, "'") {
						paramValue = paramValue[1:len(paramValue)-1]
					}
					if strings.HasPrefix(paramValue, "\"") && strings.HasSuffix(paramValue, "\"") {
						paramValue = paramValue[1:len(paramValue)-1]
					}
					// Treat as string if not a number or boolean
					currentSessionParams[paramName] = paramValue
				}
			}
			continue
		}

		currentQuery.WriteString(line)
		currentQuery.WriteString("\n")

		// Check if line ends with semicolon
		if strings.HasSuffix(trimmedLine, ";") {
			query := strings.TrimSpace(currentQuery.String())
			if len(query) > 0 {
				// Clean up the query formatting
				query = cleanQuery(query)
				
				// Create a copy of current session parameters for this query
				sessionParams := make(map[string]any, len(currentSessionParams))
				for k, v := range currentSessionParams {
					sessionParams[k] = v
				}
				queries = append(queries, QueryWithSession{
					Query:         query,
					SessionParams: sessionParams,
				})
				// Clear session parameters after query
				currentSessionParams = make(map[string]any)
			}
			currentQuery.Reset()
		}
	}

	// Handle last query if it doesn't end with semicolon
	lastQuery := strings.TrimSpace(currentQuery.String())
	if len(lastQuery) > 0 {
		sessionParams := make(map[string]any, len(currentSessionParams))
		for k, v := range currentSessionParams {
			sessionParams[k] = v
		}
		queries = append(queries, QueryWithSession{
			Query:         lastQuery,
			SessionParams: sessionParams,
		})
	}

	if err := scanner.Err(); err != nil {
		return nil, err
	}

	return queries, nil
}
