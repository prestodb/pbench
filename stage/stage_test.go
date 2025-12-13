package stage

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"strconv"
	"sync/atomic"
	"testing"

	"pbench/presto"

	"github.com/stretchr/testify/assert"
)

// mockPrestoHandler creates a handler that simulates Presto responses based on query content
func mockPrestoHandler() http.HandlerFunc {
	var queryCounter atomic.Int32

	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Handle query info requests (for saving JSON files)
		if r.Method == http.MethodGet && r.URL.Path != "/v1/statement" {
			_ = json.NewEncoder(w).Encode(map[string]any{
				"queryId": "test_query",
				"state":   "FINISHED",
			})
			return
		}

		// Read query from request body for POST /v1/statement
		if r.Method == http.MethodPost && r.URL.Path == "/v1/statement" {
			queryId := "test_query_" + strconv.Itoa(int(queryCounter.Add(1)))

			// Simulate different responses based on query content or headers
			// Check X-Presto-Schema header to simulate http_error test case
			if r.Header.Get("X-Presto-Schema") != "" && r.Header.Get("X-Presto-Catalog") == "" {
				w.WriteHeader(http.StatusBadRequest)
				_, _ = w.Write([]byte("Schema is set but catalog is not"))
				return
			}

			// Read query body to determine response
			buf := make([]byte, 1024)
			n, _ := r.Body.Read(buf)
			query := string(buf[:n])

			// Simulate error for specific queries (from stage_4.sql)
			if query == "select * from foo" {
				_ = json.NewEncoder(w).Encode(&presto.QueryResults{
					Id:      queryId,
					InfoUri: "http://localhost/ui/query/" + queryId,
					Error: &presto.QueryError{
						Message:   "Table tpch.sf1.foo does not exist",
						ErrorName: "SYNTAX_ERROR",
						ErrorType: "USER_ERROR",
					},
				})
				return
			}

			if query == "select sum1(1)" {
				_ = json.NewEncoder(w).Encode(&presto.QueryResults{
					Id:      queryId,
					InfoUri: "http://localhost/ui/query/" + queryId,
					Error: &presto.QueryError{
						Message:   "line 1:11: Function sum1 not registered",
						ErrorName: "SYNTAX_ERROR",
						ErrorType: "USER_ERROR",
					},
				})
				return
			}

			// Default successful response with row data
			// Return 2 rows per query
			rowCount := 2
			data := make([]json.RawMessage, rowCount)
			for i := 0; i < rowCount; i++ {
				data[i] = []byte(`["row` + strconv.Itoa(i+1) + `"]`)
			}

			_ = json.NewEncoder(w).Encode(&presto.QueryResults{
				Id:      queryId,
				InfoUri: "http://localhost/ui/query/" + queryId,
				Stats:   presto.StatementStats{State: "FINISHED"},
				Data:    data,
				Columns: []presto.Column{{Name: "col1", Type: "varchar"}},
			})
			return
		}

		// Handle nextUri fetches - return empty to signal completion
		_ = json.NewEncoder(w).Encode(&presto.QueryResults{
			Stats: presto.StatementStats{State: "FINISHED"},
		})
	}
}

// newMockClientFn returns a function that creates a client connected to the mock server
func newMockClientFn(serverURL string) func() *presto.Client {
	return func() *presto.Client {
		client, _ := presto.NewClient(serverURL, false)
		return client
	}
}

func assertStage(t *testing.T, stage *Stage, prerequisites, next []*Stage, queries, queryFiles int) {
	assert.NotNil(t, stage)
	assert.Equal(t, next, stage.NextStages)
	assert.Equal(t, queries, len(stage.Queries))
	assert.Equal(t, queryFiles, len(stage.QueryFiles))
}

func testParseAndExecute(t *testing.T, mockServerURL string, abortOnError bool, minQueryCount, maxQueryCount int, expectedRowCount int, expectedErrors []string, expectedScriptCount int) {
	/** from top to bottom
	       stage_1
	       /      \
	  stage_2   stage_3
	    \           |
	     \      stage_4 (has error)
	      \       /
	       stage_5
	          |
	       stage_6
	*/
	stage1, stages, parseErr := ParseStageGraphFromFile("../benchmarks/test/stage_1.json")
	assert.Nil(t, parseErr)
	stage1.InitStates()

	// Inject mock client factory
	stage1.States.NewClient = newMockClientFn(mockServerURL)

	stage2 := stages.Get("stage_2")
	stage3 := stages.Get("stage_3")
	stage4 := stages.Get("stage_4")
	stage5 := stages.Get("stage_5")
	stage6 := stages.Get("stage_6")

	assertStage(t, stage1, []*Stage(nil), []*Stage{stage2, stage3}, 1, 0)
	assertStage(t, stage2, []*Stage{stage1}, []*Stage{stage5}, 1, 0)
	assertStage(t, stage3, []*Stage{stage1}, []*Stage{stage4}, 1, 2)
	assertStage(t, stage4, []*Stage{stage3}, []*Stage{stage5}, 2, 1)
	assertStage(t, stage5, []*Stage{stage2, stage4}, []*Stage{stage6}, 2, 1)
	assertStage(t, stage6, []*Stage{stage5}, []*Stage(nil), 0, 1)

	stage4.AbortOnError = &abortOnError
	queryCount, rowCount, errs := 0, 0, make([]error, 0, len(expectedErrors))
	stage1.States.OnQueryCompletion = func(result *QueryResult) {
		rowCount += result.RowCount
		queryCount++
		if result.QueryError != nil && !isContextError(result.QueryError) {
			errs = append(errs, result.QueryError)
		}
	}

	stage1.Run(context.Background())
	defer func() {
		_ = os.RemoveAll(stage1.States.OutputPath)
	}()

	// Use range check for query count due to race conditions with context cancellation
	assert.GreaterOrEqual(t, queryCount, minQueryCount, "query count should be at least %d", minQueryCount)
	assert.LessOrEqual(t, queryCount, maxQueryCount, "query count should be at most %d", maxQueryCount)
	assert.Equal(t, len(expectedErrors), len(errs))
	for i, err := range errs {
		if i < len(expectedErrors) {
			assert.Contains(t, err.Error(), expectedErrors[i])
		}
	}
	assert.Equal(t, expectedRowCount, rowCount)

	const scriptCountFilePath = "../benchmarks/test/count.txt"
	countBytes, ioErr := os.ReadFile(scriptCountFilePath)
	if !assert.Nil(t, ioErr) {
		t.FailNow()
	}
	_ = os.Remove(scriptCountFilePath)
	scriptCount, convErr := strconv.Atoi(string(countBytes))
	if !assert.Nil(t, convErr) {
		t.FailNow()
	}
	assert.Equal(t, expectedScriptCount, scriptCount)
}

// isContextError checks if error is context-related
func isContextError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return errStr == "context canceled" || errStr == "context deadline exceeded"
}

func TestParseStageGraph(t *testing.T) {
	// Create mock server
	server := httptest.NewServer(mockPrestoHandler())
	defer server.Close()

	t.Run("abortOnError = true", func(t *testing.T) {
		// stage_4's post-stage Python script fails, causing context cancellation
		// With abortOnError=true: stage_5 may or may not start a query before cancellation
		// Min queries: stage_1(1) + stage_2(1) + stage_3(4) + stage_4(4) = 10
		// Max queries: 10 + stage_5(1 partial due to race) = 11
		// Script count: 13 (all stage_4 scripts run before post-stage script fails)
		testParseAndExecute(t, server.URL, true, 10, 11, 0, []string{}, 13)
	})
	t.Run("abortOnError = false", func(t *testing.T) {
		// With abortOnError=false: stage_5 and stage_6 run despite script error
		// Queries: stage_1(1) + stage_2(1) + stage_3(4) + stage_4(4) + stage_5(4) + stage_6(1) = 15
		testParseAndExecute(t, server.URL, false, 15, 15, 0, []string{}, 13)
	})
}

func TestHttpError(t *testing.T) {
	// Create mock server that returns 400 for schema without catalog
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")

		// Check if schema is set without catalog
		if r.Header.Get("X-Presto-Schema") != "" && r.Header.Get("X-Presto-Catalog") == "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("Schema is set but catalog is not (status code: 400)"))
			return
		}

		_ = json.NewEncoder(w).Encode(&presto.QueryResults{
			Id:    "test_query",
			Stats: presto.StatementStats{State: "FINISHED"},
		})
	}))
	defer server.Close()

	stage, _, err := ParseStageGraphFromFile("../benchmarks/test/http_error.json")
	assert.Nil(t, err)

	queryCount := 0
	stage.InitStates()

	// Inject mock client factory
	stage.States.NewClient = newMockClientFn(server.URL)

	stage.States.OnQueryCompletion = func(result *QueryResult) {
		queryCount++
		err = result.QueryError
	}
	assert.Nil(t, err)

	stage.Run(context.Background())
	_ = os.RemoveAll(stage.States.OutputPath)

	assert.Equal(t, 1, queryCount)
	assert.NotNil(t, err)
	assert.Contains(t, err.Error(), "400")
}
