package stage

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	presto "github.com/ethanyzhang/presto-go"
	"github.com/ethanyzhang/presto-go/prestotest"
	"github.com/stretchr/testify/assert"
)

func setupMockServer() *prestotest.MockPrestoServer {
	mock := prestotest.NewMockPrestoServer()
	// Add default latency so that when abort cancels context, in-flight queries
	// return context.Canceled rather than completing successfully.
	mock.SetDefaultLatency(20 * time.Millisecond)

	// Simple inline queries - each returns 1 row
	for _, q := range []string{
		"select 'query 1'", "select 'query 2'", "select 'query 3'",
		"select 'query 4'", "select 'query 5'", "select 'query 7'",
		"select 'query 8'", "select 'query 9'",
	} {
		mock.AddQuery(&prestotest.MockQueryTemplate{
			SQL:     q,
			Columns: []presto.Column{{Name: "_col0", Type: "varchar"}},
			Data:    [][]any{{q[8 : len(q)-1]}},
		})
	}

	// stage_3_1.sql: CTE with UNION ALL - 6 rows
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "with x as (select *\n" +
			"           from (values (1, 2, 'shanghai'),\n" +
			"                        (3, 4, 'dalian'),\n" +
			"                        (5, 6, 'new york')) as X(a, b, c))\n" +
			"select *\n" +
			"from x\n" +
			"union all\n" +
			"select a + 10, b + 20, c\n" +
			"from x",
		Columns: []presto.Column{
			{Name: "a", Type: "integer"}, {Name: "b", Type: "integer"}, {Name: "c", Type: "varchar"},
		},
		Data: [][]any{
			{1, 2, "shanghai"}, {3, 4, "dalian"}, {5, 6, "new york"},
			{11, 22, "shanghai"}, {13, 24, "dalian"}, {15, 26, "new york"},
		},
	})

	// stage_3_2.sql query 1: WHERE a > 2 - 2 rows
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "select b, c, a, a * 2 as q\n" +
			"from (values (1, 2, 'shanghai'),\n" +
			"             (3, 4, 'dalian'),\n" +
			"             (5, 6, 'new york')) as X(a, b, c)\n" +
			"where a > 2",
		Columns: []presto.Column{
			{Name: "b", Type: "integer"}, {Name: "c", Type: "varchar"},
			{Name: "a", Type: "integer"}, {Name: "q", Type: "integer"},
		},
		Data: [][]any{{4, "dalian", 3, 6}, {6, "new york", 5, 10}},
	})

	// stage_3_2.sql query 2: GROUP BY - 3 rows
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "select c, sum(a) as sum, count(*) as count, max(b) as max\n" +
			"from (values (1, 2, 'shanghai'),\n" +
			"             (3, 4, 'dalian'),\n" +
			"             (5, 6, 'new york'),\n" +
			"             (6, 87, 'new york'),\n" +
			"             (69, 1, 'dalian'),\n" +
			"             (15, 97, 'dalian')) as X(a, b, c)\n" +
			"group by c\n" +
			"order by count",
		Columns: []presto.Column{
			{Name: "c", Type: "varchar"}, {Name: "sum", Type: "bigint"},
			{Name: "count", Type: "bigint"}, {Name: "max", Type: "integer"},
		},
		Data: [][]any{
			{"shanghai", 1, 1, 2}, {"new york", 11, 2, 87}, {"dalian", 87, 3, 97},
		},
	})

	// stage_4.sql query 1: error - table not found
	// QueueBatches=2 ensures error is returned during Drain (not from Query()),
	// matching real Presto behavior where errors appear after initial queued state.
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:          "select 'query 6'\nfrom foo",
		QueueBatches: 2,
		Error: &presto.QueryError{
			ErrorName: "SYNTAX_ERROR",
			Message:   "Table tpch.sf1.foo does not exist",
			ErrorCode: 1,
			ErrorType: "USER_ERROR",
		},
	})

	// stage_6.sql: error - function not registered
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL: "select c, sum1(a) as sum, count(*) as count, max(b) as max\n" +
			"from (values (1, 2, 'shanghai'),\n" +
			"             (3, 4, 'dalian'),\n" +
			"             (5, 6, 'new york'),\n" +
			"             (6, 87, 'new york'),\n" +
			"             (69, 1, 'dalian'),\n" +
			"             (15, 97, 'dalian')) as X(a, b, c)\n" +
			"group by c\n" +
			"order by count",
		QueueBatches: 2,
		Error: &presto.QueryError{
			ErrorName: "SYNTAX_ERROR",
			Message:   "line 1:11: Function sum1 not registered",
			ErrorCode: 1,
			ErrorType: "USER_ERROR",
		},
	})

	return mock
}

func assertStage(t *testing.T, stage *Stage, prerequisites, next []*Stage, queries, queryFiles int) {
	assert.NotNil(t, stage)
	assert.Equal(t, next, stage.NextStages)
	assert.Equal(t, queries, len(stage.Queries))
	assert.Equal(t, queryFiles, len(stage.QueryFiles))
}

func testParseAndExecute(t *testing.T, abortOnError bool, minQueryCount, maxQueryCount int, expectedRowCount int, expectedErrors []string, expectedScriptCount int) {
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
	mock := setupMockServer()
	defer mock.Close()

	stage1, stages, parseErr := ParseStageGraphFromFile("../benchmarks/test/stage_1.json")
	assert.Nil(t, parseErr)
	stage1.InitStates()
	stage1.States.NewClient = func() *presto.Client {
		client, _ := presto.NewClient(mock.URL())
		return client
	}

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
	var mu sync.Mutex
	queryCount, rowCount, errs := 0, 0, make([]error, 0, len(expectedErrors))
	stage1.States.OnQueryCompletion = func(result *QueryResult) {
		mu.Lock()
		defer mu.Unlock()
		rowCount += result.RowCount
		queryCount++
		if result.QueryError != nil && !errors.Is(result.QueryError, context.Canceled) {
			errs = append(errs, result.QueryError)
		}
	}

	stage1.Run(context.Background())
	defer assert.Nil(t, os.RemoveAll(stage1.States.OutputPath))

	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, queryCount, minQueryCount)
	assert.LessOrEqual(t, queryCount, maxQueryCount)
	assert.Equal(t, len(expectedErrors), len(errs))
	for i, err := range errs {
		if errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("%v: mock server connection refused", err)
		}
		var qe *presto.QueryError
		if errors.As(err, &qe) {
			assert.Equal(t, expectedErrors[i], qe.Error())
		} else {
			assert.Equal(t, expectedErrors[i], err.Error())
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

func TestParseStageGraph(t *testing.T) {
	t.Run("abortOnError = true", func(t *testing.T) {
		// With abort, stage_4 stops at the error query. Stage_5 may start 0-1 queries
		// before context cancellation depending on goroutine scheduling.
		testParseAndExecute(t, true, 9, 10, 16, []string{
			"SYNTAX_ERROR: Table tpch.sf1.foo does not exist"}, 9)
	})
	t.Run("abortOnError = false", func(t *testing.T) {
		testParseAndExecute(t, false, 15, 15, 24, []string{
			"SYNTAX_ERROR: Table tpch.sf1.foo does not exist",
			"SYNTAX_ERROR: line 1:11: Function sum1 not registered"}, 13)
	})
}

func TestRandomExecution(t *testing.T) {
	mock := setupMockServer()
	defer mock.Close()

	stage, _, parseErr := ParseStageGraphFromFile("../benchmarks/test/random_stage.json")
	assert.Nil(t, parseErr)
	stage.InitStates()
	stage.States.RandSeed = 42
	stage.States.NewClient = func() *presto.Client {
		client, _ := presto.NewClient(mock.URL())
		return client
	}

	queryCount := 0
	stage.States.OnQueryCompletion = func(result *QueryResult) {
		queryCount++
		assert.Nil(t, result.QueryError)
	}

	stage.Run(context.Background())
	defer assert.Nil(t, os.RemoveAll(stage.States.OutputPath))

	// randomly_execute_until=6, so we should get exactly 6 queries
	assert.Equal(t, 6, queryCount)
	assert.True(t, stage.States.RandSeedUsed)
}

func TestWarmRuns(t *testing.T) {
	mock := setupMockServer()
	defer mock.Close()

	stage, _, parseErr := ParseStageGraphFromFile("../benchmarks/test/warm_runs_stage.json")
	assert.Nil(t, parseErr)
	stage.InitStates()
	stage.States.NewClient = func() *presto.Client {
		client, _ := presto.NewClient(mock.URL())
		return client
	}

	coldRuns, warmRuns := 0, 0
	stage.States.OnQueryCompletion = func(result *QueryResult) {
		assert.Nil(t, result.QueryError)
		if result.Query.ColdRun {
			coldRuns++
		} else {
			warmRuns++
		}
	}

	stage.Run(context.Background())
	defer assert.Nil(t, os.RemoveAll(stage.States.OutputPath))

	// cold_runs=1, warm_runs=2 for 1 query -> 1 cold + 2 warm = 3 total
	assert.Equal(t, 1, coldRuns)
	assert.Equal(t, 2, warmRuns)
}

func TestExpectedRowCount(t *testing.T) {
	mock := setupMockServer()
	defer mock.Close()

	stage, _, parseErr := ParseStageGraphFromFile("../benchmarks/test/expected_row_count_stage.json")
	assert.Nil(t, parseErr)
	stage.InitStates()
	stage.States.NewClient = func() *presto.Client {
		client, _ := presto.NewClient(mock.URL())
		return client
	}

	queryCount := 0
	stage.States.OnQueryCompletion = func(result *QueryResult) {
		queryCount++
		assert.Nil(t, result.QueryError)
		// Both queries return 1 row, and expected row count is [1, 1]
		assert.Equal(t, result.Query.ExpectedRowCount, result.RowCount)
	}

	stage.Run(context.Background())
	defer assert.Nil(t, os.RemoveAll(stage.States.OutputPath))

	assert.Equal(t, 2, queryCount)
}

func TestSaveOutput(t *testing.T) {
	mock := setupMockServer()
	defer mock.Close()

	s, _, parseErr := ParseStageGraphFromFile("../benchmarks/test/save_output_stage.json")
	assert.Nil(t, parseErr)
	s.InitStates()
	tmpDir := t.TempDir()
	s.States.OutputPath = tmpDir
	s.States.NewClient = func() *presto.Client {
		client, _ := presto.NewClient(mock.URL())
		return client
	}

	s.States.OnQueryCompletion = func(result *QueryResult) {
		assert.Nil(t, result.QueryError)
	}

	s.Run(context.Background())

	// With save_output=true, an output file should have been created
	// The actual output dir is OutputPath/RunName
	outputDir := s.States.OutputPath
	outputFiles, err := os.ReadDir(outputDir)
	assert.Nil(t, err)
	foundOutput := false
	for _, f := range outputFiles {
		if strings.HasSuffix(f.Name(), ".output") {
			foundOutput = true
			content, readErr := os.ReadFile(filepath.Join(outputDir, f.Name()))
			assert.Nil(t, readErr)
			assert.Greater(t, len(content), 0)
		}
	}
	assert.True(t, foundOutput, "expected .output file in %s, found: %v", outputDir, outputFiles)
}

func TestContextCancellation(t *testing.T) {
	mock := setupMockServer()
	// Add a long-running latency to make cancellation observable
	mock.SetDefaultLatency(100 * time.Millisecond)
	defer mock.Close()

	stage1, _, parseErr := ParseStageGraphFromFile("../benchmarks/test/stage_1.json")
	assert.Nil(t, parseErr)
	stage1.InitStates()
	stage1.States.NewClient = func() *presto.Client {
		client, _ := presto.NewClient(mock.URL())
		return client
	}

	ctx, cancel := context.WithCancel(context.Background())
	var mu sync.Mutex
	queryCount := 0
	stage1.States.OnQueryCompletion = func(result *QueryResult) {
		mu.Lock()
		defer mu.Unlock()
		queryCount++
		// Cancel after the first query finishes
		if queryCount == 1 {
			cancel()
		}
	}

	// Set abort_on_error on root stage so cancellation propagates
	trueVal := true
	stage1.AbortOnError = &trueVal
	stage1.Run(ctx)
	defer assert.Nil(t, os.RemoveAll(stage1.States.OutputPath))

	// We should have at least 1 query completed but not all 15
	mu.Lock()
	defer mu.Unlock()
	assert.GreaterOrEqual(t, queryCount, 1)
	assert.Less(t, queryCount, 15)
}

func TestAutoNewClientOnCatalogChange(t *testing.T) {
	mock := setupMockServer()
	defer mock.Close()

	// Register parent and child queries
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:     "select 'parent'",
		Columns: []presto.Column{{Name: "_col0", Type: "varchar"}},
		Data:    [][]any{{"parent"}},
	})
	mock.AddQuery(&prestotest.MockQueryTemplate{
		SQL:     "select 'child'",
		Columns: []presto.Column{{Name: "_col0", Type: "varchar"}},
		Data:    [][]any{{"child"}},
	})

	stage1, stages, parseErr := ParseStageGraphFromFile("../benchmarks/test/auto_new_client_stage.json")
	assert.Nil(t, parseErr)
	stage1.InitStates()

	clientCount := 0
	stage1.States.NewClient = func() *presto.Client {
		clientCount++
		client, _ := presto.NewClient(mock.URL())
		return client
	}

	queryCount := 0
	stage1.States.OnQueryCompletion = func(result *QueryResult) {
		queryCount++
		assert.Nil(t, result.QueryError)
	}

	stage1.Run(context.Background())
	defer assert.Nil(t, os.RemoveAll(stage1.States.OutputPath))

	childStage := stages.Get("auto_new_client_child")
	assert.NotNil(t, childStage)

	// Parent creates 1 client, child should auto-create another because catalog/schema differ
	assert.Equal(t, 2, clientCount, "expected 2 clients: parent + auto-created child")
	assert.Equal(t, 2, queryCount, "expected 2 queries: parent + child")
	// Verify child's client has the child's catalog/schema
	assert.Equal(t, "child_catalog", childStage.Client.GetCatalog())
	assert.Equal(t, "child_schema", childStage.Client.GetSchema())
}

func TestHttpError(t *testing.T) {
	// Custom httptest server that returns 400 when schema is set but catalog is not,
	// matching the behavior of a real Presto server.
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		schema := r.Header.Get("X-Presto-Schema")
		catalog := r.Header.Get("X-Presto-Catalog")
		if schema != "" && catalog == "" {
			w.WriteHeader(http.StatusBadRequest)
			_, _ = w.Write([]byte("Schema is set but catalog is not"))
			return
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		_ = json.NewEncoder(w).Encode(presto.QueryResults{})
	}))
	defer srv.Close()

	stage, _, err := ParseStageGraphFromFile("../benchmarks/test/http_error.json")
	assert.Nil(t, err)
	queryCount := 0
	stage.InitStates()
	stage.States.NewClient = func() *presto.Client {
		client, _ := presto.NewClient(srv.URL)
		return client
	}
	stage.States.OnQueryCompletion = func(result *QueryResult) {
		queryCount++
		err = result.QueryError
	}
	assert.Nil(t, err)
	stage.Run(context.Background())
	assert.Nil(t, os.RemoveAll(stage.States.OutputPath))
	assert.Equal(t, 1, queryCount)
	assert.Equal(t, "presto server error: 400: Schema is set but catalog is not", err.Error())
}
