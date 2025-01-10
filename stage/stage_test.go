package stage

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"os"
	"strconv"
	"syscall"
	"testing"
)

func assertStage(t *testing.T, stage *Stage, prerequisites, next []*Stage, queries, queryFiles int) {
	assert.NotNil(t, stage)
	assert.Equal(t, next, stage.NextStages)
	assert.Equal(t, queries, len(stage.Queries))
	assert.Equal(t, queryFiles, len(stage.QueryFiles))
}

func testParseAndExecute(t *testing.T, abortOnError bool, totalQueryCount int, expectedRowCount int, expectedErrors []string, expectedScriptCount int) {
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
		if result.QueryError != nil && !errors.Is(result.QueryError, context.Canceled) {
			errs = append(errs, result.QueryError)
		}
	}

	stage1.Run(context.Background())
	defer assert.Nil(t, os.RemoveAll(stage1.States.OutputPath))

	assert.Equal(t, totalQueryCount, queryCount)
	assert.Equal(t, len(expectedErrors), len(errs))
	for i, err := range errs {
		if errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("%v: this test requires Presto Hive query runner to run.", err)
		}
		assert.Equal(t, expectedErrors[i], err.Error())
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
		testParseAndExecute(t, true, 10, 16, []string{
			"SYNTAX_ERROR: Table tpch.sf1.foo does not exist"}, 9)
	})
	t.Run("abortOnError = false", func(t *testing.T) {
		testParseAndExecute(t, false, 15, 24, []string{
			"SYNTAX_ERROR: Table tpch.sf1.foo does not exist",
			"SYNTAX_ERROR: line 1:11: Function sum1 not registered"}, 13)
	})
}

func TestHttpError(t *testing.T) {
	stage, _, err := ParseStageGraphFromFile("../benchmarks/test/http_error.json")
	assert.Nil(t, err)
	queryCount := 0
	stage.InitStates()
	stage.States.OnQueryCompletion = func(result *QueryResult) {
		queryCount++
		err = result.QueryError
	}
	assert.Nil(t, err)
	stage.Run(context.Background())
	assert.Nil(t, os.RemoveAll(stage.States.OutputPath))
	assert.Equal(t, 1, queryCount)
	assert.Equal(t, "Schema is set but catalog is not (status code: 400)", err.Error())
}
