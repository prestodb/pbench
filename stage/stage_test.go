package stage

import (
	"context"
	"errors"
	"github.com/stretchr/testify/assert"
	"presto-benchmark/presto"
	"syscall"
	"testing"
)

func assertStage(t *testing.T, stage *Stage, prerequisites, next []*Stage, queries, queryFiles int) {
	assert.NotNil(t, stage)
	assert.Equal(t, prerequisites, stage.Prerequisites)
	assert.Equal(t, next, stage.NextStages)
	assert.Equal(t, queries, len(stage.Queries))
	assert.Equal(t, queryFiles, len(stage.QueryFiles))
}

func testParseAndExecute(t *testing.T, abortOnError bool, expectedRowCount int, expectedErrors []string) {
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
	stage1, stages, err := ParseStageGraphFromFile("../benchmarks/test/stage_1.json")
	assert.Nil(t, err)
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

	stage4.AbortOnError = abortOnError
	rowCount := 0
	stage1.OnQueryCompletion = func(_ *presto.QueryResults, rc int) {
		rowCount += rc
	}

	errs := stage1.Run(context.Background())

	assert.Equal(t, len(expectedErrors), len(errs))
	for i, err := range errs {
		if errors.Is(err, syscall.ECONNREFUSED) {
			t.Fatalf("%v: this test requires Presto Hive query runner to run.", err)
		}
		var qe *presto.QueryError
		assert.True(t, errors.As(err, &qe))
		assert.Equal(t, expectedErrors[i], qe.Message)
	}
	assert.Equal(t, expectedRowCount, rowCount)
}

func TestParseStageGraph(t *testing.T) {
	t.Run("abortOnError = true", func(t *testing.T) {
		testParseAndExecute(t, true, 16, []string{"Table tpch.sf1.foo does not exist"})
	})
	t.Run("abortOnError = false", func(t *testing.T) {
		testParseAndExecute(t, false, 24, []string{
			"Table tpch.sf1.foo does not exist",
			"line 1:11: Function sum1 not registered"})
	})
}

func TestHttpError(t *testing.T) {
	stage, _, err := ParseStageGraphFromFile("../benchmarks/test/http_error.json")
	assert.Nil(t, err)
	errs := stage.Run(context.Background())
	assert.Equal(t, 1, len(errs))
	assert.Equal(t, "Schema is set but catalog is not (status code: 400)", errs[0].Error())
}
