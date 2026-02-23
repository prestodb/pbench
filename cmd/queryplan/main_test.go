package queryplan

import (
	"encoding/csv"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// loadTestPlan reads a plan JSON fixture from the plan_node package.
func loadTestPlan(t *testing.T, name string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join("..", "..", "prestoapi", "plan_node", name))
	require.NoError(t, err)
	return string(data)
}

func TestProcessFile(t *testing.T) {
	tmpDir := t.TempDir()
	planJSON := loadTestPlan(t, "sample.plan.json")

	// Write a CSV with header + two data rows: one with a plan, one empty.
	csvPath := filepath.Join(tmpDir, "input.csv")
	csvFile, err := os.Create(csvPath)
	require.NoError(t, err)
	w := csv.NewWriter(csvFile)
	require.NoError(t, w.Write([]string{"query_id", "plan"}))
	require.NoError(t, w.Write([]string{"q1", planJSON}))
	require.NoError(t, w.Write([]string{"q2", ""})) // empty plan, should be skipped
	w.Flush()
	require.NoError(t, w.Error())
	csvFile.Close()

	// Reset package-level state.
	queryPlanColumn = 1
	hasHeader = true
	output = filepath.Join(tmpDir, "output.json")
	failureCounter = 0
	validCounter = 0

	err = processFile(csvPath)
	require.NoError(t, err)

	// Read and validate the output JSON.
	outBytes, err := os.ReadFile(output)
	require.NoError(t, err)

	var result map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(outBytes, &result))
	assert.Contains(t, result, "2", "expected row 2 in output")
	assert.Equal(t, 1, validCounter, "should have parsed 1 valid plan")
	assert.Equal(t, 0, failureCounter, "should have no failures")
}

func TestProcessFileMultiplePlans(t *testing.T) {
	tmpDir := t.TempDir()
	samplePlan := loadTestPlan(t, "sample.plan.json")
	arithmeticsPlan := loadTestPlan(t, "arithmetics.plan.json")

	csvPath := filepath.Join(tmpDir, "input.csv")
	csvFile, err := os.Create(csvPath)
	require.NoError(t, err)
	w := csv.NewWriter(csvFile)
	require.NoError(t, w.Write([]string{"plan"}))
	require.NoError(t, w.Write([]string{samplePlan}))
	require.NoError(t, w.Write([]string{arithmeticsPlan}))
	w.Flush()
	require.NoError(t, w.Error())
	csvFile.Close()

	queryPlanColumn = 0
	hasHeader = true
	output = filepath.Join(tmpDir, "output.json")
	failureCounter = 0
	validCounter = 0

	err = processFile(csvPath)
	require.NoError(t, err)

	outBytes, err := os.ReadFile(output)
	require.NoError(t, err)

	var result map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(outBytes, &result))
	assert.Contains(t, result, "2")
	assert.Contains(t, result, "3")
	assert.Equal(t, 2, validCounter)
	assert.Equal(t, 0, failureCounter)
}

func TestProcessFileNoHeader(t *testing.T) {
	tmpDir := t.TempDir()
	planJSON := loadTestPlan(t, "arithmetics.plan.json")

	csvPath := filepath.Join(tmpDir, "input.csv")
	csvFile, err := os.Create(csvPath)
	require.NoError(t, err)
	w := csv.NewWriter(csvFile)
	require.NoError(t, w.Write([]string{planJSON}))
	w.Flush()
	require.NoError(t, w.Error())
	csvFile.Close()

	queryPlanColumn = 0
	hasHeader = false
	output = filepath.Join(tmpDir, "output.json")
	failureCounter = 0
	validCounter = 0

	err = processFile(csvPath)
	require.NoError(t, err)

	outBytes, err := os.ReadFile(output)
	require.NoError(t, err)

	var result map[string]json.RawMessage
	require.NoError(t, json.Unmarshal(outBytes, &result))
	assert.Contains(t, result, "1", "expected row 1 in output (no header)")
	assert.Equal(t, 1, validCounter)
}

func TestProcessFileColumnOutOfBounds(t *testing.T) {
	tmpDir := t.TempDir()

	csvPath := filepath.Join(tmpDir, "input.csv")
	csvFile, err := os.Create(csvPath)
	require.NoError(t, err)
	w := csv.NewWriter(csvFile)
	require.NoError(t, w.Write([]string{"header"}))
	require.NoError(t, w.Write([]string{"only_one_column"}))
	w.Flush()
	require.NoError(t, w.Error())
	csvFile.Close()

	queryPlanColumn = 5 // out of bounds
	hasHeader = true
	output = filepath.Join(tmpDir, "output.json")
	failureCounter = 0
	validCounter = 0

	err = processFile(csvPath)
	require.NoError(t, err)
	assert.Equal(t, 0, validCounter)
}

func TestParseQueryPlan(t *testing.T) {
	planJSON := loadTestPlan(t, "arithmetics.plan.json")
	joins, err := parseQueryPlan(planJSON)
	require.NoError(t, err)
	assert.Len(t, joins, 1, "expected 1 join")
	assert.Equal(t, "InnerJoin", joins[0].JoinType)
}

func TestParseQueryPlanInvalidJSON(t *testing.T) {
	_, err := parseQueryPlan("not valid json")
	assert.Error(t, err)
}
