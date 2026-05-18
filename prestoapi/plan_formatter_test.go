package prestoapi

import (
	"strings"
	"testing"
)

func TestFormatQueryPlanAsText(t *testing.T) {
	// Sample JSON plan from the user
	jsonPlan := `{"0":{"plan":{"id":"22","name":"Output","identifier":"[n_name, revenue]","details":"revenue := sum (4:5)\n","children":[{"id":"1311","name":"TopN","identifier":"[1 by (sum DESC_NULLS_LAST)]","details":"","children":[{"id":"1310","name":"TopNPartial","identifier":"[1 by (sum DESC_NULLS_LAST)]","details":"","children":[{"id":"14","name":"Aggregate(FINAL)[n_name]","identifier":"","details":"sum := \"presto.default.sum\"((sum_13)) (4:5)\n","children":[{"id":"1705","name":"LocalExchange","identifier":"[SINGLE] ()","details":"","children":[{"id":"1703","name":"Aggregate(PARTIAL)[n_name]","identifier":"","details":"sum_13 := \"presto.default.sum\"((expr)) (4:5)\n","children":[{"id":"340","name":"Project","identifier":"[projectLocality = LOCAL]","details":"expr := (l_extendedprice) * ((DOUBLE'1.0') - (l_discount)) (8:6)\n","children":[{"id":"1410","name":"InnerJoin","identifier":"[(\"l_suppkey\" = \"s_suppkey\") AND (\"c_nationkey\" = \"s_nationkey\")]","details":"Distribution: REPLICATED\n","children":[{"id":"1409","name":"InnerJoin","identifier":"[(\"l_orderkey\" = \"o_orderkey\")]","details":"Distribution: REPLICATED\n","children":[{"id":"3","name":"TableScan","identifier":"[TableHandle {connectorId='tpch', connectorHandle='lineitem:sf1.0', layout='Optional[lineitem:sf1.0]'}]","details":"l_orderkey := tpch:l_orderkey (8:5)\nl_extendedprice := tpch:l_extendedprice (8:5)\nl_suppkey := tpch:l_suppkey (8:5)\nl_discount := tpch:l_discount (8:5)\n","children":[],"remoteSources":[],"estimates":[]}],"remoteSources":[],"estimates":[]}],"remoteSources":[],"estimates":[]}],"remoteSources":[],"estimates":[]}],"remoteSources":[],"estimates":[]}],"remoteSources":[],"estimates":[]}],"remoteSources":[],"estimates":[]}],"remoteSources":[],"estimates":[]}],"remoteSources":[],"estimates":[]}],"remoteSources":[],"estimates":[]}}}`

	result, err := FormatQueryPlanAsText(jsonPlan)
	if err != nil {
		t.Fatalf("FormatQueryPlanAsText failed: %v", err)
	}

	// Verify the output contains expected elements
	expectedStrings := []string{
		"Fragment 0",
		"- Output[PlanNodeId 22][n_name, revenue]",
		"revenue := sum (4:5)",
		"- TopN[PlanNodeId 1311][1 by (sum DESC_NULLS_LAST)]",
		"- TableScan[PlanNodeId 3]",
		"l_orderkey := tpch:l_orderkey",
	}

	for _, expected := range expectedStrings {
		if !strings.Contains(result, expected) {
			t.Errorf("Expected output to contain %q, but it didn't.\nGot:\n%s", expected, result)
		}
	}

	// Print the result for manual inspection
	t.Logf("Formatted plan:\n%s", result)
}

func TestFormatQueryPlanAsText_EmptyInput(t *testing.T) {
	result, err := FormatQueryPlanAsText("")
	if err != nil {
		t.Fatalf("FormatQueryPlanAsText with empty input failed: %v", err)
	}
	if result != "" {
		t.Errorf("Expected empty result for empty input, got: %q", result)
	}
}

func TestFormatQueryPlanAsText_InvalidJSON(t *testing.T) {
	_, err := FormatQueryPlanAsText("invalid json")
	if err == nil {
		t.Error("Expected error for invalid JSON, got nil")
	}
}
