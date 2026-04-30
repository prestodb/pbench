package prestoapi

import (
	"encoding/json"
	"fmt"
	"strings"
)

// PlanNode represents a node in the query execution plan
type PlanNode struct {
	ID            string                   `json:"id"`
	Name          string                   `json:"name"`
	Identifier    string                   `json:"identifier"`
	Details       string                   `json:"details"`
	Children      []PlanNode               `json:"children"`
	RemoteSources []interface{}            `json:"remoteSources"`
	Estimates     []map[string]interface{} `json:"estimates"`
}

// StagePlanWrapper wraps the plan for a stage
type StagePlanWrapper struct {
	Plan PlanNode `json:"plan"`
}

// FormatQueryPlanAsText converts a JSON query plan to human-readable text format
// The input should be the AssembledQueryPlanJson which is a map of stage IDs to plan wrappers
func FormatQueryPlanAsText(jsonPlan string) (string, error) {
	if jsonPlan == "" {
		return "", nil
	}

	// Parse the JSON plan - it's a map of stage IDs to plan wrappers
	var stages map[string]StagePlanWrapper
	if err := json.Unmarshal([]byte(jsonPlan), &stages); err != nil {
		return "", fmt.Errorf("failed to parse query plan JSON: %w", err)
	}

	var result strings.Builder

	// Process each stage (fragment) in order
	// Note: stages are typically numbered 0, 1, 2, etc.
	for stageID, stageWrapper := range stages {
		result.WriteString(fmt.Sprintf("Fragment %s\n", stageID))
		result.WriteString(formatPlanNode(&stageWrapper.Plan, 0))
		result.WriteString("\n")
	}

	return result.String(), nil
}

// formatPlanNode recursively formats a plan node and its children
func formatPlanNode(node *PlanNode, depth int) string {
	if node == nil {
		return ""
	}

	var result strings.Builder
	indent := strings.Repeat("    ", depth)

	// Format the node header with identifier if present
	if node.Identifier != "" {
		result.WriteString(fmt.Sprintf("%s- %s[PlanNodeId %s]%s\n",
			indent, node.Name, node.ID, node.Identifier))
	} else {
		result.WriteString(fmt.Sprintf("%s- %s[PlanNodeId %s]\n",
			indent, node.Name, node.ID))
	}

	// Add details if present (already formatted with proper indentation in the JSON)
	if node.Details != "" {
		detailLines := strings.Split(strings.TrimSpace(node.Details), "\n")
		for _, line := range detailLines {
			if line != "" {
				result.WriteString(fmt.Sprintf("%s        %s\n", indent, line))
			}
		}
	}

	// Recursively format children with increased indentation
	for i := range node.Children {
		result.WriteString(formatPlanNode(&node.Children[i], depth+1))
	}

	return result.String()
}
