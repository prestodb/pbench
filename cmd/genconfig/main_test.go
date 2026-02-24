package genconfig

import (
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGeneratedConfigsMatch runs genconfig with the real clusters directory
// and verifies the output matches the checked-in golden files.
func TestGeneratedConfigsMatch(t *testing.T) {
	clustersDir, err := filepath.Abs("../../clusters")
	require.NoError(t, err)

	// Snapshot all non-genconfig.json files in cluster subdirectories.
	golden := snapshotClusters(t, clustersDir)
	require.NotEmpty(t, golden, "no golden files found in clusters/")

	// Configure and run genconfig.
	TemplatePath = filepath.Join(clustersDir, "templates")
	ParameterPaths = []string{filepath.Join(clustersDir, "params.json")}
	Run(nil, []string{clustersDir})

	// Compare regenerated files against golden snapshots.
	for relPath, expected := range golden {
		actual, readErr := os.ReadFile(filepath.Join(clustersDir, relPath))
		require.NoError(t, readErr, "failed to read regenerated file %s", relPath)
		assert.Equal(t, string(expected), string(actual),
			"generated output differs from checked-in golden file: %s", relPath)
	}

	// Check no unexpected extra files were produced.
	regenerated := snapshotClusters(t, clustersDir)
	for relPath := range regenerated {
		assert.Contains(t, golden, relPath,
			"regeneration produced unexpected file: %s", relPath)
	}
}

// snapshotClusters reads all generated files (non-genconfig.json) in cluster
// subdirectories and returns a map of relative path -> contents.
func snapshotClusters(t *testing.T, clustersDir string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)

	entries, err := os.ReadDir(clustersDir)
	require.NoError(t, err)

	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		// Skip non-cluster directories (templates, shared, catalog).
		subDir := filepath.Join(clustersDir, entry.Name())
		if !hasGenconfigJson(subDir) {
			continue
		}

		err := filepath.Walk(subDir, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if info.IsDir() || info.Name() == genconfigJson {
				return nil
			}
			rel, relErr := filepath.Rel(clustersDir, path)
			if relErr != nil {
				return relErr
			}
			data, readErr := os.ReadFile(path)
			if readErr != nil {
				return readErr
			}
			files[rel] = data
			return nil
		})
		require.NoError(t, err)
	}
	return files
}

func hasGenconfigJson(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, genconfigJson))
	return err == nil
}

func TestPrintDefaultParams(t *testing.T) {
	// Capture stdout by redirecting it to a pipe.
	oldStdout := os.Stdout
	r, w, err := os.Pipe()
	require.NoError(t, err)
	os.Stdout = w

	PrintDefaultParams(nil, nil)

	w.Close()
	os.Stdout = oldStdout
	out, err := io.ReadAll(r)
	require.NoError(t, err)

	assert.Contains(t, string(out), "sys_reserved_mem_cap_gb")
}

func TestRunWithBuiltinTemplates(t *testing.T) {
	tmpDir := t.TempDir()
	clusterDir := filepath.Join(tmpDir, "test-cluster")
	require.NoError(t, os.MkdirAll(clusterDir, 0755))

	// Write a minimal genconfig.json.
	configJSON := `{
		"cluster_size": "test",
		"worker_instance_type": "r6i.xlarge",
		"number_of_workers": 2,
		"memory_per_node_gb": 30,
		"vcpu_per_worker": 4,
		"fragment_result_cache_enabled": false,
		"data_cache_enabled": false
	}`
	require.NoError(t, os.WriteFile(filepath.Join(clusterDir, genconfigJson), []byte(configJSON), 0644))

	// Run with builtin templates (no -t flag).
	TemplatePath = ""
	ParameterPaths = nil
	Run(nil, []string{tmpDir})

	// Verify some output was generated.
	entries, err := os.ReadDir(clusterDir)
	require.NoError(t, err)
	// Should have genconfig.json + generated files.
	assert.Greater(t, len(entries), 1, "expected generated files in output directory")
}

func TestStaleFileCleanup(t *testing.T) {
	tmpDir := t.TempDir()
	clusterDir := filepath.Join(tmpDir, "test-cluster")
	require.NoError(t, os.MkdirAll(filepath.Join(clusterDir, "workers"), 0755))

	// Write a minimal genconfig.json.
	configJSON := `{
		"cluster_size": "test",
		"worker_instance_type": "r6i.xlarge",
		"number_of_workers": 2,
		"memory_per_node_gb": 30,
		"vcpu_per_worker": 4,
		"fragment_result_cache_enabled": false,
		"data_cache_enabled": false
	}`
	require.NoError(t, os.WriteFile(filepath.Join(clusterDir, genconfigJson), []byte(configJSON), 0644))

	// Plant a stale file that has no corresponding template.
	staleFile := filepath.Join(clusterDir, "workers", "obsolete.properties")
	require.NoError(t, os.WriteFile(staleFile, []byte("stale"), 0644))

	// Run genconfig.
	TemplatePath = ""
	ParameterPaths = nil
	Run(nil, []string{tmpDir})

	// The stale file should have been removed.
	_, err := os.Stat(staleFile)
	assert.True(t, os.IsNotExist(err), "stale file should have been removed: %s", staleFile)

	// genconfig.json should still exist.
	_, err = os.Stat(filepath.Join(clusterDir, genconfigJson))
	assert.NoError(t, err, "genconfig.json should not be removed")
}

func TestParameterStacking(t *testing.T) {
	tmpDir := t.TempDir()

	// Create base params file.
	baseParams := map[string]any{
		"sys_reserved_mem_percent":                     0.05,
		"sys_reserved_mem_cap_gb":                      float64(2),
		"heap_size_percent_of_container_mem":           0.9,
		"headroom_percent_of_heap":                     0.2,
		"query_max_total_mem_per_node_percent_of_heap": 0.8,
		"query_max_mem_per_node_percent_of_total":      0.9,
		"proxygen_mem_per_worker_gb":                   0.125,
		"proxygen_mem_cap_gb":                          float64(2),
		"native_buffer_mem_percent":                    0.05,
		"native_buffer_mem_cap_gb":                     float64(32),
		"native_query_mem_percent_of_sys_mem":          0.95,
		"join_max_bcast_size_percent_of_container_mem": 0.01,
		"memory_push_back_start_below_limit_gb":        float64(5),
		"trino":                                        "${PROVISION_TRINO}",
	}
	baseBytes, _ := json.Marshal(baseParams)
	baseFile := filepath.Join(tmpDir, "base.json")
	require.NoError(t, os.WriteFile(baseFile, baseBytes, 0644))

	// Create override params file that changes sys_reserved_mem_cap_gb.
	overrideParams := map[string]any{
		"sys_reserved_mem_cap_gb": float64(4),
	}
	overrideBytes, _ := json.Marshal(overrideParams)
	overrideFile := filepath.Join(tmpDir, "override.json")
	require.NoError(t, os.WriteFile(overrideFile, overrideBytes, 0644))

	// Create a cluster config.
	clusterDir := filepath.Join(tmpDir, "test-cluster")
	require.NoError(t, os.MkdirAll(clusterDir, 0755))
	configJSON := `{
		"cluster_size": "test",
		"worker_instance_type": "r6i.xlarge",
		"number_of_workers": 2,
		"memory_per_node_gb": 30,
		"vcpu_per_worker": 4
	}`
	require.NoError(t, os.WriteFile(filepath.Join(clusterDir, genconfigJson), []byte(configJSON), 0644))

	// Run with stacked params (later overrides earlier).
	TemplatePath = ""
	ParameterPaths = []string{baseFile, overrideFile}
	Run(nil, []string{tmpDir})

	// Verify output was generated.
	entries, err := os.ReadDir(clusterDir)
	require.NoError(t, err)
	assert.Greater(t, len(entries), 1, "expected generated files in output directory")
}
