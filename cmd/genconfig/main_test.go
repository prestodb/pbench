package genconfig

import (
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

	// Snapshot all non-config.json files in cluster subdirectories.
	golden := snapshotClusters(t, clustersDir)
	require.NotEmpty(t, golden, "no golden files found in clusters/")

	// Configure and run genconfig.
	TemplatePath = filepath.Join(clustersDir, "templates")
	ParameterPath = filepath.Join(clustersDir, "params.json")
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

func TestCalculate(t *testing.T) {
	cfg := &ClusterConfig{
		Name:                "test",
		MemoryPerNodeGb:     62,
		NumberOfWorkers:     4,
		VCPUPerWorker:       8,
		GeneratorParameters: DefaultGeneratorParameters,
	}
	cfg.Calculate()

	assert.True(t, cfg.ContainerMemoryGb > 0, "ContainerMemoryGb should be positive")
	assert.True(t, cfg.HeapSizeGb > 0, "HeapSizeGb should be positive")
	assert.True(t, cfg.HeapSizeGb < cfg.ContainerMemoryGb, "HeapSizeGb should be less than ContainerMemoryGb")
	assert.True(t, cfg.JavaQueryMaxTotalMemPerNodeGb > 0)
	assert.True(t, cfg.JavaQueryMaxMemPerNodeGb > 0)
	assert.True(t, cfg.NativeSystemMemGb > 0)
	assert.True(t, cfg.NativeQueryMemGb > 0)
	assert.True(t, cfg.JoinMaxBroadcastTableSizeMb > 0)
}

// snapshotClusters reads all generated files (non-config.json) in cluster
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
		if !hasConfigJson(subDir) {
			continue
		}

		err := filepath.Walk(subDir, func(path string, info os.FileInfo, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if info.IsDir() || info.Name() == "config.json" {
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

func hasConfigJson(dir string) bool {
	_, err := os.Stat(filepath.Join(dir, "config.json"))
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

	// Write a minimal config.json.
	configJSON := `{
		"cluster_size": "test",
		"worker_instance_type": "r6i.xlarge",
		"number_of_workers": 2,
		"memory_per_node_gb": 30,
		"vcpu_per_worker": 4,
		"fragment_result_cache_enabled": false,
		"data_cache_enabled": false
	}`
	require.NoError(t, os.WriteFile(filepath.Join(clusterDir, "config.json"), []byte(configJSON), 0644))

	// Run with builtin templates (no -t flag).
	TemplatePath = ""
	ParameterPath = ""
	Run(nil, []string{tmpDir})

	// Verify some output was generated.
	entries, err := os.ReadDir(clusterDir)
	require.NoError(t, err)
	// Should have config.json + generated files.
	assert.Greater(t, len(entries), 1, "expected generated files in output directory")
}
