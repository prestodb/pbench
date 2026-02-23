package genddl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGeneratedExamplesMatch runs genddl with config.json and verifies the
// output in generated-examples/ matches the checked-in golden files.
func TestGeneratedExamplesMatch(t *testing.T) {
	configPath := filepath.Join("config.json")
	absConfig, err := filepath.Abs(configPath)
	require.NoError(t, err)

	configDir := filepath.Dir(absConfig)
	examplesDir := filepath.Join(configDir, "generated-examples")

	// Snapshot all golden files before regeneration.
	golden := snapshotDir(t, examplesDir)
	require.NotEmpty(t, golden, "no golden files found in generated-examples/")

	// Run genddl (overwrites generated-examples/ in place).
	Run(nil, []string{configPath})

	// Compare every regenerated file against the golden snapshot.
	for relPath, expected := range golden {
		actual, readErr := os.ReadFile(filepath.Join(examplesDir, relPath))
		require.NoError(t, readErr, "failed to read regenerated file %s", relPath)
		assert.Equal(t, string(expected), string(actual),
			"generated output differs from checked-in golden file: %s", relPath)
	}

	// Also check that no extra files were produced.
	regenerated := snapshotDir(t, examplesDir)
	for relPath := range regenerated {
		assert.Contains(t, golden, relPath,
			"regeneration produced unexpected file: %s", relPath)
	}
}

// snapshotDir reads all files under dir (recursively) and returns a map of
// relative path → file contents.
func snapshotDir(t *testing.T, dir string) map[string][]byte {
	t.Helper()
	files := make(map[string][]byte)
	err := filepath.Walk(dir, func(path string, info os.FileInfo, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if info.IsDir() {
			return nil
		}
		rel, relErr := filepath.Rel(dir, path)
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
	return files
}
