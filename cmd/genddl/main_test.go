package genddl

import (
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestGeneratedExamplesMatch runs genddl with each config and verifies the
// output in generated-examples/ matches the checked-in golden files.
func TestGeneratedExamplesMatch(t *testing.T) {
	configs := []struct {
		name       string
		configFile string
	}{
		{"tpcds", "config.json"},
		{"tpch", "tpch_config.json"},
	}

	for _, tc := range configs {
		t.Run(tc.name, func(t *testing.T) {
			configPath := filepath.Join(tc.configFile)
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
		})
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

func TestIntScaleFactor(t *testing.T) {
	tests := []struct {
		input    string
		expected int
	}{
		{"1", 1},
		{"10", 10},
		{"1000", 1000},
		{"1k", 1000},
		{"10k", 10000},
		{"abc", 0},
		{"k", 0},
	}
	for _, tc := range tests {
		s := &Schema{ScaleFactor: tc.input}
		assert.Equal(t, tc.expected, s.intScaleFactor(), "intScaleFactor(%q)", tc.input)
	}
}

func TestLoadSchemas(t *testing.T) {
	data := []byte(`{"scale_factor":"10","file_format":"parquet","compression_method":"uncompressed"}`)
	schemas, err := loadSchemas(data)
	require.NoError(t, err)
	require.Len(t, schemas, 4, "should produce 4 schema variants")

	// Verify the 4 combinations.
	assert.True(t, schemas[0].Iceberg && !schemas[0].Partitioned)
	assert.True(t, schemas[1].Iceberg && schemas[1].Partitioned)
	assert.True(t, !schemas[2].Iceberg && !schemas[2].Partitioned)
	assert.True(t, !schemas[3].Iceberg && schemas[3].Partitioned)

	// Each schema should have independent maps.
	schemas[0].Tables["foo"] = &Table{Name: "foo"}
	assert.Empty(t, schemas[1].Tables, "schema variants should not share Tables map")
}

func TestLoadSchemasDefaults(t *testing.T) {
	data := []byte(`{"scale_factor":"1","file_format":"orc","compression_method":"zstd"}`)
	schemas, err := loadSchemas(data)
	require.NoError(t, err)

	// Verify defaults are applied.
	assert.Equal(t, "tpcds", schemas[0].Workload)
	assert.Equal(t, "tpc-ds", schemas[0].WorkloadDefinition)

	// Verify zstd compression session vars.
	assert.Equal(t, "ZSTD", schemas[0].SessionVariables["iceberg.compression_codec"])
	assert.Equal(t, "ZSTD", schemas[3].SessionVariables["hive.compression_codec"])
}

func TestGetNamedOutput(t *testing.T) {
	tests := []struct {
		name     string
		data     string
		workload string
		expected string
	}{
		{
			"uncompressed",
			`{"scale_factor":"10","file_format":"parquet","compression_method":"uncompressed"}`,
			"tpcds",
			"tpcds-sf10-parquet",
		},
		{
			"zstd",
			`{"scale_factor":"100","file_format":"orc","compression_method":"zstd"}`,
			"tpch",
			"tpch-sf100-orc-zstd",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			result, err := getNamedOutput([]byte(tc.data), tc.workload)
			require.NoError(t, err)
			assert.Equal(t, tc.expected, result)
		})
	}
}

func TestCleanOutputDir(t *testing.T) {
	dir := t.TempDir()

	// Create some files.
	require.NoError(t, os.WriteFile(filepath.Join(dir, "a.sql"), []byte("test"), 0644))
	require.NoError(t, os.WriteFile(filepath.Join(dir, "b.sh"), []byte("test"), 0644))

	// Create a subdirectory (should not be deleted).
	require.NoError(t, os.MkdirAll(filepath.Join(dir, "subdir"), 0755))

	err := cleanOutputDir(dir)
	require.NoError(t, err)

	entries, err := os.ReadDir(dir)
	require.NoError(t, err)
	assert.Len(t, entries, 1, "only subdirectory should remain")
	assert.Equal(t, "subdir", entries[0].Name())
}

func TestCleanOutputDirNonExistent(t *testing.T) {
	err := cleanOutputDir("/nonexistent/path")
	assert.NoError(t, err, "should return nil for non-existent directory")
}

func TestIsPartitioned(t *testing.T) {
	// Table with explicit PartitionedMinScale.
	tbl := &Table{PartitionedMinScale: 100}
	assert.False(t, tbl.isPartitioned(10))
	assert.True(t, tbl.isPartitioned(100))
	assert.True(t, tbl.isPartitioned(1000))

	// Table without PartitionedMinScale uses Partitioned field.
	tbl2 := &Table{Partitioned: true}
	assert.True(t, tbl2.isPartitioned(1))

	tbl3 := &Table{Partitioned: false}
	assert.False(t, tbl3.isPartitioned(1))
}

func TestInitIsVarchar(t *testing.T) {
	varcharType := "VARCHAR(100)"
	intType := "INT"

	tbl := &Table{
		Columns: []*Column{
			{Name: "a", Type: &varcharType},
			{Name: "b", Type: &intType},
			{Name: "c", Type: nil},
		},
	}
	tbl.initIsVarchar()

	assert.True(t, tbl.Columns[0].IsVarchar)
	assert.False(t, tbl.Columns[1].IsVarchar)
	assert.False(t, tbl.Columns[2].IsVarchar)
}

func TestSetNames(t *testing.T) {
	s := &Schema{
		ScaleFactor:       "100",
		FileFormat:        "parquet",
		CompressionMethod: "zstd",
		Workload:          "tpcds",
		Iceberg:           true,
		Partitioned:       true,
	}
	s.setNames()

	assert.Equal(t, "tpcds_sf100_parquet_partitioned_iceberg_zstd", s.SchemaName)
	assert.Equal(t, "tpcds-sf100-parquet-partitioned-iceberg-zstd", s.LocationName)
	assert.Equal(t, "tpcds_sf100_parquet_partitioned_iceberg", s.UncompressedName)
}
