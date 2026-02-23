package genddl

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"text/template"

	"github.com/spf13/cobra"
	"pbench/log"
	"pbench/utils"
)

type Schema struct {
	ScaleFactor         string            `json:"scale_factor"`
	FileFormat          string            `json:"file_format"`
	Iceberg             bool              `json:"iceberg"`
	CompressionMethod   string            `json:"compression_method"`
	Partitioned         bool              `json:"partitioned"`
	Workload            string            `json:"workload"`
	WorkloadDefinition  string            `json:"workload_definition"`
	SchemaName          string            `json:"schema_name"`
	LocationName        string            `json:"location_name"`
	UncompressedName    string            `json:"uncompressed_name"`
	IcebergLocationName string            `json:"iceberg_location_name"`
	PartIcebergName     string            `json:"part_iceberg_name"`
	RegisterTables      []*RegisterTable  `json:"register_tables"`
	Tables              map[string]*Table `json:"tables"`
	InsertTables        map[string]*Table `json:"insert_tables"`
	SessionVariables    map[string]string `json:"session_variables"`
}

type Column struct {
	Name         string  `json:"name"`
	Type         *string `json:"type"`
	PartitionKey *bool   `json:"partition_key"`
	BucketKey    *bool   `json:"bucket_key"`
	IsVarchar    bool
}

type Table struct {
	Name                string    `json:"name"`
	Partitioned         bool      `json:"partitioned"`
	PartitionedMinScale int       `json:"partitioned_min_scale"`
	Columns             []*Column `json:"columns"`
	LastColumn          *Column
}

type RegisterTable struct {
	TableName        string
	ExternalLocation *string
}

func Run(_ *cobra.Command, args []string) {
	// Reset template cache for each invocation.
	templateCache = make(map[string]*template.Template)

	pathArg := args[0]
	absPath, absErr := filepath.Abs(pathArg)
	if absErr != nil {
		log.Fatal().Err(absErr).Msg("Error getting absolute path")
	}

	content, err := os.ReadFile(absPath)
	if err != nil {
		log.Fatal().Err(err).Str("path", absPath).Msg("Failed to read config file")
	}

	schemas, loadErr := loadSchemas(content)
	if loadErr != nil {
		log.Err(loadErr).Send()
		return
	}

	// Resolve all paths relative to the config file's directory.
	configDir := filepath.Dir(absPath)
	defDir := filepath.Join(configDir, "definition", schemas[0].WorkloadDefinition)

	// Get the specific named output directory (used in version control)
	namedOutput, namedErr := getNamedOutput(content, schemas[0].Workload)
	if namedErr != nil {
		log.Fatal().Err(namedErr).Msg("Failed to get named output dir")
	}
	namedOutputDir := filepath.Join(configDir, "generated-examples", namedOutput)

	outputDir := filepath.Join(configDir, "out")
	for _, dir := range []string{outputDir, namedOutputDir} {
		if cleanErr := cleanOutputDir(dir); cleanErr != nil {
			log.Warn().Err(cleanErr).Str("dir", dir).Msg("Error cleaning output dir")
		}
		if mkErr := os.MkdirAll(dir, os.ModePerm); mkErr != nil {
			log.Fatal().Err(mkErr).Str("path", dir).Msg("Failed to make output directory")
		}
	}

	step := 0
	for _, schema := range schemas {
		externalLoc := schema.getNonPartLocationName()

		generateSchemaFromDef(schema, defDir, configDir, []string{outputDir, namedOutputDir}, &externalLoc, &step)
	}
}

func generateSchemaFromDef(schema *Schema, defDir string, configDir string, outputDirs []string, externalLoc *string, step *int) {
	// os.ReadDir returns entries sorted by name, giving deterministic output order.
	entries, readErr := os.ReadDir(defDir)
	if readErr != nil {
		log.Fatal().Err(readErr).Str("dir", defDir).Msg("Failed to read definition directory")
	}

	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}

		filePath := filepath.Join(defDir, entry.Name())
		f, fileErr := os.ReadFile(filePath)
		if fileErr != nil {
			log.Fatal().Err(fileErr).Str("file", entry.Name()).Msg("Failed to read file")
			continue
		}

		tbl := new(Table)
		umErr := json.Unmarshal(f, tbl)
		if umErr != nil {
			log.Fatal().Err(umErr).Str("file", entry.Name()).Msg("Failed to unmarshal file to type Table")
		}
		tbl.initIsVarchar() // Populates the IsVarchar var for all columns
		// Set partitioned if above scale factor min
		if tbl.isPartitioned(schema.intScaleFactor()) {
			tbl.Partitioned = true
		}

		if isRegisterTable(tbl, schema) {
			var registerTable RegisterTable
			registerTable.TableName = tbl.Name
			registerTable.ExternalLocation = externalLoc
			schema.RegisterTables = append(schema.RegisterTables, &registerTable)
		} else {
			tbl.reorderColumns(schema) // Move PartitionKey columns to the bottom
			tbl.LastColumn = tbl.Columns[len(tbl.Columns)-1]
			schema.Tables[tbl.Name] = tbl
		}
		if isInsertTable(tbl, schema) {
			schema.InsertTables[tbl.Name] = tbl
		}
	}

	generateCreateTable(schema, configDir, outputDirs, *step)
	*step++

	if schema.shouldGenInsert() {
		generateInsertTable(schema, configDir, outputDirs, *step)
		*step++
	}
}

func generateCreateTable(schema *Schema, currDir string, outputDirs []string, step int) {
	genSubSteps := !schema.Iceberg && schema.Partitioned

	tName := "create_table.sql.tmpl"
	var fName string
	if genSubSteps {
		// If there are sub-tasks, prefix the first output file with step a
		fName = strconv.Itoa(step+1) + "a-create-" + schema.LocationName + ".sql"
	} else {
		fName = strconv.Itoa(step+1) + "-create-" + schema.LocationName + ".sql"
	}
	execTemplate(schema, getTemplate(tName, currDir), fName, outputDirs)

	if genSubSteps {
		generateAwsS3Mv(schema, currDir, outputDirs, step)     // Generate step b
		generateCallAnalyze(schema, currDir, outputDirs, step) // Generate step c
		generateAwsS3Cp(schema, currDir, outputDirs, step)     // Generate step d
	}
}

func generateInsertTable(schema *Schema, currDir string, outputDirs []string, step int) {
	tName := "insert_table.sql.tmpl"
	fName := strconv.Itoa(step+1) + "-insert-" + schema.LocationName + ".sql"

	execTemplate(schema, getTemplate(tName, currDir), fName, outputDirs)
}

func generateAwsS3Mv(schema *Schema, currDir string, outputDirs []string, step int) {
	tName := "aws_s3_mv.sh.tmpl"
	fName := strconv.Itoa(step+1) + "b-s3-mv-" + schema.LocationName + ".sh"

	execTemplate(schema, getTemplate(tName, currDir), fName, outputDirs)
}

func generateCallAnalyze(schema *Schema, currDir string, outputDirs []string, step int) {
	tName := "call_analyze.sql.tmpl"
	fName := strconv.Itoa(step+1) + "c-call-analyze-" + schema.LocationName + ".sql"

	execTemplate(schema, getTemplate(tName, currDir), fName, outputDirs)
}

func generateAwsS3Cp(schema *Schema, currDir string, outputDirs []string, step int) {
	tName := "aws_s3_cp.sh.tmpl"
	fName := strconv.Itoa(step+1) + "d-s3-cp-" + schema.LocationName + ".sh"

	execTemplate(schema, getTemplate(tName, currDir), fName, outputDirs)
}

// templateCache holds parsed templates keyed by file path, so each template
// file is read and parsed only once across all schema variants.
var templateCache = make(map[string]*template.Template)

func getTemplate(tName string, currDir string) *template.Template {
	tPath := filepath.Join(currDir, tName)
	if tmpl, ok := templateCache[tPath]; ok {
		return tmpl
	}

	templateBytes, readErr := os.ReadFile(tPath)
	if readErr != nil {
		log.Fatal().Err(readErr).Str("file", tName).Msg("Failed to read file")
	}

	tmpl, parseErr := template.New(tName).Parse(string(templateBytes))
	if parseErr != nil {
		log.Fatal().Err(parseErr).Str("file", tName).Msg("Failed to parse text template")
	}

	templateCache[tPath] = tmpl
	return tmpl
}

func execTemplate(schema *Schema, tmpl *template.Template, fName string, outputDirs []string) {
	for _, singleOutDir := range outputDirs {
		f, openErr := os.OpenFile(filepath.Join(singleOutDir, fName), utils.OpenNewFileFlags, 0644)
		if openErr != nil {
			log.Fatal().Err(openErr).Str("file", fName).Msg("Failed to open output file")
		}

		exErr := tmpl.Execute(f, *schema)
		f.Close()
		if exErr != nil {
			log.Fatal().Err(exErr).Msg("Failed to execute template")
		}
	}
}

func cleanOutputDir(dir string) error {
	files, err := os.ReadDir(dir)
	if err != nil {
		return nil
	}

	for _, file := range files {
		if !file.IsDir() {
			filePath := filepath.Join(dir, file.Name())

			err := os.Remove(filePath)
			if err != nil {
				return fmt.Errorf("failed to delete file %s: %v", filePath, err)
			}
		}
	}
	return nil
}

func (s *Schema) shouldGenInsert() bool {
	return s.Iceberg
}

func isRegisterTable(table *Table, schema *Schema) bool {
	if schema.Iceberg && schema.Partitioned {
		return !table.Partitioned
	}
	return false
}

func isInsertTable(table *Table, schema *Schema) bool {
	if schema.Partitioned {
		return table.Partitioned
	}
	return true
}

func getNamedOutput(configData []byte, workload string) (string, error) {
	var config map[string]string
	if err := json.Unmarshal(configData, &config); err != nil {
		return "", err
	}

	scaleFactor := config["scale_factor"]
	fileFormat := config["file_format"]
	compressionMethod := config["compression_method"]
	var compressionSuffix string
	if compressionMethod == "uncompressed" {
		compressionSuffix = ""
	} else {
		compressionSuffix = "-" + compressionMethod
	}

	return workload + "-sf" + scaleFactor + "-" + fileFormat + compressionSuffix, nil
}

func loadSchemas(data []byte) ([]*Schema, error) {
	var schemas []*Schema
	// Load the base schema
	var base Schema
	type Alias Schema
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(&base),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return schemas, err
	}

	// Default workload names for backward compatibility.
	if base.Workload == "" {
		base.Workload = "tpcds"
	}
	if base.WorkloadDefinition == "" {
		base.WorkloadDefinition = "tpc-ds"
	}

	combinations := []struct {
		Iceberg     bool
		Partitioned bool
	}{
		{true, false},
		{true, true},
		{false, false},
		{false, true},
	}

	for _, c := range combinations {
		s := base // Copy base (scalar fields only; re-init slices/maps below)
		s.Iceberg = c.Iceberg
		s.Partitioned = c.Partitioned

		// Always allocate fresh collections so schema variants don't share state.
		s.RegisterTables = nil
		s.Tables = make(map[string]*Table)
		s.InsertTables = make(map[string]*Table)
		s.SessionVariables = make(map[string]string)

		if s.SchemaName == "" || s.LocationName == "" {
			s.setNames()
		}
		s.setSessionVars()

		schemas = append(schemas, &s)
	}

	return schemas, nil
}

func (t *Table) initIsVarchar() {
	for _, c := range t.Columns {
		if c.Type == nil {
			c.IsVarchar = false
			continue
		}
		firstParen := strings.Index(*c.Type, "(")
		if firstParen == -1 {
			c.IsVarchar = false
		} else {
			baseType := (*c.Type)[:firstParen]
			c.IsVarchar = baseType == "VARCHAR"
		}
	}
}

func (t *Table) reorderColumns(s *Schema) {
	if len(t.Columns) == 0 {
		return
	}

	var partitionIndex = -1

	// Find the index of the first Column with PartitionKey=true
	for i, col := range t.Columns {
		if col.PartitionKey != nil && *col.PartitionKey && s.Partitioned && t.Partitioned {
			partitionIndex = i
			break
		}
	}

	if partitionIndex == -1 {
		return
	}

	// Move the partition key column to the end of the slice
	partitionColumn := t.Columns[partitionIndex]
	t.Columns = append(t.Columns[:partitionIndex], t.Columns[partitionIndex+1:]...) // Exclude partitionColumn
	t.Columns = append(t.Columns, partitionColumn)                                  // Add partitionColumn to end
}

func (s *Schema) setSessionVars() {
	s.SessionVariables["query_max_execution_time"] = "12h"
	s.SessionVariables["query_max_run_time"] = "12h"
	if s.Iceberg && s.CompressionMethod == "uncompressed" {
		s.SessionVariables["iceberg.compression_codec"] = "NONE"
	} else if s.Iceberg && s.CompressionMethod == "zstd" {
		s.SessionVariables["iceberg.compression_codec"] = "ZSTD"
	} else if !s.Iceberg && s.CompressionMethod == "uncompressed" {
		s.SessionVariables["hive.compression_codec"] = "NONE"
	} else if !s.Iceberg && s.CompressionMethod == "zstd" {
		s.SessionVariables["hive.compression_codec"] = "ZSTD"
	}
}

func (s *Schema) setNames() {
	var iceberg string
	if s.Iceberg {
		iceberg = "_iceberg"
	} else {
		iceberg = "_hive"
	}
	var partitioned string
	if s.Partitioned {
		partitioned = "_partitioned"
	} else {
		partitioned = ""
	}
	var compression string
	if s.CompressionMethod == "zstd" {
		compression = "_zstd"
	} else {
		compression = ""
	}
	s.UncompressedName = s.Workload + "_sf" + s.ScaleFactor + "_" + s.FileFormat + partitioned + iceberg
	s.SchemaName = s.UncompressedName + compression
	s.LocationName = s.Workload + "-sf" + s.ScaleFactor + "-" + s.FileFormat + toHyphen(partitioned) + toHyphen(iceberg) + toHyphen(compression)
	s.IcebergLocationName = s.Workload + "-sf" + s.ScaleFactor + "-" + s.FileFormat + "-iceberg"
	s.PartIcebergName = s.Workload + "-sf" + s.ScaleFactor + "-" + s.FileFormat + toHyphen(partitioned) + "-iceberg"
}

func (s *Schema) getNonPartLocationName() string {
	return strings.Replace(s.LocationName, "-partitioned", "", 1)
}

func toHyphen(s string) string {
	return strings.Replace(s, "_", "-", 1)
}

func (t *Table) isPartitioned(sf int) bool {
	if t.PartitionedMinScale > 0 {
		return sf >= t.PartitionedMinScale
	}
	return t.Partitioned
}

func (s *Schema) intScaleFactor() int {
	if strings.HasSuffix(s.ScaleFactor, "k") {
		numStr := strings.TrimSuffix(s.ScaleFactor, "k")
		num, err := strconv.Atoi(numStr)
		if err != nil {
			return 0
		}
		// Multiply by 1000
		return num * 1000
	}
	num, err := strconv.Atoi(s.ScaleFactor)
	if err != nil {
		return 0
	}
	return num
}
