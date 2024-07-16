package genddl

import (
	"encoding/json"
	"fmt"
	"github.com/spf13/cobra"
	"io/fs"
	"os"
	"path/filepath"
	"pbench/log"
	"pbench/utils"
	"strconv"
	"strings"
	"text/template"
)

type Schema struct {
	ScaleFactor         string            `json:"scale_factor"`
	FileFormat          string            `json:"file_format"`
	Iceberg             bool              `json:"iceberg"`
	CompressionMethod   string            `json:"compression_method"`
	Partitioned         bool              `json:"partitioned"`
	SchemaName          string            `json:"schema_name"`
	LocationName        string            `json:"location_name"`
	UncompressedName    string            `json:"uncompressed_name"`
	IcebergLocationName string            `json:"iceberg_location_name"`
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
	pathArg := args[0]
	absPath, absErr := filepath.Abs(pathArg)
	if absErr != nil {
		log.Fatal().Err(absErr).Msg("Error getting absolute path")
	}

	content, err := os.ReadFile(absPath)
	if err != nil {
		log.Err(err).Send()
	}

	schemas, loadErr := loadSchemas(content)
	if loadErr != nil {
		log.Err(err).Send()
		return
	}

	wd, wdErr := os.Getwd()
	if wdErr != nil {
		log.Fatal().Err(err).Msg("Failed to get working directory")
	}
	genDdlDir := wd + "/cmd/genddl"
	defDir := genDdlDir + "/definition/tpc-ds"

	outputDir := filepath.Join(genDdlDir, "out")
	cleanErr := cleanOutputDir(outputDir)
	if cleanErr != nil {
		log.Warn().Err(cleanErr).Msg("Error cleaning output dir")
	}
	// Generate the output directory
	mkErr := os.MkdirAll(outputDir, os.ModePerm)
	if mkErr != nil {
		log.Fatal().Err(mkErr).Str("path", outputDir).Msg("Failed to make output directory")
	}

	step := 0
	for _, schema := range schemas {
		externalLoc := schema.getNonPartLocationName()

		generateSchemaFromDef(schema, defDir, genDdlDir, outputDir, &externalLoc, &step)
	}
}

func generateSchemaFromDef(schema *Schema, defDir string, genDdlDir string, outputDir string, externalLoc *string, step *int) {
	_ = filepath.Walk(defDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Fatal().Err(err).Send()
			return err
		}

		if info.IsDir() || !strings.HasSuffix(info.Name(), ".json") {
			return nil
		}

		if f, readErr := os.ReadFile(defDir + "/" + info.Name()); err != nil {
			if readErr != nil {
				log.Fatal().Err(readErr).Str("file", info.Name()).Msg("Failed to read file")
			}
		} else {
			tbl := new(Table)
			umErr := json.Unmarshal(f, tbl)
			if umErr != nil {
				log.Fatal().Err(umErr).Str("file", info.Name()).Msg("Failed to unmarshal file to type Table")
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

		return nil
	})

	generateCreateTable(schema, outputDir, genDdlDir, *step)
	*step++

	if schema.shouldGenInsert() {
		generateInsertTable(schema, outputDir, genDdlDir, *step)
		*step++
	}
}

func generateCreateTable(schema *Schema, outputDir string, currDir string, step int) {
	templateBytes, readErr := os.ReadFile(filepath.Join(currDir, "create_table.sql"))
	if readErr != nil {
		log.Fatal().Err(readErr).Str("file", "create_table.sql").Msg("Failed to read file")
	}

	tmpl, parseErr := template.New("a name").Parse(string(templateBytes))
	if parseErr != nil {
		log.Fatal().Err(parseErr).Msg("Failed to parse text template create_table.sql")
	}

	fName := strconv.Itoa(step+1) + "-create-" + schema.LocationName + ".sql"
	f, openErr := os.OpenFile(filepath.Join(outputDir, fName), utils.OpenNewFileFlags, 0644)
	if openErr != nil {
		log.Fatal().Err(openErr).Msg("Failed to open output file create-schema-table.sql")
	}

	exErr := tmpl.Execute(f, schema)
	if exErr != nil {
		log.Fatal().Err(exErr).Msg("Failed to execute template")
	}
}

func generateInsertTable(schema *Schema, outputDir string, currDir string, step int) {
	templateBytes, readErr := os.ReadFile(filepath.Join(currDir, "insert_table.sql"))
	if readErr != nil {
		log.Fatal().Err(readErr).Msg("Failed to read file insert_table.sql")
	}

	tmpl, parseErr := template.New("a name2").Parse(string(templateBytes))
	if parseErr != nil {
		log.Fatal().Err(parseErr).Msg("Failed to parse text template insert_table.sql")
	}

	fName := strconv.Itoa(step+1) + "-insert-" + schema.LocationName + ".sql"
	f, openErr := os.OpenFile(filepath.Join(outputDir, fName), utils.OpenNewFileFlags, 0644)
	if openErr != nil {
		log.Fatal().Err(openErr).Msg("Failed to open output file insert-table.sql")
	}

	exErr := tmpl.Execute(f, *schema)
	if exErr != nil {
		log.Fatal().Err(exErr).Msg("Failed to execute template")
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
	if !s.Iceberg {
		return false
	}
	return true
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
		s := base // Copy base
		s.Iceberg = c.Iceberg
		s.Partitioned = c.Partitioned

		if s.SchemaName == "" || s.LocationName == "" {
			s.setNames()
		}
		if s.Tables == nil {
			s.Tables = make(map[string]*Table)
		}
		if s.InsertTables == nil {
			s.InsertTables = make(map[string]*Table)
		}
		if s.SessionVariables == nil {
			s.SessionVariables = make(map[string]string)
		}
		s.setSessionVars()

		schemas = append(schemas, &s)
	}

	return schemas, nil
}

func (t *Table) initIsVarchar() {
	for _, c := range t.Columns {
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
	s.UncompressedName = "tpcds_sf" + s.ScaleFactor + "_" + s.FileFormat + partitioned + iceberg
	s.SchemaName = s.UncompressedName + compression
	s.LocationName = "tpcds-sf" + s.ScaleFactor + "-" + s.FileFormat + toHyphen(partitioned) + toHyphen(iceberg) + toHyphen(compression)
	s.IcebergLocationName = "tpcds-sf" + s.ScaleFactor + "-" + s.FileFormat + "-iceberg"
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
