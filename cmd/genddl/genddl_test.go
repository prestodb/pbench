package genddl

import (
	"encoding/json"
	"github.com/stretchr/testify/assert"
	"io/fs"
	"os"
	"path/filepath"
	"pbench/log"
	"pbench/utils"
	"strings"
	"testing"
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
	SessionVariables    map[string]string `json:"session_variables"`
}

type Column struct {
	Name         string  `json:"name"`
	Type         *string `json:"type"`
	PartitionKey *bool   `json:"partition_key"`
	BucketKey    *bool   `json:"bucket_key"`
}

type Table struct {
	Name        string    `json:"name"`
	Partitioned bool      `json:"partitioned"`
	Columns     []*Column `json:"columns"`
	LastColumn  *Column
}

type RegisterTable struct {
	TableName        string
	ExternalLocation *string
}

func TestShowcase(t *testing.T) {
	content, err := os.ReadFile("./config.json")
	if err != nil {
		log.Err(err).Send()
	}

	// Now let's unmarshall the data into `payload`
	var schema Schema
	err = schema.unmarshalJson(content)
	if err != nil {
		log.Err(err).Send()
		return
	}

	externalLoc := schema.getNonPartLocationName()
	currDir := "./definition/tpc-ds"

	_ = filepath.Walk(currDir, func(path string, info fs.FileInfo, err error) error {
		if err != nil {
			log.Error().Err(err).Send()
			return err
		}
		if info.IsDir() {
			return nil
		}

		if !info.IsDir() && strings.HasSuffix(info.Name(), ".json") {
			if f, err := os.ReadFile(currDir + "/" + info.Name()); err != nil {
				if !assert.Nil(t, err) {
					t.FailNow()
				}
			} else {
				tbl := new(Table)
				assert.Nil(t, json.Unmarshal(f, tbl))

				if isRegisterTable(tbl, &schema) {
					var registerTable RegisterTable
					registerTable.TableName = tbl.Name
					registerTable.ExternalLocation = &externalLoc
					schema.RegisterTables = append(schema.RegisterTables, &registerTable)
				} else {
					tbl.reorderColumns(&schema) // Move PartitionKey columns to the bottom
					tbl.LastColumn = tbl.Columns[len(tbl.Columns)-1]
					schema.Tables[tbl.Name] = tbl
				}
			}

			outputDir := filepath.Join(".", schema.LocationName)
			mkErr := os.MkdirAll(outputDir, os.ModePerm)
			assert.Nil(t, mkErr)

			templateBytes, readErr := os.ReadFile("create_table.sql")
			assert.Nil(t, readErr)
			tmpl, err := template.New("a name").Parse(string(templateBytes))
			assert.Nil(t, err)
			f, err := os.OpenFile(outputDir+"/create-schema-table.sql", utils.OpenNewFileFlags, 0644)
			err = tmpl.Execute(f, schema)

			templateBytes2, readErr2 := os.ReadFile("insert_table.sql")
			assert.Nil(t, readErr2)
			tmpl2, err2 := template.New("a name2").Parse(string(templateBytes2))
			assert.Nil(t, err2)
			f2, err2 := os.OpenFile(outputDir+"/insert-table.sql", utils.OpenNewFileFlags, 0644)

			err2 = tmpl2.Execute(f2, schema)

		}

		return nil
	})

}

func isRegisterTable(table *Table, schema *Schema) bool {
	if schema.Iceberg && schema.Partitioned {
		return !table.Partitioned
	}
	return false
}

func (s *Schema) unmarshalJson(data []byte) error {
	type Alias Schema
	aux := &struct {
		*Alias
	}{
		Alias: (*Alias)(s),
	}

	if err := json.Unmarshal(data, &aux); err != nil {
		return err
	}

	if s.SchemaName == "" || s.LocationName == "" {
		s.setNames()
	}
	if s.Tables == nil {
		s.Tables = make(map[string]*Table)
	}
	if s.SessionVariables == nil {
		s.SessionVariables = make(map[string]string)
	}
	s.setSessionVars()

	return nil
}

func (t *Table) reorderColumns(s *Schema) {
	if len(t.Columns) == 0 {
		return
	}

	var partitionIndex = -1

	// Find the index of the first Column with PartitionKey=true
	for i, col := range t.Columns {
		if col.PartitionKey != nil && *col.PartitionKey && s.Partitioned {
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
