package genddl

import (
	"encoding/json"
	"io/fs"
	"os"
	"path/filepath"
	"pbench/log"
	"pbench/utils"
	"strings"
	"testing"
	"text/template"

	"github.com/stretchr/testify/assert"
)

type Schema struct {
	ScaleFactor       string            `json:"scale_factor"`
	FileFormat        string            `json:"file_format"`
	Iceberg           bool              `json:"iceberg"`
	CompressionMethod string            `json:"compression_method"`
	Partitioned       bool              `json:"partitioned"`
	Name              string            `json:"name"`
	Tables            map[string]*Table `json:"tables"`
	SessionVariables  map[string]string `json:"session_variables"`
}

type Column struct {
	Name         string  `json:"name"`
	Type         *string `json:"type"`
	PartitionKey *bool   `json:"partitionKey"`
	BucketKey    *bool   `json:"bucketKey"`
}

type Table struct {
	Name    string    `json:"name"`
	Columns []*Column `json:"columns"`
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

	currDir := "./tpc-ds"

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
				schema.Tables[tbl.Name] = tbl
			}

			templateBytes, readErr := os.ReadFile("create_table.sql")
			assert.Nil(t, readErr)
			tmpl, err := template.New("a name").Parse(string(templateBytes))
			assert.Nil(t, err)
			f, err := os.OpenFile(schema.Name+".sql", utils.OpenNewFileFlags, 0644)

			err = tmpl.Execute(f, schema)

		}

		return nil
	})

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

	if s.Name == "" {
		s.generateName()
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

func (s *Schema) setSessionVars() {
	s.SessionVariables["query_max_execution_time"] = "12h"
	s.SessionVariables["query_max_run_time"] = "12h"
	if s.Iceberg {
		s.SessionVariables["iceberg.compression_codec"] = "NONE"
	}
}

func (s *Schema) generateName() {
	var icebergName string
	if s.Iceberg {
		icebergName = "iceberg"
	} else {
		icebergName = "hive"
	}
	var partitionedSuffix string
	if s.Partitioned {
		partitionedSuffix = "-part"
	} else {
		partitionedSuffix = ""
	}
	s.Name = "tpcds-sf" + s.ScaleFactor + "-" + s.FileFormat + "-" + icebergName + partitionedSuffix
}
