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
	Name              string
	Iceberg           bool              `json:"iceberg"`
	CompressionMethod string            `json:"compressionMethod"`
	Tables            map[string]*Table `json:"tables"`
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
	schema := &Schema{
		Name:              "tpcds-sf1000-parquet-iceberg-part",
		Iceberg:           true,
		CompressionMethod: "zstd",
		Tables:            make(map[string]*Table),
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
