package save

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"pbench/presto"
	"pbench/utils"
)

type TableSummary struct {
	Name        string               `json:"name"`
	Catalog     string               `json:"catalog"`
	Schema      string               `json:"schema"`
	Ddl         string               `json:"ddl"`
	ColumnStats []presto.ColumnStats `json:"columnStats"`
	RowCount    int                  `json:"rowCount"`
}

func (s *TableSummary) QueryTableSummary(ctx context.Context, client *presto.Client) error {
	fullyQualifiedTableName := fmt.Sprintf("%s.%s.%s", s.Catalog, s.Schema, s.Name)
	if err := presto.QueryAndUnmarshal(ctx, client, "SHOW CREATE TABLE "+fullyQualifiedTableName, &s.Ddl); err != nil {
		return err
	}
	if err := presto.QueryAndUnmarshal(ctx, client, "SHOW STATS FOR "+fullyQualifiedTableName, &s.ColumnStats); err != nil {
		return err
	}
	if err := presto.QueryAndUnmarshal(ctx, client, "DESCRIBE "+fullyQualifiedTableName, &s.ColumnStats); err != nil {
		return err
	}
	for i := len(s.ColumnStats) - 1; i >= 0; i-- {
		if stats := s.ColumnStats[i]; stats.ColumnName == "" && stats.RowCount != nil {
			s.RowCount = int(*stats.RowCount)
			return nil
		}
	}
	return nil
}

func (s *TableSummary) SaveToFile(path string) error {
	jsonFile, fErr := os.OpenFile(path, utils.OpenNewFileFlags, 0644)
	if fErr != nil {
		return fErr
	}
	defer jsonFile.Close()
	if b, mErr := json.MarshalIndent(s, "", "  "); mErr != nil {
		return mErr
	} else if _, wErr := jsonFile.Write(b); wErr != nil {
		return wErr
	}
	return nil
}
