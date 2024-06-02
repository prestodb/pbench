package save

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"pbench/log"
	"pbench/presto"
	"pbench/utils"
	"strings"
)

const (
	BooleanType   = "boolean"
	TinyIntType   = "tinyint"
	SmallIntType  = "smallint"
	IntegerType   = "integer"
	BigIntType    = "bigint"
	RealType      = "real"
	DoubleType    = "double"
	DecimalType   = "decimal"
	DateType      = "date"
	TimestampType = "timestamp"
	VarcharType   = "varchar"
	VarbinrayType = "varbinray"
	CharType      = "char"
	ArrayType     = "array"
	MapType       = "map"
	RowType       = "row"
)

var (
	IsNumericType = map[string]bool{
		TinyIntType:  true,
		SmallIntType: true,
		IntegerType:  true,
		BigIntType:   true,
		RealType:     true,
		DoubleType:   true,
		DecimalType:  true,
	}
	IsSizable = map[string]bool{
		CharType:      true,
		VarcharType:   true,
		VarbinrayType: true,
		ArrayType:     true,
		MapType:       true,
		RowType:       true,
	}
)

type TableSummary struct {
	Name        string               `json:"name"`
	Catalog     string               `json:"catalog"`
	Schema      string               `json:"schema"`
	Ddl         string               `json:"ddl"`
	ColumnStats []presto.ColumnStats `json:"columnStats"`
	RowCount    *int                 `json:"rowCount,omitempty"`
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

	// Find the row count from the table summary row (usually the last row)
	for i := len(s.ColumnStats) - 1; i >= 0; i-- {
		if stats := s.ColumnStats[i]; stats.ColumnName == "" && stats.RowCount != nil {
			intRowCount := int(*stats.RowCount)
			s.RowCount = &intRowCount
			break
		}
	}
	// Unlikely but if the row count is still NULL, then do SELECT COUNT(*)
	if s.RowCount == nil {
		if err := presto.QueryAndUnmarshal(ctx, client, "SELECT COUNT(*) FROM "+fullyQualifiedTableName, &s.RowCount); err != nil {
			return err
		}
	}

	// Zero rows, no need to do anything more.
	if *s.RowCount == 0 {
		return nil
	}

	/* Find supported stats for each column type. References:
	 * https://github.com/prestodb/presto/blob/5f2afc97f8cdc39b93e4a3569afdf912bdc8a3c9/presto-iceberg/src/main/java/com/facebook/presto/iceberg/TableStatisticsMaker.java#L527
	 * https://github.com/prestodb/presto/blob/5cf685a714b71ac78602b50a866726c0e6cba518/presto-hive-metastore/src/main/java/com/facebook/presto/hive/metastore/MetastoreUtil.java#L896
	 */
	for i := 0; i < len(s.ColumnStats); i++ {
		stat := &s.ColumnStats[i]
		if stat.DataType == nil {
			continue
		}
		// get rid of the params of the data type, like char(n), decimal(p, s), etc.
		rawDataType := *stat.DataType
		if parenthesis := strings.IndexByte(rawDataType, '('); parenthesis >= 0 {
			rawDataType = rawDataType[:parenthesis]
		}
		statistics := make([]string, 0, 4)
		if stat.NullsFraction == nil {
			statistics = append(statistics, fmt.Sprintf("count(%s) AS non_null_values_count", stat.ColumnName))
		}
		if rawDataType == BooleanType {
			statistics = append(statistics, fmt.Sprintf("count_if(%s) AS true_values_count", stat.ColumnName))
		} else if IsSizable[rawDataType] {
			statistics = append(statistics, fmt.Sprintf("max_data_size_for_stats(%s) AS max_data_size", stat.ColumnName))
			if stat.DataSize == nil {
				statistics = append(statistics, fmt.Sprintf("sum_data_size_for_stats(%s) AS data_size", stat.ColumnName))
			}
			if stat.DistinctValuesCount == nil && (rawDataType == VarcharType || rawDataType == CharType) {
				statistics = append(statistics, fmt.Sprintf("approx_distinct(%s) AS distinct_values_count", stat.ColumnName))
			}
		} else if IsNumericType[rawDataType] || rawDataType == DateType || rawDataType == TimestampType {
			if stat.LowValue == nil {
				statistics = append(statistics, fmt.Sprintf("min(%s) AS low_value", stat.ColumnName))
			}
			if stat.HighValue == nil {
				statistics = append(statistics, fmt.Sprintf("max(%s) AS high_value", stat.ColumnName))
			}
			if stat.DistinctValuesCount == nil {
				statistics = append(statistics, fmt.Sprintf("approx_distinct(%s) AS distinct_values_count", stat.ColumnName))
			}
		}

		if len(statistics) == 0 {
			continue
		}

		query := fmt.Sprintf("SELECT %s FROM %s", strings.Join(statistics, ", "), fullyQualifiedTableName)
		log.Debug().Str("query", query).Send()
		if err := presto.QueryAndUnmarshal(ctx, client, query, stat); err != nil {
			return err
		}
		if stat.NullsFraction == nil {
			nullsFraction := 1 - *stat.NonNullValuesCount/float64(*s.RowCount)
			stat.NullsFraction = &nullsFraction
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
