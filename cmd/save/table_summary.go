package save

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"pbench/log"
	"pbench/prestoapi"
	"pbench/utils"

	presto "github.com/ethanyzhang/presto-go"
	"strings"
	"syscall"
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
	VarbinaryType = "varbinary"
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
		VarbinaryType: true,
		ArrayType:     true,
		MapType:       true,
		RowType:       true,
	}
	/*
		Before Presto 0.283, max_data_size_for_stats and sum_data_size_for_stats were called
		"$internal$max_data_size_for_stats" and "$internal$sum_data_size_for_stats"
		See https://github.com/prestodb/presto/commit/b65b50032258eb4a2a8a9269d74f54737e767387
		We don't know which name to use until the first query that uses those functions are run.
		We keep a flag for this as soon as we know, so we do not generate more failing queries.
	*/
	internalFunctionPrefix = ""
)

type TableSummary struct {
	Name        string                  `json:"name"`
	Catalog     string                  `json:"catalog"`
	Schema      string                  `json:"schema"`
	Ddl         string                  `json:"ddl"`
	ColumnStats []prestoapi.ColumnStats `json:"columnStats"`
	RowCount    *int                    `json:"rowCount,omitempty"`
}

// handleQueryError inspects a query error and returns whether the caller should retry,
// plus a non-nil fatal error when the query sequence should be aborted entirely.
func handleQueryError(err error, abortOnError bool) (retry bool, fatal error) {
	if err == nil {
		return false, nil
	}
	if abortOnError || errors.Is(err, syscall.ECONNREFUSED) {
		return false, err
	}
	var queryError *presto.QueryError
	if errors.As(err, &queryError) {
		if strings.HasSuffix(queryError.Message, "does not exist") {
			// table/schema/catalog does not exist, then no need to run more queries that will fail.
			return false, err
		}
		if internalFunctionPrefix == "" && strings.HasSuffix(queryError.Message, "data_size_for_stats not registered") {
			internalFunctionPrefix = "$internal$"
			log.Info().Msg(`using "$internal$max_data_size_for_stats" and "$internal$sum_data_size_for_stats" for later queries`)
			retry = true
		}
	}
	log.Error().Err(err).Msg("failed to query stats")
	return retry, nil
}

func (s *TableSummary) QueryTableSummary(ctx context.Context, client *presto.Session, analyze bool) {
	fullyQualifiedTableName := fmt.Sprintf("%s.%s.%s", s.Catalog, s.Schema, s.Name)

	abortLog := func(err error) {
		log.Error().Err(err).Msgf("querying table summary for %s aborted", fullyQualifiedTableName)
	}

	if _, fatal := handleQueryError(prestoapi.QueryAndUnmarshal(ctx, client, "SHOW CREATE TABLE "+fullyQualifiedTableName, &s.Ddl), true); fatal != nil {
		abortLog(fatal)
		return
	}
	if _, fatal := handleQueryError(prestoapi.QueryAndUnmarshal(ctx, client, "SHOW STATS FOR "+fullyQualifiedTableName, &s.ColumnStats), true); fatal != nil {
		abortLog(fatal)
		return
	}
	if _, fatal := handleQueryError(prestoapi.QueryAndUnmarshal(ctx, client, "DESCRIBE "+fullyQualifiedTableName, &s.ColumnStats), true); fatal != nil {
		abortLog(fatal)
		return
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
		handleQueryError(prestoapi.QueryAndUnmarshal(ctx, client, "SELECT COUNT(*) FROM "+fullyQualifiedTableName, &s.RowCount), true)
	}

	// Zero rows, no need to do anything more.
	if s.RowCount == nil || *s.RowCount == 0 || !analyze {
		return
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
			statistics = append(statistics, fmt.Sprintf("\"%smax_data_size_for_stats\"(%s) AS max_data_size", internalFunctionPrefix, stat.ColumnName))
			if stat.DataSize == nil {
				statistics = append(statistics, fmt.Sprintf("\"%ssum_data_size_for_stats\"(%s) AS data_size", internalFunctionPrefix, stat.ColumnName))
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
		err := prestoapi.QueryAndUnmarshal(ctx, client, query, stat)
		retry, fatal := handleQueryError(err, false)
		if fatal != nil {
			abortLog(fatal)
			return
		}
		if retry {
			i--
		}
		if err == nil && stat.NullsFraction == nil {
			nullsFraction := 1 - *stat.NonNullValuesCount/float64(*s.RowCount)
			stat.NullsFraction = &nullsFraction
		}
	}
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
