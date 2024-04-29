package save

import (
	"context"
	"fmt"
	"pbench/log"
	"pbench/presto"
)

func saveTable(ctx context.Context, client *presto.Client, table string) {
	if ctx.Err() != nil {
		return
	}
	clientResult, _, err := client.Query(ctx, "SHOW CREATE TABLE "+table)
	if err != nil {
		log.Error().Str("table", table).Err(err).Msg("failed to get table schema for table")
		return
	}
	clientResult.Drain(ctx, func(qr *presto.QueryResults) error {
		for _, row := range qr.Data {
			fmt.Println(string(row))
		}
		return nil
	})
}
