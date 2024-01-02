package presto

import (
	"context"
	"encoding/json"
	"errors"
	"presto-benchmark/log"
)

type QueryRow []any

type QueryResults struct {
	Id               string            `json:"id"`
	InfoUri          string            `json:"infoUri"`
	PartialCancelUri *string           `json:"partialCancelUri,omitempty"`
	NextUri          *string           `json:"nextUri,omitempty"`
	Columns          []Column          `json:"columns,omitempty"`
	Data             []json.RawMessage `json:"data,omitempty"`
	// binaryData;
	Stats       StatementStats `json:"stats"`
	Error       *QueryError    `json:"error,omitempty"`
	Warnings    []Warning      `json:"warnings"`
	UpdateType  *string        `json:"updateType,omitempty"`
	UpdateCount *int64         `json:"updateCount,omitempty"`

	client *Client
}

func (qr *QueryResults) HasMoreBatch() bool {
	return qr.NextUri != nil
}

func (qr *QueryResults) FetchNextBatch(ctx context.Context) error {
	for qr.NextUri != nil {
		newQr, _, err := qr.client.FetchNextBatch(ctx, *qr.NextUri)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				_, _, _ = qr.client.CancelQuery(context.Background(), *qr.NextUri)
				log.Debug().Str("query_id", qr.Id).Msg("canceling query because the context is cancelled")
			}
			return err
		}
		*qr = *newQr
		if len(qr.Data) > 0 {
			break
		}
	}
	return nil
}

type ResultBatchHandler func(qr *QueryResults)

func (qr *QueryResults) Drain(ctx context.Context, handler ResultBatchHandler) error {
	for qr.HasMoreBatch() {
		err := qr.FetchNextBatch(ctx)
		if err != nil {
			return err
		}
		//sort.Slice(qr.Data, func(i, j int) bool { return bytes.Compare(qr.Data[i], qr.Data[j]) < 0 })
		if handler != nil {
			handler(qr)
		}
		qr.Data = nil
	}
	return nil
}
