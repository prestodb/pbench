package presto

import (
	"context"
	"encoding/json"
	"errors"
	"pbench/log"
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
	return qr != nil && qr.NextUri != nil
}

func (qr *QueryResults) FetchNextBatch(ctx context.Context) error {
	if qr == nil {
		return errors.New("nil QueryResults")
	}
	for qr.NextUri != nil {
		newQr, _, err := qr.client.FetchNextBatch(ctx, *qr.NextUri)
		if err != nil {
			if ctx.Err() != nil {
				// ctx cannot be used now because it is canceled. Supply a new context without deadline.
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

type ResultBatchHandler func(qr *QueryResults) error

func (qr *QueryResults) Drain(ctx context.Context, handler ResultBatchHandler) error {
	if qr == nil {
		return errors.New("nil QueryResults")
	}
	for qr.HasMoreBatch() {
		err := qr.FetchNextBatch(ctx)
		if err != nil {
			return err
		}
		//sort.Slice(qr.Data, func(i, j int) bool { return bytes.Compare(qr.Data[i], qr.Data[j]) < 0 })
		if handler != nil {
			if err = handler(qr); err != nil {
				qr.Data = nil
				return err
			}
		}
		qr.Data = nil
	}
	return nil
}
