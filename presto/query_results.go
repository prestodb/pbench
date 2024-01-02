package presto

import (
	"context"
	"encoding/json"
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
		*qr = *newQr
		if err != nil {
			return err
		}
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
