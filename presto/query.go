package presto

import (
	"context"
	"io"
	"net/http"
)

func (c *Client) requestQueryResults(ctx context.Context, req *http.Request) (*QueryResults, *http.Response, error) {
	qr := new(QueryResults)
	resp, err := c.Do(ctx, req, qr)
	if err != nil {
		return nil, resp, err
	}
	qr.client = c
	if qr.Error != nil {
		return qr, resp, qr.Error
	}
	return qr, resp, nil
}

func (c *Client) Query(ctx context.Context, query string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := c.NewRequest("POST",
		"v1/statement", query, opts...)
	if err != nil {
		return nil, nil, err
	}

	return c.requestQueryResults(ctx, req)
}

func (c *Client) FetchNextBatch(ctx context.Context, nextUri string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := c.NewRequest("GET",
		nextUri, nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	return c.requestQueryResults(ctx, req)
}

func (c *Client) CancelQuery(ctx context.Context, nextUri string, opts ...RequestOption) (*QueryResults, *http.Response, error) {
	req, err := c.NewRequest("DELETE",
		nextUri, nil, opts...)
	if err != nil {
		return nil, nil, err
	}

	return c.requestQueryResults(ctx, req)
}

func (c *Client) GetQueryInfo(ctx context.Context, queryId string, writer io.Writer, opts ...RequestOption) (*http.Response, error) {
	req, err := c.NewRequest("GET",
		"v1/query/"+queryId+"?pretty", nil, opts...)
	if err != nil {
		return nil, err
	}
	resp, err := c.Do(ctx, req, writer)
	if err != nil {
		return resp, err
	}
	return resp, nil
}
