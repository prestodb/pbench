package presto

import (
	"context"
	"io"
	"net/http"
	"pbench/presto/query_json"
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

// GetQueryInfo retrieves the query JSON for the given query ID.
// If writer is nil, we return deserialized QueryInfo. Otherwise, we just return the raw buffer.
func (c *Client) GetQueryInfo(ctx context.Context, queryId string, pretty bool, writer io.Writer, opts ...RequestOption) (*query_json.QueryInfo, *http.Response, error) {
	urlStr := "v1/query/" + queryId
	if pretty {
		urlStr += "?pretty"
	}
	req, err := c.NewRequest("GET",
		urlStr, nil, opts...)
	if err != nil {
		return nil, nil, err
	}
	var (
		resp      *http.Response
		queryInfo *query_json.QueryInfo
	)
	if writer != nil {
		resp, err = c.Do(ctx, req, writer)
	} else {
		queryInfo = new(query_json.QueryInfo)
		resp, err = c.Do(ctx, req, queryInfo)
	}
	if err != nil {
		return nil, resp, err
	}
	return queryInfo, resp, nil
}
