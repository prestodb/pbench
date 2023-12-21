package presto

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	UserHeader               = "X-Presto-User"
	CatalogHeader            = "X-Presto-Catalog"
	SchemaHeader             = "X-Presto-Schema"
	SessionHeader            = "X-Presto-Session"
	TransactionHeader        = "X-Presto-Transaction-Id"
	StartedTransactionHeader = "X-Presto-Started-Transaction-Id"
	ClearTransactionHeader   = "X-Presto-Clear-Transaction-Id"
	SourceHeader             = "X-Presto-Source"
)

// RequestOption represents an option that can modify a http.Request.
type RequestOption func(req *http.Request)

type Client struct {
	client        *http.Client
	serverUrl     *url.URL
	userInfo      *url.Userinfo
	catalog       string
	schema        string
	baseHeader    http.Header
	sessionParams map[string]any
}

func NewClient(serverUrl string) (*Client, error) {
	parsedServerUrl, err := url.Parse(serverUrl)
	if err != nil {
		return nil, err
	}
	client := &Client{
		client:        &http.Client{},
		serverUrl:     parsedServerUrl,
		baseHeader:    make(http.Header),
		sessionParams: make(map[string]any),
	}
	client.User("presto-benchmark")
	client.baseHeader.Set(SourceHeader, "presto-benchmark")
	return client, nil
}

func (c *Client) User(user string) *Client {
	c.userInfo = url.User(user)
	c.baseHeader.Set(UserHeader, c.userInfo.Username())
	return c
}

func (c *Client) UserPassword(user, password string) *Client {
	c.userInfo = url.UserPassword(user, password)
	c.baseHeader.Set(UserHeader, c.userInfo.Username())
	return c
}

func (c *Client) Catalog(catalog string) *Client {
	c.catalog = catalog
	if catalog != "" {
		c.baseHeader.Set(CatalogHeader, catalog)
	} else {
		c.baseHeader.Del(CatalogHeader)
	}
	return c
}

func (c *Client) SessionParam(key string, value any) *Client {
	if value == nil {
		delete(c.sessionParams, key)
	} else {
		c.sessionParams[key] = value
	}
	if len(c.sessionParams) == 0 {
		c.baseHeader.Del(SessionHeader)
		return c
	}
	buf := strings.Builder{}
	for k, v := range c.sessionParams {
		if buf.Len() > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("%s=%v", k, v))
	}
	c.baseHeader.Set(SessionHeader, buf.String())
	return c
}

func (c *Client) Schema(schema string) *Client {
	c.schema = schema
	if schema != "" {
		c.baseHeader.Set(SchemaHeader, schema)
	} else {
		c.baseHeader.Del(SchemaHeader)
	}
	return c
}

func (c *Client) NewRequest(method, urlStr string, body interface{}, opts ...RequestOption) (*http.Request, error) {
	u, err := c.serverUrl.Parse(urlStr)
	if err != nil {
		return nil, err
	}

	var bodyReader io.Reader
	var contentType string
	if body != nil {
		if query, ok := body.(string); ok {
			contentType = "text/plain"
			bodyReader = strings.NewReader(query)
		} else {
			contentType = "application/json"
			jsonBuf := &bytes.Buffer{}
			enc := json.NewEncoder(jsonBuf)
			enc.SetEscapeHTML(false)
			err = enc.Encode(body)
			if err != nil {
				return nil, err
			}
			bodyReader = jsonBuf
		}
	}

	req, err := http.NewRequest(method, u.String(), bodyReader)
	if err != nil {
		return nil, err
	}

	req.Header = c.baseHeader.Clone()
	if password, ok := c.userInfo.Password(); ok {
		req.SetBasicAuth(c.userInfo.Username(), password)
	}
	if contentType != "" {
		req.Header.Set("Content-Type", contentType)
	}

	for _, opt := range opts {
		opt(req)
	}

	return req, nil
}

func (c *Client) Do(ctx context.Context, req *http.Request, v interface{}) (*http.Response, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	req = req.WithContext(ctx)
	retryDelay := time.Second
	const maxRetryDelay = 30 * time.Second
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-timer.C:
			resp, err := c.client.Do(req)
			if err != nil {
				// If we got an error, and the context has been canceled,
				// the context's error is probably more useful.
				select {
				case <-ctx.Done():
					return nil, ctx.Err()
				default:
				}
				return nil, err
			}
			switch resp.StatusCode {
			case http.StatusOK:
				if id := resp.Header.Get(StartedTransactionHeader); id != "" {
					c.baseHeader.Set(TransactionHeader, id)
				} else if resp.Header.Get(ClearTransactionHeader) == "true" {
					c.baseHeader.Del(TransactionHeader)
				}
				switch v := v.(type) {
				case nil:
				case io.Writer:
					_, err = io.Copy(v, resp.Body)
				default:
					decErr := json.NewDecoder(resp.Body).Decode(v)
					if decErr == io.EOF {
						decErr = nil // ignore EOF errors caused by empty response body
					}
					if decErr != nil {
						err = decErr
					}
				}
				resp.Body.Close()
				return resp, err
			case http.StatusServiceUnavailable:
				resp.Body.Close()
				log.Printf("http retry delay = %d\n", retryDelay)
				timer.Reset(retryDelay)
				retryDelay *= 2
				if retryDelay > maxRetryDelay {
					retryDelay = maxRetryDelay
				}
			default:
				return nil, NewErrorResponse(resp)
			}
		}
	}
}

func (c *Client) BareDo(req *http.Request) (*http.Response, error) {
	return c.client.Do(req)
}
