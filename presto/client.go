package presto

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"presto-benchmark/log"
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
	ClientInfoHeader         = "X-Presto-Client-Info"
	ClientTagHeader          = "X-Presto-Client-Tags"

	DefaultUser = "presto-benchmark"
)

// RequestOption represents an option that can modify a http.Request.
type RequestOption func(req *http.Request)

type Client struct {
	client        *http.Client
	serverUrl     *url.URL
	userInfo      *url.Userinfo
	sessionParams map[string]any
	clientTags    []string
	baseHeader    http.Header
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
	client.User(DefaultUser)
	client.baseHeader.Set(SourceHeader, DefaultUser)
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

func (c *Client) GetSessionParams() string {
	return c.baseHeader.Get(SessionHeader)
}

func (c *Client) ClearSessionParams() *Client {
	c.baseHeader.Del(SessionHeader)
	c.sessionParams = make(map[string]any)
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

func (c *Client) Catalog(catalog string) *Client {
	if catalog != "" {
		c.baseHeader.Set(CatalogHeader, catalog)
	} else {
		c.baseHeader.Del(CatalogHeader)
	}
	return c
}

func (c *Client) Schema(schema string) *Client {
	if schema != "" {
		c.baseHeader.Set(SchemaHeader, schema)
	} else {
		c.baseHeader.Del(SchemaHeader)
	}
	return c
}

func (c *Client) GetCatalog() string {
	return c.baseHeader.Get(CatalogHeader)
}

func (c *Client) GetSchema() string {
	return c.baseHeader.Get(SchemaHeader)
}

func (c *Client) ClientInfo(info string) *Client {
	if info != "" {
		c.baseHeader.Set(ClientInfoHeader, info)
	} else {
		c.baseHeader.Del(ClientInfoHeader)
	}
	return c
}

func (c *Client) ClientTags(tags ...string) *Client {
	if len(tags) > 0 {
		c.baseHeader.Set(ClientTagHeader, strings.Join(tags, ","))
	} else {
		c.baseHeader.Del(ClientTagHeader)
	}
	return c
}

func (c *Client) AppendClientTag(tags ...string) *Client {
	if len(tags) == 0 {
		// nothing to append.
		return c
	}
	value := c.baseHeader.Get(ClientTagHeader)
	if len(value) == 0 {
		return c.ClientTags(tags...)
	}
	value += "," + strings.Join(tags, ",")
	c.baseHeader.Set(ClientTagHeader, value)
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
	const maxRetryAttempts = 10
	timer := time.NewTimer(0)
	defer timer.Stop()
	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
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
				_ = resp.Body.Close()
				return resp, err
			case http.StatusServiceUnavailable:
				_ = resp.Body.Close()
				log.Debug().Dur("delay", retryDelay).
					Msg("retry after getting http Service Unavailable response (503)")
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
	return nil, fmt.Errorf("reached max attempts (%d)", maxRetryAttempts)
}

func (c *Client) BareDo(req *http.Request) (*http.Response, error) {
	return c.client.Do(req)
}
