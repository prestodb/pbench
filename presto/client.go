package presto

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"pbench/log"
	"reflect"
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
	TrinoSourceHeader        = "X-Trino-Source"
	ClientInfoHeader         = "X-Presto-Client-Info"
	ClientTagHeader          = "X-Presto-Client-Tags"
	TimeZoneHeader           = "X-Presto-Time-Zone"
	DefaultUser              = "pbench"
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
	isTrino       bool
	forceHttps    bool
}

func NewClient(serverUrl string, isTrino bool) (*Client, error) {
	parsedServerUrl, err := url.Parse(serverUrl)
	if err != nil {
		return nil, err
	}
	client := &Client{
		client:        &http.Client{},
		serverUrl:     parsedServerUrl,
		baseHeader:    make(http.Header),
		sessionParams: make(map[string]any),
		isTrino:       isTrino,
	}
	client.client.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		if len(via) >= 10 {
			return http.ErrUseLastResponse
		}
		if len(via) > 0 {
			req.Header = via[0].Header
		}
		return nil
	}
	client.User(DefaultUser)
	return client, nil
}

func derefValue(v *reflect.Value) reflect.Kind {
	k := v.Kind()
	for k == reflect.Pointer || k == reflect.Interface {
		*v = v.Elem()
		k = v.Kind()
	}
	return k
}

func GenerateHttpQueryParameter(v any) string {
	rv := reflect.ValueOf(v)
	if rvk := derefValue(&rv); rvk != reflect.Struct {
		return ""
	}
	queryBuilder := strings.Builder{}
	vt := rv.Type()
	for i := 0; i < vt.NumField(); i++ {
		fv, ft := rv.Field(i), vt.Field(i)
		if fvk := derefValue(&fv); fvk == reflect.Invalid || !fv.CanInterface() {
			continue
		}
		if tag := ft.Tag.Get("query"); tag != "" {
			if queryBuilder.Len() > 0 {
				queryBuilder.WriteString("&")
			}
			queryBuilder.WriteString(fmt.Sprintf("%s=%s", url.QueryEscape(tag), url.QueryEscape(fmt.Sprint(fv.Interface()))))
		}
	}
	return queryBuilder.String()
}

func (c *Client) GetHost() string {
	return c.serverUrl.Host
}

func (c *Client) setHeader(key, value string) {
	if c.isTrino {
		key = strings.Replace(key, "X-Presto", "X-Trino", 1)
	}
	c.baseHeader.Set(key, value)
}

func (c *Client) delHeader(key string) {
	if c.isTrino {
		key = strings.Replace(key, "X-Presto", "X-Trino", 1)
	}
	c.baseHeader.Del(key)
}

func (c *Client) getHeader(key string) string {
	if c.isTrino {
		key = strings.Replace(key, "X-Presto", "X-Trino", 1)
	}
	return c.baseHeader.Get(key)
}

func (c *Client) TimeZone(timezone string) *Client {
	c.setHeader(TimeZoneHeader, timezone)
	return c
}

func (c *Client) ForceHttps() *Client {
	c.forceHttps = true
	return c
}

func (c *Client) User(user string) *Client {
	c.userInfo = url.User(user)
	c.setHeader(UserHeader, user)
	return c
}

func (c *Client) UserPassword(user, password string) *Client {
	c.userInfo = url.UserPassword(user, password)
	c.setHeader(UserHeader, user)
	return c
}

func (c *Client) GetSessionParams() map[string]any {
	params := make(map[string]any)
	for k, v := range c.sessionParams {
		params[k] = v
	}
	return params
}

func (c *Client) ClearSessionParams() *Client {
	c.delHeader(SessionHeader)
	clear(c.sessionParams)
	return c
}

func (c *Client) GenerateSessionParamsHeaderValue(params map[string]any) string {
	buf := strings.Builder{}
	for k, v := range params {
		if vstr, ok := v.(string); ok {
			v = url.QueryEscape(vstr)
		}
		if buf.Len() > 0 {
			buf.WriteString(",")
		}
		buf.WriteString(fmt.Sprintf("%s=%v", k, v))
	}
	return buf.String()
}

func (c *Client) SessionParam(key string, value any) *Client {
	if value == nil {
		delete(c.sessionParams, key)
	} else {
		c.sessionParams[key] = value
	}
	if len(c.sessionParams) == 0 {
		c.delHeader(SessionHeader)
		return c
	}
	c.setHeader(SessionHeader, c.GenerateSessionParamsHeaderValue(c.sessionParams))
	return c
}

func (c *Client) Catalog(catalog string) *Client {
	if catalog != "" {
		c.setHeader(CatalogHeader, catalog)
	} else {
		c.delHeader(CatalogHeader)
	}
	return c
}

func (c *Client) Schema(schema string) *Client {
	if schema != "" {
		c.setHeader(SchemaHeader, schema)
	} else {
		c.delHeader(SchemaHeader)
	}
	return c
}

func (c *Client) GetCatalog() string {
	return c.getHeader(CatalogHeader)
}

func (c *Client) GetSchema() string {
	return c.getHeader(SchemaHeader)
}

func (c *Client) GetTimeZone() string {
	return c.getHeader(TimeZoneHeader)
}

func (c *Client) ClientInfo(info string) *Client {
	if info != "" {
		c.setHeader(ClientInfoHeader, info)
	} else {
		c.delHeader(ClientInfoHeader)
	}
	return c
}

func (c *Client) ClientTags(tags ...string) *Client {
	if len(tags) > 0 {
		c.setHeader(ClientTagHeader, strings.Join(tags, ","))
	} else {
		c.delHeader(ClientTagHeader)
	}
	return c
}

func (c *Client) AppendClientTag(tags ...string) *Client {
	if len(tags) == 0 {
		// nothing to append.
		return c
	}
	value := c.getHeader(ClientTagHeader)
	if len(value) == 0 {
		return c.ClientTags(tags...)
	}
	value += "," + strings.Join(tags, ",")
	c.setHeader(ClientTagHeader, value)
	return c
}

func (c *Client) NewRequest(method, urlStr string, body interface{}, opts ...RequestOption) (*http.Request, error) {
	u, err := c.serverUrl.Parse(urlStr)
	if err != nil {
		return nil, err
	}
	if c.forceHttps && u.Scheme == "http" {
		u.Scheme = "https"
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
				if ctx.Err() != nil {
					return nil, ctx.Err()
				}
				return nil, err
			}
			switch resp.StatusCode {
			case http.StatusOK:
				if id := resp.Header.Get(StartedTransactionHeader); id != "" {
					c.setHeader(TransactionHeader, id)
				} else if resp.Header.Get(ClearTransactionHeader) == "true" {
					c.delHeader(TransactionHeader)
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
