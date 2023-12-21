package presto

import (
	"fmt"
	"io"
	"net/http"
)

type ErrorResponse struct {
	Response *http.Response
	Message  string
}

func (e *ErrorResponse) Error() string {
	return fmt.Sprintf("%s (status code: %d)", e.Message, e.Response.StatusCode)
}

func NewErrorResponse(resp *http.Response) error {
	defer resp.Body.Close()
	bytes, err := io.ReadAll(resp.Body)
	if err != nil {
		return err
	}
	return &ErrorResponse{
		Response: resp,
		Message:  string(bytes),
	}
}
