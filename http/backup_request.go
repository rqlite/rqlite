package http

import (
	"encoding/json"
	"errors"
	"net/url"
)

type BackupHTTPRequest struct {
	URL     string            `json:"url"`
	Headers map[string]string `json:"headers,omitempty"`
	Method  string            `json:"method,omitempty"`
}

func (b *BackupHTTPRequest) Validate() error {
	if _, err := url.ParseRequestURI(b.URL); err != nil {
		return errors.New("invalid URL")
	}

	b.Method = normalizeMethod(b.Method)
	if b.Method == "" {
		return errors.New("invalid method")
	}

	return nil
}

func ParseBackupHTTPRequest(data []byte) (*BackupHTTPRequest, error) {
	var req BackupHTTPRequest
	if err := json.Unmarshal(data, &req); err != nil {
		return nil, err
	}

	if err := req.Validate(); err != nil {
		return nil, err
	}

	return &req, nil
}

func normalizeMethod(method string) string {
	switch method {
	case "":
		return "POST"
	case "PUT", "POST":
		return method
	default:
		return ""
	}
}
