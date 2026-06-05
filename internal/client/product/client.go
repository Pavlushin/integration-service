package product

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"onec-integration/internal/worksheetsexport"
)

type Client struct {
	baseURL     string
	bearerToken string
	httpClient  *http.Client
}

func NewClient(config Config) *Client {
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: config.InsecureSkipVerify,
	}

	return &Client{
		baseURL:     config.BaseURL,
		bearerToken: config.BearerToken,
		httpClient: &http.Client{
			Timeout:   config.Timeout,
			Transport: transport,
		},
	}
}

func (c *Client) ExportWorksheets(ctx context.Context, req worksheetsexport.Request) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("product client is nil")
	}

	endpoint, err := url.Parse(c.baseURL + "/api/worksheets/export")
	if err != nil {
		return nil, fmt.Errorf("parse worksheets export url: %w", err)
	}

	query := endpoint.Query()
	query.Set("date_from", req.DateFrom)
	query.Set("date_to", req.DateTo)
	endpoint.RawQuery = query.Encode()

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("create worksheets export request: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+c.bearerToken)
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		return nil, fmt.Errorf("do worksheets export request: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("read worksheets export response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("worksheets export returned status %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}
