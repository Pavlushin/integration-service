package product

import (
	"context"
	"crypto/tls"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"onec-integration/internal/telemetry"
	"onec-integration/internal/worksheets/usersreports"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/codes"
	semconv "go.opentelemetry.io/otel/semconv/v1.37.0"
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

func (c *Client) ExportWorksheets(ctx context.Context, req usersreports.Request) ([]byte, error) {
	if c == nil {
		return nil, fmt.Errorf("product client is nil")
	}
	ctx, span := telemetry.Tracer().Start(ctx, "product.export_worksheets")
	defer span.End()

	endpoint, err := url.Parse(c.baseURL + "/api/worksheets/export")
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "parse worksheets export url")
		return nil, fmt.Errorf("parse worksheets export url: %w", err)
	}

	query := endpoint.Query()
	query.Set("date_from", req.DateFrom)
	query.Set("date_to", req.DateTo)
	if req.LocationCode1C != nil && *req.LocationCode1C != "" {
		query.Set("location_code_1c", *req.LocationCode1C)
	}
	if req.EmployeeCode1C != nil && *req.EmployeeCode1C != "" {
		query.Set("employee_code_1c", *req.EmployeeCode1C)
	}
	endpoint.RawQuery = query.Encode()
	span.SetAttributes(
		semconv.HTTPRequestMethodGet,
		attribute.String("url.full", endpoint.String()),
	)

	httpReq, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint.String(), nil)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "create worksheets export request")
		return nil, fmt.Errorf("create worksheets export request: %w", err)
	}

	httpReq.Header.Set("Authorization", "Bearer "+c.bearerToken)
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.httpClient.Do(httpReq)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "do worksheets export request")
		return nil, fmt.Errorf("do worksheets export request: %w", err)
	}
	defer resp.Body.Close()
	span.SetAttributes(semconv.HTTPResponseStatusCode(resp.StatusCode))

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		span.RecordError(err)
		span.SetStatus(codes.Error, "read worksheets export response")
		return nil, fmt.Errorf("read worksheets export response: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		span.SetStatus(codes.Error, "worksheets export returned non-2xx")
		return nil, fmt.Errorf("worksheets export returned status %d: %s", resp.StatusCode, string(body))
	}

	return body, nil
}
