package main

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestWorksheetsExportReturnsDeterministicPayload(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/worksheets/export?date_from=2026-06-01&date_to=2026-06-02", nil)
	response := httptest.NewRecorder()

	newHandler().ServeHTTP(response, request)

	if response.Code != http.StatusOK {
		t.Fatalf("expected status 200, got %d", response.Code)
	}

	var body struct {
		Source   string `json:"source"`
		DateFrom string `json:"date_from"`
		DateTo   string `json:"date_to"`
		Items    []struct {
			UserID string `json:"user_id"`
			Hours  int    `json:"hours"`
		} `json:"items"`
	}
	if err := json.Unmarshal(response.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response body: %v", err)
	}
	if body.Source != "fake-product-api" {
		t.Fatalf("expected fake-product-api source, got %q", body.Source)
	}
	if body.DateFrom != "2026-06-01" || body.DateTo != "2026-06-02" {
		t.Fatalf("unexpected date range: %q/%q", body.DateFrom, body.DateTo)
	}
	if len(body.Items) != 2 {
		t.Fatalf("expected 2 fake items, got %d", len(body.Items))
	}
	if body.Items[0].UserID == "" || body.Items[0].Hours <= 0 {
		t.Fatalf("expected first fake item to include user_id and positive hours, got %+v", body.Items[0])
	}
}

func TestWorksheetsExportRequiresDateRange(t *testing.T) {
	request := httptest.NewRequest(http.MethodGet, "/api/worksheets/export?date_from=2026-06-01", nil)
	response := httptest.NewRecorder()

	newHandler().ServeHTTP(response, request)

	if response.Code != http.StatusBadRequest {
		t.Fatalf("expected status 400, got %d", response.Code)
	}
}
