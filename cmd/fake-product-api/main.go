package main

import (
	"context"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

type worksheetsExportResponse struct {
	Source   string                `json:"source"`
	DateFrom string                `json:"date_from"`
	DateTo   string                `json:"date_to"`
	Items    []worksheetsExportRow `json:"items"`
}

type worksheetsExportRow struct {
	UserID string `json:"user_id"`
	Hours  int    `json:"hours"`
}

func main() {
	addr := os.Getenv("FAKE_PRODUCT_API_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	server := &http.Server{
		Addr:              addr,
		Handler:           newHandler(),
		ReadHeaderTimeout: 5 * time.Second,
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	go func() {
		<-ctx.Done()
		shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer shutdownCancel()
		_ = server.Shutdown(shutdownCtx)
	}()

	log.Printf("fake product api listening on %s", addr)
	if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
		log.Printf("fake product api stopped: %v", err)
		os.Exit(1)
	}
}

func newHandler() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("/health", handleHealth)
	mux.HandleFunc("/api/worksheets/export", handleWorksheetsExport)
	return mux
}

func handleHealth(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"service": "fake-product-api",
		"status":  "ok",
	})
}

func handleWorksheetsExport(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		w.Header().Set("Allow", http.MethodGet)
		writeJSON(w, http.StatusMethodNotAllowed, map[string]string{"error": "method not allowed"})
		return
	}

	dateFrom := r.URL.Query().Get("date_from")
	dateTo := r.URL.Query().Get("date_to")
	if dateFrom == "" || dateTo == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "date_from and date_to are required"})
		return
	}

	writeJSON(w, http.StatusOK, worksheetsExportResponse{
		Source:   "fake-product-api",
		DateFrom: dateFrom,
		DateTo:   dateTo,
		Items: []worksheetsExportRow{
			{UserID: "user-001", Hours: 8},
			{UserID: "user-002", Hours: 6},
		},
	})
}

func writeJSON(w http.ResponseWriter, statusCode int, body any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(statusCode)
	if err := json.NewEncoder(w).Encode(body); err != nil {
		log.Printf("write json response: %v", err)
	}
}
