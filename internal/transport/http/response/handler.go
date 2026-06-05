package response

import (
	"encoding/json"
	"fmt"
	"net/http"

	"onec-integration/internal/logger"

	"go.uber.org/zap"
)

type HTTPResponseHandler struct {
	log *logger.Logger
	rw  http.ResponseWriter
}

func NewHTTPResponseHandler(log *logger.Logger, rw http.ResponseWriter) *HTTPResponseHandler {
	return &HTTPResponseHandler{
		log: log,
		rw:  rw,
	}
}

func (h *HTTPResponseHandler) JSONResponse(responseBody any, statusCode int) {
	h.rw.Header().Set("Content-Type", "application/json")
	h.rw.WriteHeader(statusCode)

	if err := json.NewEncoder(h.rw).Encode(responseBody); err != nil {
		h.log.Error("failed to encode response body", zap.Error(err))
	}
}

func (h *HTTPResponseHandler) ErrorResponse(statusCode int, err error, msg string) {
	h.log.Error(msg, zap.Error(err))
	h.JSONResponse(map[string]string{
		"message": msg,
		"error":   err.Error(),
	}, statusCode)
}

func (h *HTTPResponseHandler) PanicResponse(p any, msg string) {
	err := fmt.Errorf("unexpected panic: %v", p)

	h.log.Error(msg, zap.Error(err))
	h.JSONResponse(map[string]string{
		"message": msg,
		"error":   err.Error(),
	}, http.StatusInternalServerError)
}
