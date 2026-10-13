package middleware

import (
	json "encoding/json/v2"
	"net/http"

	"github.com/Suhaibinator/SRouter/pkg/scontext"
)

type errorResponse struct {
	Error   errorMessage `json:"error"`
	TraceID string       `json:"trace_id"`
}

type errorMessage struct {
	Message string `json:"message"`
}

// writeForbidden writes a 403 JSON error body carrying the request trace ID.
func writeForbidden(w http.ResponseWriter, r *http.Request, message string) {
	payload, _ := json.Marshal(errorResponse{Error: errorMessage{Message: message}, TraceID: scontext.GetTraceID(r.Context())})
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusForbidden)
	w.Write(payload)
}
