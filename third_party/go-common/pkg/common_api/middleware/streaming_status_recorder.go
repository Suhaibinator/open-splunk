package middleware

import (
	"bufio"
	"net"
	"net/http"
)

// streamingStatusRecorder captures response status without buffering the body.
// Hijack, Flush, and Unwrap preserve streaming and WebSocket support independently
// of the optional SQLite request logger.
type streamingStatusRecorder struct {
	http.ResponseWriter
	status int
}

func (sr *streamingStatusRecorder) WriteHeader(code int) {
	sr.status = code
	sr.ResponseWriter.WriteHeader(code)
}

func (sr *streamingStatusRecorder) Write(b []byte) (int, error) {
	if sr.status == 0 {
		sr.status = http.StatusOK
	}
	return sr.ResponseWriter.Write(b)
}

// Unwrap supports http.ResponseController-based access to the underlying writer.
func (sr *streamingStatusRecorder) Unwrap() http.ResponseWriter {
	return sr.ResponseWriter
}

func (sr *streamingStatusRecorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := sr.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (sr *streamingStatusRecorder) Flush() {
	if f, ok := sr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
