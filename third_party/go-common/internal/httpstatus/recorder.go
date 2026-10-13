// Package httpstatus provides a response writer wrapper shared by go-common
// middleware that needs the response status without buffering the body.
package httpstatus

import (
	"bufio"
	"net"
	"net/http"
)

// Recorder captures the response status code. Hijack, Flush, and Unwrap pass
// through so WebSocket upgrades and streaming responses keep working behind it.
type Recorder struct {
	http.ResponseWriter
	// Status is the written status code, or zero if nothing has been written.
	Status int
}

func (sr *Recorder) WriteHeader(code int) {
	sr.Status = code
	sr.ResponseWriter.WriteHeader(code)
}

func (sr *Recorder) Write(b []byte) (int, error) {
	if sr.Status == 0 {
		sr.Status = http.StatusOK
	}
	return sr.ResponseWriter.Write(b)
}

// Unwrap supports http.ResponseController-based access to the underlying writer.
func (sr *Recorder) Unwrap() http.ResponseWriter {
	return sr.ResponseWriter
}

func (sr *Recorder) Hijack() (net.Conn, *bufio.ReadWriter, error) {
	if hj, ok := sr.ResponseWriter.(http.Hijacker); ok {
		return hj.Hijack()
	}
	return nil, nil, http.ErrNotSupported
}

func (sr *Recorder) Flush() {
	if f, ok := sr.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}
