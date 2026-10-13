// Package bodylimit bounds the HTTP response bodies the OpenAI-protocol
// adapters (Chat Completions and Responses) let the SDK buffer.
package bodylimit

import (
	"errors"
	"fmt"
	"io"
	"net/http"
)

// MaxResponseBodyBytes bounds bytes read from any OpenAI-protocol HTTP body,
// including API error bodies and the cumulative bytes in an SSE stream. The
// 64 MiB ceiling leaves JSON-envelope headroom above go-common's 50 MiB durable
// provider-state limit while preventing an endpoint from making the SDK buffer
// an unbounded body.
const MaxResponseBodyBytes int64 = 64 * 1024 * 1024

// ErrResponseBodyTooLarge is wrapped by every limit failure; errors.Is
// distinguishes it from ordinary transport errors.
var ErrResponseBodyTooLarge = errors.New("openai: response body exceeds configured limit")

// Client returns a shallow copy of client whose transport enforces limit, so
// installing the guard never mutates a caller-owned client or transport. A nil
// client retains the standard library defaults.
func Client(client *http.Client, limit int64) *http.Client {
	if client == nil {
		client = http.DefaultClient
	}
	bounded := *client
	transport := client.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	bounded.Transport = &limitTransport{base: transport, limit: limit}
	return &bounded
}

type limitTransport struct {
	base  http.RoundTripper
	limit int64
}

func (t *limitTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	resp, err := t.base.RoundTrip(req)
	if err != nil || resp == nil || resp.Body == nil {
		return resp, err
	}
	if resp.ContentLength > t.limit {
		_ = resp.Body.Close()
		return nil, fmt.Errorf("%w: content-length %d is greater than %d bytes", ErrResponseBodyTooLarge, resp.ContentLength, t.limit)
	}
	resp.Body = &ReadCloser{ReadCloser: resp.Body, Limit: t.limit, Remaining: t.limit}
	return resp, nil
}

// ReadCloser fails a body read once more than Limit bytes have been consumed.
type ReadCloser struct {
	io.ReadCloser
	Limit     int64
	Remaining int64
	exceeded  bool
}

func (r *ReadCloser) Read(p []byte) (int, error) {
	if len(p) == 0 {
		return 0, nil
	}
	if r.exceeded {
		return 0, fmt.Errorf("%w: limit is %d bytes", ErrResponseBodyTooLarge, r.Limit)
	}

	// Read at most one byte beyond the remaining allowance. That byte is never
	// exposed to the caller; it only distinguishes an exactly-at-limit body from
	// a body that exceeds the limit when Content-Length is absent or compressed.
	readLimit := r.Remaining + 1
	if int64(len(p)) > readLimit {
		p = p[:readLimit]
	}
	n, err := r.ReadCloser.Read(p)
	if int64(n) > r.Remaining {
		allowed := int(r.Remaining)
		r.Remaining = 0
		r.exceeded = true
		return allowed, fmt.Errorf("%w: limit is %d bytes", ErrResponseBodyTooLarge, r.Limit)
	}
	r.Remaining -= int64(n)
	return n, err
}
