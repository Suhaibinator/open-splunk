//go:build !windows

package integration_test

import (
	"bytes"
	"crypto/x509"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOpenBackendHECSlowClientAdmission(t *testing.T) {
	for _, reject := range []bool{false, true} {
		name := "continue"
		if reject {
			name = "rejected"
		}
		t.Run(name, func(t *testing.T) {
			compressed := backendHECSlowCompressedBody(t)
			bodyRead := make(chan struct{})
			server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
				if request.Header.Get("Expect") != "100-continue" {
					t.Error("slow client did not request an admission acknowledgment")
				}
				if reject {
					response.WriteHeader(http.StatusServiceUnavailable)
					return
				}
				body := make([]byte, len(compressed))
				if _, err := io.ReadFull(request.Body, body); err != nil {
					t.Errorf("read slow client body: %v", err)
				} else if !bytes.Equal(body, compressed) {
					t.Error("slow client changed the gzip chunk")
				}
				close(bodyRead)
				// Keep the request held until the client closes its connection.
				_, _ = io.Copy(io.Discard, request.Body)
			}))
			t.Cleanup(server.Close)
			roots := x509.NewCertPool()
			roots.AddCert(server.Certificate())
			client, err := openBackendHECSlowClient(t.Context(), server.Listener.Addr().String(), roots, "test-credential", "test-channel", compressed)
			if client != nil {
				t.Cleanup(func() { closeBackendHECSlowClients([]*backendHECSlowClient{client}) })
			}
			if reject {
				if err == nil || !strings.Contains(err.Error(), "status = 503") {
					t.Fatalf("rejected admission = %v, want status 503", err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			select {
			case <-bodyRead:
			case <-time.After(5 * time.Second):
				t.Fatal("slow client did not send its gzip chunk after Continue")
			}
		})
	}
}
