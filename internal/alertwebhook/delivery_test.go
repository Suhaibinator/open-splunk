package alertwebhook

import (
	"context"
	"crypto/x509"
	"errors"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/netip"
	"net/url"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

type doerFunc func(*http.Request) (*http.Response, error)

func (function doerFunc) Do(request *http.Request) (*http.Response, error) {
	return function(request)
}

type contextResponseBody struct{ context context.Context }

func (body contextResponseBody) Read([]byte) (int, error) {
	<-body.context.Done()
	return 0, body.context.Err()
}

func (contextResponseBody) Close() error { return nil }

func TestDelivererSendsSignedRequestAndBoundsResponse(t *testing.T) {
	t.Parallel()
	now := time.Date(2026, time.August, 29, 1, 2, 3, 0, time.UTC)
	var capturedTransport http.RoundTripper
	deliverer, err := NewDeliverer(DeliveryOptions{
		Resolver: staticResolver{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8")}},
		Clock:    func() time.Time { return now },
		ClientFactory: func(transport http.RoundTripper, timeout time.Duration) HTTPDoer {
			capturedTransport = transport
			if timeout != DefaultDeliveryTimeout {
				t.Fatalf("timeout = %s", timeout)
			}
			return doerFunc(func(request *http.Request) (*http.Response, error) {
				body, readErr := io.ReadAll(request.Body)
				if readErr != nil {
					t.Fatalf("ReadAll(request) error = %v", readErr)
				}
				if string(body) != `{"event":"test"}` || request.Header.Get(HeaderSignature) != "v1=abc" {
					t.Fatalf("request body/header = %q / %q", body, request.Header.Get(HeaderSignature))
				}
				if request.Header.Get("X-Unapproved") != "" {
					t.Fatal("Deliver() forwarded an unapproved custom header")
				}
				return &http.Response{
					StatusCode: http.StatusNoContent,
					Body:       io.NopCloser(strings.NewReader(strings.Repeat("x", DefaultResponseReadLimit*2))),
				}, nil
			})
		},
	})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	result, err := deliverer.Deliver(context.Background(), "https://hooks.example.com/alert", SignedPayload{
		Body: []byte(`{"event":"test"}`), Headers: map[string]string{HeaderSignature: "v1=abc", "X-Unapproved": "secret"},
	})
	if err != nil {
		t.Fatalf("Deliver() error = %v", err)
	}
	if !result.Delivered || result.Category != DeliverySucceeded || result.StatusCode != http.StatusNoContent || !result.AttemptedAt.Equal(now) {
		t.Fatalf("result = %#v", result)
	}
	transport, ok := capturedTransport.(*http.Transport)
	if !ok || transport.Proxy != nil || !transport.DisableKeepAlives || transport.TLSClientConfig.ServerName != "hooks.example.com" {
		t.Fatalf("unsafe transport = %#v", capturedTransport)
	}
}

func TestDefaultClientRefusesRedirects(t *testing.T) {
	t.Parallel()
	client, ok := defaultClientFactory(http.DefaultTransport, DefaultDeliveryTimeout).(*http.Client)
	if !ok {
		t.Fatal("defaultClientFactory() did not return *http.Client")
	}
	if err := client.CheckRedirect(&http.Request{}, []*http.Request{{}}); !errors.Is(err, http.ErrUseLastResponse) {
		t.Fatalf("CheckRedirect() error = %v", err)
	}
}

func TestDelivererEnforcesDeadlineWithInjectedClient(t *testing.T) {
	t.Parallel()
	deliverer, err := NewDeliverer(DeliveryOptions{
		Resolver: staticResolver{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8")}},
		Timeout:  50 * time.Millisecond,
		ClientFactory: func(http.RoundTripper, time.Duration) HTTPDoer {
			return doerFunc(func(request *http.Request) (*http.Response, error) {
				if _, ok := request.Context().Deadline(); !ok {
					t.Fatal("delivery request context has no deadline")
				}
				<-request.Context().Done()
				return nil, request.Context().Err()
			})
		},
	})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	result, err := deliverer.Deliver(context.Background(), "https://hooks.example.com", SignedPayload{})
	if err == nil || result.Category != DeliveryTimeout {
		t.Fatalf("Deliver() = %#v, %v; want timeout", result, err)
	}
}

func TestDelivererClassifiesResponseBodyDeadline(t *testing.T) {
	t.Parallel()
	deliverer, err := NewDeliverer(DeliveryOptions{
		Resolver: staticResolver{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8")}},
		Timeout:  50 * time.Millisecond,
		ClientFactory: func(http.RoundTripper, time.Duration) HTTPDoer {
			return doerFunc(func(request *http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusNoContent, Body: contextResponseBody{context: request.Context()}}, nil
			})
		},
	})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	result, err := deliverer.Deliver(context.Background(), "https://hooks.example.com", SignedPayload{})
	if err == nil || result.Category != DeliveryTimeout {
		t.Fatalf("Deliver() = %#v, %v; want response body timeout", result, err)
	}
}

func TestDelivererPinsAddressAndVerifiesRealTLSHostname(t *testing.T) {
	t.Parallel()
	var requests atomic.Int64
	server := httptest.NewTLSServer(http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		requests.Add(1)
		if request.URL.Path == "/redirect" {
			writer.Header().Set("Location", "/followed")
			writer.WriteHeader(http.StatusFound)
			return
		}
		writer.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()
	serverURL, err := url.Parse(server.URL)
	if err != nil {
		t.Fatalf("parse TLS server URL: %v", err)
	}
	roots := x509.NewCertPool()
	roots.AddCert(server.Certificate())
	newDeliverer := func(host string) *Deliverer {
		t.Helper()
		deliverer, createErr := NewDeliverer(DeliveryOptions{
			Resolver:   staticResolver{addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}},
			TLSRootCAs: roots,
			DestinationPolicy: DestinationPolicy{
				PrivateHostAllowlist: []string{host},
			},
		})
		if createErr != nil {
			t.Fatalf("NewDeliverer(%q) error = %v", host, createErr)
		}
		return deliverer
	}
	matchingURL := "https://example.com:" + serverURL.Port() + "/hook"
	result, err := newDeliverer("example.com").Deliver(context.Background(), matchingURL, SignedPayload{})
	if err != nil || !result.Delivered || requests.Load() != 1 {
		t.Fatalf("matching TLS delivery = %#v, %v, requests=%d", result, err, requests.Load())
	}
	redirectURL := "https://example.com:" + serverURL.Port() + "/redirect"
	result, err = newDeliverer("example.com").Deliver(context.Background(), redirectURL, SignedPayload{})
	if err == nil || result.Category != DeliveryHTTPFailure || result.StatusCode != http.StatusFound || requests.Load() != 2 {
		t.Fatalf("redirect delivery = %#v, %v, requests=%d", result, err, requests.Load())
	}

	mismatchURL := "https://webhook.invalid:" + serverURL.Port() + "/hook"
	result, err = newDeliverer("webhook.invalid").Deliver(context.Background(), mismatchURL, SignedPayload{})
	if err == nil || result.Category != DeliveryTLSFailure || requests.Load() != 2 {
		t.Fatalf("mismatched TLS delivery = %#v, %v, requests=%d", result, err, requests.Load())
	}
}

func TestDelivererRejectsUnboundedOptions(t *testing.T) {
	t.Parallel()
	if _, err := NewDeliverer(DeliveryOptions{Timeout: DefaultDeliveryTimeout + time.Nanosecond}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized timeout error = %v", err)
	}
	if _, err := NewDeliverer(DeliveryOptions{ResponseReadLimit: MaximumResponseReadLimit + 1}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized response limit error = %v", err)
	}
}

func TestDelivererSnapshotsPrivateHostAllowlist(t *testing.T) {
	t.Parallel()
	allowlist := []string{"private.example.com"}
	deliverer, err := NewDeliverer(DeliveryOptions{
		Resolver:          staticResolver{addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}},
		DestinationPolicy: DestinationPolicy{PrivateHostAllowlist: allowlist},
		ClientFactory: func(http.RoundTripper, time.Duration) HTTPDoer {
			return doerFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody}, nil
			})
		},
	})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	allowlist[0] = "attacker-controlled.example.com"
	result, err := deliverer.Deliver(context.Background(), "https://private.example.com/hook", SignedPayload{})
	if err != nil || !result.Delivered {
		t.Fatalf("Deliver() = %#v, %v; destination policy was not snapshotted", result, err)
	}
}

func TestDelivererRejectsBeforeClientAndSanitizesFailures(t *testing.T) {
	t.Parallel()
	clientCalled := false
	deliverer, err := NewDeliverer(DeliveryOptions{
		Resolver: staticResolver{addresses: []netip.Addr{netip.MustParseAddr("127.0.0.1")}},
		ClientFactory: func(http.RoundTripper, time.Duration) HTTPDoer {
			clientCalled = true
			return doerFunc(func(*http.Request) (*http.Response, error) { return nil, errors.New("unused") })
		},
	})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	_, err = deliverer.Deliver(context.Background(), "https://secret.internal.invalid/hook", SignedPayload{})
	if err == nil || clientCalled {
		t.Fatalf("Deliver() error = %v, clientCalled = %v", err, clientCalled)
	}
	if strings.Contains(err.Error(), "secret.internal") {
		t.Fatalf("error disclosed destination: %v", err)
	}
}

func TestDelivererRecordsNon2xxWithoutReadingUnboundedBody(t *testing.T) {
	t.Parallel()
	deliverer, err := NewDeliverer(DeliveryOptions{
		Resolver: staticResolver{addresses: []netip.Addr{netip.MustParseAddr("8.8.8.8")}},
		ClientFactory: func(http.RoundTripper, time.Duration) HTTPDoer {
			return doerFunc(func(*http.Request) (*http.Response, error) {
				return &http.Response{StatusCode: http.StatusBadGateway, Body: io.NopCloser(strings.NewReader("upstream detail"))}, nil
			})
		},
	})
	if err != nil {
		t.Fatalf("NewDeliverer() error = %v", err)
	}
	result, err := deliverer.Deliver(context.Background(), "https://hooks.example.com", SignedPayload{})
	if err == nil || result.Category != DeliveryHTTPFailure || result.StatusCode != http.StatusBadGateway {
		t.Fatalf("result/error = %#v / %v", result, err)
	}
	if strings.Contains(err.Error(), "upstream detail") {
		t.Fatalf("error disclosed response: %v", err)
	}
}

func TestPinnedDialContextUsesOnlyValidatedAddress(t *testing.T) {
	t.Parallel()
	server, client := net.Pipe()
	defer server.Close()
	defer client.Close()
	dialer := dialerReturning{connection: server}
	destination := ResolvedDestination{
		ParsedDestination{Hostname: "hooks.example.com", Port: "443"},
		netip.MustParseAddr("8.8.8.8"),
	}
	dial := pinnedDialContext(&dialer, destination)
	connection, err := dial(context.Background(), "tcp", "hooks.example.com:443")
	if err != nil {
		t.Fatalf("dial() error = %v", err)
	}
	if connection != server || dialer.address != "8.8.8.8:443" {
		t.Fatalf("connection/address = %v / %q", connection, dialer.address)
	}
	if _, err := dial(context.Background(), "tcp", "other.example.com:443"); err == nil {
		t.Fatal("dial() accepted an unvalidated host")
	}
}

type dialerReturning struct {
	connection net.Conn
	address    string
}

func (dialer *dialerReturning) DialContext(_ context.Context, _, address string) (net.Conn, error) {
	dialer.address = address
	return dialer.connection, nil
}
