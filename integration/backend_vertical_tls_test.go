//go:build !windows

package integration_test

import (
	"crypto/tls"
	"net/http"
	"slices"
	"testing"
)

func TestWebSocketTLSConfigPinsHTTP11WithoutMutatingHTTPTransport(
	t *testing.T,
) {
	t.Parallel()
	transportConfig := &tls.Config{
		MinVersion: tls.VersionTLS12,
		NextProtos: []string{"h2", "http/1.1"},
	}
	client := &http.Client{Transport: &http.Transport{
		TLSClientConfig: transportConfig,
	}}
	config, err := webSocketTLSConfig(client)
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(config.NextProtos, []string{"http/1.1"}) {
		t.Fatalf("WebSocket ALPN protocols = %v", config.NextProtos)
	}
	if config.MinVersion != tls.VersionTLS12 {
		t.Fatalf("WebSocket minimum TLS version = %x", config.MinVersion)
	}
	if !slices.Equal(transportConfig.NextProtos, []string{"h2", "http/1.1"}) {
		t.Fatalf("HTTP transport ALPN protocols mutated to %v", transportConfig.NextProtos)
	}
}

func TestWebSocketTLSConfigRequiresTrustedHTTPTransport(t *testing.T) {
	t.Parallel()
	for name, client := range map[string]*http.Client{
		"nil client":         nil,
		"default transport":  {},
		"untyped transport":  {Transport: roundTripFunc(nil)},
		"missing TLS config": {Transport: &http.Transport{}},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := webSocketTLSConfig(client); err == nil {
				t.Fatal("webSocketTLSConfig unexpectedly succeeded")
			}
		})
	}
}

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(
	request *http.Request,
) (*http.Response, error) {
	return function(request)
}
