//go:build linux

package main

import (
	"context"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRecoveryDrillReadinessFollowsRestartedServerPort(t *testing.T) {
	t.Parallel()
	previous := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {}))
	current := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
	}))
	defer current.Close()
	previous.Close()
	fixture := &recoveryDrill{client: current.Client()}
	resolutions := 0
	resolve := func(context.Context) (string, error) {
		resolutions++
		if resolutions == 1 {
			return strings.TrimPrefix(previous.URL, "https://"), nil
		}
		return strings.TrimPrefix(current.URL, "https://"), nil
	}
	if fixture.readinessAttempt(t.Context(), resolve) {
		t.Fatal("the stopped server endpoint must not be ready")
	}
	if !fixture.readinessAttempt(t.Context(), resolve) {
		t.Fatalf("readiness did not follow the restarted server's current TLS port: %s", fixture.lastReadiness)
	}
	if resolutions != 2 || fixture.baseURL != current.URL {
		t.Fatalf("ready endpoint = %q after %d resolutions, want %q after two", fixture.baseURL, resolutions, current.URL)
	}
}

func TestRecoveryDrillReadinessRequiresVerifiedHTTPS200(t *testing.T) {
	t.Parallel()
	for _, name := range []string{"port lookup failure", "untrusted TLS", "HTTP unavailable"} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			server := httptest.NewTLSServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
				if name == "untrusted TLS" {
					response.WriteHeader(http.StatusOK)
					return
				}
				response.WriteHeader(http.StatusServiceUnavailable)
			}))
			defer server.Close()
			client := server.Client()
			if name == "untrusted TLS" {
				transport := client.Transport.(*http.Transport).Clone()
				transport.TLSClientConfig.RootCAs = x509.NewCertPool()
				client.Transport = transport
			}
			defer client.CloseIdleConnections()
			fixture := &recoveryDrill{client: client}
			if fixture.readinessAttempt(t.Context(), func(context.Context) (string, error) {
				if name == "port lookup failure" {
					return "", errors.New("container is restarting")
				}
				return strings.TrimPrefix(server.URL, "https://"), nil
			}) {
				t.Fatal("unverified or unavailable endpoint reported ready")
			}
			if fixture.baseURL != "" || fixture.lastReadiness == "" {
				t.Fatalf("failure published endpoint %q or discarded diagnostics %q", fixture.baseURL, fixture.lastReadiness)
			}
		})
	}
}
