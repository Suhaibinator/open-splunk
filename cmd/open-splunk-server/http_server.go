package main

import (
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

type httpRuntimeServer struct {
	*http.Server
}

func (server *httpRuntimeServer) ListenAndServe() error {
	if server.TLSConfig == nil {
		return server.Server.ListenAndServe()
	}
	// The identity has already been loaded into TLSConfig. Empty paths prevent
	// net/http from reopening mutable key material after startup validation.
	return server.ListenAndServeTLS("", "")
}

// loadHTTPServerTLSConfig returns nil for the explicit loopback-plaintext
// development mode. A configured HTTPS identity is loaded before either
// persistence plane opens so an unreadable or mismatched key cannot fail after
// startup has mutated durable state.
func loadHTTPServerTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	certFile = strings.TrimSpace(certFile)
	keyFile = strings.TrimSpace(keyFile)
	if certFile == "" && keyFile == "" {
		return nil, nil
	}
	if certFile == "" || keyFile == "" {
		return nil, errors.New("HTTP TLS certificate and key must be configured together")
	}
	config, err := loadServerTLSConfig(certFile, keyFile)
	if err != nil {
		return nil, fmt.Errorf("load HTTP TLS certificate: %w", err)
	}
	return config, nil
}
