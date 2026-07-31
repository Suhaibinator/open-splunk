package main

import "crypto/tls"

// loadServerTLSConfig is the shared minimum security baseline for inbound
// browser/API and collector listeners.
func loadServerTLSConfig(certFile, keyFile string) (*tls.Config, error) {
	certificate, err := tls.LoadX509KeyPair(certFile, keyFile)
	if err != nil {
		return nil, err
	}
	return &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	}, nil
}
