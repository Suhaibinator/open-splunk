package main

import (
	"bytes"
	"crypto/tls"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"fmt"
	"strings"
)

const maximumClickHouseCABundleBytes = 1 << 20

// clickHouseClientTLSProfile is the closed application policy produced by the
// trust loader. Callers cannot inject arbitrary tls.Config callbacks or shared
// mutable slices; each principal asks the profile for a fresh owned config.
type clickHouseClientTLSProfile struct {
	rootCAs    *x509.CertPool
	serverName string
}

// loadClickHouseClientTLSProfile builds an explicit trust profile for
// every native ClickHouse connection. Secure mode never falls back to the host
// trust store or derives a verification name from a mutable network address.
func loadClickHouseClientTLSProfile(
	secure bool,
	caFile string,
	serverName string,
) (*clickHouseClientTLSProfile, error) {
	caFile = strings.TrimSpace(caFile)
	serverName = strings.TrimSpace(serverName)
	if !secure {
		if caFile != "" || serverName != "" {
			return nil, errors.New(
				"configure ClickHouse TLS: CA file and server name require secure mode",
			)
		}
		return nil, nil
	}
	if caFile == "" || serverName == "" {
		return nil, errors.New(
			"configure ClickHouse TLS: secure mode requires an explicit CA file and server name",
		)
	}
	if err := validateClickHouseTLSServerName(serverName); err != nil {
		return nil, err
	}
	bundle, err := readBoundedClickHouseCABundle(caFile)
	if err != nil {
		return nil, err
	}
	roots, err := parseClickHouseCABundle(bundle)
	if err != nil {
		return nil, fmt.Errorf("configure ClickHouse TLS: parse CA file: %w", err)
	}
	return &clickHouseClientTLSProfile{
		rootCAs:    roots,
		serverName: serverName,
	}, nil
}

func readBoundedClickHouseCABundle(path string) ([]byte, error) {
	return readBoundedCABundleFile(
		path,
		"configure ClickHouse TLS",
		"CA file",
		maximumClickHouseCABundleBytes,
		sameClickHouseCredentialFileState,
	)
}

func parseClickHouseCABundle(bundle []byte) (*x509.CertPool, error) {
	remainder := bytes.TrimSpace(bundle)
	if len(remainder) == 0 {
		return nil, errors.New("CA bundle contains no certificates")
	}
	roots := x509.NewCertPool()
	certificates := 0
	for len(remainder) != 0 {
		if !bytes.HasPrefix(remainder, []byte("-----BEGIN CERTIFICATE-----")) {
			return nil, errors.New(
				"CA bundle contains data outside CERTIFICATE PEM blocks",
			)
		}
		block, rest := pem.Decode(remainder)
		if block == nil || block.Type != "CERTIFICATE" || len(block.Headers) != 0 {
			return nil, errors.New(
				"CA bundle contains an invalid CERTIFICATE PEM block",
			)
		}
		certificate, err := x509.ParseCertificate(block.Bytes)
		if err != nil {
			return nil, fmt.Errorf("invalid certificate: %w", err)
		}
		roots.AddCert(certificate)
		certificates++
		remainder = bytes.TrimSpace(rest)
	}
	if certificates == 0 {
		return nil, errors.New("CA bundle contains no certificates")
	}
	return roots, nil
}

func validateClickHouseTLSServerName(serverName string) error {
	if !validExplicitTLSServerName(serverName) {
		return errors.New(
			"configure ClickHouse TLS: server name must be a valid bounded DNS name or IP address",
		)
	}
	return nil
}

func validateClickHouseClientTLSProfile(
	secure bool,
	profile *clickHouseClientTLSProfile,
) error {
	if !secure {
		if profile != nil {
			return errors.New(
				"configure ClickHouse: TLS profile requires secure mode",
			)
		}
		return nil
	}
	if profile == nil {
		return errors.New(
			"configure ClickHouse: secure mode requires a TLS profile",
		)
	}
	if profile.rootCAs == nil || profile.rootCAs.Equal(x509.NewCertPool()) {
		return errors.New(
			"configure ClickHouse: explicit certificate verification is required",
		)
	}
	if err := validateClickHouseTLSServerName(profile.serverName); err != nil {
		return err
	}
	return nil
}

func (profile *clickHouseClientTLSProfile) newConfig() *tls.Config {
	if profile == nil {
		return nil
	}
	return &tls.Config{
		MinVersion: tls.VersionTLS12,
		RootCAs:    profile.rootCAs.Clone(),
		ServerName: profile.serverName,
	}
}
