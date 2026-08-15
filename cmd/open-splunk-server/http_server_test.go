package main

import (
	"crypto/tls"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

func TestLoadHTTPServerTLSConfigLoadsPrevalidatedIdentity(t *testing.T) {
	t.Parallel()
	identity, err := testsupport.WriteServerTLSIdentity(
		t.TempDir(),
		"localhost",
		"127.0.0.1",
	)
	if err != nil {
		t.Fatal(err)
	}
	config, err := loadHTTPServerTLSConfig(
		identity.CertificateFile,
		identity.PrivateKeyFile,
	)
	if err != nil {
		t.Fatal(err)
	}
	if config.MinVersion != tls.VersionTLS12 {
		t.Fatalf("minimum HTTP TLS version = %x, want TLS 1.2", config.MinVersion)
	}
	if config.InsecureSkipVerify {
		t.Fatal("HTTP TLS server config unexpectedly skips verification")
	}
	if len(config.Certificates) != 1 {
		t.Fatalf("loaded HTTP TLS certificates = %d, want 1", len(config.Certificates))
	}

	plaintext, err := loadHTTPServerTLSConfig("", "")
	if err != nil || plaintext != nil {
		t.Fatalf("plaintext HTTP TLS config = (%v, %v), want (nil, nil)", plaintext, err)
	}
}

func TestLoadHTTPServerTLSConfigRejectsInvalidIdentity(t *testing.T) {
	t.Parallel()
	first, err := testsupport.WriteServerTLSIdentity(
		filepath.Join(t.TempDir(), "first"),
		"localhost",
	)
	if err != nil {
		t.Fatal(err)
	}
	second, err := testsupport.WriteServerTLSIdentity(
		filepath.Join(t.TempDir(), "second"),
		"localhost",
	)
	if err != nil {
		t.Fatal(err)
	}
	malformed := filepath.Join(t.TempDir(), "malformed.pem")
	if err := os.WriteFile(malformed, []byte("not PEM"), 0o600); err != nil {
		t.Fatal(err)
	}
	for name, files := range map[string][2]string{
		"certificate only": {first.CertificateFile, ""},
		"key only":         {"", first.PrivateKeyFile},
		"missing file":     {filepath.Join(t.TempDir(), "missing.crt"), first.PrivateKeyFile},
		"malformed":        {malformed, malformed},
		"mismatched pair":  {first.CertificateFile, second.PrivateKeyFile},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := loadHTTPServerTLSConfig(files[0], files[1]); err == nil ||
				!strings.Contains(err.Error(), "HTTP TLS") {
				t.Fatalf("invalid identity error = %v", err)
			}
		})
	}
}
