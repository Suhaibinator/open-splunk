package main

import (
	"crypto/tls"
	"crypto/x509"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

func testClickHouseClientTLSProfile(t *testing.T) *clickHouseClientTLSProfile {
	t.Helper()
	identity, err := testsupport.WriteServerTLSIdentity(t.TempDir(), "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	profile, err := loadClickHouseClientTLSProfile(
		true,
		identity.CertificateFile,
		"clickhouse",
	)
	if err != nil {
		t.Fatal(err)
	}
	return profile
}

func TestLoadClickHouseClientTLSProfileUsesExplicitTrustAndServerName(
	t *testing.T,
) {
	t.Parallel()

	directory := t.TempDir()
	identity, err := testsupport.WriteServerTLSIdentity(
		directory,
		"clickhouse",
		"127.0.0.1",
	)
	if err != nil {
		t.Fatal(err)
	}
	profile, err := loadClickHouseClientTLSProfile(
		true,
		"  "+identity.CertificateFile+"  ",
		" clickhouse ",
	)
	if err != nil {
		t.Fatal(err)
	}
	config := profile.newConfig()
	if config == nil || config.MinVersion != tls.VersionTLS12 ||
		config.ServerName != "clickhouse" || config.RootCAs == nil ||
		config.InsecureSkipVerify || len(config.Certificates) != 0 {
		t.Fatalf("ClickHouse client TLS config = %#v", config)
	}
	if config.RootCAs.Equal(x509.NewCertPool()) {
		t.Fatal("ClickHouse client trust anchors are empty")
	}

	plaintext, err := loadClickHouseClientTLSProfile(false, "", "")
	if err != nil || plaintext != nil {
		t.Fatalf(
			"loadClickHouseClientTLSProfile(plaintext) = (%#v, %v), want (nil, nil)",
			plaintext,
			err,
		)
	}
}

func TestLoadClickHouseClientTLSProfileRejectsIncompleteOrUnsafeTrust(
	t *testing.T,
) {
	t.Parallel()

	directory := t.TempDir()
	identity, err := testsupport.WriteServerTLSIdentity(directory, "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	malformed := filepath.Join(directory, "malformed.pem")
	if err := os.WriteFile(malformed, []byte("not a PEM bundle\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	trailing := filepath.Join(directory, "trailing.pem")
	certificatePEM, err := os.ReadFile(identity.CertificateFile)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		trailing,
		append(append([]byte(nil), certificatePEM...), []byte("unexpected")...),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	oversized := filepath.Join(directory, "oversized.pem")
	if err := os.WriteFile(
		oversized,
		[]byte(strings.Repeat("x", maximumClickHouseCABundleBytes+1)),
		0o600,
	); err != nil {
		t.Fatal(err)
	}
	symlink := filepath.Join(directory, "symlink.pem")
	if err := os.Symlink(identity.CertificateFile, symlink); err != nil {
		t.Fatal(err)
	}

	for name, testCase := range map[string]struct {
		secure     bool
		caFile     string
		serverName string
	}{
		"CA without secure mode": {
			caFile: identity.CertificateFile,
		},
		"server name without secure mode": {
			serverName: "clickhouse",
		},
		"secure mode without CA": {
			secure: true, serverName: "clickhouse",
		},
		"secure mode without server name": {
			secure: true, caFile: identity.CertificateFile,
		},
		"missing CA file": {
			secure: true, caFile: filepath.Join(directory, "missing.pem"), serverName: "clickhouse",
		},
		"malformed CA bundle": {
			secure: true, caFile: malformed, serverName: "clickhouse",
		},
		"private key as CA bundle": {
			secure: true, caFile: identity.PrivateKeyFile, serverName: "clickhouse",
		},
		"trailing non-PEM data": {
			secure: true, caFile: trailing, serverName: "clickhouse",
		},
		"oversized CA bundle": {
			secure: true, caFile: oversized, serverName: "clickhouse",
		},
		"nonregular CA file": {
			secure: true, caFile: directory, serverName: "clickhouse",
		},
		"symlink CA file": {
			secure: true, caFile: symlink, serverName: "clickhouse",
		},
		"server name contains port": {
			secure: true, caFile: identity.CertificateFile, serverName: "clickhouse:9440",
		},
		"server name contains wildcard": {
			secure: true, caFile: identity.CertificateFile, serverName: "*.internal",
		},
		"server name contains control": {
			secure: true, caFile: identity.CertificateFile, serverName: "clickhouse\ninternal",
		},
		"server name has empty label": {
			secure: true, caFile: identity.CertificateFile, serverName: "clickhouse..internal",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			profile, loadErr := loadClickHouseClientTLSProfile(
				testCase.secure,
				testCase.caFile,
				testCase.serverName,
			)
			if loadErr == nil || profile != nil {
				t.Fatalf(
					"loadClickHouseClientTLSProfile() = (%#v, %v), want failure",
					profile,
					loadErr,
				)
			}
		})
	}
}

func TestNewClickHouseConnectionOptionsOwnsAuthenticatedTLSConfigs(
	t *testing.T,
) {
	t.Setenv(clickHousePasswordEnvironmentVariable, "shared-password")
	tlsProfile := testClickHouseClientTLSProfile(t)

	results, err := newClickHouseConnectionOptions(options{
		clickhouseAddress:  "clickhouse:9440",
		clickhouseDatabase: "open_splunk",
		clickhouseUsername: "shared-user",
		clickhouseSecure:   true,
	}, tlsProfile)
	if err != nil {
		t.Fatal(err)
	}
	configs := []*tls.Config{
		results.runtime.TLS,
		results.deletion.TLS,
		results.migration.TLS,
	}
	for index, config := range configs {
		if config == nil ||
			config.MinVersion != tls.VersionTLS12 ||
			config.ServerName != "clickhouse" ||
			config.RootCAs == nil || config.RootCAs == tlsProfile.rootCAs ||
			config.RootCAs.Equal(x509.NewCertPool()) ||
			config.InsecureSkipVerify {
			t.Fatalf("ClickHouse TLS config %d = %#v", index, config)
		}
	}
	if configs[0] == configs[1] || configs[0] == configs[2] ||
		configs[1] == configs[2] ||
		configs[0].RootCAs == configs[1].RootCAs ||
		configs[0].RootCAs == configs[2].RootCAs ||
		configs[1].RootCAs == configs[2].RootCAs {
		t.Fatal("ClickHouse principals share a mutable TLS config")
	}
	configs[0].ServerName = "mutated.invalid"
	if configs[1].ServerName != "clickhouse" ||
		configs[2].ServerName != "clickhouse" {
		t.Fatal("mutating one principal changed another principal's TLS config")
	}
}

func TestNewClickHouseConnectionOptionsRejectsMismatchedTLSMode(t *testing.T) {
	t.Setenv(clickHousePasswordEnvironmentVariable, "shared-password")
	valid := options{
		clickhouseAddress:  "127.0.0.1:9000",
		clickhouseDatabase: "open_splunk",
		clickhouseUsername: "shared-user",
	}
	tlsProfile := testClickHouseClientTLSProfile(t)

	for name, testCase := range map[string]struct {
		secure     bool
		tlsProfile *clickHouseClientTLSProfile
	}{
		"plaintext with TLS profile": {tlsProfile: tlsProfile},
		"secure without TLS profile": {secure: true},
	} {
		t.Run(name, func(t *testing.T) {
			config := valid
			config.clickhouseSecure = testCase.secure
			result, optionsErr := newClickHouseConnectionOptions(
				config,
				testCase.tlsProfile,
			)
			if optionsErr == nil || result.runtime != nil ||
				result.deletion != nil || result.migration != nil {
				t.Fatalf(
					"newClickHouseConnectionOptions() = (%#v, %v), want failure",
					result,
					optionsErr,
				)
			}
		})
	}
}

func TestRunRejectsClickHouseTLSBeforeCreatingDurableState(t *testing.T) {
	directory := t.TempDir()
	controlDBPath := filepath.Join(directory, "control.db")
	malformedCA := filepath.Join(directory, "malformed-ca.pem")
	if err := os.WriteFile(malformedCA, []byte("not a certificate\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	err := runWithOptions(options{
		httpAddress:             "127.0.0.1:0",
		controlDBPath:           controlDBPath,
		clickhouseSecure:        true,
		clickhouseCACertFile:    malformedCA,
		clickhouseServerName:    "clickhouse",
		indexRetention:          time.Hour,
		tenantID:                "tenant",
		searchHistoryMaximumAge: 0,
	})
	if err == nil || !strings.Contains(err.Error(), "parse CA file") {
		t.Fatalf("runWithOptions() error = %v, want ClickHouse CA error", err)
	}
	for _, path := range []string{
		controlDBPath,
		controlDBPath + "-shm",
		controlDBPath + "-wal",
		controlDBPath + ".key",
		controlDBPath + ".exports",
	} {
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Fatalf("durable path %q exists after TLS preflight: %v", path, statErr)
		}
	}
}
