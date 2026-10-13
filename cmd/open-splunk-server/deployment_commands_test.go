package main

import (
	"bytes"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport"
	"golang.org/x/sys/unix"
)

func TestRunDeploymentSubcommandDispatchesBeforeRuntime(t *testing.T) {
	t.Parallel()

	url, identity := startDeploymentHealthTLSServer(t, func(response http.ResponseWriter, _ *http.Request) {
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok\n"))
	})
	handled, err := runDeploymentSubcommand([]string{
		"healthcheck",
		"-url", url,
		"-ca-cert", identity.CertificateFile,
		"-server-name", "healthcheck.test",
	})
	if err != nil || !handled {
		t.Fatalf("healthcheck dispatch = (%v, %v), want (true, nil)", handled, err)
	}

	handled, err = runDeploymentSubcommand([]string{"-verify-embedded-release"})
	if err != nil || handled {
		t.Fatalf("ordinary server dispatch = (%v, %v), want (false, nil)", handled, err)
	}

	handled, err = runDeploymentSubcommand([]string{"migrate-clickhouse", "-unknown"})
	if err == nil || !handled {
		t.Fatalf("migration dispatch = (%v, %v), want (true, error)", handled, err)
	}
}

func TestVersionSubcommandReportsDevelopmentIdentity(t *testing.T) {
	t.Parallel()
	var output bytes.Buffer
	if err := runVersionSubcommand(nil, &output); err != nil {
		t.Fatal(err)
	}
	if output.String() != "source_revision=development\n" {
		t.Fatalf("version output = %q", output.String())
	}
	if err := runVersionSubcommand([]string{"unexpected"}, &output); err == nil {
		t.Fatal("version accepted arguments")
	}
}

func TestDeploymentHealthcheckUsesExplicitTLSAndExactResponse(t *testing.T) {
	t.Parallel()

	url, identity := startDeploymentHealthTLSServer(t, func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/healthz" {
			t.Errorf("request path = %q, want /healthz", request.URL.Path)
		}
		response.Header().Set("Content-Type", "text/plain; charset=utf-8")
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok\n"))
	})
	if err := runDeploymentHealthcheck(deploymentHealthcheckOptions{
		URL:        url,
		CACertFile: identity.CertificateFile,
		ServerName: "healthcheck.test",
	}); err != nil {
		t.Fatal(err)
	}

	if err := runDeploymentHealthcheck(deploymentHealthcheckOptions{
		URL:        url,
		CACertFile: identity.CertificateFile,
		ServerName: "wrong-name.test",
	}); err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("wrong server name error = %v", err)
	}

	untrusted, err := testsupport.WriteServerTLSIdentity(t.TempDir(), "healthcheck.test")
	if err != nil {
		t.Fatal(err)
	}
	if err := runDeploymentHealthcheck(deploymentHealthcheckOptions{
		URL:        url,
		CACertFile: untrusted.CertificateFile,
		ServerName: "healthcheck.test",
	}); err == nil || !strings.Contains(err.Error(), "TLS") {
		t.Fatalf("untrusted CA error = %v", err)
	}
}

func TestDeploymentHealthcheckAcceptsReadinessEndpoint(t *testing.T) {
	t.Parallel()

	url, identity := startDeploymentHealthTLSServer(
		t,
		func(response http.ResponseWriter, request *http.Request) {
			if request.URL.Path != "/readyz" {
				t.Errorf("request path = %q, want /readyz", request.URL.Path)
			}
			response.WriteHeader(http.StatusOK)
			_, _ = response.Write([]byte("ok\n"))
		},
	)
	url = strings.TrimSuffix(url, "/healthz") + "/readyz"
	if err := runDeploymentHealthcheck(deploymentHealthcheckOptions{
		URL:        url,
		CACertFile: identity.CertificateFile,
		ServerName: "healthcheck.test",
	}); err != nil {
		t.Fatal(err)
	}
}

func TestDeploymentHealthcheckAcceptsPlaintextLoopbackReadiness(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/readyz" {
			t.Errorf("request path = %q, want /readyz", request.URL.Path)
		}
		response.WriteHeader(http.StatusOK)
		_, _ = response.Write([]byte("ok\n"))
	}))
	t.Cleanup(server.Close)

	handled, err := runDeploymentSubcommand([]string{
		"healthcheck",
		"-url", server.URL + "/readyz",
	})
	if err != nil || !handled {
		t.Fatalf("plaintext healthcheck dispatch = (%v, %v), want (true, nil)", handled, err)
	}
}

func TestDeploymentHealthcheckRejectsInvalidURLAndServerName(t *testing.T) {
	t.Parallel()
	for _, rawURL := range []string{
		"http://127.0.0.1:8080/healthz",
		"http://127.0.0.1:8080/readyz",
		"https://127.0.0.1:8080/healthz",
		"https://127.0.0.1:8080/readyz",
	} {
		if _, err := validateDeploymentHealthURL(rawURL); err != nil {
			t.Errorf("validateDeploymentHealthURL(%q): %v", rawURL, err)
		}
	}

	for name, rawURL := range map[string]string{
		"unsupported scheme": "ftp://127.0.0.1:8080/healthz",
		"non loopback":       "https://192.0.2.1:8080/healthz",
		"DNS even local":     "https://localhost:8080/healthz",
		"wrong path":         "https://127.0.0.1:8080/",
		"encoded path":       "https://127.0.0.1:8080/%68ealthz",
		"query":              "https://127.0.0.1:8080/healthz?ready=true",
		"fragment":           "https://127.0.0.1:8080/healthz#ready",
		"userinfo":           "https://user@127.0.0.1:8080/healthz",
		"surrounding space":  " https://127.0.0.1:8080/healthz ",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			if _, err := validateDeploymentHealthURL(rawURL); err == nil {
				t.Fatalf("validateDeploymentHealthURL(%q) succeeded", rawURL)
			}
		})
	}

	for _, serverName := range []string{
		"", " healthcheck.test", "healthcheck.test ", "bad_name", ".invalid", "invalid.",
		strings.Repeat("a", 254), "bad\nname",
	} {
		if err := validateDeploymentHealthServerName(serverName); err == nil {
			t.Errorf("server name %q succeeded", serverName)
		}
	}
}

func TestDeploymentHealthcheckRejectsTLSOptionsForPlaintext(t *testing.T) {
	t.Parallel()

	for name, options := range map[string]deploymentHealthcheckOptions{
		"CA certificate": {
			URL:        "http://127.0.0.1:8080/readyz",
			CACertFile: "/run/open-splunk/health-ca.pem",
		},
		"server name": {
			URL:        "http://127.0.0.1:8080/readyz",
			ServerName: "healthcheck.test",
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			err := runDeploymentHealthcheck(options)
			if err == nil || !strings.Contains(err.Error(), "only valid with HTTPS") {
				t.Fatalf("plaintext TLS option error = %v", err)
			}
		})
	}
}

func TestDeploymentHealthcheckRejectsRedirectStatusAndNonExactBody(t *testing.T) {
	t.Parallel()

	for name, handler := range map[string]http.HandlerFunc{
		"redirect": func(response http.ResponseWriter, request *http.Request) {
			http.Redirect(response, request, "https://127.0.0.1/healthz", http.StatusFound)
		},
		"wrong status": func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusNoContent)
		},
		"wrong body": func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusOK)
			_, _ = response.Write([]byte("ok"))
		},
		"oversized body": func(response http.ResponseWriter, _ *http.Request) {
			response.WriteHeader(http.StatusOK)
			_, _ = response.Write([]byte(strings.Repeat("x", 1024)))
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			url, identity := startDeploymentHealthTLSServer(t, handler)
			err := runDeploymentHealthcheck(deploymentHealthcheckOptions{
				URL:        url,
				CACertFile: identity.CertificateFile,
				ServerName: "healthcheck.test",
			})
			if err == nil {
				t.Fatal("invalid health response succeeded")
			}
		})
	}
}

func TestDeploymentHealthcheckAcceptsCertificatesOnly(t *testing.T) {
	t.Parallel()
	identity, err := testsupport.WriteServerTLSIdentity(t.TempDir(), "healthcheck.test")
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := os.ReadFile(identity.CertificateFile)
	if err != nil {
		t.Fatal(err)
	}
	privateKey, err := os.ReadFile(identity.PrivateKeyFile)
	if err != nil {
		t.Fatal(err)
	}
	invalidBundle := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.WriteFile(invalidBundle, append(certificate, privateKey...), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDeploymentHealthTLSConfig(invalidBundle, "healthcheck.test"); err == nil ||
		!strings.Contains(err.Error(), "CERTIFICATE") {
		t.Fatalf("certificate plus private key error = %v", err)
	}
	oversized := filepath.Join(t.TempDir(), "oversized.pem")
	if err := os.WriteFile(oversized, []byte(strings.Repeat("x", maximumDeploymentHealthCABundleBytes+1)), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDeploymentHealthTLSConfig(oversized, "healthcheck.test"); err == nil ||
		!strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("oversized CA error = %v", err)
	}

	symlink := filepath.Join(t.TempDir(), "ca.pem")
	if err := os.Symlink(identity.CertificateFile, symlink); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDeploymentHealthTLSConfig(symlink, "healthcheck.test"); err == nil ||
		!strings.Contains(err.Error(), "regular") {
		t.Fatalf("symlink CA error = %v", err)
	}

	fifo := filepath.Join(t.TempDir(), "ca.pem")
	if err := unix.Mkfifo(fifo, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := loadDeploymentHealthTLSConfig(fifo, "healthcheck.test"); err == nil ||
		!strings.Contains(err.Error(), "regular") {
		t.Fatalf("FIFO CA error = %v", err)
	}
}

func startDeploymentHealthTLSServer(
	t *testing.T,
	handler http.HandlerFunc,
) (string, *testsupport.ServerTLSIdentity) {
	t.Helper()
	identity, err := testsupport.WriteServerTLSIdentity(
		t.TempDir(),
		"healthcheck.test",
	)
	if err != nil {
		t.Fatal(err)
	}
	certificate, err := tls.LoadX509KeyPair(
		identity.CertificateFile,
		identity.PrivateKeyFile,
	)
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS12,
		Certificates: []tls.Certificate{certificate},
	}
	server.StartTLS()
	t.Cleanup(server.Close)
	return server.URL + "/healthz", identity
}

func TestAdministratorProvisioningCommandIsUnavailable(t *testing.T) {
	handled, err := runDeploymentSubcommand([]string{"provision-administrator-token", "-source", "/missing", "-destination", "/missing"})
	if handled || err != nil {
		t.Fatalf("removed command dispatched: handled=%v err=%v", handled, err)
	}
}
