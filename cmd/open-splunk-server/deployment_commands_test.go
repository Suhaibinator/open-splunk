package main

import (
	"bytes"
	"crypto/tls"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/auth"
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

func TestProvisionAdministratorTokenCreatesRuntimeCredential(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("A", auth.MinimumBrowserBearerTokenBytes))
	for _, sourceMode := range []os.FileMode{0o444, 0o644} {
		t.Run(fmt.Sprintf("source-%#o", sourceMode), func(t *testing.T) {
			t.Parallel()
			source := writeProvisioningTokenSource(t, append(token, '\n'), sourceMode)
			destinationDirectory := secureProvisioningDirectory(t)
			destination := filepath.Join(destinationDirectory, "administrator-token")
			if err := provisionAdministratorToken(source, destination); err != nil {
				t.Fatal(err)
			}
			got, err := readAdministratorToken(destination)
			if err != nil {
				t.Fatalf("read provisioned token: %v", err)
			}
			defer clear(got)
			if string(got) != string(token) {
				t.Fatal("provisioned token differs from source")
			}
			info, err := os.Lstat(destination)
			if err != nil {
				t.Fatal(err)
			}
			stat, ok := info.Sys().(*syscall.Stat_t)
			if !ok || stat == nil {
				t.Fatal("destination ownership is unavailable")
			}
			if info.Mode().Perm() != 0o600 || int(stat.Uid) != os.Geteuid() || stat.Nlink != 1 {
				t.Fatalf("destination state = (mode %#o, uid %d, links %d)", info.Mode().Perm(), stat.Uid, stat.Nlink)
			}
			entries, err := os.ReadDir(destinationDirectory)
			if err != nil {
				t.Fatal(err)
			}
			if len(entries) != 1 || entries[0].Name() != "administrator-token" {
				t.Fatalf("destination entries = %v, want only administrator-token", entries)
			}
		})
	}
}

func TestProvisionAdministratorTokenIsIdempotentAndConcurrent(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("B", auth.MinimumBrowserBearerTokenBytes))
	source := writeProvisioningTokenSource(t, token, 0o444)
	destination := secureProvisioningDestinationPath(t)

	const callers = 24
	start := make(chan struct{})
	errorsByCaller := make([]error, callers)
	var wait sync.WaitGroup
	wait.Add(callers)
	for index := range callers {
		go func() {
			defer wait.Done()
			<-start
			errorsByCaller[index] = provisionAdministratorToken(source, destination)
		}()
	}
	close(start)
	wait.Wait()
	for index, err := range errorsByCaller {
		if err != nil {
			t.Errorf("concurrent caller %d: %v", index, err)
		}
	}
	if err := provisionAdministratorToken(source, destination); err != nil {
		t.Fatalf("idempotent repeat: %v", err)
	}
}

func TestProvisionAdministratorTokenPausedPublisherLosesIdempotently(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("P", auth.MinimumBrowserBearerTokenBytes))
	source := writeProvisioningTokenSource(t, token, 0o444)
	destination := secureProvisioningDestinationPath(t)
	paused := make(chan struct{})
	release := make(chan struct{})
	pausedResult := make(chan error, 1)
	go func() {
		pausedResult <- provisionAdministratorTokenWithHooks(
			source,
			destination,
			provisioningPublishHooks{
				beforeRename: func() {
					close(paused)
					<-release
				},
			},
		)
	}()
	<-paused
	winningErr := provisionAdministratorToken(source, destination)
	close(release)
	pausedErr := <-pausedResult
	if winningErr != nil {
		t.Fatalf("unpaused publisher: %v", winningErr)
	}
	if pausedErr != nil {
		t.Fatalf("paused publisher: %v", pausedErr)
	}
	entries, err := os.ReadDir(filepath.Dir(destination))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(destination) {
		t.Fatalf("post-contention entries = %v, want only destination", entries)
	}
}

func TestProvisionAdministratorTokenInterruptionAfterAtomicRenameIsRecoverable(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("Q", auth.MinimumBrowserBearerTokenBytes))
	source := writeProvisioningTokenSource(t, token, 0o444)
	destination := secureProvisioningDestinationPath(t)
	interrupted := errors.New("simulated interruption after atomic rename")
	err := provisionAdministratorTokenWithHooks(
		source,
		destination,
		provisioningPublishHooks{
			afterRename: func() error { return interrupted },
		},
	)
	if !errors.Is(err, interrupted) {
		t.Fatalf("interrupted publication error = %v", err)
	}

	info, err := os.Lstat(destination)
	if err != nil {
		t.Fatalf("destination missing after rename: %v", err)
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || stat == nil || stat.Nlink != 1 {
		t.Fatalf("interrupted destination link state = %#v, want one link", stat)
	}
	if _, err := readAdministratorToken(destination); err != nil {
		t.Fatalf("interrupted destination is not runtime-safe: %v", err)
	}
	if err := provisionAdministratorToken(source, destination); err != nil {
		t.Fatalf("idempotent recovery after interruption: %v", err)
	}
	entries, err := os.ReadDir(filepath.Dir(destination))
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != filepath.Base(destination) {
		t.Fatalf("post-interruption entries = %v, want only destination", entries)
	}
}

func TestProvisionAdministratorTokenRejectsDifferentExistingTokenWithoutOverwrite(t *testing.T) {
	t.Parallel()
	sourceToken := []byte(strings.Repeat("C", auth.MinimumBrowserBearerTokenBytes))
	existingToken := []byte(strings.Repeat("D", auth.MinimumBrowserBearerTokenBytes))
	source := writeProvisioningTokenSource(t, sourceToken, 0o444)
	destination := secureProvisioningDestinationPath(t)
	if err := os.WriteFile(destination, existingToken, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := provisionAdministratorToken(source, destination); err == nil ||
		!strings.Contains(err.Error(), "different") {
		t.Fatalf("different existing token error = %v", err)
	}
	got, err := os.ReadFile(destination)
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(existingToken) {
		t.Fatal("different existing destination was overwritten")
	}
}

func TestProvisionAdministratorTokenAcceptsEquivalentExistingToken(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("E", auth.MinimumBrowserBearerTokenBytes))
	source := writeProvisioningTokenSource(t, append(token, '\n'), 0o444)
	destination := secureProvisioningDestinationPath(t)
	if err := os.WriteFile(destination, append(token, '\r', '\n'), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := provisionAdministratorToken(source, destination); err != nil {
		t.Fatal(err)
	}
}

func TestProvisionAdministratorTokenRejectsUnsafeSource(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("F", auth.MinimumBrowserBearerTokenBytes))

	for name, mode := range map[string]os.FileMode{
		"owner only":     0o600,
		"group writable": 0o664,
		"world writable": 0o646,
		"executable":     0o744,
		"special":        os.ModeSticky | 0o444,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			source := writeProvisioningTokenSource(t, token, mode)
			if err := provisionAdministratorToken(source, filepath.Join(t.TempDir(), "token")); err == nil {
				t.Fatalf("unsafe source mode %#o succeeded", mode)
			}
		})
	}

	t.Run("symlink", func(t *testing.T) {
		t.Parallel()
		target := writeProvisioningTokenSource(t, token, 0o444)
		link := filepath.Join(t.TempDir(), "source")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if err := provisionAdministratorToken(link, filepath.Join(t.TempDir(), "token")); err == nil {
			t.Fatal("symlink source succeeded")
		}
	})

	t.Run("hard link", func(t *testing.T) {
		t.Parallel()
		source := writeProvisioningTokenSource(t, token, 0o444)
		if err := os.Link(source, source+".link"); err != nil {
			t.Fatal(err)
		}
		if err := provisionAdministratorToken(source, filepath.Join(t.TempDir(), "token")); err == nil {
			t.Fatal("hard-linked source succeeded")
		}
	})
}

func TestProvisionAdministratorTokenRejectsSourceReplacementRace(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("G", auth.MinimumBrowserBearerTokenBytes))
	source := writeProvisioningTokenSource(t, token, 0o444)
	replacement := writeProvisioningTokenSource(t, token, 0o444)
	original := source + ".original"

	_, err := readProvisioningAdministratorTokenWithHooks(source, provisioningTokenReadHooks{
		afterOpen: func() {
			if renameErr := os.Rename(source, original); renameErr != nil {
				t.Fatalf("move source: %v", renameErr)
			}
			if renameErr := os.Rename(replacement, source); renameErr != nil {
				t.Fatalf("publish replacement: %v", renameErr)
			}
		},
	})
	if err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("source replacement error = %v", err)
	}
}

func TestProvisionAdministratorTokenRequiresSecureExistingParent(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("H", auth.MinimumBrowserBearerTokenBytes))
	source := writeProvisioningTokenSource(t, token, 0o444)

	missingParent := filepath.Join(t.TempDir(), "missing", "administrator-token")
	if err := provisionAdministratorToken(source, missingParent); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing parent error = %v, want os.ErrNotExist", err)
	}

	unsafeParent := secureProvisioningDirectory(t)
	if err := os.Chmod(unsafeParent, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := provisionAdministratorToken(source, filepath.Join(unsafeParent, "token")); err == nil {
		t.Fatal("world-accessible parent succeeded")
	}

	secureParent := secureProvisioningDirectory(t)
	info, err := os.Lstat(secureParent)
	if err != nil {
		t.Fatal(err)
	}
	if err := validateProvisioningDestinationDirectory(info, os.Geteuid()^1); err == nil {
		t.Fatal("parent owned by another uid succeeded")
	}

	realParent := secureProvisioningDirectory(t)
	parentLink := filepath.Join(t.TempDir(), "destination")
	if err := os.Symlink(realParent, parentLink); err != nil {
		t.Fatal(err)
	}
	if err := provisionAdministratorToken(source, filepath.Join(parentLink, "token")); err == nil {
		t.Fatal("symlink destination parent succeeded")
	}
}

func TestProvisionAdministratorTokenRejectsUnsafeExistingDestination(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("I", auth.MinimumBrowserBearerTokenBytes))
	source := writeProvisioningTokenSource(t, token, 0o444)

	for name, prepare := range map[string]func(string) error{
		"world readable": func(path string) error {
			return os.WriteFile(path, token, 0o644)
		},
		"symlink": func(path string) error {
			target := filepath.Join(t.TempDir(), "target")
			if err := os.WriteFile(target, token, 0o600); err != nil {
				return err
			}
			return os.Symlink(target, path)
		},
		"directory": func(path string) error {
			return os.Mkdir(path, 0o700)
		},
		"hard link": func(path string) error {
			if err := os.WriteFile(path, token, 0o600); err != nil {
				return err
			}
			return os.Link(path, path+".link")
		},
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			destination := secureProvisioningDestinationPath(t)
			if err := prepare(destination); err != nil {
				t.Fatal(err)
			}
			if err := provisionAdministratorToken(source, destination); err == nil {
				t.Fatal("unsafe existing destination succeeded")
			}
		})
	}
}

func TestProvisionAdministratorTokenRejectsMalformedSourceWithoutDisclosure(t *testing.T) {
	t.Parallel()
	secret := strings.Repeat("Z", auth.MinimumBrowserBearerTokenBytes-1) + "!"
	source := writeProvisioningTokenSource(t, []byte(secret), 0o444)
	err := provisionAdministratorToken(source, filepath.Join(t.TempDir(), "token"))
	if err == nil {
		t.Fatal("malformed source succeeded")
	}
	if strings.Contains(err.Error(), secret) {
		t.Fatalf("error disclosed source token: %v", err)
	}
}

func TestRunProvisionAdministratorTokenSubcommand(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("J", auth.MinimumBrowserBearerTokenBytes))
	source := writeProvisioningTokenSource(t, token, 0o444)
	destination := secureProvisioningDestinationPath(t)
	handled, err := runDeploymentSubcommand([]string{
		"provision-administrator-token",
		"-source", source,
		"-destination", destination,
	})
	if err != nil || !handled {
		t.Fatalf("provision dispatch = (%v, %v), want (true, nil)", handled, err)
	}
	if _, err := readAdministratorToken(destination); err != nil {
		t.Fatalf("read provisioned token: %v", err)
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

func writeProvisioningTokenSource(
	t *testing.T,
	contents []byte,
	mode os.FileMode,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "administrator-token-source")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}

func secureProvisioningDirectory(t *testing.T) string {
	t.Helper()
	directory := t.TempDir()
	if err := os.Chmod(directory, 0o700); err != nil {
		t.Fatal(err)
	}
	return directory
}

func secureProvisioningDestinationPath(t *testing.T) string {
	t.Helper()
	return filepath.Join(secureProvisioningDirectory(t), "administrator-token")
}
