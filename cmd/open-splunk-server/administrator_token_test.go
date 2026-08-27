package main

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/auth"
	"golang.org/x/sys/unix"
)

func TestAdministratorTokenEnvironmentIsValidatedAndDiscarded(t *testing.T) {
	valid := strings.Repeat("A", auth.MinimumBrowserBearerTokenBytes)

	t.Run("valid", func(t *testing.T) {
		t.Setenv(administratorTokenEnvironmentVariable, valid)
		authenticator, err := newAdministratorBrowserAuthenticator(
			"",
			"tenant",
			"owner",
		)
		if err != nil || authenticator == nil {
			t.Fatalf("environment authenticator = (%#v, %v)", authenticator, err)
		}
		if _, exists := os.LookupEnv(administratorTokenEnvironmentVariable); exists {
			t.Fatal("administrator token remained in the environment")
		}
	})

	t.Run("mutually exclusive", func(t *testing.T) {
		t.Setenv(administratorTokenEnvironmentVariable, valid)
		path := writeAdministratorTokenFile(t, []byte(valid), 0o600)
		authenticator, err := newAdministratorBrowserAuthenticator(
			path,
			"tenant",
			"owner",
		)
		if err == nil || authenticator != nil ||
			!strings.Contains(err.Error(), "mutually exclusive") {
			t.Fatalf("ambiguous authenticator = (%#v, %v)", authenticator, err)
		}
		if strings.Contains(err.Error(), valid) {
			t.Fatal("ambiguous-token error disclosed the token")
		}
		if _, exists := os.LookupEnv(administratorTokenEnvironmentVariable); exists {
			t.Fatal("ambiguous administrator token remained in the environment")
		}
	})

	t.Run("invalid", func(t *testing.T) {
		const invalid = "too-short"
		t.Setenv(administratorTokenEnvironmentVariable, invalid)
		authenticator, err := newAdministratorBrowserAuthenticator(
			"",
			"tenant",
			"owner",
		)
		if err == nil || authenticator != nil {
			t.Fatalf("invalid authenticator = (%#v, %v)", authenticator, err)
		}
		if strings.Contains(err.Error(), invalid) {
			t.Fatal("invalid-token error disclosed the token")
		}
		if _, exists := os.LookupEnv(administratorTokenEnvironmentVariable); exists {
			t.Fatal("invalid administrator token remained in the environment")
		}
	})
}

func TestRunRejectsInvalidAdministratorEnvironmentBeforeDurableState(t *testing.T) {
	directory := t.TempDir()
	controlDBPath := filepath.Join(directory, "control.db")
	t.Setenv(administratorTokenEnvironmentVariable, "invalid")
	t.Setenv(clickHousePasswordEnvironmentVariable, "clickhouse-password")

	err := runWithOptions(options{
		httpAddress:             "127.0.0.1:0",
		controlDBPath:           controlDBPath,
		clickhouseAddress:       "per-clickhouse:9000",
		clickhouseDatabase:      "open_splunk",
		clickhouseUsername:      "clickhouse",
		indexRetention:          time.Hour,
		tenantID:                "tenant",
		searchHistoryMaximumAge: 0,
	})
	if err == nil || !strings.Contains(err.Error(), "invalid bearer token") {
		t.Fatalf("runWithOptions() error = %v, want invalid administrator token", err)
	}
	if _, exists := os.LookupEnv(administratorTokenEnvironmentVariable); exists {
		t.Fatal("invalid administrator token remained in the environment")
	}
	for _, path := range []string{
		controlDBPath,
		controlDBPath + "-shm",
		controlDBPath + "-wal",
		controlDBPath + ".key",
		controlDBPath + ".exports",
	} {
		if _, statErr := os.Stat(path); !os.IsNotExist(statErr) {
			t.Fatalf("durable path %q exists after token preflight: %v", path, statErr)
		}
	}
}

func TestReadAdministratorTokenAcceptsOnlyOptionalLineTerminator(t *testing.T) {
	t.Parallel()
	token := strings.Repeat("A", auth.MinimumBrowserBearerTokenBytes)
	for name, suffix := range map[string]string{
		"none": "",
		"LF":   "\n",
		"CRLF": "\r\n",
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			for _, mode := range []os.FileMode{0o400, 0o600} {
				path := writeAdministratorTokenFile(t, []byte(token+suffix), mode)
				got, err := readAdministratorToken(path)
				if err != nil {
					t.Fatalf("readAdministratorToken(%#o): %v", mode, err)
				}
				defer clear(got)
				if string(got) != token {
					t.Fatalf("token = %q, want configured token", got)
				}
			}
		})
	}
}

func TestReadAdministratorTokenAcceptsMaximumTokenWithCRLF(t *testing.T) {
	t.Parallel()

	token := strings.Repeat("Z", auth.MaximumBrowserBearerTokenBytes)
	path := writeAdministratorTokenFile(t, []byte(token+"\r\n"), 0o600)
	got, err := readAdministratorToken(path)
	if err != nil {
		t.Fatal(err)
	}
	defer clear(got)
	if string(got) != token {
		t.Fatalf("maximum token length = %d, want %d", len(got), len(token))
	}
}

func TestReadAdministratorTokenRequiresPreprovisionedPath(t *testing.T) {
	t.Parallel()
	if _, err := readAdministratorToken(""); err == nil ||
		!strings.Contains(err.Error(), "file path is required") {
		t.Fatalf("empty path error = %v", err)
	}
	if _, err := readAdministratorToken("administrator\x00token"); err == nil ||
		!strings.Contains(err.Error(), "NUL") {
		t.Fatalf("NUL path error = %v", err)
	}
	path := filepath.Join(t.TempDir(), "missing-token")
	if _, err := readAdministratorToken(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing path error = %v, want os.ErrNotExist", err)
	}
	if _, err := os.Lstat(path); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("missing token path was created: %v", err)
	}
}

func TestReadAdministratorTokenRejectsMalformedContentsWithoutDisclosure(t *testing.T) {
	t.Parallel()
	valid := strings.Repeat("A", auth.MinimumBrowserBearerTokenBytes)
	secret := strings.Repeat("Z", auth.MinimumBrowserBearerTokenBytes-1) + "!"
	for name, contents := range map[string][]byte{
		"too short":       []byte(valid[:len(valid)-1]),
		"too long":        bytes.Repeat([]byte{'A'}, auth.MaximumBrowserBearerTokenBytes+1),
		"invalid token68": []byte(secret),
		"double LF":       []byte(valid + "\n\n"),
		"lone CR":         []byte(valid + "\r"),
		"embedded LF":     []byte(valid[:16] + "\n" + valid[16:]),
	} {
		testContents := append([]byte(nil), contents...)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			path := writeAdministratorTokenFile(t, testContents, 0o600)
			_, err := readAdministratorToken(path)
			if err == nil {
				t.Fatal("readAdministratorToken unexpectedly succeeded")
			}
			if strings.Contains(err.Error(), secret) ||
				strings.Contains(err.Error(), string(testContents)) {
				t.Fatalf("error disclosed token contents: %v", err)
			}
		})
	}
}

func TestReadAdministratorTokenRejectsUnsafeFilesystemObjects(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("A", auth.MinimumBrowserBearerTokenBytes))

	for name, mode := range map[string]os.FileMode{
		"group writable":   0o660,
		"world readable":   0o644,
		"executable":       0o700,
		"inaccessible":     0o000,
		"owner write only": 0o200,
	} {
		t.Run(name, func(t *testing.T) {
			path := writeAdministratorTokenFile(t, token, mode)
			if _, err := readAdministratorToken(path); err == nil {
				t.Fatalf("token file mode %#o succeeded", mode)
			}
		})
	}

	t.Run("hard link", func(t *testing.T) {
		path := writeAdministratorTokenFile(t, token, 0o600)
		if err := os.Link(path, path+".link"); err != nil {
			t.Fatal(err)
		}
		if _, err := readAdministratorToken(path); err == nil {
			t.Fatal("multiply linked token file succeeded")
		}
	})

	t.Run("symlink", func(t *testing.T) {
		target := writeAdministratorTokenFile(t, token, 0o600)
		link := filepath.Join(t.TempDir(), "administrator-token")
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}
		if _, err := readAdministratorToken(link); err == nil {
			t.Fatal("symlink token file succeeded")
		}
	})

	t.Run("directory", func(t *testing.T) {
		if _, err := readAdministratorToken(t.TempDir()); err == nil {
			t.Fatal("directory token path succeeded")
		}
	})

	t.Run("FIFO", func(t *testing.T) {
		path := filepath.Join(t.TempDir(), "administrator-token")
		if err := unix.Mkfifo(path, 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readAdministratorToken(path); err == nil {
			t.Fatal("FIFO token path succeeded")
		}
	})
}

func TestValidateAdministratorTokenFileRequiresCurrentOwner(t *testing.T) {
	t.Parallel()
	path := writeAdministratorTokenFile(
		t,
		[]byte(strings.Repeat("A", auth.MinimumBrowserBearerTokenBytes)),
		0o600,
	)
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	wrongUID := os.Geteuid() ^ 1
	if err := validateAdministratorTokenFile(info, wrongUID); err == nil {
		t.Fatal("token file owned by a different user succeeded")
	}
}

func TestReadAdministratorTokenRejectsPathReplacementRace(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("A", auth.MinimumBrowserBearerTokenBytes))
	path := writeAdministratorTokenFile(t, token, 0o600)
	replacement := writeAdministratorTokenFile(t, token, 0o600)
	original := path + ".original"

	_, err := readAdministratorTokenWithHooks(path, administratorTokenReadHooks{
		afterOpen: func() {
			if renameErr := os.Rename(path, original); renameErr != nil {
				t.Fatalf("move original token: %v", renameErr)
			}
			if renameErr := os.Rename(replacement, path); renameErr != nil {
				t.Fatalf("publish replacement token: %v", renameErr)
			}
		},
	})
	if err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("path replacement error = %v", err)
	}
}

func TestReadAdministratorTokenRejectsInodeMutationRace(t *testing.T) {
	t.Parallel()
	token := []byte(strings.Repeat("A", auth.MinimumBrowserBearerTokenBytes))
	path := writeAdministratorTokenFile(t, token, 0o600)

	_, err := readAdministratorTokenWithHooks(path, administratorTokenReadHooks{
		afterRead: func() {
			replacement := bytes.Repeat([]byte{'B'}, len(token))
			if writeErr := os.WriteFile(path, replacement, 0o600); writeErr != nil {
				t.Fatalf("mutate token: %v", writeErr)
			}
			changedAt := time.Now().Add(2 * time.Second)
			if chtimesErr := os.Chtimes(path, changedAt, changedAt); chtimesErr != nil {
				t.Fatalf("change token timestamp: %v", chtimesErr)
			}
		},
	})
	if err == nil || !strings.Contains(err.Error(), "changed") {
		t.Fatalf("inode mutation error = %v", err)
	}
}

func TestNewAdministratorBrowserAuthenticatorUsesConfiguredScope(t *testing.T) {
	t.Parallel()
	token := strings.Repeat("A", auth.MinimumBrowserBearerTokenBytes)
	path := writeAdministratorTokenFile(t, []byte(token+"\n"), 0o400)
	authenticator, err := newAdministratorBrowserAuthenticator(path, "tenant", "owner")
	if err != nil {
		t.Fatal(err)
	}
	principal, err := authenticator.Authenticate(context.Background(), []byte(token))
	if err != nil {
		t.Fatal(err)
	}
	if !principal.IsAdministrator() ||
		principal.TenantID() != "tenant" ||
		principal.OwnerID() != "owner" {
		t.Fatalf(
			"principal = (%q, %q, %s)",
			principal.TenantID(),
			principal.OwnerID(),
			principal.Role(),
		)
	}
}

func writeAdministratorTokenFile(
	t *testing.T,
	contents []byte,
	mode os.FileMode,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "administrator-token")
	if err := os.WriteFile(path, contents, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}
