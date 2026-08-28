package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestLoadClickHouseCredentialUsesFileOrInlineValue(t *testing.T) {
	t.Run("file", func(t *testing.T) {
		path := writeClickHouseCredentialFixture(t, "file-secret\n", 0o600)
		credential, err := loadClickHouseCredential(path, "")
		if err != nil {
			t.Fatal(err)
		}
		if credential != "file-secret" {
			t.Fatalf("loaded credential = %q, want file-secret", credential)
		}
	})

	t.Run("inline", func(t *testing.T) {
		credential, err := loadClickHouseCredential("", "inline-secret")
		if err != nil {
			t.Fatal(err)
		}
		if credential != "inline-secret" {
			t.Fatalf(
				"loaded credential = %q, want inline-secret",
				credential,
			)
		}
	})

	t.Run("ambiguous", func(t *testing.T) {
		const secret = "must-not-appear-in-errors"
		path := writeClickHouseCredentialFixture(t, secret, 0o600)
		credential, err := loadClickHouseCredential(
			path,
			secret,
		)
		if err == nil || credential != "" {
			t.Fatalf("ambiguous credential = (%q, %v), want failure", credential, err)
		}
		if strings.Contains(err.Error(), secret) {
			t.Fatal("ambiguous-credential error disclosed secret contents")
		}
	})

	t.Run("missing", func(t *testing.T) {
		credential, err := loadClickHouseCredential("", "")
		if err == nil || credential != "" {
			t.Fatalf("missing credential = (%q, %v), want failure", credential, err)
		}
	})
}

func TestReadClickHouseCredentialFileBoundsAndTerminator(t *testing.T) {
	for name, testCase := range map[string]struct {
		contents string
		want     string
		wantErr  bool
	}{
		"no terminator":     {contents: "opaque-secret", want: "opaque-secret"},
		"one terminator":    {contents: "opaque-secret\n", want: "opaque-secret"},
		"only terminator":   {contents: "\n", wantErr: true},
		"two terminators":   {contents: "opaque-secret\n\n", want: "opaque-secret\n"},
		"maximum":           {contents: strings.Repeat("x", maximumClickHouseCredentialBytes), want: strings.Repeat("x", maximumClickHouseCredentialBytes)},
		"maximum plus line": {contents: strings.Repeat("x", maximumClickHouseCredentialBytes) + "\n", want: strings.Repeat("x", maximumClickHouseCredentialBytes)},
		"oversized":         {contents: strings.Repeat("x", maximumClickHouseCredentialBytes+1), wantErr: true},
	} {
		t.Run(name, func(t *testing.T) {
			path := writeClickHouseCredentialFixture(t, testCase.contents, 0o600)
			credential, err := readClickHouseCredentialFile(path)
			if testCase.wantErr {
				if err == nil || credential != nil {
					t.Fatalf("read credential = (%q, %v), want failure", credential, err)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if string(credential) != testCase.want {
				t.Fatalf(
					"read credential = %q, want %q",
					credential,
					testCase.want,
				)
			}
		})
	}
}

func TestReadClickHouseCredentialFileRejectsUnsafeMetadata(t *testing.T) {
	t.Run("directory", func(t *testing.T) {
		credential, err := readClickHouseCredentialFile(t.TempDir())
		if err == nil || credential != nil {
			t.Fatalf("directory credential = (%q, %v), want failure", credential, err)
		}
	})

	t.Run("symlink", func(t *testing.T) {
		path := writeClickHouseCredentialFixture(t, "secret", 0o600)
		link := filepath.Join(t.TempDir(), "credential-link")
		if err := os.Symlink(path, link); err != nil {
			t.Fatal(err)
		}
		credential, err := readClickHouseCredentialFile(link)
		if err == nil || credential != nil {
			t.Fatalf("symlink credential = (%q, %v), want failure", credential, err)
		}
	})

	for name, mode := range map[string]os.FileMode{
		"owner execute": 0o700,
		"group write":   0o620,
		"other write":   0o602,
	} {
		t.Run(name, func(t *testing.T) {
			path := writeClickHouseCredentialFixture(t, "secret", mode)
			credential, err := readClickHouseCredentialFile(path)
			if err == nil || credential != nil {
				t.Fatalf("unsafe credential = (%q, %v), want failure", credential, err)
			}
		})
	}

	t.Run("hard link", func(t *testing.T) {
		path := writeClickHouseCredentialFixture(t, "secret", 0o600)
		link := filepath.Join(t.TempDir(), "credential-hard-link")
		if err := os.Link(path, link); err != nil {
			t.Skipf("hard links are unavailable: %v", err)
		}
		credential, err := readClickHouseCredentialFile(path)
		if err == nil || credential != nil {
			t.Fatalf("hard-linked credential = (%q, %v), want failure", credential, err)
		}
	})
}

func TestReadClickHouseCredentialFileRejectsReplacement(t *testing.T) {
	path := writeClickHouseCredentialFixture(t, "original-secret", 0o600)
	replacement := writeClickHouseCredentialFixture(t, "replacement-secret", 0o600)
	credential, err := readClickHouseCredentialFileWithHooks(
		path,
		clickHouseCredentialReadHooks{afterOpen: func() {
			if renameErr := os.Rename(replacement, path); renameErr != nil {
				t.Fatalf("replace credential during read: %v", renameErr)
			}
		}},
	)
	if err == nil || credential != nil {
		t.Fatalf("replaced credential = (%q, %v), want failure", credential, err)
	}
}

func TestNewClickHouseConnectionOptionsLoadsCredentialFiles(t *testing.T) {
	passwordFile := writeClickHouseCredentialFixture(
		t,
		"shared-file-secret",
		0o600,
	)

	results, err := newClickHouseConnectionOptions(options{
		clickhouseAddress:      "per-clickhouse:9000",
		clickhouseDatabase:     "open_splunk",
		clickhouseUsername:     "clickhouse",
		clickhousePasswordFile: passwordFile,
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if results.runtime.Auth.Password != "shared-file-secret" ||
		results.deletion.Auth.Password != "shared-file-secret" ||
		results.migration.Auth.Password != "shared-file-secret" {
		t.Fatal("ClickHouse connection options did not use file credentials")
	}
}

func writeClickHouseCredentialFixture(
	t *testing.T,
	contents string,
	mode os.FileMode,
) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "clickhouse.password")
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatal(err)
	}
	return path
}
