package main

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/testsupport"
)

func TestCABundleLoadersEnforceCustody(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		mode      os.FileMode
		wantError bool
	}{
		{mode: 0o400}, {mode: 0o600}, {mode: 0o440},
		{mode: 0o640}, {mode: 0o444}, {mode: 0o644},
		{mode: 0o620, wantError: true}, {mode: 0o602, wantError: true},
		{mode: 0o666, wantError: true}, {mode: 0o700, wantError: true},
	} {
		t.Run(test.mode.String(), func(t *testing.T) {
			t.Parallel()
			identity, err := testsupport.WriteServerTLSIdentity(t.TempDir(), "clickhouse")
			if err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(identity.CertificateFile, test.mode); err != nil {
				t.Fatal(err)
			}
			profile, err := loadClickHouseClientTLSProfile(true, identity.CertificateFile, "clickhouse")
			if (err != nil) != test.wantError || (profile == nil) != test.wantError {
				t.Fatalf("ClickHouse profile for mode %o: profile=%v err=%v", test.mode, profile, err)
			}
			config, err := loadDeploymentHealthTLSConfig(identity.CertificateFile, "clickhouse")
			if (err != nil) != test.wantError || (config == nil) != test.wantError {
				t.Fatalf("healthcheck config for mode %o: config=%v err=%v", test.mode, config, err)
			}
		})
	}
}

func TestValidateCABundleFileMetadata(t *testing.T) {
	t.Parallel()
	path := writeClickHouseCredentialFixture(t, "fixture", 0o644)
	info, err := os.Lstat(path)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name      string
		uid       uint32
		mode      os.FileMode
		links     uint64
		missing   bool
		wantError bool
	}{
		{name: "root readable", uid: 0, mode: 0o444, links: 1},
		{name: "service writable", uid: 1000, mode: 0o644, links: 1},
		{name: "foreign owner", uid: 1001, mode: 0o444, links: 1, wantError: true},
		{name: "missing metadata", mode: 0o444, missing: true, wantError: true},
		{name: "extra hard link", mode: 0o444, links: 2, wantError: true},
		{name: "unlinked", mode: 0o444, wantError: true},
		{name: "setuid", mode: 0o444 | os.ModeSetuid, links: 1, wantError: true},
		{name: "setgid", mode: 0o444 | os.ModeSetgid, links: 1, wantError: true},
		{name: "sticky", mode: 0o444 | os.ModeSticky, links: 1, wantError: true},
		{name: "not owner readable", mode: 0o044, links: 1, wantError: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			stat := *info.Sys().(*syscall.Stat_t)
			stat.Uid = test.uid
			// Darwin uses a narrower link-count field than Linux.
			stat.Nlink = 1
			if test.links == 0 {
				stat.Nlink = 0
			}
			if test.links == 2 {
				stat.Nlink = 2
			}
			var metadata any = &stat
			if test.missing {
				metadata = nil
			}
			fixture := caBundleFileInfo{FileInfo: info, mode: test.mode, metadata: metadata}
			if err := validateCABundleFile(fixture, 1000); (err != nil) != test.wantError {
				t.Fatalf("metadata validation error = %v, wantError=%v", err, test.wantError)
			}
		})
	}
	if err := validateCABundleFile(info, -1); err == nil {
		t.Fatal("unknown effective user accepted")
	}
}

func TestReadBoundedCABundleRejectsCustodyChanges(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"before", "after open", "after read"} {
		for _, mutation := range []string{"write permissions", "hard link", "replacement"} {
			t.Run(phase+"/"+mutation, func(t *testing.T) {
				t.Parallel()
				path := writeClickHouseCredentialFixture(t, "fixture", 0o644)
				mutate := func() {
					var err error
					switch mutation {
					case "write permissions":
						err = os.Chmod(path, 0o646)
					case "hard link":
						err = os.Link(path, path+".link")
					case "replacement":
						replacement := writeClickHouseCredentialFixture(t, "substitute", 0o666)
						err = os.Rename(replacement, path)
					}
					if err != nil {
						t.Fatal(err)
					}
				}
				hooks := stablePathFileReadHooks{}
				switch phase {
				case "before":
					mutate()
				case "after open":
					hooks.afterOpen = mutate
				case "after read":
					hooks.afterRead = mutate
				}
				contents, err := readBoundedCABundleFileWithHooks(path, "test TLS", "CA file", 1024, hooks)
				if err == nil || contents != nil {
					t.Fatalf("changed trust file returned contents=%q err=%v", contents, err)
				}
			})
		}
	}
}

func TestCABundleRejectsBeforeStartupState(t *testing.T) {
	identity, err := testsupport.WriteServerTLSIdentity(t.TempDir(), "clickhouse")
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(identity.CertificateFile, 0o666); err != nil {
		t.Fatal(err)
	}
	controlPath := filepath.Join(t.TempDir(), "control.db")
	err = runWithOptions(options{
		httpAddress:          "127.0.0.1:0",
		clickhouseSecure:     true,
		clickhouseCACertFile: identity.CertificateFile,
		clickhouseServerName: "clickhouse",
		controlDBPath:        controlPath,
		indexRetention:       time.Hour,
		tenantID:             "tenant",
	})
	if err == nil || !strings.Contains(err.Error(), "permissions") {
		t.Fatalf("startup error = %v", err)
	}
	if _, err := os.Stat(controlPath); !os.IsNotExist(err) {
		t.Fatalf("startup created state: %v", err)
	}
}

type caBundleFileInfo struct {
	os.FileInfo
	mode     os.FileMode
	metadata any
}

func (info caBundleFileInfo) Mode() os.FileMode { return info.mode }
func (info caBundleFileInfo) Sys() any          { return info.metadata }
