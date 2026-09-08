//go:build linux

package main

import (
	"errors"
	"strings"
	"testing"

	"golang.org/x/sys/unix"
)

func TestReadBoundedCABundleRejectsLinuxACL(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"before", "after open", "after read"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			path := writeClickHouseCredentialFixture(t, "fixture", 0o600)
			installACL := func() {
				// The zero mask retains an extended ACL without adding mode-bit
				// writes, so this exercises the descriptor ACL check itself.
				acl := []byte{
					2, 0, 0, 0,
					1, 0, 6, 0, 0xff, 0xff, 0xff, 0xff,
					4, 0, 0, 0, 0xff, 0xff, 0xff, 0xff,
					16, 0, 0, 0, 0xff, 0xff, 0xff, 0xff,
					32, 0, 0, 0, 0xff, 0xff, 0xff, 0xff,
				}
				err := unix.Setxattr(path, "system.posix_acl_access", acl, 0)
				if errors.Is(err, unix.ENOTSUP) {
					t.Skipf("ACL fixture unsupported: %v", err)
				}
				if err != nil {
					t.Fatal(err)
				}
			}
			hooks := stablePathFileReadHooks{}
			switch phase {
			case "before":
				installACL()
			case "after open":
				hooks.afterOpen = installACL
			case "after read":
				hooks.afterRead = installACL
			}
			contents, err := readBoundedCABundleFileWithHooks(path, "test TLS", "CA file", 1024, hooks)
			if contents != nil || err == nil || !strings.Contains(err.Error(), "access-control") {
				t.Fatalf("ACL-bearing CA returned contents=%q err=%v", contents, err)
			}
		})
	}
}
