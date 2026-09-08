//go:build darwin

package main

import (
	"os/exec"
	"strings"
	"testing"
)

func TestReadBoundedCABundleRejectsDarwinACL(t *testing.T) {
	t.Parallel()
	for _, phase := range []string{"before", "after read"} {
		t.Run(phase, func(t *testing.T) {
			t.Parallel()
			path := writeClickHouseCredentialFixture(t, "fixture", 0o600)
			installACL := func() {
				output, err := exec.CommandContext(t.Context(), "/bin/chmod", "+a", "everyone allow write", path).CombinedOutput()
				if err != nil {
					t.Fatalf("install ACL: %v: %s", err, output)
				}
			}
			hooks := stablePathFileReadHooks{}
			if phase == "before" {
				installACL()
			} else {
				hooks.afterRead = installACL
			}
			contents, err := readBoundedCABundleFileWithHooks(path, "test TLS", "CA file", 1024, hooks)
			if contents != nil || err == nil || !strings.Contains(err.Error(), "access-control") {
				t.Fatalf("ACL-bearing CA returned contents=%q err=%v", contents, err)
			}
		})
	}
}
