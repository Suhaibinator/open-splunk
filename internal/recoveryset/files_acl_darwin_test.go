//go:build darwin

package recoveryset

import (
	"os/exec"
	"testing"
)

func addArchiveTestACL(t *testing.T, path string, _ byte) {
	t.Helper()
	output, err := exec.CommandContext(
		t.Context(),
		"/bin/chmod",
		"+a",
		"everyone allow read",
		path,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("add Darwin ACL: %v: %s", err, output)
	}
}
