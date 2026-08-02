//go:build darwin

package privatefs

import (
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOpenDirectoryRejectsDarwinExtendedACL(t *testing.T) {
	t.Parallel()

	path := t.TempDir()
	if err := os.Chmod(path, 0o700); err != nil {
		t.Fatal(err)
	}
	addDarwinACL(t, path, "everyone allow read")
	assertExactMode(t, path, 0o700)
	if _, err := OpenDirectory(path); err == nil ||
		!strings.Contains(err.Error(), "extended ACL") {
		t.Fatalf("OpenDirectory ACL error = %v", err)
	}
}

func TestOpenRegularRejectsDarwinExtendedACL(t *testing.T) {
	t.Parallel()

	path, directory := openTestDirectory(t)
	filePath := filepath.Join(path, "member")
	mustWriteFile(t, filePath, []byte("data"), 0o600)
	addDarwinACL(t, filePath, "everyone allow read")
	assertExactMode(t, filePath, 0o600)
	if opened, err := directory.OpenRegular("member", FilePolicy{
		AllowedModes: []fs.FileMode{0o600},
		MinimumSize:  4,
		MaximumSize:  4,
	}); err == nil {
		_ = opened.Close()
		t.Fatal("OpenRegular accepted an extended ACL")
	} else if !strings.Contains(err.Error(), "extended ACL") {
		t.Fatalf("OpenRegular ACL error = %v", err)
	}
}

func addDarwinACL(t *testing.T, path string, entry string) {
	t.Helper()
	output, err := exec.CommandContext(
		t.Context(),
		"/bin/chmod",
		"+a",
		entry,
		path,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("add Darwin ACL: %v: %s", err, output)
	}
}
