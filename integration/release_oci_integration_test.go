//go:build !windows

package integration_test

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func releaseOCIBindSourceMatchesForOS(hostOS, actual, expected string) bool {
	if !filepath.IsAbs(actual) || !filepath.IsAbs(expected) ||
		filepath.Clean(actual) != actual || filepath.Clean(expected) != expected {
		return false
	}
	actualHostPath := actual
	if hostOS == "darwin" && strings.HasPrefix(actualHostPath, "/host_mnt/") {
		actualHostPath = strings.TrimPrefix(actualHostPath, "/host_mnt")
	}
	physicalActual, actualErr := filepath.EvalSymlinks(actualHostPath)
	physicalExpected, expectedErr := filepath.EvalSymlinks(expected)
	return actualErr == nil && expectedErr == nil && physicalActual == physicalExpected
}

func TestReleaseOCIBindSourceIdentity(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "secret")
	if err := os.WriteFile(path, []byte("fixture"), 0o600); err != nil {
		t.Fatal(err)
	}
	physical, err := filepath.EvalSymlinks(path)
	if err != nil {
		t.Fatal(err)
	}
	if !releaseOCIBindSourceMatchesForOS("darwin", "/host_mnt"+physical, path) {
		t.Fatal("Darwin Docker Desktop physical bind identity was rejected")
	}
	if releaseOCIBindSourceMatchesForOS("linux", "/host_mnt"+physical, path) {
		t.Fatal("Linux bind identity accepted the Docker Desktop transport prefix")
	}
	if releaseOCIBindSourceMatchesForOS("darwin", "/host_mnt"+physical+"-other", path) {
		t.Fatal("different Darwin Docker Desktop bind source was accepted")
	}
}
