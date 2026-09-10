package input

import (
	"errors"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"
)

func TestBoundedDiscoveryMatchesFilepathGlob(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, subdir := range []string{"one", "two", "escaped[dir]"} {
		if err := os.Mkdir(filepath.Join(dir, subdir), 0o700); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"a.log", "b.log", "skip.tmp", ".hidden.log"} {
			writeFileT(t, filepath.Join(dir, subdir, name), "")
		}
	}
	if runtime.GOOS != "windows" {
		if err := os.Symlink(filepath.Join(dir, "one"), filepath.Join(dir, "alias")); err != nil {
			t.Fatal(err)
		}
	}
	patterns := []string{
		filepath.Join(dir, "*", "*.log"),
		filepath.Join(dir, "[ot]*", "?.log"),
		filepath.Join(dir, "one", "a.log"),
		filepath.Join(dir, "none", "*.log"),
		filepath.Join(dir, "one", "*") + string(filepath.Separator),
	}
	if runtime.GOOS != "windows" {
		patterns = append(patterns, filepath.Join(dir, `escaped\[dir\]`, "*.log"))
	}
	for _, pattern := range patterns {
		t.Run(pattern, func(t *testing.T) {
			want, err := filepath.Glob(pattern)
			if err != nil {
				t.Fatal(err)
			}
			got, err := MatchPaths([]string{pattern}, nil)
			if err != nil || len(got) != len(want) || (len(want) > 0 && !reflect.DeepEqual(got, want)) {
				t.Fatalf("bounded glob = %v, %v; filepath.Glob = %v", got, err, want)
			}
		})
	}
}

func TestBoundedDiscoveryRejectsIncompleteSnapshots(t *testing.T) {
	t.Parallel()
	dir := t.TempDir()
	for _, name := range []string{"a.log", "b.log", "c.tmp"} {
		writeFileT(t, filepath.Join(dir, name), "")
	}
	for _, test := range []struct {
		name             string
		include, exclude []string
		paths, entries   int
	}{
		{"paths", []string{filepath.Join(dir, "*")}, nil, 2, 3},
		{"excluded work", []string{filepath.Join(dir, "*")}, []string{"*"}, 3, 2},
		{"unmatched work", []string{filepath.Join(dir, "missing*")}, nil, 3, 2},
		{"overlapping patterns", []string{filepath.Join(dir, "*"), filepath.Join(dir, "*.log")}, nil, 3, 5},
	} {
		t.Run(test.name, func(t *testing.T) {
			got, err := matchPathsWithin(test.include, test.exclude, test.paths, test.entries)
			if !errors.Is(err, errDiscoveryCapacity) || got != nil {
				t.Fatalf("limited snapshot = %v, %v; want no partial snapshot", got, err)
			}
		})
	}
	got, err := matchPathsWithin([]string{filepath.Join(dir, "*")}, []string{"*.tmp"}, 2, 3)
	if err != nil || len(got) != 2 {
		t.Fatalf("exact limits = %v, %v", got, err)
	}
	if err := os.Mkdir(filepath.Join(dir, "nested"), 0o700); err != nil {
		t.Fatal(err)
	}
	writeFileT(t, filepath.Join(dir, "nested", "d.log"), "")
	if got, err := matchPathsWithin([]string{filepath.Join(dir, "*", "*.log")}, nil, 2, 4); !errors.Is(err, errDiscoveryCapacity) || got != nil {
		t.Fatalf("intermediate directory work escaped budget: %v, %v", got, err)
	}
}
