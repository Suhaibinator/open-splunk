package input

import (
	"errors"
	"os"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
)

const maximumDiscoveryPaths = 4096
const maximumDiscoveryEntries = 65536
const maximumDiscoveryDepth = 128

var errDiscoveryCapacity = errors.New("collector/input: discovery capacity reached; narrow include globs or reduce matching directory entries")

// MatchPaths returns a complete sorted, deduplicated discovery snapshot or an
// error. Directory reads, intermediate glob expansion, and excluded entries
// share a finite work budget; no directory's entire contents are materialized.
func MatchPaths(include, exclude []string) ([]string, error) {
	return matchPathsWithin(include, exclude, maximumDiscoveryPaths, maximumDiscoveryEntries)
}

func matchPathsWithin(include, exclude []string, pathLimit, entryLimit int) ([]string, error) {
	set := make(map[string]struct{})
	remaining := entryLimit
	for _, pattern := range include {
		err := visitGlob(pattern, 0, &remaining, func(path string) error {
			if excludedPath(path, exclude) {
				return nil
			}
			if _, exists := set[path]; !exists && len(set) == pathLimit {
				return errDiscoveryCapacity
			}
			set[path] = struct{}{}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	out := make([]string, 0, len(set))
	for path := range set {
		out = append(out, path)
	}
	slices.Sort(out)
	return out, nil
}

func visitGlob(pattern string, depth int, remaining *int, visit func(string) error) error {
	if depth >= maximumDiscoveryDepth {
		return errDiscoveryCapacity
	}
	if _, err := filepath.Match(pattern, ""); err != nil {
		return err
	}
	if !globHasMeta(pattern) {
		if *remaining == 0 {
			return errDiscoveryCapacity
		}
		*remaining--
		if _, err := os.Lstat(pattern); err != nil {
			return nil // Match filepath.Glob's missing/unreadable-path behavior.
		}
		return visit(pattern)
	}
	dir, name := filepath.Split(pattern)
	if dir == "" {
		dir = "."
	} else if len(dir) > len(filepath.VolumeName(dir))+1 {
		dir = dir[:len(dir)-1]
	}
	if !globHasMeta(dir[len(filepath.VolumeName(dir)):]) {
		return visitGlobDirectory(dir, name, remaining, visit)
	}
	if dir == pattern {
		return filepath.ErrBadPattern
	}
	return visitGlob(dir, depth+1, remaining, func(parent string) error {
		return visitGlobDirectory(parent, name, remaining, visit)
	})
}

func visitGlobDirectory(dir, pattern string, remaining *int, visit func(string) error) error {
	info, err := os.Stat(dir)
	if err != nil || !info.IsDir() {
		return nil
	}
	f, err := os.Open(dir)
	if err != nil {
		return nil
	}
	defer f.Close()
	for {
		// One extra entry distinguishes an exactly exhausted budget from EOF.
		names, readErr := f.Readdirnames(min(128, *remaining+1))
		for _, name := range names {
			if *remaining == 0 {
				return errDiscoveryCapacity
			}
			*remaining--
			matched, err := filepath.Match(pattern, name)
			if err != nil {
				return err
			}
			if matched {
				if err := visit(filepath.Join(dir, name)); err != nil {
					return err
				}
			}
		}
		if readErr != nil {
			return nil
		}
	}
}

func globHasMeta(path string) bool {
	if runtime.GOOS == "windows" {
		return strings.ContainsAny(path, "*?[")
	}
	return strings.ContainsAny(path, `*?[\`)
}

func excludedPath(path string, exclude []string) bool {
	base := filepath.Base(path)
	for _, pattern := range exclude {
		if matched, _ := filepath.Match(pattern, path); matched {
			return true
		}
		if matched, _ := filepath.Match(pattern, base); matched {
			return true
		}
	}
	return false
}
