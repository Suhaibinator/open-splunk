// Package ianatimezone provides host-independent validation and cached loading
// of explicit IANA timezone names used by search admission and execution.
package ianatimezone

import (
	"errors"
	"strings"
	"sync"
	"time"
	"unicode/utf8"

	_ "time/tzdata" // Embed IANA data for deployments without a system database.
)

const MaximumNameBytes = 255

var (
	ErrInvalid = errors.New("timezone is invalid")
	ErrLocal   = errors.New("timezone must not depend on the server's local configuration")

	locationCache sync.Map
)

// Load validates an already-canonical explicit timezone and returns one shared
// immutable location. Failed names and the process-local pseudo-zone are never
// cached.
func Load(name string) (*time.Location, error) {
	if name == "Local" {
		return nil, ErrLocal
	}
	if name == "" || len(name) > MaximumNameBytes || !utf8.ValidString(name) ||
		strings.TrimSpace(name) != name || serverDependentName(name) {
		return nil, ErrInvalid
	}
	if name == "UTC" {
		return time.UTC, nil
	}
	if cached, ok := locationCache.Load(name); ok {
		return cached.(*time.Location), nil
	}
	location, err := time.LoadLocation(name)
	if err != nil {
		return nil, ErrInvalid
	}
	cached, _ := locationCache.LoadOrStore(strings.Clone(name), location)
	return cached.(*time.Location), nil
}

func serverDependentName(name string) bool {
	return name == "localtime" || name == "posixrules" ||
		strings.HasPrefix(name, "posix/") || strings.HasPrefix(name, "right/")
}
