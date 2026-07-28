package ianatimezone

import (
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestLoadCachesOnlyCanonicalSuccessfulIANAZonesConcurrently(t *testing.T) {
	t.Parallel()

	const (
		workers = 32
		zone    = "Pacific/Chatham"
	)
	start := make(chan struct{})
	locations := make(chan *time.Location, workers)
	errs := make(chan error, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for range workers {
		go func() {
			defer group.Done()
			<-start
			location, err := Load(zone)
			if err != nil {
				errs <- err
				return
			}
			locations <- location
		}()
	}
	close(start)
	group.Wait()
	close(locations)
	close(errs)

	for err := range errs {
		t.Errorf("Load(%q) error = %v", zone, err)
	}
	var cached *time.Location
	for location := range locations {
		if cached == nil {
			cached = location
			continue
		}
		if location != cached {
			t.Fatalf(
				"concurrent cache returned distinct locations: %p and %p",
				cached,
				location,
			)
		}
	}
	if cached == nil {
		t.Fatal("concurrent cache returned no locations")
	}
	if again, err := Load(zone); err != nil || again != cached {
		t.Fatalf("cached Load(%q) = (%p, %v), want %p", zone, again, err, cached)
	}
}

func TestLoadRejectsInvalidOrHostLocalZones(t *testing.T) {
	t.Parallel()

	for _, name := range []string{
		"",
		" UTC",
		"UTC ",
		"OpenSplunk/Invalid",
		"bad\xffzone",
		"localtime",
		"posixrules",
		"posix/UTC",
		"right/UTC",
		strings.Repeat("x", MaximumNameBytes+1),
	} {
		if _, err := Load(name); !errors.Is(err, ErrInvalid) {
			t.Errorf("Load(%q) error = %v, want ErrInvalid", name, err)
		}
		if _, cached := locationCache.Load(name); cached {
			t.Errorf("invalid Load(%q) populated the success cache", name)
		}
	}
	if _, err := Load("Local"); !errors.Is(err, ErrLocal) {
		t.Fatalf("Load(Local) error = %v, want ErrLocal", err)
	}
	if _, cached := locationCache.Load("Local"); cached {
		t.Fatal("Load(Local) populated the success cache")
	}
	if location, err := Load("UTC"); err != nil || location != time.UTC {
		t.Fatalf("Load(UTC) = (%p, %v), want time.UTC", location, err)
	}
}
