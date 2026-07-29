package collectorfleet

import (
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestNewCatalogLivenessViewCanonicalizesAndDetaches(t *testing.T) {
	t.Parallel()

	scope := Scope{TenantID: "tenant-a"}
	snapshot := []CollectorLiveness{
		catalogProjectionLiveness(
			scope,
			"collector-b",
			"boot-b",
			"stream-b",
			2,
			LivenessStateStale,
		),
		catalogProjectionLiveness(
			scope,
			"collector-a",
			"boot-a",
			"stream-a",
			1,
			LivenessStateOnline,
		),
	}
	wantDigest, err := collectorLivenessDigest(scope, snapshot)
	if err != nil {
		t.Fatalf("collectorLivenessDigest(): %v", err)
	}

	view, err := newCatalogLivenessView(scope, snapshot)
	if err != nil {
		t.Fatalf("newCatalogLivenessView(): %v", err)
	}
	if view.digest != wantDigest {
		t.Fatalf("digest = %q, want %q", view.digest, wantDigest)
	}
	if got := []string{
		view.entries[0].Lease.CollectorID,
		view.entries[1].Lease.CollectorID,
	}; !reflect.DeepEqual(got, []string{"collector-a", "collector-b"}) {
		t.Fatalf("canonical collector order = %v", got)
	}
	for _, item := range view.entries {
		if got, exists := view.byCollectorID[item.Lease.CollectorID]; !exists ||
			got != item {
			t.Fatalf(
				"lookup[%q] = %+v, %t; want %+v",
				item.Lease.CollectorID,
				got,
				exists,
				item,
			)
		}
	}

	snapshot[0].Lease.BootEpoch = "caller-mutated"
	snapshot[0].State = LivenessStateOnline
	if got := view.byCollectorID["collector-b"]; got.Lease.BootEpoch != "boot-b" ||
		got.State != LivenessStateStale {
		t.Fatalf("view retained caller snapshot: %+v", got)
	}

	view.entries[0].Lease.StreamID = "entry-mutated"
	if got := view.byCollectorID["collector-a"]; got.Lease.StreamID != "stream-a" {
		t.Fatalf("lookup aliases canonical entries: %+v", got)
	}
	changed := view.byCollectorID["collector-b"]
	changed.Lease.Generation = 99
	view.byCollectorID["collector-b"] = changed
	if view.entries[1].Lease.Generation != 2 {
		t.Fatalf("canonical entries alias lookup: %+v", view.entries[1])
	}
}

func TestNewCatalogLivenessViewRejectsInvalidSnapshots(t *testing.T) {
	t.Parallel()

	scope := Scope{TenantID: "tenant-a"}
	valid := catalogProjectionLiveness(
		scope,
		"collector-a",
		"boot-a",
		"stream-a",
		1,
		LivenessStateOnline,
	)
	tests := []struct {
		name     string
		scope    Scope
		snapshot []CollectorLiveness
	}{
		{
			name:  "invalid scope",
			scope: Scope{TenantID: " tenant-a"},
		},
		{
			name:  "invalid lease",
			scope: scope,
			snapshot: []CollectorLiveness{func() CollectorLiveness {
				item := valid
				item.Lease.Generation = 0
				return item
			}()},
		},
		{
			name:  "invalid state",
			scope: scope,
			snapshot: []CollectorLiveness{func() CollectorLiveness {
				item := valid
				item.State = LivenessStateOffline
				return item
			}()},
		},
		{
			name:  "duplicate collector",
			scope: scope,
			snapshot: []CollectorLiveness{
				valid,
				func() CollectorLiveness {
					item := valid
					item.Lease.BootEpoch = "boot-b"
					item.Lease.StreamID = "stream-b"
					item.Lease.Generation = 2
					return item
				}(),
			},
		},
		{
			name:  "cross tenant",
			scope: scope,
			snapshot: []CollectorLiveness{func() CollectorLiveness {
				item := valid
				item.Lease.TenantID = "tenant-b"
				return item
			}()},
		},
		{
			name:  "over capacity",
			scope: scope,
			snapshot: func() []CollectorLiveness {
				result := make(
					[]CollectorLiveness,
					maximumCollectorListLiveness+1,
				)
				for index := range result {
					result[index] = valid
					result[index].Lease.CollectorID = fmt.Sprintf(
						"collector-%02d",
						index,
					)
				}
				return result
			}(),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			_, err := newCatalogLivenessView(test.scope, test.snapshot)
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf(
					"newCatalogLivenessView() error = %v, want ErrInvalidArgument",
					err,
				)
			}
		})
	}
}

func TestCatalogLivenessViewConnectionStateRequiresExactDurableLease(
	t *testing.T,
) {
	t.Parallel()

	scope := Scope{TenantID: "tenant-a"}
	exact := catalogProjectionLiveness(
		scope,
		"collector-a",
		"boot-a",
		"stream-a",
		7,
		LivenessStateStale,
	)
	view, err := newCatalogLivenessView(scope, []CollectorLiveness{exact})
	if err != nil {
		t.Fatalf("newCatalogLivenessView(): %v", err)
	}
	base := Collector{
		TenantID:            scope.TenantID,
		CollectorID:         exact.Lease.CollectorID,
		AdministrativeState: AdministrativeStateEnabled,
		ActiveLease: &ActiveLease{
			BootEpoch:  exact.Lease.BootEpoch,
			StreamID:   exact.Lease.StreamID,
			InstanceID: "instance-a",
			Generation: exact.Lease.Generation,
		},
	}
	tests := []struct {
		name   string
		mutate func(*Collector)
		want   ConnectionState
	}{
		{
			name: "exact stale lease",
			want: ConnectionStateStale,
		},
		{
			name: "disabled wins over stale runtime",
			mutate: func(collector *Collector) {
				collector.AdministrativeState = AdministrativeStateDisabled
			},
			want: ConnectionStateDisabled,
		},
		{
			name: "inactive durable collector",
			mutate: func(collector *Collector) {
				collector.ActiveLease = nil
			},
			want: ConnectionStateOffline,
		},
		{
			name: "boot epoch mismatch",
			mutate: func(collector *Collector) {
				collector.ActiveLease.BootEpoch = "boot-b"
			},
			want: ConnectionStateOffline,
		},
		{
			name: "stream ID mismatch",
			mutate: func(collector *Collector) {
				collector.ActiveLease.StreamID = "stream-b"
			},
			want: ConnectionStateOffline,
		},
		{
			name: "generation mismatch",
			mutate: func(collector *Collector) {
				collector.ActiveLease.Generation++
			},
			want: ConnectionStateOffline,
		},
		{
			name: "tenant mismatch",
			mutate: func(collector *Collector) {
				collector.TenantID = "tenant-b"
			},
			want: ConnectionStateOffline,
		},
		{
			name: "collector ID mismatch",
			mutate: func(collector *Collector) {
				collector.CollectorID = "collector-b"
			},
			want: ConnectionStateOffline,
		},
		{
			name: "invalid administrative state",
			mutate: func(collector *Collector) {
				collector.AdministrativeState = "invented"
			},
			want: ConnectionStateOffline,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			collector := base
			activeLease := *base.ActiveLease
			collector.ActiveLease = &activeLease
			if test.mutate != nil {
				test.mutate(&collector)
			}
			if got := view.connectionState(collector); got != test.want {
				t.Fatalf("connectionState() = %q, want %q", got, test.want)
			}
		})
	}

	online := exact
	online.State = LivenessStateOnline
	onlineView, err := newCatalogLivenessView(scope, []CollectorLiveness{online})
	if err != nil {
		t.Fatalf("newCatalogLivenessView(online): %v", err)
	}
	if got := onlineView.connectionState(base); got != ConnectionStateOnline {
		t.Fatalf("online connectionState() = %q, want online", got)
	}
	emptyView, err := newCatalogLivenessView(scope, nil)
	if err != nil {
		t.Fatalf("newCatalogLivenessView(empty): %v", err)
	}
	if got := emptyView.connectionState(base); got != ConnectionStateOffline {
		t.Fatalf("absent connectionState() = %q, want offline", got)
	}
}

func catalogProjectionLiveness(
	scope Scope,
	collectorID string,
	bootEpoch string,
	streamID string,
	generation uint64,
	state LivenessState,
) CollectorLiveness {
	return CollectorLiveness{
		Lease: Lease{
			Scope:       scope,
			CollectorID: collectorID,
			BootEpoch:   bootEpoch,
			StreamID:    streamID,
			Generation:  generation,
		},
		State: state,
	}
}
