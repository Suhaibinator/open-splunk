package collectorfleet

import (
	"slices"
	"strings"
)

// CatalogEntry is one detached durable collector snapshot with its exact
// administrator-facing connection-state overlay.
type CatalogEntry struct {
	Collector       Collector
	ConnectionState ConnectionState
}

// catalogLivenessView is the canonical, bounded process-local input to one
// catalog operation. Entries and byCollectorID intentionally own independent
// value copies so neither the caller nor either representation can mutate the
// other.
type catalogLivenessView struct {
	entries       []CollectorLiveness
	digest        string
	byCollectorID map[string]CollectorLiveness
}

func newCatalogLivenessView(
	scope Scope,
	snapshot []CollectorLiveness,
) (catalogLivenessView, error) {
	digest, err := collectorLivenessDigest(scope, snapshot)
	if err != nil {
		return catalogLivenessView{}, err
	}

	entries := slices.Clone(snapshot)
	slices.SortFunc(entries, func(left, right CollectorLiveness) int {
		return strings.Compare(left.Lease.CollectorID, right.Lease.CollectorID)
	})
	byCollectorID := make(map[string]CollectorLiveness, len(entries))
	for _, item := range entries {
		byCollectorID[item.Lease.CollectorID] = item
	}
	return catalogLivenessView{
		entries:       entries,
		digest:        digest,
		byCollectorID: byCollectorID,
	}, nil
}

func (view catalogLivenessView) connectionState(
	collector Collector,
) ConnectionState {
	if collector.AdministrativeState == AdministrativeStateDisabled {
		return ConnectionStateDisabled
	}
	if collector.AdministrativeState != AdministrativeStateEnabled ||
		collector.ActiveLease == nil {
		return ConnectionStateOffline
	}

	liveness, exists := view.byCollectorID[collector.CollectorID]
	if !exists ||
		liveness.Lease.TenantID != collector.TenantID ||
		liveness.Lease.CollectorID != collector.CollectorID ||
		liveness.Lease.BootEpoch != collector.ActiveLease.BootEpoch ||
		liveness.Lease.StreamID != collector.ActiveLease.StreamID ||
		liveness.Lease.Generation != collector.ActiveLease.Generation {
		return ConnectionStateOffline
	}
	switch liveness.State {
	case LivenessStateOnline:
		return ConnectionStateOnline
	case LivenessStateStale:
		return ConnectionStateStale
	default:
		return ConnectionStateOffline
	}
}

func (view catalogLivenessView) entry(collector Collector) CatalogEntry {
	return CatalogEntry{
		// Catalog callers transfer ownership of the freshly hydrated collector
		// into the result. Re-copying its bounded child snapshots here would
		// duplicate every allocation without adding isolation.
		Collector:       collector,
		ConnectionState: view.connectionState(collector),
	}
}
