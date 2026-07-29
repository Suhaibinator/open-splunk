package collectorfleet

// ValidateLivenessSnapshot verifies that a process-owned snapshot is a
// complete, bounded input for one tenant-scoped catalog read. The catalog
// performs the same fail-closed checks while building its signed liveness
// digest; this narrow exported seam lets a transport adapter classify invalid
// trusted runtime state separately from invalid client input.
func ValidateLivenessSnapshot(
	scope Scope,
	snapshot []CollectorLiveness,
) error {
	_, _, err := normalizeCollectorLivenessSnapshot(scope, snapshot)
	return err
}

func normalizeCollectorLivenessSnapshot(
	scope Scope,
	snapshot []CollectorLiveness,
) (Scope, []CollectorLiveness, error) {
	normalizedScope, err := normalizeScope(scope)
	if err != nil {
		return Scope{}, nil, err
	}
	if len(snapshot) > maximumCollectorListLiveness {
		return Scope{}, nil, invalid(
			"collector liveness snapshot cannot contain more than %d values",
			maximumCollectorListLiveness,
		)
	}
	normalized := make([]CollectorLiveness, len(snapshot))
	for index, item := range snapshot {
		lease, normalizeErr := normalizeLease(item.Lease)
		if normalizeErr != nil {
			return Scope{}, nil, normalizeErr
		}
		if lease.TenantID != normalizedScope.TenantID {
			return Scope{}, nil, invalid(
				"collector liveness lease is outside the requested tenant",
			)
		}
		if item.State != LivenessStateOnline &&
			item.State != LivenessStateStale {
			return Scope{}, nil, invalid(
				"collector liveness state is invalid",
			)
		}
		// The snapshot is capped at 16 values, so a pairwise check avoids a
		// map allocation while remaining strictly bounded.
		for previous := range index {
			if normalized[previous].Lease.CollectorID == lease.CollectorID {
				return Scope{}, nil, invalid(
					"collector liveness snapshot contains a duplicate collector",
				)
			}
		}
		normalized[index] = CollectorLiveness{
			Lease: lease,
			State: item.State,
		}
	}
	return normalizedScope, normalized, nil
}
