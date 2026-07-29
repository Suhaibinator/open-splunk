package collectorfleet

import (
	"cmp"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"slices"
	"strings"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

const (
	collectorListCursorPurpose = "collector-fleet-list-cursor"
	collectorListCursorVersion = 1
)

type collectorListCursor struct {
	Version        int                          `json:"v"`
	RequestHash    string                       `json:"f"`
	Revision       int64                        `json:"r"`
	LivenessDigest string                       `json:"l"`
	StringKey      *collectorListNullableString `json:"s,omitempty"`
	IntegerKey     *collectorListNullableInt64  `json:"n,omitempty"`
	CollectorID    string                       `json:"i"`
}

type collectorListNullableString struct {
	Valid bool   `json:"v"`
	Value string `json:"x,omitempty"`
}

type collectorListNullableInt64 struct {
	Valid bool  `json:"v"`
	Value int64 `json:"x,omitempty"`
}

type collectorListFingerprint struct {
	Version   int      `json:"v"`
	TenantID  string   `json:"t"`
	PageSize  uint32   `json:"p"`
	Total     bool     `json:"z"`
	States    []string `json:"s"`
	IndexName *string  `json:"i"`
	Text      *string  `json:"x"`
	SortBy    string   `json:"b"`
	Direction string   `json:"d"`
}

type collectorLivenessFingerprint struct {
	Version  int                                 `json:"v"`
	TenantID string                              `json:"t"`
	Entries  []collectorLivenessFingerprintEntry `json:"e"`
}

type collectorLivenessFingerprintEntry struct {
	TenantID    string `json:"t"`
	CollectorID string `json:"c"`
	BootEpoch   string `json:"b"`
	StreamID    string `json:"s"`
	Generation  uint64 `json:"g"`
	State       string `json:"l"`
}

func normalizeCatalogCursorKey(input []byte) ([]byte, error) {
	if len(input) < minimumCollectorCursorKeyBytes ||
		len(input) > maximumCollectorCursorKeyBytes {
		return nil, invalid(
			"collector catalog cursor key must contain between %d and %d bytes",
			minimumCollectorCursorKeyBytes,
			maximumCollectorCursorKeyBytes,
		)
	}
	return slices.Clone(input), nil
}

func collectorListFilterHash(request normalizedListRequest) (string, error) {
	states := make([]string, len(request.stateFilters))
	for index, state := range request.stateFilters {
		states[index] = string(state)
	}
	payload, err := json.Marshal(collectorListFingerprint{
		Version:   collectorListCursorVersion,
		TenantID:  request.tenantID,
		PageSize:  request.pageSize,
		Total:     request.includeTotal,
		States:    states,
		IndexName: request.indexNameFilter,
		Text:      request.textFilter,
		SortBy:    string(request.sortBy),
		Direction: string(request.direction),
	})
	if err != nil {
		return "", fmt.Errorf("encode collector list filter: %w", err)
	}
	return collectorSHA256Digest(payload), nil
}

// collectorLivenessDigest authenticates the complete process-local input to a
// page. Ordering from the bounded runtime snapshot is immaterial, but a lease
// identity or online/stale transition invalidates every continuation.
func collectorLivenessDigest(
	scope Scope,
	snapshot []CollectorLiveness,
) (string, error) {
	normalizedScope, err := normalizeScope(scope)
	if err != nil {
		return "", err
	}
	if len(snapshot) > maximumCollectorListLiveness {
		return "", invalid(
			"collector liveness snapshot cannot contain more than %d values",
			maximumCollectorListLiveness,
		)
	}
	entries := make(
		[]collectorLivenessFingerprintEntry,
		0,
		len(snapshot),
	)
	seen := make(map[string]struct{}, len(snapshot))
	for _, item := range snapshot {
		lease, normalizeErr := normalizeLease(item.Lease)
		if normalizeErr != nil {
			return "", normalizeErr
		}
		if lease.TenantID != normalizedScope.TenantID {
			return "", invalid(
				"collector liveness lease is outside the requested tenant",
			)
		}
		if item.State != LivenessStateOnline &&
			item.State != LivenessStateStale {
			return "", invalid("collector liveness state is invalid")
		}
		if _, exists := seen[lease.CollectorID]; exists {
			return "", invalid(
				"collector liveness snapshot contains a duplicate collector",
			)
		}
		seen[lease.CollectorID] = struct{}{}
		entries = append(entries, collectorLivenessFingerprintEntry{
			TenantID:    lease.TenantID,
			CollectorID: lease.CollectorID,
			BootEpoch:   lease.BootEpoch,
			StreamID:    lease.StreamID,
			Generation:  lease.Generation,
			State:       string(item.State),
		})
	}
	slices.SortFunc(entries, compareCollectorLivenessFingerprintEntries)
	payload, err := json.Marshal(collectorLivenessFingerprint{
		Version:  collectorListCursorVersion,
		TenantID: normalizedScope.TenantID,
		Entries:  entries,
	})
	if err != nil {
		return "", fmt.Errorf("encode collector liveness fingerprint: %w", err)
	}
	return collectorSHA256Digest(payload), nil
}

func compareCollectorLivenessFingerprintEntries(
	left collectorLivenessFingerprintEntry,
	right collectorLivenessFingerprintEntry,
) int {
	for _, comparison := range []int{
		strings.Compare(left.TenantID, right.TenantID),
		strings.Compare(left.CollectorID, right.CollectorID),
		strings.Compare(left.BootEpoch, right.BootEpoch),
		strings.Compare(left.StreamID, right.StreamID),
		cmp.Compare(left.Generation, right.Generation),
		strings.Compare(left.State, right.State),
	} {
		if comparison != 0 {
			return comparison
		}
	}
	return 0
}

func encodeCollectorListCursor(
	key []byte,
	cursor collectorListCursor,
	sortBy CollectorSortBy,
) (string, error) {
	normalizedKey, err := normalizeCatalogCursorKey(key)
	if err != nil {
		return "", err
	}
	cursor.Version = collectorListCursorVersion
	if !validCollectorListCursor(cursor, sortBy) {
		return "", invalid("collector list cursor is invalid")
	}
	token, err := cursorcodec.Encode(
		normalizedKey,
		collectorListCursorPurpose,
		collectorListCursorVersion,
		maximumCollectorCursorBytes,
		cursor,
	)
	if err != nil {
		return "", fmt.Errorf("encode collector list cursor: %w", err)
	}
	return token, nil
}

func decodeCollectorListCursor(
	key []byte,
	token string,
	requestHash string,
	currentRevision int64,
	currentLivenessDigest string,
	sortBy CollectorSortBy,
) (collectorListCursor, error) {
	invalidCursor := func() (collectorListCursor, error) {
		return collectorListCursor{}, invalid(
			"collector page token is invalid or does not match the request",
		)
	}
	normalizedKey, err := normalizeCatalogCursorKey(key)
	if err != nil {
		return collectorListCursor{}, err
	}
	if !validCollectorDigest(requestHash) ||
		currentRevision < 1 ||
		!validCollectorDigest(currentLivenessDigest) ||
		!validCollectorSortBy(sortBy) {
		return invalidCursor()
	}
	var cursor collectorListCursor
	if err := cursorcodec.Decode(
		normalizedKey,
		collectorListCursorPurpose,
		collectorListCursorVersion,
		maximumCollectorCursorBytes,
		token,
		&cursor,
	); err != nil {
		return invalidCursor()
	}
	if !validCollectorListCursor(cursor, sortBy) ||
		cursor.RequestHash != requestHash {
		return invalidCursor()
	}
	if cursor.Revision != currentRevision ||
		cursor.LivenessDigest != currentLivenessDigest {
		return collectorListCursor{}, control.ErrPageInvalidated
	}
	return cursor, nil
}

func validCollectorListCursor(
	cursor collectorListCursor,
	sortBy CollectorSortBy,
) bool {
	if cursor.Version != collectorListCursorVersion ||
		!validCollectorDigest(cursor.RequestHash) ||
		cursor.Revision < 1 ||
		!validCollectorDigest(cursor.LivenessDigest) ||
		!validIdentifier(cursor.CollectorID) {
		return false
	}
	switch sortBy {
	case CollectorSortByDisplayName:
		return cursor.IntegerKey == nil &&
			validCollectorStringCursorKey(
				cursor.StringKey,
				maximumDisplayNameBytes,
				false,
				true,
			)
	case CollectorSortByHostname:
		return cursor.IntegerKey == nil &&
			cursor.StringKey != nil &&
			cursor.StringKey.Valid &&
			validCollectorStringCursorKey(
				cursor.StringKey,
				maximumHostnameBytes,
				true,
				false,
			)
	case CollectorSortByLastSeenAt:
		return cursor.StringKey == nil &&
			validCollectorIntegerCursorKey(
				cursor.IntegerKey,
				1,
				maximumPublicUnixMicro,
			)
	case CollectorSortByQueueBytes:
		return cursor.StringKey == nil &&
			validCollectorIntegerCursorKey(
				cursor.IntegerKey,
				0,
				int64(^uint64(0)>>1),
			)
	default:
		return false
	}
}

func validCollectorStringCursorKey(
	key *collectorListNullableString,
	maximum int,
	allowEmpty bool,
	requireTrimmed bool,
) bool {
	if key == nil {
		return false
	}
	if !key.Valid {
		return key.Value == ""
	}
	if requireTrimmed && strings.TrimSpace(key.Value) != key.Value {
		return false
	}
	return validateText("collector cursor string key", key.Value, maximum, allowEmpty) == nil
}

func validCollectorIntegerCursorKey(
	key *collectorListNullableInt64,
	minimum int64,
	maximum int64,
) bool {
	if key == nil || !key.Valid {
		return false
	}
	return key.Value >= minimum && key.Value <= maximum
}

func collectorSHA256Digest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func validCollectorDigest(value string) bool {
	decoded, err := base64.RawURLEncoding.DecodeString(value)
	return err == nil &&
		len(decoded) == sha256.Size &&
		base64.RawURLEncoding.EncodeToString(decoded) == value
}
