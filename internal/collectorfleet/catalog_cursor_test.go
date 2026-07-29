package collectorfleet

import (
	"bytes"
	"errors"
	"reflect"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/cursorcodec"
)

func TestNormalizeCatalogCursorKeyBoundsAndClone(t *testing.T) {
	t.Parallel()

	for _, size := range []int{
		0,
		minimumCollectorCursorKeyBytes - 1,
		maximumCollectorCursorKeyBytes + 1,
	} {
		if _, err := normalizeCatalogCursorKey(make([]byte, size)); !errors.Is(
			err,
			control.ErrInvalidArgument,
		) {
			t.Fatalf(
				"normalizeCatalogCursorKey(%d bytes) error = %v, want ErrInvalidArgument",
				size,
				err,
			)
		}
	}
	for _, size := range []int{
		minimumCollectorCursorKeyBytes,
		maximumCollectorCursorKeyBytes,
	} {
		input := bytes.Repeat([]byte{byte(size)}, size)
		normalized, err := normalizeCatalogCursorKey(input)
		if err != nil {
			t.Fatalf(
				"normalizeCatalogCursorKey(%d bytes): %v",
				size,
				err,
			)
		}
		if !bytes.Equal(normalized, input) {
			t.Fatalf("normalized %d-byte key changed", size)
		}
		input[0] ^= 0xff
		if normalized[0] == input[0] {
			t.Fatalf("normalized %d-byte key aliases the caller", size)
		}
	}
}

func TestCollectorLivenessDigestIsCanonicalAndBindsExactLease(t *testing.T) {
	t.Parallel()

	scope := Scope{TenantID: "tenant-a"}
	snapshot := []CollectorLiveness{
		{
			Lease: Lease{
				Scope:       scope,
				CollectorID: "collector-b",
				BootEpoch:   "boot-b",
				StreamID:    "stream-b",
				Generation:  7,
			},
			State: LivenessStateStale,
		},
		{
			Lease: Lease{
				Scope:       scope,
				CollectorID: "collector-a",
				BootEpoch:   "boot-a",
				StreamID:    "stream-a",
				Generation:  3,
			},
			State: LivenessStateOnline,
		},
	}
	original := append([]CollectorLiveness(nil), snapshot...)
	digest, err := collectorLivenessDigest(scope, snapshot)
	if err != nil {
		t.Fatalf("collectorLivenessDigest(): %v", err)
	}
	if !validCollectorDigest(digest) {
		t.Fatalf("digest is not canonical SHA-256: %q", digest)
	}
	if !reflect.DeepEqual(snapshot, original) {
		t.Fatalf("collectorLivenessDigest mutated input: %+v", snapshot)
	}
	reversed := []CollectorLiveness{snapshot[1], snapshot[0]}
	reversedDigest, err := collectorLivenessDigest(scope, reversed)
	if err != nil {
		t.Fatalf("collectorLivenessDigest(reversed): %v", err)
	}
	if reversedDigest != digest {
		t.Fatalf(
			"reordered snapshot digest = %q, want %q",
			reversedDigest,
			digest,
		)
	}

	mutations := []struct {
		name   string
		mutate func(*CollectorLiveness)
	}{
		{
			name: "collector ID",
			mutate: func(value *CollectorLiveness) {
				value.Lease.CollectorID = "collector-c"
			},
		},
		{
			name: "boot epoch",
			mutate: func(value *CollectorLiveness) {
				value.Lease.BootEpoch = "boot-c"
			},
		},
		{
			name: "stream ID",
			mutate: func(value *CollectorLiveness) {
				value.Lease.StreamID = "stream-c"
			},
		},
		{
			name: "generation",
			mutate: func(value *CollectorLiveness) {
				value.Lease.Generation++
			},
		},
		{
			name: "online to stale",
			mutate: func(value *CollectorLiveness) {
				value.State = LivenessStateStale
			},
		},
	}
	for _, mutation := range mutations {
		mutation := mutation
		t.Run(mutation.name, func(t *testing.T) {
			t.Parallel()
			changed := append([]CollectorLiveness(nil), snapshot...)
			mutation.mutate(&changed[1])
			got, digestErr := collectorLivenessDigest(scope, changed)
			if digestErr != nil {
				t.Fatalf("collectorLivenessDigest(): %v", digestErr)
			}
			if got == digest {
				t.Fatalf("%s did not change liveness digest", mutation.name)
			}
		})
	}

	emptyA, err := collectorLivenessDigest(scope, nil)
	if err != nil {
		t.Fatalf("empty tenant-a digest: %v", err)
	}
	emptyB, err := collectorLivenessDigest(
		Scope{TenantID: "tenant-b"},
		nil,
	)
	if err != nil {
		t.Fatalf("empty tenant-b digest: %v", err)
	}
	if emptyA == emptyB {
		t.Fatal("tenant identity did not change empty liveness digest")
	}
}

func TestCollectorLivenessDigestRejectsInvalidSnapshot(t *testing.T) {
	t.Parallel()

	scope := Scope{TenantID: "tenant-a"}
	valid := CollectorLiveness{
		Lease: Lease{
			Scope:       scope,
			CollectorID: "collector-a",
			BootEpoch:   "boot-a",
			StreamID:    "stream-a",
			Generation:  1,
		},
		State: LivenessStateOnline,
	}
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
			name:  "cross tenant",
			scope: scope,
			snapshot: []CollectorLiveness{func() CollectorLiveness {
				value := valid
				value.Lease.TenantID = "tenant-b"
				return value
			}()},
		},
		{
			name:  "duplicate collector",
			scope: scope,
			snapshot: []CollectorLiveness{
				valid,
				func() CollectorLiveness {
					value := valid
					value.Lease.Generation = 2
					value.Lease.BootEpoch = "boot-b"
					return value
				}(),
			},
		},
		{
			name:  "offline state",
			scope: scope,
			snapshot: []CollectorLiveness{func() CollectorLiveness {
				value := valid
				value.State = LivenessStateOffline
				return value
			}()},
		},
		{
			name:  "invented state",
			scope: scope,
			snapshot: []CollectorLiveness{func() CollectorLiveness {
				value := valid
				value.State = "invented"
				return value
			}()},
		},
		{
			name:  "invalid lease",
			scope: scope,
			snapshot: []CollectorLiveness{func() CollectorLiveness {
				value := valid
				value.Lease.Generation = 0
				return value
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
					result[index].Lease.CollectorID = "collector-" +
						string(rune('a'+index))
				}
				return result
			}(),
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, err := collectorLivenessDigest(test.scope, test.snapshot)
			if !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf(
					"collectorLivenessDigest() error = %v, want ErrInvalidArgument",
					err,
				)
			}
		})
	}
}

func TestCollectorListCursorRoundTripsTypedNullableKeys(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x5a}, minimumCollectorCursorKeyBytes)
	requestHash := collectorSHA256Digest([]byte("request"))
	livenessDigest := collectorSHA256Digest([]byte("liveness"))
	tests := []struct {
		name   string
		sortBy CollectorSortBy
		cursor collectorListCursor
	}{
		{
			name:   "display name",
			sortBy: CollectorSortByDisplayName,
			cursor: collectorListCursor{
				StringKey: &collectorListNullableString{
					Valid: true,
					Value: "Edge",
				},
			},
		},
		{
			name:   "null display name",
			sortBy: CollectorSortByDisplayName,
			cursor: collectorListCursor{
				StringKey: &collectorListNullableString{},
			},
		},
		{
			name:   "empty hostname",
			sortBy: CollectorSortByHostname,
			cursor: collectorListCursor{
				StringKey: &collectorListNullableString{Valid: true},
			},
		},
		{
			name:   "last seen",
			sortBy: CollectorSortByLastSeenAt,
			cursor: collectorListCursor{
				IntegerKey: &collectorListNullableInt64{
					Valid: true,
					Value: 1_000_000,
				},
			},
		},
		{
			name:   "zero queue",
			sortBy: CollectorSortByQueueBytes,
			cursor: collectorListCursor{
				IntegerKey: &collectorListNullableInt64{Valid: true},
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cursor := test.cursor
			cursor.RequestHash = requestHash
			cursor.Revision = 9
			cursor.LivenessDigest = livenessDigest
			cursor.CollectorID = "collector-a"
			token, err := encodeCollectorListCursor(key, cursor, test.sortBy)
			if err != nil {
				t.Fatalf("encodeCollectorListCursor(): %v", err)
			}
			if len(token) > MaximumCollectorListCursorBytes {
				t.Fatalf("token length = %d, exceeds bound", len(token))
			}
			cursor.Version = collectorListCursorVersion
			decoded, err := decodeCollectorListCursor(
				key,
				token,
				requestHash,
				cursor.Revision,
				livenessDigest,
				test.sortBy,
				false,
			)
			if err != nil {
				t.Fatalf("decodeCollectorListCursor(): %v", err)
			}
			if !reflect.DeepEqual(decoded, cursor) {
				t.Fatalf("decoded = %#v, want %#v", decoded, cursor)
			}
		})
	}
}

func TestCollectorListCursorBindsOptionalExactTotal(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x52}, minimumCollectorCursorKeyBytes)
	livenessDigest := collectorSHA256Digest([]byte("liveness"))
	zero := uint64(0)
	tests := []struct {
		name         string
		includeTotal bool
		totalSize    *uint64
		wantErr      bool
	}{
		{
			name:         "included zero total",
			includeTotal: true,
			totalSize:    &zero,
		},
		{
			name:         "included total missing",
			includeTotal: true,
			wantErr:      true,
		},
		{
			name:      "unrequested total present",
			totalSize: &zero,
			wantErr:   true,
		},
		{
			name: "unrequested total absent",
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			request, err := normalizeListRequest(
				Scope{TenantID: "tenant-a"},
				ListRequest{
					PageSize:     2,
					IncludeTotal: test.includeTotal,
				},
			)
			if err != nil {
				t.Fatalf("normalize request: %v", err)
			}
			requestHash, err := collectorListFilterHash(request)
			if err != nil {
				t.Fatalf("hash request: %v", err)
			}
			cursor := collectorListCursor{
				RequestHash:    requestHash,
				Revision:       3,
				LivenessDigest: livenessDigest,
				TotalSize:      test.totalSize,
				StringKey: &collectorListNullableString{
					Valid: true,
					Value: "Edge",
				},
				CollectorID: "collector-a",
			}
			token, err := encodeCollectorListCursor(
				key,
				cursor,
				CollectorSortByDisplayName,
			)
			if err != nil {
				t.Fatalf("encode cursor: %v", err)
			}
			decoded, err := decodeCollectorListCursor(
				key,
				token,
				requestHash,
				cursor.Revision,
				livenessDigest,
				CollectorSortByDisplayName,
				test.includeTotal,
			)
			if test.wantErr {
				if !errors.Is(err, control.ErrInvalidArgument) {
					t.Fatalf(
						"decode total-presence mismatch error = %v, want ErrInvalidArgument",
						err,
					)
				}
				return
			}
			if err != nil {
				t.Fatalf("decode cursor: %v", err)
			}
			if !reflect.DeepEqual(decoded.TotalSize, test.totalSize) {
				t.Fatalf(
					"decoded total = %v, want %v",
					decoded.TotalSize,
					test.totalSize,
				)
			}
		})
	}

	request, err := normalizeListRequest(
		Scope{TenantID: "tenant-a"},
		ListRequest{PageSize: 2, IncludeTotal: true},
	)
	if err != nil {
		t.Fatalf("normalize negative-total request: %v", err)
	}
	requestHash, err := collectorListFilterHash(request)
	if err != nil {
		t.Fatalf("hash negative-total request: %v", err)
	}
	negativeToken, err := cursorcodec.Encode(
		key,
		collectorListCursorPurpose,
		collectorListCursorVersion,
		MaximumCollectorListCursorBytes,
		map[string]any{
			"v": collectorListCursorVersion,
			"f": requestHash,
			"r": 3,
			"l": livenessDigest,
			"t": -1,
			"s": map[string]any{"v": true, "x": "Edge"},
			"i": "collector-a",
		},
	)
	if err != nil {
		t.Fatalf("sign negative-total cursor: %v", err)
	}
	if _, err := decodeCollectorListCursor(
		key,
		negativeToken,
		requestHash,
		3,
		livenessDigest,
		CollectorSortByDisplayName,
		true,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf(
			"decode negative total error = %v, want ErrInvalidArgument",
			err,
		)
	}
}

func TestCollectorListCursorRejectsTamperingAndRequestReplay(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x61}, minimumCollectorCursorKeyBytes)
	otherKey := bytes.Repeat([]byte{0x62}, minimumCollectorCursorKeyBytes)
	indexName := "main"
	tenantA, err := normalizeListRequest(
		Scope{TenantID: "tenant-a"},
		ListRequest{
			PageSize:        2,
			IndexNameFilter: &indexName,
		},
	)
	if err != nil {
		t.Fatalf("normalize tenant-a request: %v", err)
	}
	tenantAHash, err := collectorListFilterHash(tenantA)
	if err != nil {
		t.Fatalf("hash tenant-a request: %v", err)
	}
	tenantB, err := normalizeListRequest(
		Scope{TenantID: "tenant-b"},
		ListRequest{
			PageSize:        2,
			IndexNameFilter: &indexName,
		},
	)
	if err != nil {
		t.Fatalf("normalize tenant-b request: %v", err)
	}
	tenantBHash, err := collectorListFilterHash(tenantB)
	if err != nil {
		t.Fatalf("hash tenant-b request: %v", err)
	}
	otherFilter, err := normalizeListRequest(
		Scope{TenantID: "tenant-a"},
		ListRequest{
			PageSize: 2,
			SortBy:   CollectorSortByHostname,
		},
	)
	if err != nil {
		t.Fatalf("normalize other-filter request: %v", err)
	}
	otherFilterHash, err := collectorListFilterHash(otherFilter)
	if err != nil {
		t.Fatalf("hash other-filter request: %v", err)
	}
	livenessDigest := collectorSHA256Digest([]byte("liveness"))
	cursor := collectorListCursor{
		RequestHash:    tenantAHash,
		Revision:       4,
		LivenessDigest: livenessDigest,
		StringKey: &collectorListNullableString{
			Valid: true,
			Value: "Edge",
		},
		CollectorID: "collector-a",
	}
	token, err := encodeCollectorListCursor(
		key,
		cursor,
		CollectorSortByDisplayName,
	)
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	tampered := []byte(token)
	if tampered[0] == 'A' {
		tampered[0] = 'B'
	} else {
		tampered[0] = 'A'
	}

	wrongPurpose, err := cursorcodec.Encode(
		key,
		"another-purpose",
		collectorListCursorVersion,
		MaximumCollectorListCursorBytes,
		func() collectorListCursor {
			value := cursor
			value.Version = collectorListCursorVersion
			return value
		}(),
	)
	if err != nil {
		t.Fatalf("encode wrong-purpose cursor: %v", err)
	}
	wrongEnvelopeVersion, err := cursorcodec.Encode(
		key,
		collectorListCursorPurpose,
		collectorListCursorVersion+1,
		MaximumCollectorListCursorBytes,
		func() collectorListCursor {
			value := cursor
			value.Version = collectorListCursorVersion
			return value
		}(),
	)
	if err != nil {
		t.Fatalf("encode wrong-envelope-version cursor: %v", err)
	}
	wrongPayloadVersion, err := cursorcodec.Encode(
		key,
		collectorListCursorPurpose,
		collectorListCursorVersion,
		MaximumCollectorListCursorBytes,
		func() collectorListCursor {
			value := cursor
			value.Version = collectorListCursorVersion + 1
			return value
		}(),
	)
	if err != nil {
		t.Fatalf("encode wrong-payload-version cursor: %v", err)
	}
	unknownField, err := cursorcodec.Encode(
		key,
		collectorListCursorPurpose,
		collectorListCursorVersion,
		MaximumCollectorListCursorBytes,
		map[string]any{
			"v":       collectorListCursorVersion,
			"f":       tenantAHash,
			"r":       4,
			"l":       livenessDigest,
			"s":       map[string]any{"v": true, "x": "Edge"},
			"i":       "collector-a",
			"unknown": true,
		},
	)
	if err != nil {
		t.Fatalf("encode unknown-field cursor: %v", err)
	}
	tests := []struct {
		name        string
		key         []byte
		token       string
		requestHash string
	}{
		{
			name:        "tampered",
			key:         key,
			token:       string(tampered),
			requestHash: tenantAHash,
		},
		{
			name:        "wrong key",
			key:         otherKey,
			token:       token,
			requestHash: tenantAHash,
		},
		{
			name:        "wrong purpose",
			key:         key,
			token:       wrongPurpose,
			requestHash: tenantAHash,
		},
		{
			name:        "wrong envelope version",
			key:         key,
			token:       wrongEnvelopeVersion,
			requestHash: tenantAHash,
		},
		{
			name:        "wrong payload version",
			key:         key,
			token:       wrongPayloadVersion,
			requestHash: tenantAHash,
		},
		{
			name:        "unknown field",
			key:         key,
			token:       unknownField,
			requestHash: tenantAHash,
		},
		{
			name:        "oversized",
			key:         key,
			token:       strings.Repeat("x", MaximumCollectorListCursorBytes+1),
			requestHash: tenantAHash,
		},
		{
			name:        "padded",
			key:         key,
			token:       " " + token,
			requestHash: tenantAHash,
		},
		{
			name:        "cross tenant",
			key:         key,
			token:       token,
			requestHash: tenantBHash,
		},
		{
			name:        "changed filter",
			key:         key,
			token:       token,
			requestHash: otherFilterHash,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, decodeErr := decodeCollectorListCursor(
				test.key,
				test.token,
				test.requestHash,
				cursor.Revision,
				livenessDigest,
				CollectorSortByDisplayName,
				false,
			)
			if !errors.Is(decodeErr, control.ErrInvalidArgument) {
				t.Fatalf(
					"decodeCollectorListCursor() error = %v, want ErrInvalidArgument",
					decodeErr,
				)
			}
			if errors.Is(decodeErr, control.ErrPageInvalidated) {
				t.Fatalf("malformed/replayed cursor leaked page invalidation")
			}
		})
	}
}

func TestCollectorListCursorRevisionAndLivenessChangesInvalidatePage(
	t *testing.T,
) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x41}, minimumCollectorCursorKeyBytes)
	requestHash := collectorSHA256Digest([]byte("request"))
	livenessDigest := collectorSHA256Digest([]byte("liveness"))
	cursor := collectorListCursor{
		RequestHash:    requestHash,
		Revision:       8,
		LivenessDigest: livenessDigest,
		IntegerKey: &collectorListNullableInt64{
			Valid: true,
			Value: 32,
		},
		CollectorID: "collector-a",
	}
	token, err := encodeCollectorListCursor(
		key,
		cursor,
		CollectorSortByQueueBytes,
	)
	if err != nil {
		t.Fatalf("encode cursor: %v", err)
	}
	for _, test := range []struct {
		name        string
		revision    int64
		liveness    string
		requestHash string
		want        error
	}{
		{
			name:        "SQL revision",
			revision:    cursor.Revision + 1,
			liveness:    livenessDigest,
			requestHash: requestHash,
			want:        control.ErrPageInvalidated,
		},
		{
			name:        "liveness",
			revision:    cursor.Revision,
			liveness:    collectorSHA256Digest([]byte("changed")),
			requestHash: requestHash,
			want:        control.ErrPageInvalidated,
		},
		{
			name:        "request mismatch takes precedence",
			revision:    cursor.Revision + 1,
			liveness:    collectorSHA256Digest([]byte("changed")),
			requestHash: collectorSHA256Digest([]byte("other request")),
			want:        control.ErrInvalidArgument,
		},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			_, decodeErr := decodeCollectorListCursor(
				key,
				token,
				test.requestHash,
				test.revision,
				test.liveness,
				CollectorSortByQueueBytes,
				false,
			)
			if !errors.Is(decodeErr, test.want) {
				t.Fatalf(
					"decodeCollectorListCursor() error = %v, want %v",
					decodeErr,
					test.want,
				)
			}
		})
	}
}

func TestCollectorListCursorRejectsInvalidPayloadShape(t *testing.T) {
	t.Parallel()

	key := bytes.Repeat([]byte{0x33}, minimumCollectorCursorKeyBytes)
	requestHash := collectorSHA256Digest([]byte("request"))
	livenessDigest := collectorSHA256Digest([]byte("liveness"))
	base := collectorListCursor{
		Version:        collectorListCursorVersion,
		RequestHash:    requestHash,
		Revision:       1,
		LivenessDigest: livenessDigest,
		CollectorID:    "collector-a",
	}
	tests := []struct {
		name   string
		sortBy CollectorSortBy
		mutate func(*collectorListCursor)
	}{
		{
			name:   "display missing key",
			sortBy: CollectorSortByDisplayName,
		},
		{
			name:   "display has both key types",
			sortBy: CollectorSortByDisplayName,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{
					Valid: true,
					Value: "Edge",
				}
				cursor.IntegerKey = &collectorListNullableInt64{}
			},
		},
		{
			name:   "display null carries value",
			sortBy: CollectorSortByDisplayName,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{Value: "Edge"}
			},
		},
		{
			name:   "display valid empty",
			sortBy: CollectorSortByDisplayName,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{Valid: true}
			},
		},
		{
			name:   "display not canonical",
			sortBy: CollectorSortByDisplayName,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{
					Valid: true,
					Value: " Edge ",
				}
			},
		},
		{
			name:   "hostname missing key",
			sortBy: CollectorSortByHostname,
		},
		{
			name:   "hostname null carries value",
			sortBy: CollectorSortByHostname,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{
					Value: "edge",
				}
			},
		},
		{
			name:   "hostname null",
			sortBy: CollectorSortByHostname,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{}
			},
		},
		{
			name:   "last seen missing key",
			sortBy: CollectorSortByLastSeenAt,
		},
		{
			name:   "last seen has both key types",
			sortBy: CollectorSortByLastSeenAt,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{}
				cursor.IntegerKey = &collectorListNullableInt64{
					Valid: true,
					Value: 1,
				}
			},
		},
		{
			name:   "last seen null carries value",
			sortBy: CollectorSortByLastSeenAt,
			mutate: func(cursor *collectorListCursor) {
				cursor.IntegerKey = &collectorListNullableInt64{Value: 1}
			},
		},
		{
			name:   "last seen null",
			sortBy: CollectorSortByLastSeenAt,
			mutate: func(cursor *collectorListCursor) {
				cursor.IntegerKey = &collectorListNullableInt64{}
			},
		},
		{
			name:   "last seen zero",
			sortBy: CollectorSortByLastSeenAt,
			mutate: func(cursor *collectorListCursor) {
				cursor.IntegerKey = &collectorListNullableInt64{Valid: true}
			},
		},
		{
			name:   "last seen too large",
			sortBy: CollectorSortByLastSeenAt,
			mutate: func(cursor *collectorListCursor) {
				cursor.IntegerKey = &collectorListNullableInt64{
					Valid: true,
					Value: maximumPublicUnixMicro + 1,
				}
			},
		},
		{
			name:   "queue missing key",
			sortBy: CollectorSortByQueueBytes,
		},
		{
			name:   "queue null carries value",
			sortBy: CollectorSortByQueueBytes,
			mutate: func(cursor *collectorListCursor) {
				cursor.IntegerKey = &collectorListNullableInt64{Value: 1}
			},
		},
		{
			name:   "queue null",
			sortBy: CollectorSortByQueueBytes,
			mutate: func(cursor *collectorListCursor) {
				cursor.IntegerKey = &collectorListNullableInt64{}
			},
		},
		{
			name:   "queue negative",
			sortBy: CollectorSortByQueueBytes,
			mutate: func(cursor *collectorListCursor) {
				cursor.IntegerKey = &collectorListNullableInt64{
					Valid: true,
					Value: -1,
				}
			},
		},
		{
			name:   "unknown sort",
			sortBy: "invented",
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{}
			},
		},
		{
			name:   "invalid collector ID",
			sortBy: CollectorSortByDisplayName,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{}
				cursor.CollectorID = " invalid"
			},
		},
		{
			name:   "zero revision",
			sortBy: CollectorSortByDisplayName,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{}
				cursor.Revision = 0
			},
		},
		{
			name:   "invalid request hash",
			sortBy: CollectorSortByDisplayName,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{}
				cursor.RequestHash = "not-a-digest"
			},
		},
		{
			name:   "invalid liveness digest",
			sortBy: CollectorSortByDisplayName,
			mutate: func(cursor *collectorListCursor) {
				cursor.StringKey = &collectorListNullableString{}
				cursor.LivenessDigest = "not-a-digest"
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			cursor := base
			if test.mutate != nil {
				test.mutate(&cursor)
			}
			if _, err := encodeCollectorListCursor(
				key,
				cursor,
				test.sortBy,
			); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf(
					"encodeCollectorListCursor() error = %v, want ErrInvalidArgument",
					err,
				)
			}

			token, err := cursorcodec.Encode(
				key,
				collectorListCursorPurpose,
				collectorListCursorVersion,
				MaximumCollectorListCursorBytes,
				cursor,
			)
			if err != nil {
				t.Fatalf("sign invalid cursor fixture: %v", err)
			}
			if _, err := decodeCollectorListCursor(
				key,
				token,
				requestHash,
				base.Revision,
				livenessDigest,
				test.sortBy,
				false,
			); !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf(
					"decodeCollectorListCursor() error = %v, want ErrInvalidArgument",
					err,
				)
			}
		})
	}
}
