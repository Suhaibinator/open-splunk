package server

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"google.golang.org/protobuf/proto"
)

const hecOperationsAdministratorToken = "hec-operations-administrator-token-0123456789"

type staticHECOperationalSnapshotter struct {
	mu       sync.Mutex
	snapshot HECOperationalSnapshot
	err      error
	calls    int
}

func (snapshotter *staticHECOperationalSnapshotter) HECOperationalSnapshot(
	context.Context,
) (HECOperationalSnapshot, error) {
	snapshotter.mu.Lock()
	defer snapshotter.mu.Unlock()
	snapshotter.calls++
	return snapshotter.snapshot, snapshotter.err
}

func (snapshotter *staticHECOperationalSnapshotter) callCount() int {
	snapshotter.mu.Lock()
	defer snapshotter.mu.Unlock()
	return snapshotter.calls
}

func TestHECOperationalSnapshotRouteIsBoundedAndAdministratorOnly(t *testing.T) {
	t.Parallel()
	observedAt := time.Date(2026, time.August, 10, 23, 24, 25, 0, time.UTC)
	service := &staticHECOperationalSnapshotter{snapshot: HECOperationalSnapshot{
		ObservedAt:                observedAt,
		Requests:                  1,
		Events:                    2,
		UncompressedBytes:         3,
		AuthenticationFailures:    4,
		DecodeFailures:            5,
		EventPolicyFailures:       6,
		AcceptedRequests:          7,
		RateLimitedRequests:       8,
		StagingFailures:           9,
		StagingDuration:           10 * time.Millisecond,
		PendingOutboxReservations: 11,
		PendingOutboxBytes:        12,
		PendingMetadataBytes:      29,
		PendingUngrouped:          30,
		ReadyWriteGroups:          31,
		AmbiguousWriteGroups:      32,
		LiveWriteGroupLeases:      33,
		OldestPendingOutboxAge:    13 * time.Second,
		RequestCapacityAvailable:  true,
		RetainedRequests:          28,
		QueueAvailable:            true,
		ReconciliationAvailable:   true,
		ReconciliationSuccesses:   14,
		ReconciliationRetries:     15,
		ReconciliationAmbiguities: 16,
		StagedLogicalBatches:      34,
		StagedLogicalRows:         35,
		FormedWriteGroups:         36,
		PhysicalInsertSends:       37,
		SuccessfulWriteGroups:     38,
		WriteGroupMemberBatches:   39,
		WriteGroupRows:            40,
		WriteGroupDecodedBytes:    41,
		WriteGroupMonthlyParts:    42,
		MemberBatchesPerGroup:     testHECFixedHistogram(53),
		RowsPerGroup:              testHECFixedHistogram(54),
		DecodedBytesPerGroup:      testHECFixedHistogram(55),
		MonthlyPartitionsPerGroup: testHECFixedHistogram(56),
		RowsPerPhysicalInsert:     testHECFixedHistogram(57),
		FillRowTarget:             43,
		FillByteTarget:            44,
		FillHardBoundary:          45,
		FillLinger:                46,
		FillDrain:                 47,
		FillRecovery:              48,
		NativeWaiters:             49,
		PeakNativeWaiters:         58,
		NativeWaiterWakeups:       50,
		NativeWaiterCancellations: 51,
		NativeTerminalLookups:     52,
		SealLatencyBuckets:        [8]uint64{1, 2, 3, 4, 5, 6, 7, 8},
		SendLatencyBuckets:        [8]uint64{8, 7, 6, 5, 4, 3, 2, 1},
		CommitLatencyBuckets:      [8]uint64{9, 10, 11, 12, 13, 14, 15, 16},
		LatencyUpperBoundsMicros:  [7]uint64{1_000, 10_000, 50_000, 200_000, 1_000_000, 5_000_000, 30_000_000},
		ActiveChannels:            17,
		RetainedChannels:          18,
		PendingAcknowledgments:    19,
		IndexedAcknowledgments:    20,
		ExpiredAcknowledgments:    21,
		TerminalFailedRequests:    22,
		AcknowledgmentAvailable:   true,
		AcknowledgmentQueries:     23,
		AcknowledgmentIDsQueried:  24,
		AcknowledgmentMisses:      25,
		ShutdownRejections:        26,
	}}
	service.snapshot.ProtocolFailures[4] = 27
	handler := newHECOperationsHTTPHandler(t, service)

	unauthorized := postHECOperations(t, handler, "")
	if unauthorized.Code != http.StatusUnauthorized || service.callCount() != 0 {
		t.Fatalf("unauthorized HEC operations = %d calls=%d body=%q", unauthorized.Code, service.callCount(), unauthorized.Body.String())
	}
	authorized := postHECOperations(t, handler, hecOperationsAdministratorToken)
	if authorized.Code != http.StatusOK || service.callCount() != 1 {
		t.Fatalf("authorized HEC operations = %d calls=%d body=%q", authorized.Code, service.callCount(), authorized.Body.String())
	}
	var decoded opensplunk.GetHECOperationalSnapshotResponse
	if err := proto.Unmarshal(authorized.Body.Bytes(), &decoded); err != nil {
		t.Fatal(err)
	}
	if !decoded.GetObservedAt().AsTime().Equal(observedAt) ||
		decoded.GetRequest().GetRequests() != 1 || decoded.GetRequest().GetEvents() != 2 ||
		decoded.GetRequest().GetEventPolicyFailures() != 6 ||
		decoded.GetRequest().GetRateLimitedRequests() != 8 ||
		decoded.GetRequest().GetStagingDuration().AsDuration() != 10*time.Millisecond ||
		decoded.GetRequest().GetShutdownRejections() != 26 ||
		decoded.GetDurable().GetPendingOutboxReservations() != 11 ||
		decoded.GetDurable().GetPendingMetadataBytes() != 29 ||
		decoded.GetDurable().GetPendingUngrouped() != 30 ||
		decoded.GetDurable().GetReadyWriteGroups() != 31 ||
		decoded.GetDurable().GetAmbiguousWriteGroups() != 32 ||
		decoded.GetDurable().GetLiveWriteGroupLeases() != 33 ||
		decoded.GetDurable().GetOldestPendingOutboxAge().AsDuration() != 13*time.Second ||
		!decoded.GetDurable().GetRequestCapacityAvailable() ||
		decoded.GetDurable().GetRetainedRequests() != 28 ||
		!decoded.GetReconciliation().GetAvailable() ||
		decoded.GetReconciliation().GetAmbiguities() != 16 ||
		decoded.GetReconciliation().GetStagedLogicalBatches() != 34 ||
		decoded.GetReconciliation().GetStagedLogicalRows() != 35 ||
		decoded.GetReconciliation().GetFormedWriteGroups() != 36 ||
		decoded.GetReconciliation().GetPhysicalInsertSends() != 37 ||
		decoded.GetReconciliation().GetSuccessfulWriteGroups() != 38 ||
		decoded.GetReconciliation().GetWriteGroupMemberBatches() != 39 ||
		decoded.GetReconciliation().GetWriteGroupRows() != 40 ||
		decoded.GetReconciliation().GetWriteGroupDecodedBytes() != 41 ||
		decoded.GetReconciliation().GetWriteGroupMonthlyPartitions() != 42 ||
		decoded.GetReconciliation().GetFillRowTarget() != 43 ||
		decoded.GetReconciliation().GetFillByteTarget() != 44 ||
		decoded.GetReconciliation().GetFillHardBoundary() != 45 ||
		decoded.GetReconciliation().GetFillLinger() != 46 ||
		decoded.GetReconciliation().GetFillDrain() != 47 ||
		decoded.GetReconciliation().GetFillRecovery() != 48 ||
		decoded.GetReconciliation().GetNativeWaiters() != 49 ||
		decoded.GetReconciliation().GetPeakNativeWaiters() != 58 ||
		decoded.GetReconciliation().GetNativeWaiterWakeups() != 50 ||
		decoded.GetReconciliation().GetNativeWaiterCancellations() != 51 ||
		decoded.GetReconciliation().GetNativeTerminalLookups() != 52 ||
		!reflect.DeepEqual(decoded.GetReconciliation().GetSealLatencyBuckets(), []uint64{1, 2, 3, 4, 5, 6, 7, 8}) ||
		!reflect.DeepEqual(decoded.GetReconciliation().GetSendLatencyBuckets(), []uint64{8, 7, 6, 5, 4, 3, 2, 1}) ||
		!reflect.DeepEqual(decoded.GetReconciliation().GetCommitLatencyBuckets(), []uint64{9, 10, 11, 12, 13, 14, 15, 16}) ||
		!reflect.DeepEqual(decoded.GetReconciliation().GetLatencyUpperBoundsMicroseconds(), []uint64{
			1_000, 10_000, 50_000, 200_000, 1_000_000, 5_000_000, 30_000_000,
		}) ||
		decoded.GetReconciliation().GetMemberBatchesPerGroup().GetCount() != 53 ||
		decoded.GetReconciliation().GetMemberBatchesPerGroup().GetSum() != 54 ||
		decoded.GetReconciliation().GetMemberBatchesPerGroup().GetMax() != 55 ||
		!reflect.DeepEqual(
			decoded.GetReconciliation().GetMemberBatchesPerGroup().GetUpperBounds(),
			[]uint64{1, 10, 64, 100, 500, 1_000, 2_500, 5_000, 10_000, 16_384, 32_768, 50_000, 65_536},
		) ||
		!reflect.DeepEqual(
			decoded.GetReconciliation().GetMemberBatchesPerGroup().GetBucketCounts(),
			[]uint64{53, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
		) ||
		decoded.GetReconciliation().GetRowsPerGroup().GetCount() != 54 ||
		decoded.GetReconciliation().GetDecodedBytesPerGroup().GetCount() != 55 ||
		decoded.GetReconciliation().GetMonthlyPartitionsPerGroup().GetCount() != 56 ||
		decoded.GetReconciliation().GetRowsPerPhysicalInsert().GetCount() != 57 ||
		decoded.GetAcknowledgments().GetActiveChannels() != 17 ||
		decoded.GetAcknowledgments().GetRetainedChannels() != 18 ||
		decoded.GetAcknowledgments().GetExpiredRows() != 21 ||
		decoded.GetAcknowledgments().GetMisses() != 25 {
		t.Fatalf("HEC operational response = %+v", &decoded)
	}
	if len(decoded.GetProtocolFailures()) != 27 ||
		decoded.GetProtocolFailures()[3].GetCode() != 4 ||
		decoded.GetProtocolFailures()[3].GetCount() != 27 {
		t.Fatalf("protocol failure projection = %+v", decoded.GetProtocolFailures())
	}
	assertHECOperationalSnapshotHasNoIdentityFields(t)
}

func testHECFixedHistogram(count uint64) HECFixedHistogramSnapshot {
	return HECFixedHistogramSnapshot{
		UpperBounds:  [13]uint64{1, 10, 64, 100, 500, 1_000, 2_500, 5_000, 10_000, 16_384, 32_768, 50_000, 65_536},
		BucketCounts: [14]uint64{count},
		Count:        count,
		Sum:          count + 1,
		Max:          count + 2,
	}
}

func TestHECOperationalSnapshotErrorsAreSanitized(t *testing.T) {
	t.Parallel()
	for _, test := range []struct {
		name     string
		snapshot HECOperationalSnapshot
		err      error
		status   int
	}{
		{name: "dependency", err: errors.New("private sqlite path and token material"), status: http.StatusServiceUnavailable},
		{name: "invalid observed time", snapshot: HECOperationalSnapshot{}, status: http.StatusInternalServerError},
		{name: "negative age", snapshot: HECOperationalSnapshot{ObservedAt: time.Now(), OldestPendingOutboxAge: -1}, status: http.StatusInternalServerError},
	} {
		t.Run(test.name, func(t *testing.T) {
			service := &staticHECOperationalSnapshotter{snapshot: test.snapshot, err: test.err}
			response := postHECOperations(t, newHECOperationsHTTPHandler(t, service), hecOperationsAdministratorToken)
			if response.Code != test.status || strings.Contains(response.Body.String(), "private") ||
				strings.Contains(response.Body.String(), "token") ||
				strings.Contains(response.Body.String(), "sqlite") {
				t.Fatalf("HEC operational error = %d %q", response.Code, response.Body.String())
			}
		})
	}
}

func TestHECOperationalServiceRequiresBrowserAuthentication(t *testing.T) {
	t.Parallel()
	_, err := NewHandler(Config{
		SearchJobs:    &fakeSearchJobs{},
		Indexes:       fakeIndexCatalog{},
		HECOperations: &staticHECOperationalSnapshotter{},
		SavedSearches: &fakeSavedSearches{},
		WebUI:         testUI(),
	})
	if err == nil || !strings.Contains(err.Error(), "administrative services require browser authentication") {
		t.Fatalf("NewHandler HEC operations error = %v", err)
	}
}

func newHECOperationsHTTPHandler(
	t *testing.T,
	service HECOperationalSnapshotter,
) *Handler {
	t.Helper()
	authenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(hecOperationsAdministratorToken),
		"hec-tenant",
		"hec-owner",
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatal(err)
	}
	return newTestHandler(t, Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		HECOperations:              service,
		BrowserAuthenticator:       authenticator,
		WebUI:                      testUI(),
		TenantID:                   "hec-tenant",
		OwnerID:                    "hec-owner",
		AdministrativeAllowedHosts: []string{"example.com"},
	})
}

func postHECOperations(
	t *testing.T,
	handler http.Handler,
	token string,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(&opensplunk.GetHECOperationalSnapshotRequest{})
	if err != nil {
		t.Fatal(err)
	}
	request := httptest.NewRequestWithContext(
		t.Context(),
		http.MethodPost,
		hecOperationsPath,
		bytes.NewReader(payload),
	)
	request.Host = "example.com"
	request.Header.Set("Content-Type", "application/x-protobuf")
	if token != "" {
		request.Header.Set("Authorization", "Bearer "+token)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func assertHECOperationalSnapshotHasNoIdentityFields(t *testing.T) {
	t.Helper()
	typeOfSnapshot := reflect.TypeFor[HECOperationalSnapshot]()
	for field := range typeOfSnapshot.Fields() {
		kind := field.Type.Kind()
		if field.Type == reflect.TypeFor[time.Time]() || kind == reflect.Bool ||
			kind == reflect.Uint64 || kind == reflect.Int64 ||
			kind == reflect.Array && field.Type.Elem().Kind() == reflect.Uint64 ||
			field.Type == reflect.TypeFor[HECFixedHistogramSnapshot]() {
			continue
		}
		t.Errorf("identity-capable HEC operational field %s has type %s", field.Name, field.Type)
	}
	formatted := fmt.Sprintf("%+v", HECOperationalSnapshot{})
	for _, private := range []string{"Token", "ChannelID", "IndexName", "RequestID", "EventField"} {
		if strings.Contains(formatted, private) {
			t.Errorf("HEC operational snapshot shape contains private field %q", private)
		}
	}
}
