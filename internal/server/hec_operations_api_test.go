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
		OldestPendingOutboxAge:    13 * time.Second,
		RequestCapacityAvailable:  true,
		RetainedRequests:          28,
		QueueAvailable:            true,
		ReconciliationAvailable:   true,
		ReconciliationSuccesses:   14,
		ReconciliationRetries:     15,
		ReconciliationAmbiguities: 16,
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
		InsertCoalescing: HECInsertCoalescingSnapshot{
			StagedLogicalBatches: 29,
			StagedLogicalRows:    30,
			FormedGroups:         31,
			PhysicalSends:        32,
			SuccessfulGroups:     33,
			Retries:              34,
			Ambiguities:          35,
			NativeWaiters:        36,
			Queue: HECInsertCoalescingQueueSnapshot{
				PendingReservations:   37,
				UngroupedReservations: 38,
				ReadyGroups:           39,
				AmbiguousGroups:       40,
				LeasedGroups:          41,
				PendingOutboxBytes:    42,
				PendingMetadataBytes:  43,
				OldestPendingAge:      44 * time.Second,
			},
		},
	}}
	service.snapshot.ProtocolFailures[4] = 27
	service.snapshot.InsertCoalescing.GroupsByFillReason[4] = 45
	service.snapshot.InsertCoalescing.RowsPerPhysicalInsert.Bounds[0] = 1_000
	service.snapshot.InsertCoalescing.RowsPerPhysicalInsert.Counts[0] = 46
	service.snapshot.InsertCoalescing.RowsPerPhysicalInsert.Count = 46
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
		decoded.GetDurable().GetOldestPendingOutboxAge().AsDuration() != 13*time.Second ||
		!decoded.GetDurable().GetRequestCapacityAvailable() ||
		decoded.GetDurable().GetRetainedRequests() != 28 ||
		!decoded.GetReconciliation().GetAvailable() ||
		decoded.GetReconciliation().GetAmbiguities() != 16 ||
		decoded.GetAcknowledgments().GetActiveChannels() != 17 ||
		decoded.GetAcknowledgments().GetRetainedChannels() != 18 ||
		decoded.GetAcknowledgments().GetExpiredRows() != 21 ||
		decoded.GetAcknowledgments().GetMisses() != 25 ||
		decoded.GetInsertCoalescing().GetStagedLogicalBatches() != 29 ||
		decoded.GetInsertCoalescing().GetPhysicalSends() != 32 ||
		decoded.GetInsertCoalescing().GetQueue().GetUngroupedReservations() != 38 ||
		decoded.GetInsertCoalescing().GetQueue().GetOldestPendingAge().AsDuration() != 44*time.Second ||
		decoded.GetInsertCoalescing().GetGroupsByFillReason()[4] != 45 ||
		decoded.GetInsertCoalescing().GetRowsPerPhysicalInsert().GetUpperBounds()[0] != 1_000 ||
		decoded.GetInsertCoalescing().GetRowsPerPhysicalInsert().GetBucketCounts()[0] != 46 {
		t.Fatalf("HEC operational response = %+v", &decoded)
	}
	if len(decoded.GetProtocolFailures()) != 27 ||
		decoded.GetProtocolFailures()[3].GetCode() != 4 ||
		decoded.GetProtocolFailures()[3].GetCount() != 27 {
		t.Fatalf("protocol failure projection = %+v", decoded.GetProtocolFailures())
	}
	assertHECOperationalSnapshotHasNoIdentityFields(t)
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
		{
			name: "negative coalescing age",
			snapshot: HECOperationalSnapshot{
				ObservedAt: time.Now(),
				InsertCoalescing: HECInsertCoalescingSnapshot{
					Queue: HECInsertCoalescingQueueSnapshot{OldestPendingAge: -1},
				},
			},
			status: http.StatusInternalServerError,
		},
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
	assertHECOperationalTypeHasNoIdentityFields(t, reflect.TypeFor[HECOperationalSnapshot](), "")
	formatted := fmt.Sprintf("%+v", HECOperationalSnapshot{})
	for _, private := range []string{"Token", "ChannelID", "IndexName", "RequestID", "EventField"} {
		if strings.Contains(formatted, private) {
			t.Errorf("HEC operational snapshot shape contains private field %q", private)
		}
	}
}

func assertHECOperationalTypeHasNoIdentityFields(t *testing.T, value reflect.Type, path string) {
	t.Helper()
	for field := range value.Fields() {
		fieldPath := field.Name
		if path != "" {
			fieldPath = path + "." + field.Name
		}
		kind := field.Type.Kind()
		switch {
		case field.Type == reflect.TypeFor[time.Time](), kind == reflect.Bool,
			kind == reflect.Uint64, kind == reflect.Int64,
			kind == reflect.Array && field.Type.Elem().Kind() == reflect.Uint64:
			continue
		case kind == reflect.Struct:
			assertHECOperationalTypeHasNoIdentityFields(t, field.Type, fieldPath)
		default:
			t.Errorf("identity-capable HEC operational field %s has type %s", fieldPath, field.Type)
		}
	}
}
