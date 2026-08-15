package server

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

const collectorAdministrationBearerToken = "open-splunk-collector-administrator-test-token-0123456789"

type fakeCollectorAdministration struct {
	mu sync.Mutex

	getCalls    int
	listCalls   int
	updateCalls int
	stateCalls  int

	getFn func(
		context.Context,
		collectorfleet.Scope,
		string,
	) (collectorfleet.CatalogEntry, error)
	listFn func(
		context.Context,
		collectorfleet.Scope,
		collectorfleet.ListRequest,
	) (collectorfleet.ListResult, error)
	updateFn func(
		context.Context,
		collectorfleet.Scope,
		string,
		uint64,
		*string,
		time.Time,
	) (collectorfleet.AdministrationSnapshot, error)
	stateFn func(
		context.Context,
		collectorfleet.Scope,
		string,
		uint64,
		collectorfleet.AdministrativeState,
		time.Time,
	) (collectorfleet.AdministrationSnapshot, error)
}

func (service *fakeCollectorAdministration) Get(
	ctx context.Context,
	scope collectorfleet.Scope,
	collectorID string,
) (collectorfleet.CatalogEntry, error) {
	service.mu.Lock()
	service.getCalls++
	fn := service.getFn
	service.mu.Unlock()
	if fn == nil {
		return collectorfleet.CatalogEntry{}, errors.New("unexpected Get")
	}
	return fn(ctx, scope, collectorID)
}

func (service *fakeCollectorAdministration) List(
	ctx context.Context,
	scope collectorfleet.Scope,
	request collectorfleet.ListRequest,
) (collectorfleet.ListResult, error) {
	service.mu.Lock()
	service.listCalls++
	fn := service.listFn
	service.mu.Unlock()
	if fn == nil {
		return collectorfleet.ListResult{}, errors.New("unexpected List")
	}
	return fn(ctx, scope, request)
}

func (service *fakeCollectorAdministration) UpdateDisplayName(
	ctx context.Context,
	scope collectorfleet.Scope,
	collectorID string,
	expectedVersion uint64,
	displayName *string,
	receivedAt time.Time,
) (collectorfleet.AdministrationSnapshot, error) {
	service.mu.Lock()
	service.updateCalls++
	fn := service.updateFn
	service.mu.Unlock()
	if fn == nil {
		return collectorfleet.AdministrationSnapshot{}, errors.New(
			"unexpected UpdateDisplayName",
		)
	}
	return fn(
		ctx,
		scope,
		collectorID,
		expectedVersion,
		displayName,
		receivedAt,
	)
}

func (service *fakeCollectorAdministration) SetAdministrativeState(
	ctx context.Context,
	scope collectorfleet.Scope,
	collectorID string,
	expectedVersion uint64,
	state collectorfleet.AdministrativeState,
	receivedAt time.Time,
) (collectorfleet.AdministrationSnapshot, error) {
	service.mu.Lock()
	service.stateCalls++
	fn := service.stateFn
	service.mu.Unlock()
	if fn == nil {
		return collectorfleet.AdministrationSnapshot{}, errors.New(
			"unexpected SetAdministrativeState",
		)
	}
	return fn(
		ctx,
		scope,
		collectorID,
		expectedVersion,
		state,
		receivedAt,
	)
}

func (service *fakeCollectorAdministration) calls() [4]int {
	service.mu.Lock()
	defer service.mu.Unlock()
	return [4]int{
		service.getCalls,
		service.listCalls,
		service.updateCalls,
		service.stateCalls,
	}
}

func TestCollectorAdministrationListPassesOpaqueTokenAndReleasesPermit(
	t *testing.T,
) {
	t.Parallel()

	const (
		tenantID = "tenant-collector-list"
		ownerID  = "owner-collector-list"
	)
	pageSize := uint32(2)
	requestToken := "opaque.request-token.v1"
	responseToken := "opaque.response-token.v1"
	total := uint64(5)
	service := &fakeCollectorAdministration{
		listFn: func(
			_ context.Context,
			scope collectorfleet.Scope,
			request collectorfleet.ListRequest,
		) (collectorfleet.ListResult, error) {
			if scope.TenantID != tenantID ||
				request.PageSize != pageSize ||
				request.PageToken != requestToken ||
				!request.IncludeTotal ||
				request.SortBy !=
					collectorfleet.CollectorSortByQueueBytes ||
				request.Direction != collectorfleet.SortDescending ||
				!slices.Equal(
					request.StateFilters,
					[]collectorfleet.ConnectionState{
						collectorfleet.ConnectionStateOnline,
						collectorfleet.ConnectionStateStale,
					},
				) ||
				request.IndexNameFilter == nil ||
				*request.IndexNameFilter != "main" ||
				request.TextFilter == nil ||
				*request.TextFilter != "needle" {
				t.Fatalf("List scope/request = %#v/%#v", scope, request)
			}
			entry := validCollectorCatalogEntry(tenantID, "collector-list")
			return collectorfleet.ListResult{
				Entries:         []collectorfleet.CatalogEntry{entry},
				NextPageToken:   new(responseToken),
				TotalSize:       &total,
				TotalSizeExact:  true,
				CatalogRevision: 9,
			}, nil
		},
	}
	handler := collectorAdministrationAPIHandler(
		t,
		tenantID,
		ownerID,
		service,
	)
	request := collectorAdministrationDirectRequest(
		t,
		context.Background(),
		tenantID,
		ownerID,
		"/api/v1/collectors/list",
	)
	result, err := handler.listCollectors(
		request,
		&opensplunkv1.ListCollectorsRequest{
			Page: &opensplunkv1.PageRequest{
				PageSize:         &pageSize,
				PageToken:        &requestToken,
				IncludeTotalSize: true,
			},
			StateFilters: []opensplunkv1.CollectorConnectionState{
				opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_STALE,
				opensplunkv1.CollectorConnectionState_COLLECTOR_CONNECTION_STATE_ONLINE,
			},
			IndexNameFilter: new("main"),
			TextFilter:      new(" needle "),
			SortBy:          opensplunkv1.CollectorSortBy_COLLECTOR_SORT_BY_QUEUE_BYTES,
			SortDirection:   opensplunkv1.SortDirection_SORT_DIRECTION_DESCENDING,
		},
	)
	if err != nil || result == nil {
		t.Fatalf("listCollectors() = %#v, %v", result, err)
	}
	if len(handler.serializationGate) != 1 {
		t.Fatalf(
			"serialization permits = %d, want 1 before encoding",
			len(handler.serializationGate),
		)
	}
	if got := result.message.GetPage().GetNextPageToken(); got != responseToken {
		t.Fatalf("next page token = %q, want %q", got, responseToken)
	}
	response := httptest.NewRecorder()
	if err := newSerializedListCollectorsCodec().Encode(
		response,
		result,
	); err != nil {
		t.Fatalf("Encode() error = %v", err)
	}
	if len(handler.serializationGate) != 0 {
		t.Fatalf(
			"serialization permit was not released: %d",
			len(handler.serializationGate),
		)
	}
	var decoded opensplunkv1.ListCollectorsResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if decoded.GetPage().GetNextPageToken() != responseToken ||
		len(decoded.GetCollectors()) != 1 ||
		decoded.GetCollectors()[0].GetCollectorId() != "collector-list" {
		t.Fatalf("decoded response = %v", &decoded)
	}
}

func TestCollectorAdministrationReadSerializationCancellationReleasesPermit(
	t *testing.T,
) {
	t.Parallel()

	const (
		tenantID = "tenant-collector-encode-cancel"
		ownerID  = "owner-collector-encode-cancel"
	)
	service := &fakeCollectorAdministration{
		listFn: func(
			context.Context,
			collectorfleet.Scope,
			collectorfleet.ListRequest,
		) (collectorfleet.ListResult, error) {
			return collectorfleet.ListResult{
				Entries: []collectorfleet.CatalogEntry{
					validCollectorCatalogEntry(
						tenantID,
						"collector-encode-cancel",
					),
				},
				CatalogRevision: 1,
			}, nil
		},
	}
	handler := collectorAdministrationAPIHandler(
		t,
		tenantID,
		ownerID,
		service,
	)
	ctx, cancel := context.WithCancel(context.Background())
	request := collectorAdministrationDirectRequest(
		t,
		ctx,
		tenantID,
		ownerID,
		"/api/v1/collectors/list",
	)
	result, err := handler.listCollectors(
		request,
		&opensplunkv1.ListCollectorsRequest{},
	)
	if err != nil || result == nil ||
		len(handler.serializationGate) != 1 {
		t.Fatalf(
			"list result/error/gate = %#v/%v/%d",
			result,
			err,
			len(handler.serializationGate),
		)
	}
	cancel()
	err = newSerializedListCollectorsCodec().Encode(
		httptest.NewRecorder(),
		result,
	)
	if !errors.Is(err, context.Canceled) ||
		len(handler.serializationGate) != 0 {
		t.Fatalf(
			"canceled Encode error/gate = %v/%d",
			err,
			len(handler.serializationGate),
		)
	}
}

func TestCollectorAdministrationCommittedMutationSurvivesCancellation(
	t *testing.T,
) {
	t.Parallel()

	const (
		tenantID    = "tenant-collector-update"
		ownerID     = "owner-collector-update"
		collectorID = "collector-update"
	)
	now := collectorAdministrationTestTime()
	ctx, cancel := context.WithCancel(context.Background())
	service := &fakeCollectorAdministration{
		updateFn: func(
			_ context.Context,
			scope collectorfleet.Scope,
			gotCollectorID string,
			expectedVersion uint64,
			displayName *string,
			receivedAt time.Time,
		) (collectorfleet.AdministrationSnapshot, error) {
			if scope.TenantID != tenantID ||
				gotCollectorID != collectorID ||
				expectedVersion != 1 ||
				displayName == nil ||
				*displayName != "Production Collector" ||
				!receivedAt.Equal(now) {
				t.Fatalf(
					"UpdateDisplayName arguments = %#v, %q, %d, %v, %v",
					scope,
					gotCollectorID,
					expectedVersion,
					displayName,
					receivedAt,
				)
			}
			cancel()
			return validCollectorAdministrationSnapshot(
				tenantID,
				collectorID,
				2,
				displayName,
				collectorfleet.AdministrativeStateEnabled,
			), nil
		},
	}
	handler := collectorAdministrationAPIHandler(
		t,
		tenantID,
		ownerID,
		service,
	)
	handler.now = func() time.Time { return now }
	request := collectorAdministrationDirectRequest(
		t,
		ctx,
		tenantID,
		ownerID,
		"/api/v1/collectors/update",
	)
	displayName := " Production Collector "
	result, err := handler.updateCollector(
		request,
		&opensplunkv1.UpdateCollectorRequest{
			CollectorId:     collectorID,
			ExpectedVersion: 1,
			DisplayName:     &displayName,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"display_name"},
			},
		},
	)
	if err != nil || result == nil {
		t.Fatalf("updateCollector() = %#v, %v", result, err)
	}
	if ctx.Err() == nil || result.ctx != nil ||
		len(handler.serializationGate) != 1 {
		t.Fatalf(
			"committed response context/gate = %v/%d",
			result.ctx,
			len(handler.serializationGate),
		)
	}
	response := httptest.NewRecorder()
	if err := newSerializedUpdateCollectorCodec().Encode(
		response,
		result,
	); err != nil {
		t.Fatalf("Encode(committed response) error = %v", err)
	}
	if len(handler.serializationGate) != 0 {
		t.Fatalf(
			"serialization permit was not released: %d",
			len(handler.serializationGate),
		)
	}
	var decoded opensplunkv1.UpdateCollectorResponse
	if err := proto.Unmarshal(response.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}
	if decoded.GetCollector().GetVersion() != 2 ||
		decoded.GetCollector().GetDisplayName() !=
			"Production Collector" {
		t.Fatalf("committed response = %v", &decoded)
	}
}

func TestCollectorAdministrationRejectsInvalidListOutputAndReleasesPermit(
	t *testing.T,
) {
	t.Parallel()

	const (
		tenantID = "tenant-invalid-collector-list-output"
		ownerID  = "owner-invalid-collector-list-output"
	)
	entry := func() collectorfleet.CatalogEntry {
		return validCollectorCatalogEntry(
			tenantID,
			"collector-invalid-list-output",
		)
	}
	one := uint64(1)
	overCapacity := uint64(
		collectorfleet.MaximumDurableCollectorsPerTenant + 1,
	)
	tests := []struct {
		name         string
		includeTotal bool
		result       func() collectorfleet.ListResult
	}{
		{
			name: "continuation without revision",
			result: func() collectorfleet.ListResult {
				return collectorfleet.ListResult{
					Entries:       []collectorfleet.CatalogEntry{entry()},
					NextPageToken: new("cursor"),
				}
			},
		},
		{
			name: "continuation without a row",
			result: func() collectorfleet.ListResult {
				return collectorfleet.ListResult{
					NextPageToken:   new("cursor"),
					CatalogRevision: 1,
				}
			},
		},
		{
			name: "oversized continuation",
			result: func() collectorfleet.ListResult {
				return collectorfleet.ListResult{
					Entries:         []collectorfleet.CatalogEntry{entry()},
					NextPageToken:   new(strings.Repeat("x", collectorfleet.MaximumCollectorListCursorBytes+1)),
					CatalogRevision: 1,
				}
			},
		},
		{
			name:         "requested total missing",
			includeTotal: true,
			result: func() collectorfleet.ListResult {
				return collectorfleet.ListResult{
					Entries:         []collectorfleet.CatalogEntry{entry()},
					CatalogRevision: 1,
				}
			},
		},
		{
			name:         "requested total is not exact",
			includeTotal: true,
			result: func() collectorfleet.ListResult {
				return collectorfleet.ListResult{
					Entries:         []collectorfleet.CatalogEntry{entry()},
					TotalSize:       &one,
					CatalogRevision: 1,
				}
			},
		},
		{
			name: "unrequested total",
			result: func() collectorfleet.ListResult {
				return collectorfleet.ListResult{
					Entries:         []collectorfleet.CatalogEntry{entry()},
					TotalSize:       &one,
					TotalSizeExact:  true,
					CatalogRevision: 1,
				}
			},
		},
		{
			name:         "total exceeds durable fleet capacity",
			includeTotal: true,
			result: func() collectorfleet.ListResult {
				return collectorfleet.ListResult{
					Entries:         []collectorfleet.CatalogEntry{entry()},
					TotalSize:       &overCapacity,
					TotalSizeExact:  true,
					CatalogRevision: 1,
				}
			},
		},
		{
			name:         "continuation total does not exceed page",
			includeTotal: true,
			result: func() collectorfleet.ListResult {
				return collectorfleet.ListResult{
					Entries:         []collectorfleet.CatalogEntry{entry()},
					NextPageToken:   new("cursor"),
					TotalSize:       &one,
					TotalSizeExact:  true,
					CatalogRevision: 1,
				}
			},
		},
		{
			name: "duplicate collector",
			result: func() collectorfleet.ListResult {
				duplicate := entry()
				return collectorfleet.ListResult{
					Entries: []collectorfleet.CatalogEntry{
						duplicate,
						duplicate,
					},
					CatalogRevision: 1,
				}
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			service := &fakeCollectorAdministration{
				listFn: func(
					context.Context,
					collectorfleet.Scope,
					collectorfleet.ListRequest,
				) (collectorfleet.ListResult, error) {
					return test.result(), nil
				},
			}
			handler := collectorAdministrationAPIHandler(
				t,
				tenantID,
				ownerID,
				service,
			)
			input := &opensplunkv1.ListCollectorsRequest{}
			if test.includeTotal {
				input.Page = &opensplunkv1.PageRequest{
					IncludeTotalSize: true,
				}
			}
			result, err := handler.listCollectors(
				collectorAdministrationDirectRequest(
					t,
					context.Background(),
					tenantID,
					ownerID,
					"/api/v1/collectors/list",
				),
				input,
			)
			if result != nil {
				t.Fatalf("list result = %#v, want nil", result)
			}
			assertCollectorAdministrationHTTPError(
				t,
				err,
				http.StatusInternalServerError,
			)
			if service.calls() != ([4]int{0, 1, 0, 0}) ||
				len(handler.serializationGate) != 0 {
				t.Fatalf(
					"calls/gate = %v/%d",
					service.calls(),
					len(handler.serializationGate),
				)
			}
		})
	}
}

func TestCollectorAdministrationRejectsInvalidMutationVersionAndReleasesPermit(
	t *testing.T,
) {
	t.Parallel()

	const (
		tenantID    = "tenant-invalid-collector-mutation"
		ownerID     = "owner-invalid-collector-mutation"
		collectorID = "collector-invalid-mutation"
	)
	displayName := "Invalid mutation response"
	service := &fakeCollectorAdministration{
		updateFn: func(
			context.Context,
			collectorfleet.Scope,
			string,
			uint64,
			*string,
			time.Time,
		) (collectorfleet.AdministrationSnapshot, error) {
			return validCollectorAdministrationSnapshot(
				tenantID,
				collectorID,
				1,
				&displayName,
				collectorfleet.AdministrativeStateEnabled,
			), nil
		},
	}
	handler := collectorAdministrationAPIHandler(
		t,
		tenantID,
		ownerID,
		service,
	)
	result, err := handler.updateCollector(
		collectorAdministrationDirectRequest(
			t,
			context.Background(),
			tenantID,
			ownerID,
			"/api/v1/collectors/update",
		),
		&opensplunkv1.UpdateCollectorRequest{
			CollectorId:     collectorID,
			ExpectedVersion: 1,
			DisplayName:     &displayName,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"display_name"},
			},
		},
	)
	if result != nil {
		t.Fatalf("update result = %#v, want nil", result)
	}
	assertCollectorAdministrationHTTPError(
		t,
		err,
		http.StatusInternalServerError,
	)
	if service.calls() != ([4]int{0, 0, 1, 0}) ||
		len(handler.serializationGate) != 0 {
		t.Fatalf(
			"calls/gate = %v/%d",
			service.calls(),
			len(handler.serializationGate),
		)
	}
}

func TestCollectorAdministrationRejectsTenantMismatchAndReleasesPermit(
	t *testing.T,
) {
	t.Parallel()

	const (
		tenantID = "tenant-collector-output"
		ownerID  = "owner-collector-output"
	)
	service := &fakeCollectorAdministration{
		getFn: func(
			context.Context,
			collectorfleet.Scope,
			string,
		) (collectorfleet.CatalogEntry, error) {
			return validCollectorCatalogEntry(
				"other-tenant",
				"collector-output",
			), nil
		},
	}
	handler := collectorAdministrationAPIHandler(
		t,
		tenantID,
		ownerID,
		service,
	)
	request := collectorAdministrationDirectRequest(
		t,
		context.Background(),
		tenantID,
		ownerID,
		"/api/v1/collectors/get",
	)
	result, err := handler.getCollector(
		request,
		&opensplunkv1.GetCollectorRequest{
			CollectorId: "collector-output",
		},
	)
	if result != nil {
		t.Fatalf("getCollector() result = %#v, want nil", result)
	}
	assertCollectorAdministrationHTTPError(
		t,
		err,
		http.StatusInternalServerError,
	)
	if service.calls() != ([4]int{1, 0, 0, 0}) ||
		len(handler.serializationGate) != 0 {
		t.Fatalf(
			"calls/gate = %v/%d",
			service.calls(),
			len(handler.serializationGate),
		)
	}
}

func TestCollectorAdministrationRejectsMalformedNestedOutput(t *testing.T) {
	t.Parallel()

	const tenantID = "tenant-malformed-collector-output"
	tests := []struct {
		name   string
		mutate func(*collectorfleet.CatalogEntry)
	}{
		{
			name: "oversized capabilities",
			mutate: func(entry *collectorfleet.CatalogEntry) {
				entry.Collector.Capabilities = make(
					[]uint32,
					maximumCollectorCapabilities+1,
				)
			},
		},
		{
			name: "oversized authorized indexes",
			mutate: func(entry *collectorfleet.CatalogEntry) {
				entry.Collector.AuthorizedIndexes = make(
					[]string,
					maximumCollectorAuthorizedIndexes+1,
				)
			},
		},
		{
			name: "oversized input health",
			mutate: func(entry *collectorfleet.CatalogEntry) {
				entry.Collector.InputHealth = make(
					[]collectorfleet.InputHealth,
					maximumCollectorInputHealth+1,
				)
			},
		},
		{
			name: "non-microsecond lifecycle timestamp",
			mutate: func(entry *collectorfleet.CatalogEntry) {
				entry.Collector.FirstSeenAt =
					entry.Collector.FirstSeenAt.Add(time.Nanosecond)
			},
		},
		{
			name: "non-microsecond nested timestamp",
			mutate: func(entry *collectorfleet.CatalogEntry) {
				lastEvent := entry.Collector.InputHealth[0].
					LastEventAt.
					Add(time.Nanosecond)
				entry.Collector.InputHealth[0].LastEventAt = &lastEvent
			},
		},
		{
			name: "invalid connection enum",
			mutate: func(entry *collectorfleet.CatalogEntry) {
				entry.ConnectionState =
					collectorfleet.ConnectionState("future")
			},
		},
		{
			name: "invalid administrative enum",
			mutate: func(entry *collectorfleet.CatalogEntry) {
				entry.Collector.AdministrativeState =
					collectorfleet.AdministrativeState("future")
			},
		},
		{
			name: "invalid capability enum",
			mutate: func(entry *collectorfleet.CatalogEntry) {
				entry.Collector.Capabilities = []uint32{0}
			},
		},
		{
			name: "invalid input health enum",
			mutate: func(entry *collectorfleet.CatalogEntry) {
				entry.Collector.InputHealth[0].State = 0
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			entry := validCollectorCatalogEntry(
				tenantID,
				"collector-malformed-output",
			)
			test.mutate(&entry)
			if converted, err := collectorCatalogEntryToProto(
				collectorfleet.Scope{TenantID: tenantID},
				entry,
			); err == nil || converted != nil {
				t.Fatalf(
					"collectorCatalogEntryToProto() = %#v, %v",
					converted,
					err,
				)
			}
		})
	}
}

func TestCollectorAdministrationActiveInstanceRequiresLiveOverlay(
	t *testing.T,
) {
	t.Parallel()

	const tenantID = "tenant-live-overlay"
	base := validCollectorCatalogEntry(tenantID, "collector-live")
	base.Collector.ActiveLease = &collectorfleet.ActiveLease{
		BootEpoch:  "boot-live",
		StreamID:   "stream-live",
		InstanceID: "instance-live",
		Generation: 1,
	}
	for _, state := range []collectorfleet.ConnectionState{
		collectorfleet.ConnectionStateOffline,
		collectorfleet.ConnectionStateDisabled,
	} {
		entry := base
		entry.ConnectionState = state
		entry.Collector.AdministrativeState =
			collectorfleet.AdministrativeStateEnabled
		if state == collectorfleet.ConnectionStateDisabled {
			entry.Collector.AdministrativeState =
				collectorfleet.AdministrativeStateDisabled
		}
		converted, err := collectorCatalogEntryToProto(
			collectorfleet.Scope{TenantID: tenantID},
			entry,
		)
		if err != nil {
			t.Fatalf("convert %s entry: %v", state, err)
		}
		if converted.ActiveInstanceId != nil {
			t.Fatalf(
				"%s active instance = %q, want absent",
				state,
				converted.GetActiveInstanceId(),
			)
		}
	}
	for _, state := range []collectorfleet.ConnectionState{
		collectorfleet.ConnectionStateOnline,
		collectorfleet.ConnectionStateStale,
	} {
		entry := base
		entry.ConnectionState = state
		converted, err := collectorCatalogEntryToProto(
			collectorfleet.Scope{TenantID: tenantID},
			entry,
		)
		if err != nil {
			t.Fatalf("convert %s entry: %v", state, err)
		}
		if converted.GetActiveInstanceId() != "instance-live" {
			t.Fatalf(
				"%s active instance = %q",
				state,
				converted.GetActiveInstanceId(),
			)
		}
		entry.Collector.ActiveLease = nil
		if _, err := collectorCatalogEntryToProto(
			collectorfleet.Scope{TenantID: tenantID},
			entry,
		); err == nil {
			t.Fatalf("%s entry without a lease was accepted", state)
		}
	}
}

func TestCollectorAdministrationRejectsMalformedRequestsBeforeService(
	t *testing.T,
) {
	t.Parallel()

	const (
		tenantID    = "tenant-malformed-collector-request"
		ownerID     = "owner-malformed-collector-request"
		collectorID = "collector-malformed-request"
	)
	service := &fakeCollectorAdministration{}
	handler := newCollectorAdministrationHTTPHandler(
		t,
		service,
		tenantID,
		ownerID,
		auth.BrowserRoleAdministrator,
	)
	zero := uint32(0)
	oversizedPage := collectorfleet.MaximumCollectorListPageSize + 1
	emptyToken := ""
	oversizedToken := strings.Repeat(
		"x",
		collectorfleet.MaximumCollectorListCursorBytes+1,
	)
	controlText := "needle\ncontrol"
	displayName := "valid"
	tests := []struct {
		name    string
		path    string
		request proto.Message
	}{
		{
			name: "explicit zero page size",
			path: "/api/v1/collectors/list",
			request: &opensplunkv1.ListCollectorsRequest{
				Page: &opensplunkv1.PageRequest{PageSize: &zero},
			},
		},
		{
			name: "oversized page size",
			path: "/api/v1/collectors/list",
			request: &opensplunkv1.ListCollectorsRequest{
				Page: &opensplunkv1.PageRequest{
					PageSize: &oversizedPage,
				},
			},
		},
		{
			name: "explicit empty page token",
			path: "/api/v1/collectors/list",
			request: &opensplunkv1.ListCollectorsRequest{
				Page: &opensplunkv1.PageRequest{
					PageToken: &emptyToken,
				},
			},
		},
		{
			name: "oversized page token",
			path: "/api/v1/collectors/list",
			request: &opensplunkv1.ListCollectorsRequest{
				Page: &opensplunkv1.PageRequest{
					PageToken: &oversizedToken,
				},
			},
		},
		{
			name: "unknown state enum",
			path: "/api/v1/collectors/list",
			request: &opensplunkv1.ListCollectorsRequest{
				StateFilters: []opensplunkv1.CollectorConnectionState{
					opensplunkv1.CollectorConnectionState(99),
				},
			},
		},
		{
			name: "control text",
			path: "/api/v1/collectors/list",
			request: &opensplunkv1.ListCollectorsRequest{
				TextFilter: &controlText,
			},
		},
		{
			name: "unsupported update mask",
			path: "/api/v1/collectors/update",
			request: &opensplunkv1.UpdateCollectorRequest{
				CollectorId:     collectorID,
				ExpectedVersion: 1,
				DisplayName:     &displayName,
				UpdateMask: &fieldmaskpb.FieldMask{
					Paths: []string{"collector_id"},
				},
			},
		},
		{
			name: "unknown administrative enum",
			path: "/api/v1/collectors/state/set",
			request: &opensplunkv1.SetCollectorEnabledRequest{
				CollectorId:     collectorID,
				ExpectedVersion: 1,
				AdministrativeState: opensplunkv1.
					CollectorAdministrativeState(99),
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			response := postCollectorAdministrationProto(
				t,
				handler,
				test.path,
				test.request,
				collectorAdministrationBearerToken,
			)
			if response.Code != http.StatusBadRequest {
				t.Fatalf(
					"response = %d, %s",
					response.Code,
					response.Body,
				)
			}
		})
	}
	malformed := postCollectorAdministrationBytes(
		t,
		handler,
		"/api/v1/collectors/get",
		[]byte{0x0a, 0xff},
		collectorAdministrationBearerToken,
	)
	if malformed.Code != http.StatusBadRequest {
		t.Fatalf(
			"malformed protobuf response = %d, %s",
			malformed.Code,
			malformed.Body,
		)
	}
	if service.calls() != ([4]int{}) {
		t.Fatalf("malformed requests reached service: %v", service.calls())
	}
}

func TestCollectorAdministrationUpdateMaskIsExact(t *testing.T) {
	t.Parallel()

	valid := func() *opensplunkv1.UpdateCollectorRequest {
		return &opensplunkv1.UpdateCollectorRequest{
			CollectorId:     "collector-mask",
			ExpectedVersion: 7,
			UpdateMask: &fieldmaskpb.FieldMask{
				Paths: []string{"display_name"},
			},
		}
	}
	clearUpdate := valid()
	collectorID, version, displayName, err :=
		normalizeCollectorDisplayNameUpdate(clearUpdate)
	if err != nil ||
		collectorID != "collector-mask" ||
		version != 7 ||
		displayName != nil {
		t.Fatalf(
			"clear update = %q, %d, %v, %v",
			collectorID,
			version,
			displayName,
			err,
		)
	}
	padded := " Friendly Collector "
	replacement := valid()
	replacement.DisplayName = &padded
	_, _, normalized, err := normalizeCollectorDisplayNameUpdate(
		replacement,
	)
	if err != nil || normalized == nil ||
		*normalized != "Friendly Collector" {
		t.Fatalf("normalized display name = %v, %v", normalized, err)
	}

	tests := []struct {
		name string
		mask *fieldmaskpb.FieldMask
	}{
		{name: "nil", mask: nil},
		{name: "empty", mask: &fieldmaskpb.FieldMask{}},
		{
			name: "wildcard",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"*"}},
		},
		{
			name: "JSON alias",
			mask: &fieldmaskpb.FieldMask{Paths: []string{"displayName"}},
		},
		{
			name: "duplicate",
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{"display_name", "display_name"},
			},
		},
		{
			name: "other field",
			mask: &fieldmaskpb.FieldMask{
				Paths: []string{"expected_version"},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			request := valid()
			request.UpdateMask = test.mask
			if _, _, _, err := normalizeCollectorDisplayNameUpdate(
				request,
			); err == nil {
				t.Fatalf("mask %v was accepted", test.mask)
			}
		})
	}
}

func TestCollectorAdministrationErrorMappingIsSanitized(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name         string
		err          error
		hasPageToken bool
		status       int
		message      string
	}{
		{
			name:    "page invalidated",
			err:     control.ErrPageInvalidated,
			status:  http.StatusBadRequest,
			message: "page token is invalid",
		},
		{
			name:         "tampered token",
			err:          control.ErrInvalidArgument,
			hasPageToken: true,
			status:       http.StatusBadRequest,
			message:      "page token is invalid",
		},
		{
			name:    "not found",
			err:     control.ErrNotFound,
			status:  http.StatusNotFound,
			message: "collector not found",
		},
		{
			name:    "version conflict",
			err:     control.ErrVersionConflict,
			status:  http.StatusConflict,
			message: "collector version conflict",
		},
		{
			name:    "capacity",
			err:     control.ErrCapacityExceeded,
			status:  http.StatusTooManyRequests,
			message: "collector capacity is exhausted",
		},
		{
			name:    "canceled",
			err:     context.Canceled,
			status:  http.StatusRequestTimeout,
			message: "collector administration request was canceled",
		},
		{
			name:    "unknown",
			err:     errors.New("sensitive database detail"),
			status:  http.StatusServiceUnavailable,
			message: "collector service is unavailable",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			mapped := mapCollectorAdministrationCallError(
				context.Background(),
				test.err,
				test.hasPageToken,
			)
			var httpErr *router.HTTPError
			if !errors.As(mapped, &httpErr) ||
				httpErr.StatusCode != test.status ||
				httpErr.Message != test.message {
				t.Fatalf(
					"mapped error = %T %v, want %d %q",
					mapped,
					mapped,
					test.status,
					test.message,
				)
			}
		})
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if err := mapCollectorAdministrationCallError(
		canceled,
		nil,
		false,
	); err != nil {
		t.Fatalf("nil committed operation error mapped to %v", err)
	}
}

func TestCollectorAdministrationRoutesRequireAuthAndAdvertiseFeature(
	t *testing.T,
) {
	t.Parallel()

	const (
		tenantID    = "tenant-collector-http"
		ownerID     = "owner-collector-http"
		collectorID = "collector-http"
	)
	displayName := "HTTP Collector"
	service := &fakeCollectorAdministration{
		getFn: func(
			context.Context,
			collectorfleet.Scope,
			string,
		) (collectorfleet.CatalogEntry, error) {
			return validCollectorCatalogEntry(
				tenantID,
				collectorID,
			), nil
		},
		listFn: func(
			context.Context,
			collectorfleet.Scope,
			collectorfleet.ListRequest,
		) (collectorfleet.ListResult, error) {
			return collectorfleet.ListResult{}, nil
		},
		updateFn: func(
			context.Context,
			collectorfleet.Scope,
			string,
			uint64,
			*string,
			time.Time,
		) (collectorfleet.AdministrationSnapshot, error) {
			return validCollectorAdministrationSnapshot(
				tenantID,
				collectorID,
				2,
				&displayName,
				collectorfleet.AdministrativeStateEnabled,
			), nil
		},
		stateFn: func(
			context.Context,
			collectorfleet.Scope,
			string,
			uint64,
			collectorfleet.AdministrativeState,
			time.Time,
		) (collectorfleet.AdministrationSnapshot, error) {
			return validCollectorAdministrationSnapshot(
				tenantID,
				collectorID,
				2,
				nil,
				collectorfleet.AdministrativeStateDisabled,
			), nil
		},
	}
	handler := newCollectorAdministrationHTTPHandler(
		t,
		service,
		tenantID,
		ownerID,
		auth.BrowserRoleAdministrator,
	)
	unauthorized := postCollectorAdministrationProto(
		t,
		handler,
		"/api/v1/collectors/get",
		&opensplunkv1.GetCollectorRequest{CollectorId: collectorID},
		"",
	)
	if unauthorized.Code != http.StatusUnauthorized ||
		service.calls() != ([4]int{}) {
		t.Fatalf(
			"unauthorized response/calls = %d %s/%v",
			unauthorized.Code,
			unauthorized.Body,
			service.calls(),
		)
	}

	routes := []struct {
		path    string
		request proto.Message
	}{
		{
			path: "/api/v1/collectors/get",
			request: &opensplunkv1.GetCollectorRequest{
				CollectorId: collectorID,
			},
		},
		{
			path:    "/api/v1/collectors/list",
			request: &opensplunkv1.ListCollectorsRequest{},
		},
		{
			path: "/api/v1/collectors/update",
			request: &opensplunkv1.UpdateCollectorRequest{
				CollectorId:     collectorID,
				ExpectedVersion: 1,
				DisplayName:     &displayName,
				UpdateMask: &fieldmaskpb.FieldMask{
					Paths: []string{"display_name"},
				},
			},
		},
		{
			path: "/api/v1/collectors/state/set",
			request: &opensplunkv1.SetCollectorEnabledRequest{
				CollectorId:         collectorID,
				ExpectedVersion:     1,
				AdministrativeState: opensplunkv1.CollectorAdministrativeState_COLLECTOR_ADMINISTRATIVE_STATE_DISABLED,
			},
		},
	}
	for _, route := range routes {
		route.request.ProtoReflect().SetUnknown(
			futureProtobufField("future-collector-request"),
		)
		switch request := route.request.(type) {
		case *opensplunkv1.ListCollectorsRequest:
			request.Page = &opensplunkv1.PageRequest{}
			request.Page.ProtoReflect().SetUnknown(
				futureProtobufField("future-collector-page"),
			)
		case *opensplunkv1.UpdateCollectorRequest:
			request.UpdateMask.ProtoReflect().SetUnknown(
				futureProtobufField("future-collector-mask"),
			)
		}
		response := postCollectorAdministrationProto(
			t,
			handler,
			route.path,
			route.request,
			collectorAdministrationBearerToken,
		)
		if response.Code != http.StatusOK {
			t.Fatalf(
				"%s response = %d, %s",
				route.path,
				response.Code,
				response.Body,
			)
		}
	}
	if service.calls() != ([4]int{1, 1, 1, 1}) {
		t.Fatalf("route calls = %v", service.calls())
	}

	bootstrap := postCollectorAdministrationProto(
		t,
		handler,
		"/api/v1/system/bootstrap",
		&opensplunkv1.GetSystemBootstrapRequest{},
		"",
	)
	if bootstrap.Code != http.StatusOK {
		t.Fatalf(
			"bootstrap response = %d, %s",
			bootstrap.Code,
			bootstrap.Body,
		)
	}
	var decoded opensplunkv1.GetSystemBootstrapResponse
	if err := proto.Unmarshal(bootstrap.Body.Bytes(), &decoded); err != nil {
		t.Fatalf("unmarshal bootstrap: %v", err)
	}
	if !slices.Contains(
		decoded.GetFeatures(),
		opensplunkv1.ServerFeature_SERVER_FEATURE_COLLECTOR_ADMIN,
	) {
		t.Fatalf(
			"bootstrap features = %v, want collector administration",
			decoded.GetFeatures(),
		)
	}
}

func collectorAdministrationAPIHandler(
	t *testing.T,
	tenantID string,
	ownerID string,
	service CollectorAdministration,
) *apiHandler {
	t.Helper()
	return &apiHandler{
		collectorAdmin:    service,
		tenantID:          tenantID,
		ownerID:           ownerID,
		maximumPageSize:   defaultMaximumPageSize,
		now:               collectorAdministrationTestTime,
		serializationGate: make(chan struct{}, 1),
	}
}

func collectorAdministrationDirectRequest(
	t *testing.T,
	ctx context.Context,
	tenantID string,
	ownerID string,
	path string,
) *http.Request {
	t.Helper()
	authenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(collectorAdministrationBearerToken),
		tenantID,
		ownerID,
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatalf("NewBearerTokenAuthenticator: %v", err)
	}
	principal, err := authenticator.Authenticate(
		context.Background(),
		[]byte(collectorAdministrationBearerToken),
	)
	if err != nil {
		t.Fatalf("Authenticate: %v", err)
	}
	request := httptest.NewRequestWithContext(ctx, http.MethodPost, path, nil)
	return request.WithContext(context.WithValue(
		request.Context(),
		browserPrincipalContextKey{},
		principal,
	))
}

func newCollectorAdministrationHTTPHandler(
	t *testing.T,
	service CollectorAdministration,
	tenantID string,
	ownerID string,
	role auth.BrowserRole,
) *Handler {
	t.Helper()
	authenticator, err := auth.NewBearerTokenAuthenticator(
		[]byte(collectorAdministrationBearerToken),
		tenantID,
		ownerID,
		role,
	)
	if err != nil {
		t.Fatalf("NewBearerTokenAuthenticator: %v", err)
	}
	handler, err := NewHandler(Config{
		SearchJobs:                 &fakeSearchJobs{},
		Indexes:                    fakeIndexCatalog{},
		CollectorAdmin:             service,
		SavedSearches:              &fakeSavedSearches{},
		BrowserAuthenticator:       authenticator,
		WebUI:                      testUI(),
		TenantID:                   tenantID,
		OwnerID:                    ownerID,
		Now:                        collectorAdministrationTestTime,
		AdministrativeAllowedHosts: []string{"example.com"},
	})
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func postCollectorAdministrationProto(
	t *testing.T,
	handler http.Handler,
	path string,
	message proto.Message,
	token string,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal collector request: %v", err)
	}
	return postCollectorAdministrationBytes(t, handler, path, payload, token)
}

func postCollectorAdministrationBytes(
	t *testing.T,
	handler http.Handler,
	path string,
	payload []byte,
	token string,
) *httptest.ResponseRecorder {
	t.Helper()
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		path,
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

func validCollectorCatalogEntry(
	tenantID string,
	collectorID string,
) collectorfleet.CatalogEntry {
	now := collectorAdministrationTestTime()
	oldest := 2 * time.Second
	lastEvent := now.Add(-30 * time.Second)
	return collectorfleet.CatalogEntry{
		ConnectionState: collectorfleet.ConnectionStateOffline,
		Collector: collectorfleet.Collector{
			TenantID:            tenantID,
			CollectorID:         collectorID,
			Version:             1,
			AdministrativeState: collectorfleet.AdministrativeStateEnabled,
			FirstSeenAt:         now.Add(-3 * time.Minute),
			UpdatedAt:           now,
			CollectorVersion:    "1.2.3",
			Hostname:            "collector.example.test",
			OperatingSystem:     "linux",
			Architecture:        "amd64",
			ConnectedAt:         now.Add(-2 * time.Minute),
			LastSeenAt:          now.Add(-time.Minute),
			Capabilities:        []uint32{1, 2},
			AuthorizedIndexes:   []string{"main"},
			Queue: collectorfleet.QueueTelemetry{
				QueuedEvents:            1,
				QueuedBytes:             2,
				OldestEventAge:          &oldest,
				SentEventsTotal:         3,
				AcknowledgedEventsTotal: 3,
			},
			InputHealth: []collectorfleet.InputHealth{
				{
					InputID:           "input-main",
					State:             2,
					StatusMessage:     "healthy",
					DiscoveredSources: 1,
					ActiveSources:     1,
					EventsReadTotal:   4,
					BytesReadTotal:    5,
					LastEventAt:       &lastEvent,
				},
			},
		},
	}
}

func validCollectorAdministrationSnapshot(
	tenantID string,
	collectorID string,
	version uint64,
	displayName *string,
	state collectorfleet.AdministrativeState,
) collectorfleet.AdministrationSnapshot {
	now := collectorAdministrationTestTime()
	return collectorfleet.AdministrationSnapshot{
		TenantID:            tenantID,
		CollectorID:         collectorID,
		Version:             version,
		DisplayName:         cloneOptionalString(displayName),
		AdministrativeState: state,
		FirstSeenAt:         now.Add(-time.Hour),
		UpdatedAt:           now,
	}
}

func collectorAdministrationTestTime() time.Time {
	return time.Date(2026, 7, 29, 12, 34, 56, 789_123_000, time.UTC)
}

func assertCollectorAdministrationHTTPError(
	t *testing.T,
	err error,
	status int,
) {
	t.Helper()
	var httpErr *router.HTTPError
	if !errors.As(err, &httpErr) || httpErr.StatusCode != status {
		t.Fatalf("error = %T %v, want HTTP status %d", err, err, status)
	}
}
