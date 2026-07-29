package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/collectorfleet"
	"github.com/Suhaibinator/open-splunk/internal/control"
)

type stubRuntimeCollectorCatalog struct {
	get func(
		context.Context,
		collectorfleet.Scope,
		string,
		[]collectorfleet.CollectorLiveness,
	) (collectorfleet.CatalogEntry, error)
	list func(
		context.Context,
		collectorfleet.Scope,
		[]collectorfleet.CollectorLiveness,
		collectorfleet.ListRequest,
	) (collectorfleet.ListResult, error)
}

func (catalog *stubRuntimeCollectorCatalog) Get(
	ctx context.Context,
	scope collectorfleet.Scope,
	collectorID string,
	liveness []collectorfleet.CollectorLiveness,
) (collectorfleet.CatalogEntry, error) {
	if catalog.get == nil {
		return collectorfleet.CatalogEntry{}, errors.New("unexpected Get")
	}
	return catalog.get(ctx, scope, collectorID, liveness)
}

func (catalog *stubRuntimeCollectorCatalog) List(
	ctx context.Context,
	scope collectorfleet.Scope,
	liveness []collectorfleet.CollectorLiveness,
	request collectorfleet.ListRequest,
) (collectorfleet.ListResult, error) {
	if catalog.list == nil {
		return collectorfleet.ListResult{}, errors.New("unexpected List")
	}
	return catalog.list(ctx, scope, liveness, request)
}

type stubRuntimeCollectorStore struct {
	updateDisplayName func(
		context.Context,
		collectorfleet.Scope,
		string,
		uint64,
		*string,
		time.Time,
	) (collectorfleet.AdministrationSnapshot, error)
	setAdministrativeState func(
		context.Context,
		collectorfleet.Scope,
		string,
		uint64,
		collectorfleet.AdministrativeState,
		time.Time,
	) (collectorfleet.AdministrationSnapshot, error)
}

func (store *stubRuntimeCollectorStore) UpdateDisplayName(
	ctx context.Context,
	scope collectorfleet.Scope,
	collectorID string,
	expectedVersion uint64,
	displayName *string,
	receivedAt time.Time,
) (collectorfleet.AdministrationSnapshot, error) {
	if store.updateDisplayName == nil {
		return collectorfleet.AdministrationSnapshot{},
			errors.New("unexpected UpdateDisplayName")
	}
	return store.updateDisplayName(
		ctx,
		scope,
		collectorID,
		expectedVersion,
		displayName,
		receivedAt,
	)
}

func (store *stubRuntimeCollectorStore) SetAdministrativeState(
	ctx context.Context,
	scope collectorfleet.Scope,
	collectorID string,
	expectedVersion uint64,
	state collectorfleet.AdministrativeState,
	receivedAt time.Time,
) (collectorfleet.AdministrationSnapshot, error) {
	if store.setAdministrativeState == nil {
		return collectorfleet.AdministrationSnapshot{},
			errors.New("unexpected SetAdministrativeState")
	}
	return store.setAdministrativeState(
		ctx,
		scope,
		collectorID,
		expectedVersion,
		state,
		receivedAt,
	)
}

type countedRuntimeCollectorLiveness struct {
	calls  int
	scopes []collectorfleet.Scope
	result []collectorfleet.CollectorLiveness
	err    error
}

func (liveness *countedRuntimeCollectorLiveness) SnapshotLiveness(
	scope collectorfleet.Scope,
) ([]collectorfleet.CollectorLiveness, error) {
	liveness.calls++
	liveness.scopes = append(liveness.scopes, scope)
	return liveness.result, liveness.err
}

func TestDeriveCollectorCatalogCursorKeyIsStableAndPurposeSeparated(
	t *testing.T,
) {
	t.Parallel()

	master := bytes.Repeat([]byte{0x91}, masterKeyBytes)
	first, err := deriveCollectorCatalogCursorKey(master)
	if err != nil {
		t.Fatal(err)
	}
	second, err := deriveCollectorCatalogCursorKey(master)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 32 || !bytes.Equal(first, second) {
		t.Fatal("collector catalog cursor key is not stable")
	}
	for _, purpose := range []string{
		"collector-token-digests",
		"saved-search-cursors",
		"search-history-cursors",
		"app-catalog-cursors",
		"app-administration-cursors",
	} {
		other, deriveErr := deriveServerKey(master, purpose)
		if deriveErr != nil {
			t.Fatal(deriveErr)
		}
		if bytes.Equal(first, other) {
			t.Fatalf(
				"collector catalog cursor key collides with purpose %q",
				purpose,
			)
		}
	}
}

func TestRuntimeCollectorAdministrationSnapshotsOncePerReadAndPassesCursor(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.WithValue(
		context.Background(),
		runtimeCollectorContextKey{},
		"request",
	)
	scope := collectorfleet.Scope{TenantID: "tenant-a"}
	lease := collectorfleet.Lease{
		Scope:       scope,
		CollectorID: "collector-a",
		BootEpoch:   "boot-a",
		StreamID:    "stream-a",
		Generation:  3,
	}
	live := []collectorfleet.CollectorLiveness{{
		Lease: lease,
		State: collectorfleet.LivenessStateOnline,
	}}
	liveness := &countedRuntimeCollectorLiveness{result: live}

	stateFilters := []collectorfleet.ConnectionState{
		collectorfleet.ConnectionStateOnline,
	}
	indexName := "main"
	text := "collector"
	request := collectorfleet.ListRequest{
		PageSize:        1,
		PageToken:       "opaque.signed.cursor",
		IncludeTotal:    true,
		StateFilters:    stateFilters,
		IndexNameFilter: &indexName,
		TextFilter:      &text,
		SortBy:          collectorfleet.CollectorSortByHostname,
		Direction:       collectorfleet.SortDescending,
	}
	var capturedRequest collectorfleet.ListRequest
	nextToken := "opaque.next.signed.cursor"
	catalog := &stubRuntimeCollectorCatalog{
		get: func(
			gotContext context.Context,
			gotScope collectorfleet.Scope,
			collectorID string,
			gotLiveness []collectorfleet.CollectorLiveness,
		) (collectorfleet.CatalogEntry, error) {
			if gotContext != ctx ||
				gotScope != scope ||
				collectorID != lease.CollectorID ||
				!reflect.DeepEqual(gotLiveness, live) {
				t.Fatalf(
					"Get inputs = %v, %#v, %q, %#v",
					gotContext,
					gotScope,
					collectorID,
					gotLiveness,
				)
			}
			return collectorfleet.CatalogEntry{
				Collector: collectorfleet.Collector{
					TenantID:    scope.TenantID,
					CollectorID: collectorID,
				},
				ConnectionState: collectorfleet.ConnectionStateOnline,
			}, nil
		},
		list: func(
			gotContext context.Context,
			gotScope collectorfleet.Scope,
			gotLiveness []collectorfleet.CollectorLiveness,
			gotRequest collectorfleet.ListRequest,
		) (collectorfleet.ListResult, error) {
			if gotContext != ctx ||
				gotScope != scope ||
				!reflect.DeepEqual(gotLiveness, live) {
				t.Fatalf(
					"List inputs = %v, %#v, %#v",
					gotContext,
					gotScope,
					gotLiveness,
				)
			}
			if !reflect.DeepEqual(gotRequest, request) {
				t.Fatalf("List request = %#v, want %#v", gotRequest, request)
			}
			if gotRequest.IndexNameFilter == request.IndexNameFilter ||
				gotRequest.TextFilter == request.TextFilter {
				t.Fatal("List retained caller-owned optional string pointers")
			}
			capturedRequest = gotRequest
			return collectorfleet.ListResult{
				NextPageToken: &nextToken,
			}, nil
		},
	}
	adapter := &runtimeCollectorAdministration{
		catalog:  catalog,
		store:    &stubRuntimeCollectorStore{},
		liveness: liveness,
	}

	entry, err := adapter.Get(ctx, scope, lease.CollectorID)
	if err != nil {
		t.Fatal(err)
	}
	if entry.ConnectionState != collectorfleet.ConnectionStateOnline {
		t.Fatalf("Get connection state = %q", entry.ConnectionState)
	}
	page, err := adapter.List(ctx, scope, request)
	if err != nil {
		t.Fatal(err)
	}
	if page.NextPageToken == nil || *page.NextPageToken != nextToken {
		t.Fatalf("List next page token = %#v, want exact catalog token", page)
	}
	if liveness.calls != 2 ||
		!slices.Equal(
			liveness.scopes,
			[]collectorfleet.Scope{scope, scope},
		) {
		t.Fatalf(
			"SnapshotLiveness calls/scopes = %d/%#v",
			liveness.calls,
			liveness.scopes,
		)
	}

	stateFilters[0] = collectorfleet.ConnectionStateDisabled
	indexName = "audit"
	text = "changed"
	if !reflect.DeepEqual(capturedRequest.StateFilters, []collectorfleet.ConnectionState{
		collectorfleet.ConnectionStateOnline,
	}) ||
		capturedRequest.IndexNameFilter == nil ||
		*capturedRequest.IndexNameFilter != "main" ||
		capturedRequest.TextFilter == nil ||
		*capturedRequest.TextFilter != "collector" {
		t.Fatalf(
			"captured request changed with caller input: %#v",
			capturedRequest,
		)
	}
}

func TestRuntimeCollectorAdministrationPropagatesReadErrors(t *testing.T) {
	t.Parallel()

	t.Run("liveness", func(t *testing.T) {
		snapshotErr := errors.New("snapshot failed")
		catalogCalls := 0
		adapter := &runtimeCollectorAdministration{
			catalog: &stubRuntimeCollectorCatalog{
				get: func(
					context.Context,
					collectorfleet.Scope,
					string,
					[]collectorfleet.CollectorLiveness,
				) (collectorfleet.CatalogEntry, error) {
					catalogCalls++
					return collectorfleet.CatalogEntry{}, nil
				},
				list: func(
					context.Context,
					collectorfleet.Scope,
					[]collectorfleet.CollectorLiveness,
					collectorfleet.ListRequest,
				) (collectorfleet.ListResult, error) {
					catalogCalls++
					return collectorfleet.ListResult{}, nil
				},
			},
			store: &stubRuntimeCollectorStore{},
			liveness: &countedRuntimeCollectorLiveness{
				err: snapshotErr,
			},
		}
		if _, err := adapter.Get(
			context.Background(),
			collectorfleet.Scope{TenantID: "tenant-a"},
			"collector-a",
		); err == nil ||
			errors.Is(err, snapshotErr) ||
			!strings.Contains(err.Error(), snapshotErr.Error()) {
			t.Fatalf("Get error = %v, want detached snapshot failure", err)
		}
		if _, err := adapter.List(
			context.Background(),
			collectorfleet.Scope{TenantID: "tenant-a"},
			collectorfleet.ListRequest{},
		); err == nil ||
			errors.Is(err, snapshotErr) ||
			!strings.Contains(err.Error(), snapshotErr.Error()) {
			t.Fatalf("List error = %v, want detached snapshot failure", err)
		}
		if catalogCalls != 0 {
			t.Fatalf("catalog calls after snapshot failure = %d", catalogCalls)
		}
	})

	t.Run("trusted invalid liveness error is not a client error", func(t *testing.T) {
		runtimeErr := fmt.Errorf(
			"runtime rejected its state: %w",
			control.ErrInvalidArgument,
		)
		adapter := &runtimeCollectorAdministration{
			catalog: &stubRuntimeCollectorCatalog{},
			store:   &stubRuntimeCollectorStore{},
			liveness: &countedRuntimeCollectorLiveness{
				err: runtimeErr,
			},
		}
		_, err := adapter.Get(
			context.Background(),
			collectorfleet.Scope{TenantID: "tenant-a"},
			"collector-a",
		)
		if err == nil || errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf(
				"Get trusted runtime error = %v, must sever ErrInvalidArgument",
				err,
			)
		}
	})

	t.Run("corrupt trusted snapshot is not a client error", func(t *testing.T) {
		catalogCalls := 0
		adapter := &runtimeCollectorAdministration{
			catalog: &stubRuntimeCollectorCatalog{
				list: func(
					context.Context,
					collectorfleet.Scope,
					[]collectorfleet.CollectorLiveness,
					collectorfleet.ListRequest,
				) (collectorfleet.ListResult, error) {
					catalogCalls++
					return collectorfleet.ListResult{}, nil
				},
			},
			store: &stubRuntimeCollectorStore{},
			liveness: &countedRuntimeCollectorLiveness{
				result: []collectorfleet.CollectorLiveness{{
					Lease: collectorfleet.Lease{
						Scope: collectorfleet.Scope{
							TenantID: "tenant-a",
						},
						CollectorID: "collector-a",
						BootEpoch:   "boot-a",
						StreamID:    "stream-a",
						Generation:  1,
					},
					State: collectorfleet.LivenessStateOffline,
				}},
			},
		}
		_, err := adapter.List(
			context.Background(),
			collectorfleet.Scope{TenantID: "tenant-a"},
			collectorfleet.ListRequest{},
		)
		if err == nil || errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf(
				"List corrupt runtime snapshot error = %v, must sever ErrInvalidArgument",
				err,
			)
		}
		if catalogCalls != 0 {
			t.Fatalf("catalog calls after corrupt snapshot = %d", catalogCalls)
		}
	})

	t.Run("oversized trusted snapshot is rejected before catalog", func(t *testing.T) {
		snapshot := make(
			[]collectorfleet.CollectorLiveness,
			collectorfleet.MaximumActiveCollectors+1,
		)
		catalogCalls := 0
		adapter := &runtimeCollectorAdministration{
			catalog: &stubRuntimeCollectorCatalog{
				get: func(
					context.Context,
					collectorfleet.Scope,
					string,
					[]collectorfleet.CollectorLiveness,
				) (collectorfleet.CatalogEntry, error) {
					catalogCalls++
					return collectorfleet.CatalogEntry{}, nil
				},
			},
			store: &stubRuntimeCollectorStore{},
			liveness: &countedRuntimeCollectorLiveness{
				result: snapshot,
			},
		}
		_, err := adapter.Get(
			context.Background(),
			collectorfleet.Scope{TenantID: "tenant-a"},
			"collector-a",
		)
		if err == nil || errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf(
				"Get oversized runtime snapshot error = %v, must be internal",
				err,
			)
		}
		if catalogCalls != 0 {
			t.Fatalf("catalog calls after oversized snapshot = %d", catalogCalls)
		}
	})

	t.Run("catalog", func(t *testing.T) {
		getErr := errors.New("get failed")
		listErr := errors.New("list failed")
		adapter := &runtimeCollectorAdministration{
			catalog: &stubRuntimeCollectorCatalog{
				get: func(
					context.Context,
					collectorfleet.Scope,
					string,
					[]collectorfleet.CollectorLiveness,
				) (collectorfleet.CatalogEntry, error) {
					return collectorfleet.CatalogEntry{}, getErr
				},
				list: func(
					context.Context,
					collectorfleet.Scope,
					[]collectorfleet.CollectorLiveness,
					collectorfleet.ListRequest,
				) (collectorfleet.ListResult, error) {
					return collectorfleet.ListResult{}, listErr
				},
			},
			store:    &stubRuntimeCollectorStore{},
			liveness: &countedRuntimeCollectorLiveness{},
		}
		if _, err := adapter.Get(
			context.Background(),
			collectorfleet.Scope{TenantID: "tenant-a"},
			"collector-a",
		); !errors.Is(err, getErr) {
			t.Fatalf("Get error = %v, want catalog error", err)
		}
		if _, err := adapter.List(
			context.Background(),
			collectorfleet.Scope{TenantID: "tenant-a"},
			collectorfleet.ListRequest{},
		); !errors.Is(err, listErr) {
			t.Fatalf("List error = %v, want catalog error", err)
		}
	})
}

func TestRuntimeCollectorAdministrationDelegatesMutationsWithoutHydration(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	scope := collectorfleet.Scope{TenantID: "tenant-a"}
	receivedAt := time.Date(2026, 7, 29, 14, 0, 0, 123_456_000, time.UTC)
	displayName := "Edge Collector"
	updateResultName := "persisted edge"
	updateResult := collectorfleet.AdministrationSnapshot{
		TenantID:            scope.TenantID,
		CollectorID:         "collector-a",
		Version:             8,
		DisplayName:         &updateResultName,
		AdministrativeState: collectorfleet.AdministrativeStateEnabled,
	}
	stateResult := collectorfleet.AdministrationSnapshot{
		TenantID:            scope.TenantID,
		CollectorID:         "collector-a",
		Version:             9,
		DisplayName:         &updateResultName,
		AdministrativeState: collectorfleet.AdministrativeStateDisabled,
	}
	updateCalls := 0
	stateCalls := 0
	var capturedDisplayName *string
	storeErr := errors.New("state mutation failed")
	store := &stubRuntimeCollectorStore{
		updateDisplayName: func(
			gotContext context.Context,
			gotScope collectorfleet.Scope,
			collectorID string,
			expectedVersion uint64,
			gotDisplayName *string,
			gotReceivedAt time.Time,
		) (collectorfleet.AdministrationSnapshot, error) {
			updateCalls++
			if gotContext != ctx ||
				gotScope != scope ||
				collectorID != "collector-a" ||
				expectedVersion != 7 ||
				gotDisplayName == nil ||
				*gotDisplayName != displayName ||
				gotDisplayName == &displayName ||
				!gotReceivedAt.Equal(receivedAt) {
				t.Fatalf(
					"UpdateDisplayName inputs = %v, %#v, %q, %d, %#v, %s",
					gotContext,
					gotScope,
					collectorID,
					expectedVersion,
					gotDisplayName,
					gotReceivedAt,
				)
			}
			capturedDisplayName = gotDisplayName
			return updateResult, nil
		},
		setAdministrativeState: func(
			gotContext context.Context,
			gotScope collectorfleet.Scope,
			collectorID string,
			expectedVersion uint64,
			state collectorfleet.AdministrativeState,
			gotReceivedAt time.Time,
		) (collectorfleet.AdministrationSnapshot, error) {
			stateCalls++
			if gotContext != ctx ||
				gotScope != scope ||
				collectorID != "collector-a" ||
				expectedVersion != 8 ||
				state != collectorfleet.AdministrativeStateDisabled ||
				!gotReceivedAt.Equal(receivedAt.Add(time.Second)) {
				t.Fatalf(
					"SetAdministrativeState inputs = %v, %#v, %q, %d, %q, %s",
					gotContext,
					gotScope,
					collectorID,
					expectedVersion,
					state,
					gotReceivedAt,
				)
			}
			if stateCalls == 1 {
				return stateResult, nil
			}
			return collectorfleet.AdministrationSnapshot{}, storeErr
		},
	}
	adapter := &runtimeCollectorAdministration{
		// Mutations must never depend on a post-commit catalog hydration or a
		// process-liveness snapshot.
		catalog:  &stubRuntimeCollectorCatalog{},
		store:    store,
		liveness: &countedRuntimeCollectorLiveness{err: errors.New("unused")},
	}

	updated, err := adapter.UpdateDisplayName(
		ctx,
		scope,
		"collector-a",
		7,
		&displayName,
		receivedAt,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(updated, updateResult) {
		t.Fatalf("UpdateDisplayName result = %#v", updated)
	}
	displayName = "caller mutation"
	if capturedDisplayName == nil || *capturedDisplayName != "Edge Collector" {
		t.Fatalf(
			"stored display-name input changed with caller pointer: %#v",
			capturedDisplayName,
		)
	}

	disabled, err := adapter.SetAdministrativeState(
		ctx,
		scope,
		"collector-a",
		8,
		collectorfleet.AdministrativeStateDisabled,
		receivedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(disabled, stateResult) {
		t.Fatalf("SetAdministrativeState result = %#v", disabled)
	}
	if _, err := adapter.SetAdministrativeState(
		ctx,
		scope,
		"collector-a",
		8,
		collectorfleet.AdministrativeStateDisabled,
		receivedAt.Add(time.Second),
	); !errors.Is(err, storeErr) {
		t.Fatalf("SetAdministrativeState error = %v, want store error", err)
	}
	if updateCalls != 1 || stateCalls != 2 {
		t.Fatalf(
			"mutation calls = update %d, state %d",
			updateCalls,
			stateCalls,
		)
	}
}

func TestRuntimeCollectorAdministrationNilRuntimeIsOfflineAndResultsDetach(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	directory := t.TempDir()
	database, err := control.Open(
		ctx,
		filepath.Join(directory, "control.db"),
	)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := collectorfleet.New(database)
	if err != nil {
		t.Fatal(err)
	}
	adapter, err := newRuntimeCollectorAdministration(
		ctx,
		database,
		store,
		nil,
		filepath.Join(directory, "server.key"),
	)
	if err != nil {
		t.Fatal(err)
	}
	claimedAt := time.Date(2026, 7, 29, 15, 0, 0, 0, time.UTC)
	collector, _ := claimRuntimeCollector(
		t,
		store,
		"tenant-a",
		"collector-a",
		claimedAt,
	)
	displayName := "Collector A"
	administration, err := adapter.UpdateDisplayName(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		"collector-a",
		collector.Version,
		&displayName,
		claimedAt.Add(time.Second),
	)
	if err != nil {
		t.Fatal(err)
	}
	displayName = "caller changed input"
	if administration.DisplayName == nil ||
		*administration.DisplayName != "Collector A" {
		t.Fatalf("administration result = %#v", administration)
	}
	*administration.DisplayName = "caller changed result"
	persistedAdministration, err := store.GetAdministration(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		"collector-a",
	)
	if err != nil {
		t.Fatal(err)
	}
	if persistedAdministration.DisplayName == nil ||
		*persistedAdministration.DisplayName != "Collector A" {
		t.Fatalf(
			"persisted administration aliased result: %#v",
			persistedAdministration,
		)
	}

	entry, err := adapter.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		"collector-a",
	)
	if err != nil {
		t.Fatal(err)
	}
	if entry.ConnectionState != collectorfleet.ConnectionStateOffline {
		t.Fatalf(
			"Get connection state without heartbeat runtime = %q",
			entry.ConnectionState,
		)
	}
	if entry.Collector.DisplayName == nil ||
		len(entry.Collector.Capabilities) == 0 ||
		len(entry.Collector.AuthorizedIndexes) == 0 ||
		len(entry.Collector.Inputs) == 0 ||
		entry.Collector.ActiveLease == nil {
		t.Fatalf("Get returned incomplete fixture: %#v", entry)
	}
	*entry.Collector.DisplayName = "mutated"
	entry.Collector.Capabilities[0] = 999
	entry.Collector.AuthorizedIndexes[0] = "mutated"
	entry.Collector.Inputs[0].InputID = "mutated"
	entry.Collector.ActiveLease.StreamID = "mutated"

	fresh, err := adapter.Get(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		"collector-a",
	)
	if err != nil {
		t.Fatal(err)
	}
	if fresh.Collector.DisplayName == nil ||
		*fresh.Collector.DisplayName != "Collector A" ||
		!reflect.DeepEqual(fresh.Collector.Capabilities, []uint32{1, 2}) ||
		!reflect.DeepEqual(fresh.Collector.AuthorizedIndexes, []string{"main"}) ||
		fresh.Collector.Inputs[0].InputID != "input-collector-a" ||
		fresh.Collector.ActiveLease.StreamID != "stream-collector-a" {
		t.Fatalf("fresh Get aliased prior result: %#v", fresh)
	}
	page, err := adapter.List(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		collectorfleet.ListRequest{PageSize: 1},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(page.Entries) != 1 ||
		page.Entries[0].ConnectionState != collectorfleet.ConnectionStateOffline {
		t.Fatalf("List without heartbeat runtime = %#v", page)
	}
	if err := database.SQLDB().PingContext(ctx); err != nil {
		t.Fatalf("borrowing adapter closed the control database: %v", err)
	}
}

func TestRuntimeCollectorAdministrationCursorSurvivesReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	directory := t.TempDir()
	databasePath := filepath.Join(directory, "control.db")
	keyPath := filepath.Join(directory, "server.key")

	firstDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	firstStore, err := collectorfleet.New(firstDatabase)
	if err != nil {
		_ = firstDatabase.Close()
		t.Fatal(err)
	}
	first, err := newRuntimeCollectorAdministration(
		ctx,
		firstDatabase,
		firstStore,
		nil,
		keyPath,
	)
	if err != nil {
		_ = firstDatabase.Close()
		t.Fatal(err)
	}
	base := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC)
	for index, collectorID := range []string{
		"collector-a",
		"collector-b",
		"collector-c",
	} {
		claimRuntimeCollector(
			t,
			firstStore,
			"tenant-a",
			collectorID,
			base.Add(time.Duration(index)*time.Second),
		)
	}
	firstPage, err := first.List(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		collectorfleet.ListRequest{PageSize: 1},
	)
	if err != nil {
		_ = firstDatabase.Close()
		t.Fatal(err)
	}
	if len(firstPage.Entries) != 1 ||
		firstPage.Entries[0].Collector.CollectorID != "collector-a" ||
		firstPage.NextPageToken == nil ||
		*firstPage.NextPageToken == "" ||
		firstPage.CatalogRevision == 0 {
		_ = firstDatabase.Close()
		t.Fatalf("first page = %#v", firstPage)
	}
	token := *firstPage.NextPageToken
	revision := firstPage.CatalogRevision
	if err := firstDatabase.Close(); err != nil {
		t.Fatal(err)
	}

	secondDatabase, err := control.Open(ctx, databasePath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := secondDatabase.Close(); err != nil {
			t.Error(err)
		}
	})
	secondStore, err := collectorfleet.New(secondDatabase)
	if err != nil {
		t.Fatal(err)
	}
	second, err := newRuntimeCollectorAdministration(
		ctx,
		secondDatabase,
		secondStore,
		nil,
		keyPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	secondPage, err := second.List(
		ctx,
		collectorfleet.Scope{TenantID: "tenant-a"},
		collectorfleet.ListRequest{
			PageSize:  1,
			PageToken: token,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Entries) != 1 ||
		secondPage.Entries[0].Collector.CollectorID != "collector-b" ||
		secondPage.CatalogRevision != revision {
		t.Fatalf("continued page after reopen = %#v", secondPage)
	}
}

type runtimeCollectorContextKey struct{}

func claimRuntimeCollector(
	t *testing.T,
	store *collectorfleet.Store,
	tenantID string,
	collectorID string,
	receivedAt time.Time,
) (collectorfleet.Collector, collectorfleet.Lease) {
	t.Helper()
	collector, lease, err := store.Claim(
		context.Background(),
		collectorfleet.ClaimRequest{
			Scope:       collectorfleet.Scope{TenantID: tenantID},
			CollectorID: collectorID,
			BootEpoch:   "boot-" + collectorID,
			StreamID:    "stream-" + collectorID,
			ReceivedAt:  receivedAt,
			Hello: collectorfleet.Hello{
				InstanceID:       "instance-" + collectorID,
				ProtocolMajor:    1,
				CollectorVersion: "1.0.0",
				Hostname:         collectorID + ".example",
				OperatingSystem:  "linux",
				Architecture:     "amd64",
				StartedAt:        receivedAt.Add(-time.Minute),
				Capabilities:     []uint32{2, 1},
				AuthorizedIndexes: []string{
					"main",
				},
				Inputs: []collectorfleet.InputRegistration{{
					InputID:   "input-" + collectorID,
					InputType: 1,
					IndexName: "main",
				}},
			},
		},
	)
	if err != nil {
		t.Fatalf("Claim(%s/%s): %v", tenantID, collectorID, err)
	}
	return collector, lease
}
