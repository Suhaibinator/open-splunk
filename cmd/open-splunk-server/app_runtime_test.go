package main

import (
	"bytes"
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/server"
	"google.golang.org/protobuf/proto"
)

type stubControlAppCatalog struct {
	create func(
		context.Context,
		control.AppAccessScope,
		control.AppDefinition,
	) (control.AppWorkspace, error)
	get func(
		context.Context,
		control.AppAccessScope,
		control.AppSelector,
	) (control.AppWorkspace, error)
	list func(
		context.Context,
		control.AppAccessScope,
		control.AppListRequest,
	) (control.AppListResult, error)
	update func(
		context.Context,
		control.AppAccessScope,
		control.AppSelector,
		uint64,
		control.AppDefinition,
	) (control.AppWorkspace, error)
	setState func(
		context.Context,
		control.AppAccessScope,
		control.AppSelector,
		uint64,
		control.AppState,
	) (control.AppWorkspace, error)
	delete func(
		context.Context,
		control.AppAccessScope,
		control.AppSelector,
		uint64,
		string,
	) (string, error)
}

func (catalog *stubControlAppCatalog) CreateApp(
	ctx context.Context,
	scope control.AppAccessScope,
	definition control.AppDefinition,
) (control.AppWorkspace, error) {
	if catalog.create == nil {
		return control.AppWorkspace{}, errors.New("unexpected CreateApp")
	}
	return catalog.create(ctx, scope, definition)
}

func (catalog *stubControlAppCatalog) GetApp(
	ctx context.Context,
	scope control.AppAccessScope,
	selector control.AppSelector,
) (control.AppWorkspace, error) {
	if catalog.get == nil {
		return control.AppWorkspace{}, errors.New("unexpected GetApp")
	}
	return catalog.get(ctx, scope, selector)
}

func (catalog *stubControlAppCatalog) ListApps(
	ctx context.Context,
	scope control.AppAccessScope,
	request control.AppListRequest,
) (control.AppListResult, error) {
	if catalog.list == nil {
		return control.AppListResult{}, errors.New("unexpected ListApps")
	}
	return catalog.list(ctx, scope, request)
}

func (catalog *stubControlAppCatalog) UpdateApp(
	ctx context.Context,
	scope control.AppAccessScope,
	selector control.AppSelector,
	expectedVersion uint64,
	definition control.AppDefinition,
) (control.AppWorkspace, error) {
	if catalog.update == nil {
		return control.AppWorkspace{}, errors.New("unexpected UpdateApp")
	}
	return catalog.update(ctx, scope, selector, expectedVersion, definition)
}

func (catalog *stubControlAppCatalog) SetAppState(
	ctx context.Context,
	scope control.AppAccessScope,
	selector control.AppSelector,
	expectedVersion uint64,
	state control.AppState,
) (control.AppWorkspace, error) {
	if catalog.setState == nil {
		return control.AppWorkspace{}, errors.New("unexpected SetAppState")
	}
	return catalog.setState(ctx, scope, selector, expectedVersion, state)
}

func (catalog *stubControlAppCatalog) DeleteApp(
	ctx context.Context,
	scope control.AppAccessScope,
	selector control.AppSelector,
	expectedVersion uint64,
	confirmationSlug string,
) (string, error) {
	if catalog.delete == nil {
		return "", errors.New("unexpected DeleteApp")
	}
	return catalog.delete(
		ctx,
		scope,
		selector,
		expectedVersion,
		confirmationSlug,
	)
}

func TestRuntimeAppCatalogConvertsPresenceAndDetachesValues(t *testing.T) {
	t.Parallel()

	description := "workspace description"
	latest := ""
	timezone := "America/Los_Angeles"
	input := server.AppAdministrationDefinition{
		Slug:              "app-one",
		DisplayName:       "App One",
		Description:       &description,
		DefaultIndexNames: []string{"audit", "main"},
		DefaultTimeRange: &server.AppAdministrationTimeRange{
			Latest:   &latest,
			Timezone: &timezone,
		},
	}
	createdAt := time.Unix(1_700_000_000, 123_000).UTC()
	earliest := "-24h"
	persistedDescription := "persisted description"
	persisted := control.AppWorkspace{
		ID:      "app_000000000000000000000A",
		Version: 4,
		Definition: control.AppDefinition{
			Slug:           "app-one",
			DisplayName:    "App One",
			Description:    persistedDescription,
			DefaultIndexes: []string{"audit", "main"},
			DefaultTimeRange: &control.AppTimeRange{
				Earliest: &earliest,
				Latest:   &latest,
			},
		},
		State:     control.AppStateArchived,
		CreatedAt: createdAt,
		UpdatedAt: createdAt.Add(time.Minute),
	}
	var captured control.AppDefinition
	backend := &stubControlAppCatalog{
		create: func(
			_ context.Context,
			scope control.AppAccessScope,
			definition control.AppDefinition,
		) (control.AppWorkspace, error) {
			if scope != (control.AppAccessScope{TenantID: "tenant-a"}) {
				t.Fatalf("scope = %#v", scope)
			}
			captured = definition
			return persisted, nil
		},
	}
	adapter := &runtimeAppCatalog{catalog: backend}
	got, err := adapter.CreateApp(
		context.Background(),
		server.AppAdministrationScope{
			TenantID: "tenant-a",
			ActorID:  "administrator",
		},
		input,
	)
	if err != nil {
		t.Fatal(err)
	}
	if captured.Description != description ||
		!slices.Equal(captured.DefaultIndexes, input.DefaultIndexNames) ||
		captured.DefaultTimeRange == nil ||
		captured.DefaultTimeRange.Earliest != nil ||
		captured.DefaultTimeRange.Latest == nil ||
		*captured.DefaultTimeRange.Latest != "" ||
		captured.DefaultTimeRange.Timezone == nil ||
		*captured.DefaultTimeRange.Timezone != timezone {
		t.Fatalf("converted definition = %#v", captured)
	}
	if got.AppID != persisted.ID ||
		got.Version != persisted.Version ||
		got.State != server.AppAdministrationStateArchived ||
		got.Definition.Description == nil ||
		*got.Definition.Description != persistedDescription ||
		got.Definition.DefaultTimeRange == nil ||
		got.Definition.DefaultTimeRange.Earliest == nil ||
		*got.Definition.DefaultTimeRange.Earliest != earliest ||
		got.Definition.DefaultTimeRange.Latest == nil ||
		*got.Definition.DefaultTimeRange.Latest != "" ||
		got.Definition.DefaultTimeRange.Timezone != nil ||
		!got.CreatedAt.Equal(persisted.CreatedAt) ||
		!got.UpdatedAt.Equal(persisted.UpdatedAt) {
		t.Fatalf("converted workspace = %#v", got)
	}

	input.DefaultIndexNames[0] = "input-mutated"
	*input.Description = "input-mutated"
	*input.DefaultTimeRange.Latest = "input-mutated"
	if captured.DefaultIndexes[0] != "audit" ||
		captured.Description != "workspace description" ||
		*captured.DefaultTimeRange.Latest != "" {
		t.Fatal("caller input remained aliased to the control request")
	}
	persisted.Definition.DefaultIndexes[0] = "backend-mutated"
	persisted.Definition.Description = "backend-mutated"
	*persisted.Definition.DefaultTimeRange.Earliest = "backend-mutated"
	if got.Definition.DefaultIndexNames[0] != "audit" ||
		*got.Definition.Description != persistedDescription ||
		*got.Definition.DefaultTimeRange.Earliest != "-24h" {
		t.Fatal("control result remained aliased to the server result")
	}

	empty, err := serverAppWorkspace(control.AppWorkspace{
		Definition: control.AppDefinition{},
		State:      control.AppStateActive,
	})
	if err != nil {
		t.Fatal(err)
	}
	if empty.Definition.Description != nil ||
		empty.Definition.DefaultTimeRange != nil {
		t.Fatalf("canonical absence = %#v", empty.Definition)
	}
	if converted := controlAppDefinition(server.AppAdministrationDefinition{
		Description:      nil,
		DefaultTimeRange: &server.AppAdministrationTimeRange{},
	}); converted.Description != "" ||
		converted.DefaultTimeRange == nil ||
		converted.DefaultTimeRange.Earliest != nil ||
		converted.DefaultTimeRange.Latest != nil ||
		converted.DefaultTimeRange.Timezone != nil {
		t.Fatalf("absent/empty presence conversion = %#v", converted)
	}
}

func TestRuntimeAppCatalogListMapsCursorRevisionFiltersAndResult(t *testing.T) {
	t.Parallel()

	revision := uint64(41)
	total := uint64(9)
	next := "inner-cursor"
	earliest := "-15m"
	result := control.AppListResult{
		Apps: []control.AppWorkspace{{
			ID:      "app_000000000000000000000A",
			Version: 3,
			Definition: control.AppDefinition{
				Slug:             "alpha",
				DisplayName:      "Alpha",
				DefaultIndexes:   []string{"main"},
				DefaultTimeRange: &control.AppTimeRange{Earliest: &earliest},
			},
			State:     control.AppStateActive,
			CreatedAt: time.Unix(100, 0).UTC(),
			UpdatedAt: time.Unix(200, 0).UTC(),
		}},
		NextPageToken:   &next,
		TotalSize:       &total,
		TotalSizeExact:  true,
		CatalogRevision: revision,
	}
	var captured control.AppListRequest
	backend := &stubControlAppCatalog{
		list: func(
			_ context.Context,
			scope control.AppAccessScope,
			request control.AppListRequest,
		) (control.AppListResult, error) {
			if scope.TenantID != "tenant-a" {
				t.Fatalf("scope = %#v", scope)
			}
			captured = request
			return result, nil
		},
	}
	request := server.AppAdministrationListRequest{
		PageSize:                7,
		PageCursor:              "opaque-control-token",
		RequiredCatalogRevision: &revision,
		IncludeTotal:            true,
		StateFilters: []server.AppAdministrationState{
			server.AppAdministrationStateActive,
			server.AppAdministrationStateArchived,
		},
		TextFilter:     "grade",
		SortBy:         server.AppAdministrationSortUpdatedAt,
		SortDescending: true,
	}
	adapter := &runtimeAppCatalog{catalog: backend}
	got, err := adapter.ListApps(
		context.Background(),
		server.AppAdministrationScope{TenantID: "tenant-a"},
		request,
	)
	if err != nil {
		t.Fatal(err)
	}
	wantRequest := control.AppListRequest{
		PageSize:                7,
		PageToken:               "opaque-control-token",
		RequiredCatalogRevision: &revision,
		IncludeTotal:            true,
		StateFilters: []control.AppState{
			control.AppStateActive,
			control.AppStateArchived,
		},
		TextFilter: stringPointerForRuntimeAppTest("grade"),
		SortBy:     control.AppSortByUpdatedAt,
		Direction:  control.AppSortDescending,
	}
	if !reflect.DeepEqual(captured, wantRequest) {
		t.Fatalf("control request = %#v, want %#v", captured, wantRequest)
	}
	if captured.RequiredCatalogRevision == request.RequiredCatalogRevision {
		t.Fatal("required catalog revision pointer was not detached")
	}
	if got.CatalogRevision != revision ||
		got.NextPageCursor != next ||
		got.TotalSize == nil ||
		*got.TotalSize != total ||
		!got.TotalSizeExact ||
		len(got.Apps) != 1 ||
		got.Apps[0].Definition.DefaultTimeRange == nil ||
		got.Apps[0].Definition.DefaultTimeRange.Earliest == nil ||
		*got.Apps[0].Definition.DefaultTimeRange.Earliest != earliest {
		t.Fatalf("server result = %#v", got)
	}

	request.StateFilters[0] = "mutated"
	*request.RequiredCatalogRevision = 99
	result.Apps[0].Definition.DefaultIndexes[0] = "mutated"
	*result.TotalSize = 99
	*result.NextPageToken = "mutated"
	if captured.StateFilters[0] != control.AppStateActive ||
		*captured.RequiredCatalogRevision != 41 ||
		got.Apps[0].Definition.DefaultIndexNames[0] != "main" ||
		*got.TotalSize != 9 ||
		got.NextPageCursor != "inner-cursor" {
		t.Fatal("list request or result remained aliased across the adapter")
	}
}

func TestRuntimeAppCatalogListsCompleteDetachedActiveBootstrapCatalog(
	t *testing.T,
) {
	t.Parallel()

	source := control.AppListResult{
		Apps: []control.AppWorkspace{{
			ID: "app_000000000000000000000A",
			Definition: control.AppDefinition{
				Slug:           "alpha",
				DisplayName:    "Alpha",
				DefaultIndexes: []string{"audit", "main"},
			},
			State: control.AppStateActive,
		}},
	}
	var capturedScope control.AppAccessScope
	var capturedRequest control.AppListRequest
	backend := &stubControlAppCatalog{
		list: func(
			_ context.Context,
			scope control.AppAccessScope,
			request control.AppListRequest,
		) (control.AppListResult, error) {
			capturedScope = scope
			capturedRequest = request
			return source, nil
		},
	}
	adapter := &runtimeAppCatalog{catalog: backend}
	got, err := adapter.ListActiveApps(
		context.Background(),
		"tenant-a",
		256,
	)
	if err != nil {
		t.Fatal(err)
	}
	if capturedScope.TenantID != "tenant-a" ||
		capturedRequest.PageSize != 256 ||
		capturedRequest.PageToken != "" ||
		capturedRequest.RequiredCatalogRevision != nil ||
		capturedRequest.IncludeTotal ||
		!slices.Equal(
			capturedRequest.StateFilters,
			[]control.AppState{control.AppStateActive},
		) ||
		capturedRequest.TextFilter != nil ||
		capturedRequest.SortBy != control.AppSortByDisplayName ||
		capturedRequest.Direction != control.AppSortAscending {
		t.Fatalf(
			"bootstrap scope/request = %#v %#v",
			capturedScope,
			capturedRequest,
		)
	}
	if !got.Complete ||
		len(got.Apps) != 1 ||
		got.Apps[0].AppID != source.Apps[0].ID ||
		got.Apps[0].Slug != "alpha" ||
		got.Apps[0].DisplayName != "Alpha" ||
		!slices.Equal(got.Apps[0].DefaultIndexNames, []string{"audit", "main"}) {
		t.Fatalf("bootstrap result = %#v", got)
	}
	source.Apps[0].Definition.DefaultIndexes[0] = "mutated"
	if got.Apps[0].DefaultIndexNames[0] != "audit" {
		t.Fatal("bootstrap result remained aliased to the control result")
	}

	next := "more"
	source.NextPageToken = &next
	incomplete, err := adapter.ListActiveApps(
		context.Background(),
		"tenant-a",
		256,
	)
	if err != nil {
		t.Fatal(err)
	}
	if incomplete.Complete {
		t.Fatal("catalog with a continuation was reported complete")
	}

	source.NextPageToken = nil
	source.Apps = append(source.Apps, source.Apps[0])
	if _, err := adapter.ListActiveApps(
		context.Background(),
		"tenant-a",
		1,
	); err == nil {
		t.Fatal("oversized storage page was accepted")
	}
	source.Apps = source.Apps[:1]
	source.Apps[0].State = control.AppStateArchived
	if _, err := adapter.ListActiveApps(
		context.Background(),
		"tenant-a",
		256,
	); err == nil || errors.Is(err, server.ErrAppAdministrationInvalidArgument) {
		t.Fatalf("archived row corruption error = %v", err)
	}
	if _, err := adapter.ListActiveApps(
		context.Background(),
		"tenant-a",
		0,
	); !errors.Is(err, server.ErrAppAdministrationInvalidArgument) {
		t.Fatalf("zero maximum error = %v", err)
	}
}

func TestRuntimeAppCatalogMapsSortStateAndDeleteArguments(t *testing.T) {
	t.Parallel()

	var stateCall control.AppState
	var deleteScope control.AppAccessScope
	var deleteSelector control.AppSelector
	var deleteVersion uint64
	var deleteConfirmation string
	backend := &stubControlAppCatalog{
		setState: func(
			_ context.Context,
			_ control.AppAccessScope,
			_ control.AppSelector,
			_ uint64,
			state control.AppState,
		) (control.AppWorkspace, error) {
			stateCall = state
			return control.AppWorkspace{State: state}, nil
		},
		delete: func(
			_ context.Context,
			scope control.AppAccessScope,
			selector control.AppSelector,
			expectedVersion uint64,
			confirmationSlug string,
		) (string, error) {
			deleteScope = scope
			deleteSelector = selector
			deleteVersion = expectedVersion
			deleteConfirmation = confirmationSlug
			return "app_000000000000000000000A", nil
		},
	}
	adapter := &runtimeAppCatalog{catalog: backend}
	if _, err := adapter.SetAppState(
		context.Background(),
		server.AppAdministrationScope{TenantID: "tenant-a"},
		server.AppAdministrationSelector{Slug: "alpha"},
		7,
		server.AppAdministrationStateArchived,
	); err != nil {
		t.Fatal(err)
	}
	if stateCall != control.AppStateArchived {
		t.Fatalf("state = %q", stateCall)
	}
	const confirmation = " Alpha \n"
	deleted, err := adapter.DeleteApp(
		context.Background(),
		server.AppAdministrationScope{
			TenantID: "tenant-a",
			ActorID:  "administrator",
		},
		server.AppAdministrationSelector{
			AppID: "app_000000000000000000000A",
		},
		8,
		confirmation,
	)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != "app_000000000000000000000A" ||
		deleteScope.TenantID != "tenant-a" ||
		deleteSelector.AppID != deleted ||
		deleteSelector.Slug != "" ||
		deleteVersion != 8 ||
		deleteConfirmation != confirmation {
		t.Fatalf(
			"delete arguments/result = %#v %#v %d %q %q",
			deleteScope,
			deleteSelector,
			deleteVersion,
			deleteConfirmation,
			deleted,
		)
	}

	if _, err := adapter.SetAppState(
		context.Background(),
		server.AppAdministrationScope{},
		server.AppAdministrationSelector{},
		1,
		"unknown",
	); !errors.Is(err, server.ErrAppAdministrationInvalidArgument) {
		t.Fatalf("invalid state error = %v", err)
	}
	if _, err := controlAppListRequest(server.AppAdministrationListRequest{
		SortBy: 99,
	}); !errors.Is(err, server.ErrAppAdministrationInvalidArgument) {
		t.Fatalf("invalid sort error = %v", err)
	}
}

func TestMapRuntimeAppCatalogErrorIsStableAndSanitized(t *testing.T) {
	t.Parallel()

	unknown := errors.New("sqlite row is corrupt and contains a secret detail")
	canceled := fmtErrorWithSentinel("wrapped cancellation", context.Canceled)
	deadline := fmtErrorWithSentinel("wrapped deadline", context.DeadlineExceeded)
	tests := []struct {
		name string
		err  error
		want error
	}{
		{name: "nil", err: nil, want: nil},
		{name: "invalid", err: fmtErrorWithSentinel("detail", control.ErrInvalidArgument), want: server.ErrAppAdministrationInvalidArgument},
		{name: "not found", err: fmtErrorWithSentinel("detail", control.ErrNotFound), want: server.ErrAppAdministrationNotFound},
		{name: "already exists", err: fmtErrorWithSentinel("detail", control.ErrAlreadyExists), want: server.ErrAppAdministrationAlreadyExists},
		{name: "version", err: fmtErrorWithSentinel("detail", control.ErrVersionConflict), want: server.ErrAppAdministrationConflict},
		{name: "immutable slug", err: fmtErrorWithSentinel("detail", control.ErrImmutableSlug), want: server.ErrAppAdministrationConflict},
		{name: "immutable name", err: fmtErrorWithSentinel("detail", control.ErrImmutableName), want: server.ErrAppAdministrationConflict},
		{name: "dependency", err: fmtErrorWithSentinel("detail", control.ErrDependencyConflict), want: server.ErrAppAdministrationConflict},
		{name: "capacity", err: fmtErrorWithSentinel("detail", control.ErrCapacityExceeded), want: server.ErrAppAdministrationCapacity},
		{name: "page invalidated", err: fmtErrorWithSentinel("detail", control.ErrPageInvalidated), want: server.ErrAppAdministrationInvalidPageToken},
		{name: "canceled", err: canceled, want: canceled},
		{name: "deadline", err: deadline, want: deadline},
		{name: "unknown", err: unknown, want: unknown},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := mapRuntimeAppCatalogError(test.err); !reflect.DeepEqual(got, test.want) {
				t.Fatalf("mapped error = %v, want exact %v", got, test.want)
			}
		})
	}
}

func TestDeriveAppCursorKeysIsStableAndPurposeSeparated(t *testing.T) {
	t.Parallel()

	master := bytes.Repeat([]byte{0x35}, masterKeyBytes)
	firstCatalog, firstAdministration, err := deriveAppCursorKeys(master)
	if err != nil {
		t.Fatal(err)
	}
	secondCatalog, secondAdministration, err := deriveAppCursorKeys(master)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstCatalog) != 32 ||
		len(firstAdministration) != 32 ||
		bytes.Equal(firstCatalog, firstAdministration) ||
		!bytes.Equal(firstCatalog, secondCatalog) ||
		!bytes.Equal(firstAdministration, secondAdministration) {
		t.Fatal("app cursor keys are not stable and purpose-separated")
	}
	savedSearchKey, err := deriveServerKey(master, "saved-search-cursors")
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Equal(firstCatalog, savedSearchKey) ||
		bytes.Equal(firstAdministration, savedSearchKey) {
		t.Fatal("app cursor keys collide with another runtime purpose")
	}
}

func TestRuntimeAppCatalogPersistsAcrossReopenAndBorrowsControlDB(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	dbPath := filepath.Join(directory, "control.db")
	keyPath := filepath.Join(directory, "server.key")

	firstDB, err := control.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	first, firstOuterKey, err := newRuntimeAppCatalog(ctx, firstDB, keyPath)
	if err != nil {
		_ = firstDB.Close()
		t.Fatal(err)
	}
	firstCreated, err := first.CreateApp(
		ctx,
		server.AppAdministrationScope{
			TenantID: "tenant-a",
			ActorID:  "administrator",
		},
		server.AppAdministrationDefinition{
			Slug:        "alpha",
			DisplayName: "Alpha",
		},
	)
	if err != nil {
		_ = firstDB.Close()
		t.Fatal(err)
	}
	if _, err := first.CreateApp(
		ctx,
		server.AppAdministrationScope{TenantID: "tenant-a"},
		server.AppAdministrationDefinition{
			Slug:        "beta",
			DisplayName: "Beta",
		},
	); err != nil {
		_ = firstDB.Close()
		t.Fatal(err)
	}
	if _, err := first.GetApp(
		ctx,
		server.AppAdministrationScope{TenantID: "tenant-b"},
		server.AppAdministrationSelector{AppID: firstCreated.AppID},
	); !errors.Is(err, server.ErrAppAdministrationNotFound) {
		_ = firstDB.Close()
		t.Fatalf("cross-tenant get error = %v", err)
	}
	firstPage, err := first.ListApps(
		ctx,
		server.AppAdministrationScope{TenantID: "tenant-a"},
		server.AppAdministrationListRequest{
			PageSize: 1,
			SortBy:   server.AppAdministrationSortDisplayName,
		},
	)
	if err != nil {
		_ = firstDB.Close()
		t.Fatal(err)
	}
	if len(firstPage.Apps) != 1 ||
		firstPage.NextPageCursor == "" ||
		firstPage.CatalogRevision == 0 {
		_ = firstDB.Close()
		t.Fatalf("first page = %#v", firstPage)
	}
	// Constructing and using the borrowing adapter must not close the shared
	// database before the process-level shutdown owner does so.
	if err := firstDB.SQLDB().PingContext(ctx); err != nil {
		_ = firstDB.Close()
		t.Fatalf("control database was closed prematurely: %v", err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatal(err)
	}

	secondDB, err := control.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := secondDB.Close(); err != nil {
			t.Error(err)
		}
	})
	second, secondOuterKey, err := newRuntimeAppCatalog(ctx, secondDB, keyPath)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstOuterKey, secondOuterKey) ||
		len(secondOuterKey) != 32 {
		t.Fatal("administration cursor key did not survive process reopen")
	}
	revision := firstPage.CatalogRevision
	secondPage, err := second.ListApps(
		ctx,
		server.AppAdministrationScope{TenantID: "tenant-a"},
		server.AppAdministrationListRequest{
			PageSize:                1,
			PageCursor:              firstPage.NextPageCursor,
			RequiredCatalogRevision: &revision,
			SortBy:                  server.AppAdministrationSortDisplayName,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondPage.Apps) != 1 ||
		secondPage.NextPageCursor != "" ||
		secondPage.CatalogRevision != revision ||
		secondPage.Apps[0].Definition.Slug != "beta" {
		t.Fatalf("continued page after reopen = %#v", secondPage)
	}
	reopened, err := second.GetApp(
		ctx,
		server.AppAdministrationScope{TenantID: "tenant-a"},
		server.AppAdministrationSelector{AppID: firstCreated.AppID},
	)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Definition.Slug != "alpha" {
		t.Fatalf("reopened app = %#v", reopened)
	}
	archived, err := second.SetAppState(
		ctx,
		server.AppAdministrationScope{TenantID: "tenant-a"},
		server.AppAdministrationSelector{AppID: firstCreated.AppID},
		reopened.Version,
		server.AppAdministrationStateArchived,
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := second.DeleteApp(
		ctx,
		server.AppAdministrationScope{TenantID: "tenant-a"},
		server.AppAdministrationSelector{AppID: firstCreated.AppID},
		archived.Version,
		" alpha ",
	); !errors.Is(err, server.ErrAppAdministrationInvalidArgument) {
		t.Fatalf("non-exact confirmation error = %v", err)
	}
	deleted, err := second.DeleteApp(
		ctx,
		server.AppAdministrationScope{TenantID: "tenant-a"},
		server.AppAdministrationSelector{AppID: firstCreated.AppID},
		archived.Version,
		"alpha",
	)
	if err != nil {
		t.Fatal(err)
	}
	if deleted != firstCreated.AppID {
		t.Fatalf("deleted ID = %q, want %q", deleted, firstCreated.AppID)
	}
	clear(firstOuterKey)
	clear(secondOuterKey)
}

func TestRuntimeAppCatalogEndToEndHTTPAndCursorReopen(t *testing.T) {
	ctx := context.Background()
	directory := t.TempDir()
	dbPath := filepath.Join(directory, "control.db")
	keyPath := filepath.Join(directory, "server.key")
	bearerToken := bytes.Repeat([]byte("a"), auth.MinimumBrowserBearerTokenBytes)
	authenticator, err := auth.NewBearerTokenAuthenticator(
		bearerToken,
		"tenant-a",
		"administrator",
		auth.BrowserRoleAdministrator,
	)
	if err != nil {
		t.Fatal(err)
	}

	firstDB, err := control.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	firstCatalog, firstCursorKey, err := newRuntimeAppCatalog(
		ctx,
		firstDB,
		keyPath,
	)
	if err != nil {
		_ = firstDB.Close()
		t.Fatal(err)
	}
	stableCursorKey := slices.Clone(firstCursorKey)
	firstHandler := newRuntimeAppHTTPHandlerForTest(
		t,
		firstCatalog,
		firstCursorKey,
		authenticator,
	)
	if !allRuntimeAppBytesCleared(firstCursorKey) {
		_ = firstDB.Close()
		t.Fatal("caller app-administration key was not cleared after handler construction")
	}

	created := make(map[string]*opensplunkv1.AppWorkspace, 3)
	for _, slug := range []string{"alpha", "beta", "gamma"} {
		response := postRuntimeAppProto(
			t,
			firstHandler,
			"/api/v1/apps/create",
			&opensplunkv1.CreateAppRequest{
				Definition: &opensplunkv1.AppDefinition{
					Slug:        slug,
					DisplayName: strings.ToUpper(slug[:1]) + slug[1:],
				},
			},
			bearerToken,
		)
		if response.Code != http.StatusOK {
			_ = firstDB.Close()
			t.Fatalf(
				"create %s status = %d, body = %s",
				slug,
				response.Code,
				response.Body.String(),
			)
		}
		var decoded opensplunkv1.CreateAppResponse
		unmarshalRuntimeAppResponse(t, response, &decoded)
		created[slug] = decoded.GetApp()
	}

	initialBootstrap := getRuntimeAppBootstrap(t, firstHandler)
	if got := runtimeBootstrapSlugs(initialBootstrap); !slices.Equal(
		got,
		[]string{"alpha", "beta", "gamma"},
	) {
		_ = firstDB.Close()
		t.Fatalf("initial active bootstrap apps = %v", got)
	}

	archivedResponse := postRuntimeAppProto(
		t,
		firstHandler,
		"/api/v1/apps/state/set",
		&opensplunkv1.SetAppStateRequest{
			Selector: &opensplunkv1.AppSelector{
				Selector: &opensplunkv1.AppSelector_AppId{
					AppId: created["alpha"].GetAppId(),
				},
			},
			ExpectedVersion: created["alpha"].GetVersion(),
			State:           opensplunkv1.AppState_APP_STATE_ARCHIVED,
		},
		bearerToken,
	)
	if archivedResponse.Code != http.StatusOK {
		_ = firstDB.Close()
		t.Fatalf(
			"archive status = %d, body = %s",
			archivedResponse.Code,
			archivedResponse.Body.String(),
		)
	}
	var archived opensplunkv1.SetAppStateResponse
	unmarshalRuntimeAppResponse(t, archivedResponse, &archived)
	if archived.GetApp().GetState() !=
		opensplunkv1.AppState_APP_STATE_ARCHIVED {
		_ = firstDB.Close()
		t.Fatalf("archived response = %#v", archived.GetApp())
	}
	afterArchive := getRuntimeAppBootstrap(t, firstHandler)
	if got := runtimeBootstrapSlugs(afterArchive); !slices.Equal(
		got,
		[]string{"beta", "gamma"},
	) {
		_ = firstDB.Close()
		t.Fatalf("active bootstrap apps after archive = %v", got)
	}

	one := uint32(1)
	firstListResponse := postRuntimeAppProto(
		t,
		firstHandler,
		"/api/v1/apps/list",
		&opensplunkv1.ListAppsRequest{
			Page: &opensplunkv1.PageRequest{PageSize: &one},
			StateFilters: []opensplunkv1.AppState{
				opensplunkv1.AppState_APP_STATE_ACTIVE,
			},
			SortBy:        opensplunkv1.AppSortBy_APP_SORT_BY_DISPLAY_NAME,
			SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING,
		},
		bearerToken,
	)
	if firstListResponse.Code != http.StatusOK {
		_ = firstDB.Close()
		t.Fatalf(
			"first list status = %d, body = %s",
			firstListResponse.Code,
			firstListResponse.Body.String(),
		)
	}
	var firstPage opensplunkv1.ListAppsResponse
	unmarshalRuntimeAppResponse(t, firstListResponse, &firstPage)
	if len(firstPage.GetApps()) != 1 ||
		firstPage.GetApps()[0].GetDefinition().GetSlug() != "beta" ||
		firstPage.GetPage().GetNextPageToken() == "" {
		_ = firstDB.Close()
		t.Fatalf("first active page = %#v", &firstPage)
	}
	outerPageToken := strings.Clone(firstPage.GetPage().GetNextPageToken())
	if err := firstHandler.Close(ctx); err != nil {
		_ = firstDB.Close()
		t.Fatal(err)
	}
	if err := firstDB.Close(); err != nil {
		t.Fatal(err)
	}

	secondDB, err := control.Open(ctx, dbPath)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := secondDB.Close(); err != nil {
			t.Error(err)
		}
	})
	secondCatalog, secondCursorKey, err := newRuntimeAppCatalog(
		ctx,
		secondDB,
		keyPath,
	)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(stableCursorKey, secondCursorKey) {
		t.Fatal("app-administration key changed across runtime reopen")
	}
	secondHandler := newRuntimeAppHTTPHandlerForTest(
		t,
		secondCatalog,
		secondCursorKey,
		authenticator,
	)
	if !allRuntimeAppBytesCleared(secondCursorKey) {
		t.Fatal("reopened caller key was not cleared after handler construction")
	}
	t.Cleanup(func() {
		if err := secondHandler.Close(context.Background()); err != nil {
			t.Error(err)
		}
	})

	reopenedBootstrap := getRuntimeAppBootstrap(t, secondHandler)
	if got := runtimeBootstrapSlugs(reopenedBootstrap); !slices.Equal(
		got,
		[]string{"beta", "gamma"},
	) {
		t.Fatalf("reopened active bootstrap apps = %v", got)
	}
	secondListResponse := postRuntimeAppProto(
		t,
		secondHandler,
		"/api/v1/apps/list",
		&opensplunkv1.ListAppsRequest{
			Page: &opensplunkv1.PageRequest{
				PageSize:  &one,
				PageToken: &outerPageToken,
			},
			StateFilters: []opensplunkv1.AppState{
				opensplunkv1.AppState_APP_STATE_ACTIVE,
			},
			SortBy:        opensplunkv1.AppSortBy_APP_SORT_BY_DISPLAY_NAME,
			SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING,
		},
		bearerToken,
	)
	if secondListResponse.Code != http.StatusOK {
		t.Fatalf(
			"continued list status = %d, body = %s",
			secondListResponse.Code,
			secondListResponse.Body.String(),
		)
	}
	var secondPage opensplunkv1.ListAppsResponse
	unmarshalRuntimeAppResponse(t, secondListResponse, &secondPage)
	if len(secondPage.GetApps()) != 1 ||
		secondPage.GetApps()[0].GetDefinition().GetSlug() != "gamma" ||
		secondPage.GetPage().GetNextPageToken() != "" {
		t.Fatalf("continued active page after reopen = %#v", &secondPage)
	}
	clear(stableCursorKey)
}

func newRuntimeAppHTTPHandlerForTest(
	t *testing.T,
	catalog *runtimeAppCatalog,
	cursorKey []byte,
	authenticator auth.BrowserAuthenticator,
) *server.Handler {
	t.Helper()
	config := runtimeServerConfig()
	config.AppAdmin = catalog
	config.AppCatalog = catalog
	config.AppCursorKey = cursorKey
	config.BrowserAuthenticator = authenticator
	config.TenantID = "tenant-a"
	config.OwnerID = "administrator"
	config.AdministrativeAllowedHosts = []string{"127.0.0.1"}
	handler, err := server.NewHandler(config)
	clear(cursorKey)
	if err != nil {
		t.Fatalf("NewHandler: %v", err)
	}
	return handler
}

func postRuntimeAppProto(
	t *testing.T,
	handler http.Handler,
	path string,
	message proto.Message,
	bearerToken []byte,
) *httptest.ResponseRecorder {
	t.Helper()
	payload, err := proto.Marshal(message)
	if err != nil {
		t.Fatalf("marshal %T: %v", message, err)
	}
	request := httptest.NewRequestWithContext(
		context.Background(),
		http.MethodPost,
		"http://127.0.0.1"+path,
		bytes.NewReader(payload),
	)
	request.Header.Set("Content-Type", "application/x-protobuf")
	if len(bearerToken) != 0 {
		request.Header.Set(
			"Authorization",
			"Bearer "+string(bearerToken),
		)
	}
	response := httptest.NewRecorder()
	handler.ServeHTTP(response, request)
	return response
}

func getRuntimeAppBootstrap(
	t *testing.T,
	handler http.Handler,
) *opensplunkv1.GetSystemBootstrapResponse {
	t.Helper()
	response := postRuntimeAppProto(
		t,
		handler,
		"/api/v1/system/bootstrap",
		&opensplunkv1.GetSystemBootstrapRequest{},
		nil,
	)
	if response.Code != http.StatusOK {
		t.Fatalf(
			"bootstrap status = %d, body = %s",
			response.Code,
			response.Body.String(),
		)
	}
	var decoded opensplunkv1.GetSystemBootstrapResponse
	unmarshalRuntimeAppResponse(t, response, &decoded)
	return &decoded
}

func unmarshalRuntimeAppResponse(
	t *testing.T,
	response *httptest.ResponseRecorder,
	message proto.Message,
) {
	t.Helper()
	if err := proto.Unmarshal(response.Body.Bytes(), message); err != nil {
		t.Fatalf("unmarshal %T: %v", message, err)
	}
}

func runtimeBootstrapSlugs(
	response *opensplunkv1.GetSystemBootstrapResponse,
) []string {
	result := make([]string, len(response.GetApps()))
	for index, app := range response.GetApps() {
		result[index] = strings.Clone(app.GetSlug())
	}
	return result
}

func allRuntimeAppBytesCleared(value []byte) bool {
	for _, element := range value {
		if element != 0 {
			return false
		}
	}
	return true
}

func stringPointerForRuntimeAppTest(value string) *string {
	result := strings.Clone(value)
	return &result
}

func fmtErrorWithSentinel(detail string, sentinel error) error {
	return &runtimeAppTestWrappedError{detail: detail, sentinel: sentinel}
}

type runtimeAppTestWrappedError struct {
	detail   string
	sentinel error
}

func (err *runtimeAppTestWrappedError) Error() string {
	return err.detail + ": " + err.sentinel.Error()
}

func (err *runtimeAppTestWrappedError) Unwrap() error {
	return err.sentinel
}
