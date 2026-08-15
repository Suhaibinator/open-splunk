package lookupcatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"path/filepath"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
)

type observingRepository struct {
	lookupasset.Repository
	getVersionCalls atomic.Uint64
	block           atomic.Bool
	started         chan struct{}
	release         chan struct{}
	signaled        atomic.Bool
}

func (repository *observingRepository) GetVersion(
	ctx context.Context,
	ref lookupasset.VersionRef,
) (lookupasset.Version, error) {
	repository.getVersionCalls.Add(1)
	if repository.block.Load() {
		if repository.signaled.CompareAndSwap(false, true) && repository.started != nil {
			close(repository.started)
		}
		if repository.release != nil {
			select {
			case <-repository.release:
			case <-ctx.Done():
				return lookupasset.Version{}, ctx.Err()
			}
		}
	}
	return repository.Repository.GetVersion(ctx, ref)
}

func (repository *observingRepository) reset() {
	repository.getVersionCalls.Store(0)
}

func TestCatalogHistoricalVersionsRetainExactLifecycleAndMonotonicTime(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	baseTime := time.Date(2026, time.August, 13, 12, 0, 0, 0, time.UTC)
	clockCalls := atomic.Uint64{}
	catalog, err := New(database, assets, Options{
		Clock: func() time.Time {
			if clockCalls.Add(1) == 1 {
				return baseTime
			}
			return baseTime.Add(-time.Hour)
		},
		IDGenerator: func() (string, error) { return "lookup-history", nil },
	})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	firstDefinition := catalogDefinition(
		appIDs[0],
		"history-v1",
		opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		false,
	)
	created, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: firstDefinition, Asset: asset,
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	secondDefinition := catalogDefinition(
		appIDs[0],
		"history-v2",
		opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		true,
	)
	replaced, err := catalog.Replace(t.Context(), ReplaceRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		LookupID: created.GetLookupId(), ExpectedVersion: created.GetVersion(),
		Definition: secondDefinition, Asset: asset,
	})
	if err != nil {
		t.Fatalf("Replace(): %v", err)
	}
	disabled, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		LookupID: created.GetLookupId(), ExpectedVersion: replaced.GetVersion(),
		State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	})
	if err != nil {
		t.Fatalf("SetState(disabled): %v", err)
	}
	deleted, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		LookupID: created.GetLookupId(), ExpectedVersion: disabled.GetVersion(),
		State: opensplunkv1.LookupState_LOOKUP_STATE_DELETED,
	})
	if err != nil {
		t.Fatalf("SetState(deleted): %v", err)
	}

	wants := []struct {
		version    uint64
		name       string
		state      opensplunkv1.LookupState
		updated    time.Time
		disabledAt time.Time
		deletedAt  time.Time
	}{
		{1, "history-v1", opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE, baseTime, time.Time{}, time.Time{}},
		{2, "history-v2", opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE, baseTime.Add(time.Microsecond), time.Time{}, time.Time{}},
		{3, "history-v2", opensplunkv1.LookupState_LOOKUP_STATE_DISABLED, baseTime.Add(2 * time.Microsecond), baseTime.Add(2 * time.Microsecond), time.Time{}},
		{4, "history-v2", opensplunkv1.LookupState_LOOKUP_STATE_DELETED, baseTime.Add(3 * time.Microsecond), baseTime.Add(2 * time.Microsecond), baseTime.Add(3 * time.Microsecond)},
	}
	for _, want := range wants {
		got, getErr := catalog.Get(t.Context(), GetRequest{
			TenantID: "tenant-lookups", OwnerID: "owner-lookups",
			LookupID: created.GetLookupId(), Version: want.version,
		})
		if getErr != nil {
			t.Fatalf("Get(version %d): %v", want.version, getErr)
		}
		if got.GetVersion() != want.version || got.GetDefinition().GetName() != want.name ||
			got.GetState() != want.state || !got.GetUpdatedAt().AsTime().Equal(want.updated) ||
			(got.GetDisabledAt() != nil) != !want.disabledAt.IsZero() ||
			(got.GetDeletedAt() != nil) != !want.deletedAt.IsZero() {
			t.Fatalf("Get(version %d) = %#v", want.version, got)
		}
		if !want.disabledAt.IsZero() && !got.GetDisabledAt().AsTime().Equal(want.disabledAt) {
			t.Fatalf("Get(version %d) disabled_at = %v, want %v", want.version, got.GetDisabledAt(), want.disabledAt)
		}
		if !want.deletedAt.IsZero() && !got.GetDeletedAt().AsTime().Equal(want.deletedAt) {
			t.Fatalf("Get(version %d) deleted_at = %v, want %v", want.version, got.GetDeletedAt(), want.deletedAt)
		}
	}
	if !disabled.GetUpdatedAt().AsTime().Equal(baseTime.Add(2 * time.Microsecond)) {
		t.Fatalf("disabled mutation time = %v", disabled.GetUpdatedAt())
	}
	if !deleted.GetUpdatedAt().AsTime().Equal(baseTime.Add(3*time.Microsecond)) ||
		!deleted.GetDisabledAt().AsTime().Equal(disabled.GetDisabledAt().AsTime()) ||
		!deleted.GetDeletedAt().AsTime().Equal(baseTime.Add(3*time.Microsecond)) {
		t.Fatalf("deleted lifecycle = %#v", deleted)
	}
}

func TestCatalogRejectsExactNoOpReplacementButAllowsNewPhysicalVersion(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	catalog, err := New(database, assets, Options{IDGenerator: func() (string, error) {
		return "lookup-no-op", nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	definition := catalogDefinition(appIDs[0], "no-op", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false)
	created, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", Definition: definition, Asset: asset,
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	if _, err := catalog.Replace(t.Context(), ReplaceRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: created.GetVersion(), Definition: definition, Asset: asset,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("exact no-op Replace() error = %v", err)
	}
	assertLogicalVersionCount(t, database, 1)
	bypassTx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("BeginTx(no-op bypass): %v", err)
	}
	var currentUpdated int64
	if err := bypassTx.QueryRowContext(t.Context(), `
		SELECT updated_at_unix_micro FROM knowledge_lookup_definitions
		WHERE tenant_id = 'tenant-lookups' AND lookup_id = ?`, created.GetLookupId()).Scan(&currentUpdated); err != nil {
		_ = bypassTx.Rollback()
		t.Fatalf("read no-op bypass timestamp: %v", err)
	}
	if _, err := bypassTx.ExecContext(t.Context(), `
		UPDATE knowledge_lookup_definitions
		SET current_version = 2, updated_at_unix_micro = ?
		WHERE tenant_id = 'tenant-lookups' AND lookup_id = ?`, currentUpdated+1, created.GetLookupId()); err != nil {
		_ = bypassTx.Rollback()
		t.Fatalf("advance no-op bypass registry: %v", err)
	}
	_, bypassErr := bypassTx.ExecContext(t.Context(), `
		INSERT INTO knowledge_lookup_definition_versions (
			tenant_id, lookup_id, definition_version, lookup_asset_id,
			asset_version, asset_size_bytes, asset_content_sha256,
			definition_proto, columns_blob, mutation_kind, state,
			disabled_at_unix_micro, deleted_at_unix_micro,
			created_at_unix_micro
		)
		SELECT tenant_id, lookup_id, 2, lookup_asset_id, asset_version,
			asset_size_bytes, asset_content_sha256, definition_proto,
			columns_blob, 'REPLACE', state, disabled_at_unix_micro,
			deleted_at_unix_micro, ?
		FROM knowledge_lookup_definition_versions
		WHERE tenant_id = 'tenant-lookups' AND lookup_id = ?
		  AND definition_version = 1`, currentUpdated+1, created.GetLookupId())
	_ = bypassTx.Rollback()
	if bypassErr == nil || !strings.Contains(bypassErr.Error(), "not the current registry authority") {
		t.Fatalf("direct exact no-op bypass error = %v", bypassErr)
	}

	replacementAsset := publishCatalogAssetVersion(t, assets, asset, "service_id,owner\napi,alice\n")
	replaced, err := catalog.Replace(t.Context(), ReplaceRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: created.GetVersion(), Definition: definition, Asset: replacementAsset,
	})
	if err != nil || replaced.GetVersion() != 2 ||
		replacementAsset.Ref.ContentSHA256 != asset.Ref.ContentSHA256 {
		t.Fatalf("physical-version Replace() = %#v, %v", replaced, err)
	}
}

func TestCatalogManagementProjectionDoesNotLoadAssetBodies(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	observed := &observingRepository{Repository: assets}
	catalog, err := New(database, observed, Options{IDGenerator: func() (string, error) {
		return "lookup-metadata-only", nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	created, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "metadata-only", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	observed.reset()
	if _, err := catalog.Get(t.Context(), GetRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
	}); err != nil {
		t.Fatalf("Get(): %v", err)
	}
	listed, err := catalog.List(t.Context(), ListRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", Limit: 10,
	})
	if err != nil || len(listed) != 1 || observed.getVersionCalls.Load() != 0 {
		t.Fatalf("List() = %#v, %v; asset loads = %d", listed, err, observed.getVersionCalls.Load())
	}
	if _, err := catalog.GetResolved(t.Context(), GetRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
	}); err != nil || observed.getVersionCalls.Load() != 1 {
		t.Fatalf("GetResolved() error = %v; asset loads = %d", err, observed.getVersionCalls.Load())
	}
}

func TestCatalogListPageUsesOneBoundedJoinedProjection(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	observed := &observingRepository{Repository: assets}
	sequence := atomic.Uint64{}
	catalog, err := New(database, observed, Options{IDGenerator: func() (string, error) {
		return fmt.Sprintf("lookup-page-%03d", sequence.Add(1)), nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	for index := range 75 {
		definition := catalogDefinition(
			appIDs[0],
			fmt.Sprintf("page-%03d", index),
			opensplunkv1.SharingScope_SHARING_SCOPE_APP,
			false,
		)
		description := fmt.Sprintf("bounded page row %03d", index)
		if index == 74 {
			description = "International CAFÉ sentinel"
		}
		definition.Description = &description
		if _, err := catalog.Create(t.Context(), CreateRequest{
			TenantID: "tenant-lookups", OwnerID: "owner-lookups",
			Definition: definition, Asset: asset,
		}); err != nil {
			t.Fatalf("Create(%d): %v", index, err)
		}
	}

	request := ListPageRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", AppID: appIDs[0],
		SortBy:        opensplunkv1.LookupSortBy_LOOKUP_SORT_BY_NAME,
		SortDirection: opensplunkv1.SortDirection_SORT_DIRECTION_ASCENDING,
		Limit:         50, IncludeTotal: true,
	}
	normalized, err := normalizeListPageRequest(request)
	if err != nil {
		t.Fatalf("normalizeListPageRequest(): %v", err)
	}
	query, arguments := lookupListPageSQL(normalized)
	if strings.Count(strings.ToUpper(query), "SELECT ") != 1 ||
		strings.Count(strings.ToUpper(query), "JOIN KNOWLEDGE_LOOKUP_") != 2 ||
		!strings.Contains(strings.ToUpper(query), " LIMIT ?") {
		t.Fatalf("list page is not one bounded joined statement:\n%s", query)
	}
	tx, err := database.SQLDB().BeginTx(t.Context(), &sql.TxOptions{ReadOnly: true})
	if err != nil {
		t.Fatalf("BeginTx(query bound): %v", err)
	}
	rows, err := tx.QueryContext(t.Context(), query, arguments...)
	if err != nil {
		_ = tx.Rollback()
		t.Fatalf("QueryContext(query bound): %v", err)
	}
	loaded := 0
	for rows.Next() {
		if _, _, err := scanJoinedProjection(rows); err != nil {
			_ = rows.Close()
			_ = tx.Rollback()
			t.Fatalf("scanJoinedProjection(): %v", err)
		}
		loaded++
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		_ = tx.Rollback()
		t.Fatalf("iterate bounded query: %v", err)
	}
	if err := rows.Close(); err != nil {
		_ = tx.Rollback()
		t.Fatalf("close bounded query: %v", err)
	}
	if err := tx.Rollback(); err != nil {
		t.Fatalf("rollback bounded query: %v", err)
	}
	if loaded != 51 {
		t.Fatalf("joined projection rows loaded = %d, want page size + 1 = 51", loaded)
	}

	observed.reset()
	first, err := catalog.ListPage(t.Context(), request)
	if err != nil || len(first.Lookups) != 50 || first.NextPosition == nil ||
		first.TotalSize == nil || *first.TotalSize != 75 || observed.getVersionCalls.Load() != 0 {
		t.Fatalf("ListPage(first) = %#v, %v; asset loads = %d", first, err, observed.getVersionCalls.Load())
	}
	request.Position = first.NextPosition
	request.ExpectedSnapshot = &first.Snapshot
	second, err := catalog.ListPage(t.Context(), request)
	if err != nil || len(second.Lookups) != 25 || second.NextPosition != nil ||
		second.Snapshot != first.Snapshot || observed.getVersionCalls.Load() != 0 {
		t.Fatalf("ListPage(second) = %#v, %v; asset loads = %d", second, err, observed.getVersionCalls.Load())
	}
	for index, lookup := range append(first.Lookups, second.Lookups...) {
		if want := fmt.Sprintf("page-%03d", index); lookup.GetDefinition().GetName() != want {
			t.Fatalf("page item %d name = %q, want %q", index, lookup.GetDefinition().GetName(), want)
		}
	}
	filtered := request
	filtered.Position = nil
	filtered.ExpectedSnapshot = nil
	filtered.TextFilter = "café"
	filtered.IncludeTotal = true
	matching, err := catalog.ListPage(t.Context(), filtered)
	if err != nil || len(matching.Lookups) != 1 ||
		matching.Lookups[0].GetDefinition().GetName() != "page-074" ||
		matching.TotalSize == nil || *matching.TotalSize != 1 {
		t.Fatalf("ListPage(unicode description filter) = %#v, %v", matching, err)
	}
	if _, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		LookupID: first.Lookups[0].GetLookupId(), ExpectedVersion: first.Lookups[0].GetVersion(),
		State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	}); err != nil {
		t.Fatalf("SetState(cursor invalidation): %v", err)
	}
	request.ExpectedSnapshot = &first.Snapshot
	if _, err := catalog.ListPage(t.Context(), request); !errors.Is(err, ErrPageInvalidated) {
		t.Fatalf("ListPage(stale snapshot) error = %v, want ErrPageInvalidated", err)
	}
}

func TestCatalogAutomaticResolutionRejectsOverLimitBeforeAssetLoads(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	observed := &observingRepository{Repository: assets}
	sequence := atomic.Uint64{}
	catalog, err := New(database, observed, Options{IDGenerator: func() (string, error) {
		return fmt.Sprintf("lookup-auto-bound-%02d", sequence.Add(1)), nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	created := make([]*opensplunkv1.Lookup, MaximumResolvedLookups+1)
	for index := range created {
		created[index], err = catalog.Create(t.Context(), CreateRequest{
			TenantID: "tenant-lookups", OwnerID: "owner-lookups",
			Definition: catalogDefinition(appIDs[0], fmt.Sprintf("auto-%02d", index), opensplunkv1.SharingScope_SHARING_SCOPE_APP, true),
			Asset:      asset,
		})
		if err != nil {
			t.Fatalf("Create(%d): %v", index, err)
		}
	}
	observed.reset()
	scope := ResolveScope{TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: appIDs[0]}
	if _, err := catalog.ResolveAutomatic(t.Context(), scope); !errors.Is(err, ErrCapacity) || observed.getVersionCalls.Load() != 0 {
		t.Fatalf("ResolveAutomatic(over limit) error = %v; asset loads = %d", err, observed.getVersionCalls.Load())
	}
	if _, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created[0].GetLookupId(),
		ExpectedVersion: created[0].GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	}); err != nil {
		t.Fatalf("disable one automatic lookup: %v", err)
	}
	observed.reset()
	resolved, err := catalog.ResolveAutomatic(t.Context(), scope)
	if err != nil || len(resolved) != MaximumResolvedLookups || observed.getVersionCalls.Load() != 1 {
		t.Fatalf("ResolveAutomatic(bound) len=%d error=%v asset loads=%d", len(resolved), err, observed.getVersionCalls.Load())
	}
}

func TestCatalogResolutionRejectsAggregateCellsBeforeAssetLoads(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, catalogCSVWithShape(6_251, 64))
	if cells := asset.Asset.RowCount() * uint64(len(asset.Asset.Headers())); cells*MaximumResolvedLookups <= MaximumResolvedCells {
		t.Fatalf("aggregate cell fixture = %d, want over %d", cells*MaximumResolvedLookups, MaximumResolvedCells)
	}
	observed := &observingRepository{Repository: assets}
	sequence := atomic.Uint64{}
	catalog, err := New(database, observed, Options{IDGenerator: func() (string, error) {
		return fmt.Sprintf("lookup-cell-bound-%02d", sequence.Add(1)), nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	names := make([]string, MaximumResolvedLookups)
	for index := range names {
		names[index] = fmt.Sprintf("cell-bound-%02d", index)
		if _, err := catalog.Create(t.Context(), CreateRequest{
			TenantID: "tenant-lookups", OwnerID: "owner-lookups",
			Definition: catalogDefinition(appIDs[0], names[index], opensplunkv1.SharingScope_SHARING_SCOPE_APP, true),
			Asset:      asset,
		}); err != nil {
			t.Fatalf("Create(%d): %v", index, err)
		}
	}
	observed.reset()
	scope := ResolveScope{TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: appIDs[0]}
	if _, err := catalog.Resolve(t.Context(), ResolveScope{
		TenantID: scope.TenantID, PrincipalID: scope.PrincipalID, AppID: scope.AppID, Names: names,
	}); !errors.Is(err, ErrCapacity) || observed.getVersionCalls.Load() != 0 {
		t.Fatalf("Resolve(over cell bound) error = %v; asset loads = %d", err, observed.getVersionCalls.Load())
	}
	observed.reset()
	if _, err := catalog.ResolveAutomatic(t.Context(), scope); !errors.Is(err, ErrCapacity) || observed.getVersionCalls.Load() != 0 {
		t.Fatalf("ResolveAutomatic(over cell bound) error = %v; asset loads = %d", err, observed.getVersionCalls.Load())
	}
}

func TestCatalogAdmissionRejectsCombinedCellsBeforeEitherAssetSetLoads(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, catalogCSVWithShape(6_251, 64))
	observed := &observingRepository{Repository: assets}
	sequence := atomic.Uint64{}
	catalog, err := New(database, observed, Options{IDGenerator: func() (string, error) {
		return fmt.Sprintf("lookup-combined-bound-%02d", sequence.Add(1)), nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	explicitNames := make([]string, MaximumResolvedLookups/2)
	for index := range MaximumResolvedLookups {
		automatic := index >= len(explicitNames)
		name := fmt.Sprintf("combined-bound-%02d", index)
		if !automatic {
			explicitNames[index] = name
		}
		if _, err := catalog.Create(t.Context(), CreateRequest{
			TenantID: "tenant-lookups", OwnerID: "owner-lookups",
			Definition: catalogDefinition(appIDs[0], name, opensplunkv1.SharingScope_SHARING_SCOPE_APP, automatic),
			Asset:      asset,
		}); err != nil {
			t.Fatalf("Create(%d): %v", index, err)
		}
	}
	perHalfCells := asset.Asset.RowCount() * uint64(len(asset.Asset.Headers())) * uint64(len(explicitNames))
	if perHalfCells > MaximumResolvedCells || perHalfCells*2 <= MaximumResolvedCells {
		t.Fatalf("combined fixture half=%d combined=%d bound=%d", perHalfCells, perHalfCells*2, MaximumResolvedCells)
	}
	observed.reset()
	resolved, err := catalog.ResolveAdmission(t.Context(), ResolveScope{
		TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: appIDs[0], Names: explicitNames,
	})
	if !errors.Is(err, ErrCapacity) || len(resolved.Explicit) != 0 || len(resolved.Automatic) != 0 ||
		observed.getVersionCalls.Load() != 0 {
		t.Fatalf("ResolveAdmission(over combined cells) = %#v, %v; asset loads = %d", resolved, err, observed.getVersionCalls.Load())
	}
}

func TestCatalogAdmissionReturnsOrderedExplicitAndAutomaticAuthority(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	observed := &observingRepository{Repository: assets}
	sequence := atomic.Uint64{}
	catalog, err := New(database, observed, Options{IDGenerator: func() (string, error) {
		return fmt.Sprintf("lookup-combined-%d", sequence.Add(1)), nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	explicit, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "z-explicit", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(explicit): %v", err)
	}
	automatic, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "a-automatic", opensplunkv1.SharingScope_SHARING_SCOPE_APP, true),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(automatic): %v", err)
	}
	observed.reset()
	resolved, err := catalog.ResolveAdmission(t.Context(), ResolveScope{
		TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: appIDs[0], Names: []string{"z-explicit"},
	})
	if err != nil || len(resolved.Explicit) != 1 || len(resolved.Automatic) != 1 ||
		resolved.Explicit[0].Lookup.GetLookupId() != explicit.GetLookupId() ||
		resolved.Automatic[0].Lookup.GetLookupId() != automatic.GetLookupId() ||
		observed.getVersionCalls.Load() != 1 {
		t.Fatalf("ResolveAdmission() = %#v, %v; asset loads = %d", resolved, err, observed.getVersionCalls.Load())
	}
}

func TestCatalogResolutionSealsLogicalSnapshotBeforeAssetLoad(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	baseCatalog, err := New(database, assets, Options{IDGenerator: func() (string, error) {
		return "lookup-snapshot", nil
	}})
	if err != nil {
		t.Fatalf("New(base): %v", err)
	}
	created, err := baseCatalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "snapshot", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	observed := &observingRepository{
		Repository: assets,
		started:    make(chan struct{}),
		release:    make(chan struct{}),
	}
	observed.block.Store(true)
	resolutionCatalog, err := New(database, observed, Options{})
	if err != nil {
		t.Fatalf("New(resolution): %v", err)
	}
	type outcome struct {
		resolved []Resolved
		err      error
	}
	completed := make(chan outcome, 1)
	go func() {
		resolved, resolveErr := resolutionCatalog.Resolve(t.Context(), ResolveScope{
			TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: appIDs[0], Names: []string{"snapshot"},
		})
		completed <- outcome{resolved: resolved, err: resolveErr}
	}()
	select {
	case <-observed.started:
	case <-time.After(5 * time.Second):
		t.Fatal("resolution did not reach exact immutable asset load")
	}
	if _, err := baseCatalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: created.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	}); err != nil {
		t.Fatalf("disable after resolution snapshot: %v", err)
	}
	close(observed.release)
	result := <-completed
	if result.err != nil || len(result.resolved) != 1 || result.resolved[0].Lookup.GetVersion() != 1 ||
		result.resolved[0].Lookup.GetState() != opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE {
		t.Fatalf("sealed Resolve() = %#v, %v", result.resolved, result.err)
	}
	observed.block.Store(false)
	if _, err := resolutionCatalog.Resolve(t.Context(), ResolveScope{
		TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: appIDs[0], Names: []string{"snapshot"},
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Resolve(after disable) error = %v", err)
	}
}

func TestCatalogRejectsSameStateTransitionsWithoutBurningVersions(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	catalog, err := New(database, assets, Options{IDGenerator: func() (string, error) {
		return "lookup-same-state", nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	created, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "same-state", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	if _, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: created.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("active-to-active SetState() error = %v", err)
	}
	assertLogicalVersionCount(t, database, 1)
	disabled, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: created.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	})
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if _, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: disabled.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("disabled-to-disabled SetState() error = %v", err)
	}
	assertLogicalVersionCount(t, database, 2)
}

func TestCatalogOrdinaryCapacityRetainsTerminalLifecycleReserve(t *testing.T) {
	if MaximumVersions-MaximumOrdinaryVersions != 2*MaximumDefinitions {
		t.Fatalf("terminal reserve = %d, want %d", MaximumVersions-MaximumOrdinaryVersions, 2*MaximumDefinitions)
	}
	database, assets, appIDs := newCatalogHarness(t)
	firstAsset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	secondAsset := publishCatalogAssetVersion(t, assets, firstAsset, "service_id,owner\napi,alice\n")
	catalog, err := New(database, assets, Options{IDGenerator: func() (string, error) {
		return "lookup-version-reserve", nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	definition := catalogDefinition(appIDs[0], "version-reserve", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false)
	created, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", Definition: definition, Asset: firstAsset,
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	seedOrdinaryVersionCapacity(t, database, created, firstAsset, secondAsset)
	assertLogicalVersionCount(t, database, MaximumOrdinaryVersions)

	bypassTx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("BeginTx(bypass): %v", err)
	}
	var currentUpdated int64
	if err := bypassTx.QueryRowContext(t.Context(), `
		SELECT updated_at_unix_micro FROM knowledge_lookup_definitions
		WHERE tenant_id = 'tenant-lookups' AND lookup_id = ?`, created.GetLookupId()).Scan(&currentUpdated); err != nil {
		_ = bypassTx.Rollback()
		t.Fatalf("read current reserve timestamp: %v", err)
	}
	if _, err := bypassTx.ExecContext(t.Context(), `
		UPDATE knowledge_lookup_definitions
		SET current_version = current_version + 1, updated_at_unix_micro = ?
		WHERE tenant_id = 'tenant-lookups' AND lookup_id = ?`, currentUpdated+1, created.GetLookupId()); err != nil {
		_ = bypassTx.Rollback()
		t.Fatalf("advance registry for reserve bypass: %v", err)
	}
	_, bypassErr := bypassTx.ExecContext(t.Context(), `
		INSERT INTO knowledge_lookup_definition_versions (
			tenant_id, lookup_id, definition_version, lookup_asset_id,
			asset_version, asset_size_bytes, asset_content_sha256,
			definition_proto, columns_blob, mutation_kind, state,
			disabled_at_unix_micro, deleted_at_unix_micro,
			created_at_unix_micro
		)
		SELECT tenant_id, lookup_id, definition_version + 1, ?, ?, ?, ?,
			definition_proto, columns_blob, 'REPLACE', 'ACTIVE', NULL, NULL, ?
		FROM knowledge_lookup_definition_versions
		WHERE tenant_id = 'tenant-lookups' AND lookup_id = ?
		  AND definition_version = ?`,
		firstAsset.Ref.LookupAssetID,
		firstAsset.Ref.Version,
		firstAsset.Ref.SizeBytes,
		firstAsset.Ref.ContentSHA256[:],
		currentUpdated+1,
		created.GetLookupId(),
		MaximumOrdinaryVersions,
	)
	_ = bypassTx.Rollback()
	if bypassErr == nil || !strings.Contains(bypassErr.Error(), "ordinary version capacity") {
		t.Fatalf("direct ordinary reserve bypass error = %v", bypassErr)
	}

	changedDefinition := catalogDefinition(appIDs[0], "version-reserve", opensplunkv1.SharingScope_SHARING_SCOPE_APP, true)
	if _, err := catalog.Replace(t.Context(), ReplaceRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: MaximumOrdinaryVersions, Definition: changedDefinition, Asset: secondAsset,
	}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Replace(at ordinary cap) error = %v", err)
	}
	disabled, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: MaximumOrdinaryVersions, State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	})
	if err != nil {
		t.Fatalf("disable from terminal reserve: %v", err)
	}
	if _, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: disabled.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE,
	}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("enable from terminal reserve error = %v", err)
	}
	deleted, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: disabled.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DELETED,
	})
	if err != nil || deleted.GetVersion() != MaximumOrdinaryVersions+2 {
		t.Fatalf("delete from terminal reserve = %#v, %v", deleted, err)
	}
	assertLogicalVersionCount(t, database, MaximumOrdinaryVersions+2)
}

func TestCatalogDefinitionCapacityCountsRetainedTombstones(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	catalog, err := New(database, assets, Options{IDGenerator: func() (string, error) {
		return "lookup-retained-tombstone", nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	created, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "retained-tombstone", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, false),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	disabled, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: created.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	})
	if err != nil {
		t.Fatalf("disable: %v", err)
	}
	if _, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: disabled.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DELETED,
	}); err != nil {
		t.Fatalf("delete: %v", err)
	}
	seedDefinitionIdentityCapacity(t, database, appIDs[0], asset, MaximumDefinitions-1)
	var retained int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*) FROM knowledge_lookup_definitions
		WHERE tenant_id = 'tenant-lookups' AND state = 'DELETED'`).Scan(&retained); err != nil || retained != 1 {
		t.Fatalf("retained tombstones = %d, %v", retained, err)
	}
	if _, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "over-identity-cap", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, false),
		Asset:      asset,
	}); !errors.Is(err, ErrCapacity) {
		t.Fatalf("Create(over retained identity cap) error = %v", err)
	}
}

func TestCatalogMetadataProjectionRejectsCorruptRegistryTimestamp(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	catalog, err := New(database, assets, Options{IDGenerator: func() (string, error) {
		return "lookup-corrupt-projection", nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	created, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "corrupt-projection", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	connection, err := database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("Conn(): %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `DROP TRIGGER knowledge_lookup_definition_transition_is_valid`); err != nil {
		_ = connection.Close()
		t.Fatalf("drop transition trigger in isolated corruption fixture: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `PRAGMA ignore_check_constraints = ON`); err != nil {
		_ = connection.Close()
		t.Fatalf("enable isolated constraint bypass: %v", err)
	}
	if _, err := connection.ExecContext(t.Context(), `
		UPDATE knowledge_lookup_definitions SET updated_at_unix_micro = 0
		WHERE tenant_id = 'tenant-lookups' AND lookup_id = ?`, created.GetLookupId()); err != nil {
		_ = connection.Close()
		t.Fatalf("seed corrupt registry timestamp: %v", err)
	}
	if err := connection.Close(); err != nil {
		t.Fatalf("close corruption fixture connection: %v", err)
	}
	if _, err := catalog.Get(t.Context(), GetRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
	}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Get(corrupt registry timestamp) error = %v", err)
	}
}

func TestCatalogResolutionUsesVisibleWinnerAndAutomaticFlag(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	sequence := atomic.Uint64{}
	catalog, err := New(database, assets, Options{IDGenerator: func() (string, error) {
		return fmt.Sprintf("lookup-%d", sequence.Add(1)), nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	appWinner, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "services", opensplunkv1.SharingScope_SHARING_SCOPE_APP, true),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(app winner): %v", err)
	}
	global, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[1], "services", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, true),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(global fallback): %v", err)
	}
	scope := ResolveScope{TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: appIDs[0]}
	explicit, err := catalog.Resolve(t.Context(), ResolveScope{
		TenantID: scope.TenantID, PrincipalID: scope.PrincipalID, AppID: scope.AppID, Names: []string{"services"},
	})
	if err != nil || len(explicit) != 1 || explicit[0].Lookup.GetLookupId() != appWinner.GetLookupId() {
		t.Fatalf("Resolve() = %#v, %v", explicit, err)
	}
	automatic, err := catalog.ResolveAutomatic(t.Context(), scope)
	if err != nil || len(automatic) != 1 || automatic[0].Lookup.GetLookupId() != appWinner.GetLookupId() {
		t.Fatalf("ResolveAutomatic(app winner) = %#v, %v", automatic, err)
	}

	nonAutomatic := catalogDefinition(appIDs[0], "services", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false)
	replaced, err := catalog.Replace(t.Context(), ReplaceRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: appWinner.GetLookupId(),
		ExpectedVersion: appWinner.GetVersion(), Definition: nonAutomatic, Asset: asset,
	})
	if err != nil {
		t.Fatalf("Replace(nonautomatic winner): %v", err)
	}
	automatic, err = catalog.ResolveAutomatic(t.Context(), scope)
	if err != nil || len(automatic) != 0 {
		t.Fatalf("shadowed global automatic = %#v, %v", automatic, err)
	}

	disabled, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: appWinner.GetLookupId(),
		ExpectedVersion: replaced.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	})
	if err != nil || disabled.GetState() != opensplunkv1.LookupState_LOOKUP_STATE_DISABLED {
		t.Fatalf("SetState(disabled) = %#v, %v", disabled, err)
	}
	automatic, err = catalog.ResolveAutomatic(t.Context(), scope)
	if err != nil || len(automatic) != 1 || automatic[0].Lookup.GetLookupId() != global.GetLookupId() {
		t.Fatalf("ResolveAutomatic(global fallback) = %#v, %v", automatic, err)
	}

	automatic[0].Lookup.Definition.Name = "mutated"
	again, err := catalog.ResolveAutomatic(t.Context(), scope)
	if err != nil || again[0].Lookup.GetDefinition().GetName() != "services" {
		t.Fatalf("automatic result aliases caller memory: %#v, %v", again, err)
	}
}

func TestCatalogNamespaceSupportsPrivateAppGlobalPrecedenceWithoutAmbiguity(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	sequence := atomic.Uint64{}
	catalog, err := New(database, assets, Options{IDGenerator: func() (string, error) {
		return fmt.Sprintf("lookup-shadow-%d", sequence.Add(1)), nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	create := func(owner string, sharing opensplunkv1.SharingScope, appID string) *opensplunkv1.Lookup {
		t.Helper()
		lookup, createErr := catalog.Create(t.Context(), CreateRequest{
			TenantID: "tenant-lookups",
			OwnerID:  owner,
			Definition: catalogDefinition(
				appID,
				"shadowed",
				sharing,
				true,
			),
			Asset: asset,
		})
		if createErr != nil {
			t.Fatalf("Create(%s, %s): %v", owner, sharing, createErr)
		}
		return lookup
	}
	app := create("app-owner", opensplunkv1.SharingScope_SHARING_SCOPE_APP, appIDs[0])
	privateA := create("private-a", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, appIDs[0])
	privateB := create("private-b", opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, appIDs[0])
	global := create("global-owner", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, appIDs[1])

	for _, test := range []struct {
		principal string
		want      string
	}{
		{principal: "private-a", want: privateA.GetLookupId()},
		{principal: "private-b", want: privateB.GetLookupId()},
		{principal: "other-owner", want: app.GetLookupId()},
	} {
		resolved, resolveErr := catalog.Resolve(t.Context(), ResolveScope{
			TenantID:    "tenant-lookups",
			PrincipalID: test.principal,
			AppID:       appIDs[0],
			Names:       []string{"shadowed"},
		})
		if resolveErr != nil || len(resolved) != 1 || resolved[0].Lookup.GetLookupId() != test.want {
			t.Fatalf("Resolve(%s) = %#v, %v", test.principal, resolved, resolveErr)
		}
	}
	resolved, err := catalog.Resolve(t.Context(), ResolveScope{
		TenantID: "tenant-lookups", PrincipalID: "other-owner", AppID: appIDs[1], Names: []string{"shadowed"},
	})
	if err != nil || len(resolved) != 1 || resolved[0].Lookup.GetLookupId() != global.GetLookupId() {
		t.Fatalf("Resolve(global fallback) = %#v, %v", resolved, err)
	}
	if _, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "second-global",
		Definition: catalogDefinition(appIDs[0], "shadowed", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, true),
		Asset:      asset,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("second global Create() error = %v", err)
	}
	disabled, err := catalog.SetState(t.Context(), StateRequest{
		TenantID:        "tenant-lookups",
		OwnerID:         "global-owner",
		LookupID:        global.GetLookupId(),
		ExpectedVersion: global.GetVersion(),
		State:           opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	})
	if err != nil {
		t.Fatalf("disable global: %v", err)
	}
	if _, err := catalog.SetState(t.Context(), StateRequest{
		TenantID:        "tenant-lookups",
		OwnerID:         "global-owner",
		LookupID:        global.GetLookupId(),
		ExpectedVersion: disabled.GetVersion(),
		State:           opensplunkv1.LookupState_LOOKUP_STATE_DELETED,
	}); err != nil {
		t.Fatalf("delete global: %v", err)
	}
	if _, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "second-global",
		Definition: catalogDefinition(appIDs[0], "shadowed", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, true),
		Asset:      asset,
	}); err != nil {
		t.Fatalf("recreate deleted global name: %v", err)
	}
}

func TestCatalogRejectsDuplicateDefinitionKeysAndMigrationPointerCorruption(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	duplicate := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\napi,bob\n")
	catalog, err := New(database, assets, Options{IDGenerator: func() (string, error) { return "lookup-stable", nil }})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	definition := catalogDefinition(appIDs[0], "services", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false)
	if _, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", Definition: definition, Asset: duplicate,
	}); !errors.Is(err, lookupasset.ErrDuplicateKey) {
		t.Fatalf("Create(duplicate key) error = %v", err)
	}

	unique := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	created, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", Definition: definition, Asset: unique,
	})
	if err != nil {
		t.Fatalf("Create(unique): %v", err)
	}
	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("BeginTx(): %v", err)
	}
	_, pointerErr := tx.ExecContext(t.Context(), `
		UPDATE knowledge_lookup_definitions SET current_version = 99
		WHERE tenant_id = ? AND lookup_id = ?`, "tenant-lookups", created.GetLookupId())
	if pointerErr == nil {
		pointerErr = tx.Commit()
	} else {
		_ = tx.Rollback()
	}
	if pointerErr == nil {
		t.Fatal("unpaired logical current-version pointer committed")
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), `
		UPDATE knowledge_lookup_definition_versions SET asset_size_bytes = asset_size_bytes + 1
		WHERE tenant_id = ? AND lookup_id = ? AND definition_version = 1`,
		"tenant-lookups", created.GetLookupId(),
	); err == nil {
		t.Fatal("immutable logical definition version was updated")
	}
}

func TestCatalogResolveUsesThePublishedExactLookupNameLanguage(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	catalog, err := New(database, assets, Options{IDGenerator: func() (string, error) {
		return "lookup-exact-name", nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	const name = "service/catalog+世界"
	created, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], name, opensplunkv1.SharingScope_SHARING_SCOPE_APP, false),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(%q): %v", name, err)
	}
	resolved, err := catalog.Resolve(t.Context(), ResolveScope{
		TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: appIDs[0], Names: []string{name},
	})
	if err != nil || len(resolved) != 1 || resolved[0].Lookup.GetLookupId() != created.GetLookupId() {
		t.Fatalf("Resolve(%q) = %#v, %v", name, resolved, err)
	}
}

func TestCatalogEnforcesLookupAppLifecycleAuthority(t *testing.T) {
	database, assets, appIDs := newCatalogHarness(t)
	asset := publishCatalogAsset(t, assets, "service_id,owner\napi,alice\n")
	catalog, err := New(database, assets, Options{IDGenerator: func() (string, error) {
		return "lookup-app-lifecycle", nil
	}})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	if _, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition("app_AAAAAAAAAAAAAAAAAAAAAA", "missing-app", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false),
		Asset:      asset,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("Create(missing app) error = %v, want conflict", err)
	}
	created, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "app-lifecycle", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, true),
		Asset:      asset,
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}

	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: []byte("0123456789abcdef0123456789abcdef"),
	})
	if err != nil {
		t.Fatalf("NewAppCatalog(): %v", err)
	}
	if _, err := apps.SetAppState(
		t.Context(),
		control.AppAccessScope{TenantID: "tenant-lookups"},
		control.AppSelector{AppID: appIDs[0]},
		1,
		control.AppStateArchived,
	); err == nil || !strings.Contains(err.Error(), "active lookup definitions") {
		t.Fatalf("archive with active lookup error = %v", err)
	}
	disabled, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: created.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	})
	if err != nil {
		t.Fatalf("disable lookup: %v", err)
	}
	archived, err := apps.SetAppState(
		t.Context(),
		control.AppAccessScope{TenantID: "tenant-lookups"},
		control.AppSelector{AppID: appIDs[0]},
		1,
		control.AppStateArchived,
	)
	if err != nil {
		t.Fatalf("archive after disable: %v", err)
	}
	if _, err := catalog.Resolve(t.Context(), ResolveScope{
		TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: appIDs[1], Names: []string{"app-lifecycle"},
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("Resolve(global from archived app) error = %v, want not found", err)
	}
	automatic, err := catalog.ResolveAutomatic(t.Context(), ResolveScope{
		TenantID: "tenant-lookups", PrincipalID: "owner-lookups", AppID: appIDs[1],
	})
	if err != nil || len(automatic) != 0 {
		t.Fatalf("ResolveAutomatic(global from archived app) = %#v, %v", automatic, err)
	}
	if _, err := catalog.Replace(t.Context(), ReplaceRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: disabled.GetVersion(),
		Definition:      catalogDefinition(appIDs[0], "app-lifecycle-replaced", opensplunkv1.SharingScope_SHARING_SCOPE_GLOBAL, true),
		Asset:           asset,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("Replace(in archived app) error = %v, want conflict", err)
	}
	if _, err := catalog.Create(t.Context(), CreateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups",
		Definition: catalogDefinition(appIDs[0], "archived-app-create", opensplunkv1.SharingScope_SHARING_SCOPE_APP, false),
		Asset:      asset,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("Create(in archived app) error = %v, want conflict", err)
	}
	if _, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: disabled.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("reactivate in archived app error = %v, want conflict", err)
	}
	deleted, err := catalog.SetState(t.Context(), StateRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", LookupID: created.GetLookupId(),
		ExpectedVersion: disabled.GetVersion(), State: opensplunkv1.LookupState_LOOKUP_STATE_DELETED,
	})
	if err != nil || deleted.GetState() != opensplunkv1.LookupState_LOOKUP_STATE_DELETED {
		t.Fatalf("delete disabled lookup in archived app = %#v, %v", deleted, err)
	}
	if _, err := apps.DeleteApp(
		t.Context(),
		control.AppAccessScope{TenantID: "tenant-lookups"},
		control.AppSelector{AppID: appIDs[0]},
		archived.Version,
		"lookup-primary",
	); err == nil || !strings.Contains(err.Error(), "referenced by lookup definitions") {
		t.Fatalf("delete app with retained lookup error = %v", err)
	}
}

func newCatalogHarness(t *testing.T) (*control.DB, *lookupasset.Store, []string) {
	t.Helper()
	database, err := control.Open(context.Background(), filepath.Join(t.TempDir(), "control.db"))
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	appIDs := []string{
		fmt.Sprintf("app_%021dA", 1),
		fmt.Sprintf("app_%021dA", 2),
	}
	var appIndex atomic.Uint64
	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: []byte("0123456789abcdef0123456789abcdef"),
		IDGenerator: func() (string, error) {
			index := appIndex.Add(1) - 1
			if index >= uint64(len(appIDs)) {
				return "", errors.New("app ID sequence exhausted")
			}
			return appIDs[index], nil
		},
	})
	if err != nil {
		t.Fatalf("NewAppCatalog(): %v", err)
	}
	for index, slug := range []string{"lookup-primary", "lookup-secondary"} {
		created, createErr := apps.CreateApp(t.Context(), control.AppAccessScope{TenantID: "tenant-lookups"}, control.AppDefinition{
			Slug: slug, DisplayName: fmt.Sprintf("Lookup %d", index+1),
		})
		if createErr != nil || created.ID != appIDs[index] {
			t.Fatalf("CreateApp(%d) = %#v, %v", index, created, createErr)
		}
	}
	var stageSequence, assetSequence atomic.Uint64
	assets, err := lookupasset.NewStore(database, lookupasset.StoreOptions{
		StageIDGenerator: func() (string, error) { return fmt.Sprintf("stage-%d", stageSequence.Add(1)), nil },
		AssetIDGenerator: func() (string, error) { return fmt.Sprintf("asset-%d", assetSequence.Add(1)), nil },
	})
	if err != nil {
		t.Fatalf("lookupasset.NewStore(): %v", err)
	}
	return database, assets, appIDs
}

func publishCatalogAsset(t *testing.T, store *lookupasset.Store, source string) lookupasset.Version {
	t.Helper()
	asset, err := lookupasset.ParseCSV(strings.NewReader(source), lookupasset.Limits{})
	if err != nil {
		t.Fatalf("ParseCSV(): %v", err)
	}
	stage, err := store.StageCSV(t.Context(), lookupasset.StageRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", Asset: asset,
	})
	if err != nil {
		t.Fatalf("StageCSV(): %v", err)
	}
	version, err := store.Publish(t.Context(), lookupasset.PublishRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", StageID: stage.StageID,
	})
	if err != nil {
		t.Fatalf("Publish(): %v", err)
	}
	return version
}

func catalogCSVWithShape(rows, columns int) string {
	if rows < 0 || columns < 2 {
		return ""
	}
	var builder strings.Builder
	builder.Grow(rows * (columns + 8))
	builder.WriteString("service_id,owner")
	for column := 2; column < columns; column++ {
		builder.WriteString(",column_")
		builder.WriteString(strconv.Itoa(column))
	}
	builder.WriteByte('\n')
	for row := range rows {
		builder.WriteString(strconv.Itoa(row))
		for column := 1; column < columns; column++ {
			builder.WriteByte(',')
		}
		builder.WriteByte('\n')
	}
	return builder.String()
}

func publishCatalogAssetVersion(
	t *testing.T,
	store *lookupasset.Store,
	current lookupasset.Version,
	source string,
) lookupasset.Version {
	t.Helper()
	asset, err := lookupasset.ParseCSV(strings.NewReader(source), lookupasset.Limits{})
	if err != nil {
		t.Fatalf("ParseCSV(replacement): %v", err)
	}
	stage, err := store.StageCSV(t.Context(), lookupasset.StageRequest{
		TenantID: "tenant-lookups", OwnerID: "owner-lookups", Asset: asset,
	})
	if err != nil {
		t.Fatalf("StageCSV(replacement): %v", err)
	}
	version, err := store.Publish(t.Context(), lookupasset.PublishRequest{
		TenantID:               "tenant-lookups",
		OwnerID:                "owner-lookups",
		StageID:                stage.StageID,
		LookupAssetID:          current.Ref.LookupAssetID,
		ExpectedCurrentVersion: current.Ref.Version,
	})
	if err != nil {
		t.Fatalf("Publish(replacement): %v", err)
	}
	return version
}

func assertLogicalVersionCount(t *testing.T, database *control.DB, want int) {
	t.Helper()
	var got int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*) FROM knowledge_lookup_definition_versions
		WHERE tenant_id = 'tenant-lookups'`).Scan(&got); err != nil {
		t.Fatalf("count logical lookup versions: %v", err)
	}
	if got != want {
		t.Fatalf("logical lookup version count = %d, want %d", got, want)
	}
}

func seedOrdinaryVersionCapacity(
	t *testing.T,
	database *control.DB,
	created *opensplunkv1.Lookup,
	firstAsset lookupasset.Version,
	secondAsset lookupasset.Version,
) {
	t.Helper()
	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("BeginTx(seed ordinary versions): %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	baseMicro := created.GetUpdatedAt().AsTime().UnixMicro()
	for version := uint64(2); version <= MaximumOrdinaryVersions; version++ {
		asset := firstAsset
		if version%2 == 0 {
			asset = secondAsset
		}
		createdMicro := baseMicro + int64(version-1) // #nosec G115 -- test bound is 4,096.
		result, updateErr := tx.ExecContext(t.Context(), `
			UPDATE knowledge_lookup_definitions
			SET current_version = ?, updated_at_unix_micro = ?
			WHERE tenant_id = 'tenant-lookups' AND lookup_id = ?
			  AND current_version = ?`,
			version,
			createdMicro,
			created.GetLookupId(),
			version-1,
		)
		if updateErr != nil {
			t.Fatalf("advance seeded registry version %d: %v", version, updateErr)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			t.Fatalf("advance seeded registry version %d changed %d rows", version, changed)
		}
		result, insertErr := tx.ExecContext(t.Context(), `
			INSERT INTO knowledge_lookup_definition_versions (
				tenant_id, lookup_id, definition_version, lookup_asset_id,
				asset_version, asset_size_bytes, asset_content_sha256,
				definition_proto, columns_blob, mutation_kind, state,
				disabled_at_unix_micro, deleted_at_unix_micro,
				created_at_unix_micro
			)
			SELECT tenant_id, lookup_id, ?, ?, ?, ?, ?, definition_proto,
				columns_blob, 'REPLACE', 'ACTIVE', NULL, NULL, ?
			FROM knowledge_lookup_definition_versions
			WHERE tenant_id = 'tenant-lookups' AND lookup_id = ?
			  AND definition_version = ?`,
			version,
			asset.Ref.LookupAssetID,
			asset.Ref.Version,
			asset.Ref.SizeBytes,
			asset.Ref.ContentSHA256[:],
			createdMicro,
			created.GetLookupId(),
			version-1,
		)
		if insertErr != nil {
			t.Fatalf("insert seeded logical version %d: %v", version, insertErr)
		}
		if changed, _ := result.RowsAffected(); changed != 1 {
			t.Fatalf("insert seeded logical version %d changed %d rows", version, changed)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seeded ordinary versions: %v", err)
	}
}

func seedDefinitionIdentityCapacity(
	t *testing.T,
	database *control.DB,
	appID string,
	asset lookupasset.Version,
	count int,
) {
	t.Helper()
	columnsBlob, err := encodeColumns(asset.Asset.Headers())
	if err != nil {
		t.Fatalf("encodeColumns(): %v", err)
	}
	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("BeginTx(seed identities): %v", err)
	}
	defer func() { _ = tx.Rollback() }()
	const baseMicro = int64(1_800_000_000_000_000)
	for index := range count {
		lookupID := fmt.Sprintf("lookup-retained-%04d", index)
		name := fmt.Sprintf("retained-%04d", index)
		definition := catalogDefinition(appID, name, opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE, false)
		definitionBytes, marshalErr := deterministicDefinition(definition)
		if marshalErr != nil {
			t.Fatalf("deterministicDefinition(%d): %v", index, marshalErr)
		}
		createdMicro := baseMicro + int64(index)
		if _, insertErr := tx.ExecContext(t.Context(), `
			INSERT INTO knowledge_lookup_definitions (
				tenant_id, lookup_id, owner_id, app_id, name, sharing_scope,
				automatic, current_version, state, created_at_unix_micro,
				updated_at_unix_micro
			) VALUES ('tenant-lookups', ?, 'owner-lookups', ?, ?, 1, 0, 1,
				'ACTIVE', ?, ?)`,
			lookupID,
			appID,
			name,
			createdMicro,
			createdMicro,
		); insertErr != nil {
			t.Fatalf("insert seeded lookup identity %d: %v", index, insertErr)
		}
		if _, insertErr := tx.ExecContext(t.Context(), `
			INSERT INTO knowledge_lookup_definition_versions (
				tenant_id, lookup_id, definition_version, lookup_asset_id,
				asset_version, asset_size_bytes, asset_content_sha256,
				definition_proto, columns_blob, mutation_kind, state,
				disabled_at_unix_micro, deleted_at_unix_micro,
				created_at_unix_micro
			) VALUES ('tenant-lookups', ?, 1, ?, ?, ?, ?, ?, ?, 'CREATE',
				'ACTIVE', NULL, NULL, ?)`,
			lookupID,
			asset.Ref.LookupAssetID,
			asset.Ref.Version,
			asset.Ref.SizeBytes,
			asset.Ref.ContentSHA256[:],
			definitionBytes,
			columnsBlob,
			createdMicro,
		); insertErr != nil {
			t.Fatalf("insert seeded lookup version %d: %v", index, insertErr)
		}
	}
	if err := tx.Commit(); err != nil {
		t.Fatalf("commit seeded identities: %v", err)
	}
}

func catalogDefinition(
	appID string,
	name string,
	sharing opensplunkv1.SharingScope,
	automatic bool,
) *opensplunkv1.LookupDefinition {
	return &opensplunkv1.LookupDefinition{
		AppId: appID, Name: name, SharingScope: sharing, Automatic: automatic,
		KeyMappings:       []*opensplunkv1.LookupFieldMapping{{LookupField: "service_id", EventField: "service"}},
		OutputMappings:    []*opensplunkv1.LookupFieldMapping{{LookupField: "owner", EventField: "owner"}},
		OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
	}
}
