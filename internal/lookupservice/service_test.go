package lookupservice

import (
	"errors"
	"math"
	"path/filepath"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
	"google.golang.org/protobuf/proto"
)

const (
	testTenant = "lookup-tenant"
	testOwner  = "lookup-owner"
	testAppID  = "app_000000000000000000001A"
)

func TestServiceLifecycleBindsImmutableAssetsAndDetaches(t *testing.T) {
	service := newTestService(t)
	scope := Scope{TenantID: testTenant, OwnerID: testOwner}
	definition := testDefinition("services")
	createRequest := &opensplunkv1.CreateLookupRequest{
		Definition: definition,
		CsvData:    []byte("service_id,owner\napi,alice\nweb,bob\n"),
	}
	created, err := service.Create(t.Context(), scope, createRequest)
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	if created.GetLookup().GetVersion() != 1 || created.GetLookup().GetRowCount() != 2 || created.GetLookup().GetState() != opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE {
		t.Fatalf("created lookup = %+v", created.GetLookup())
	}
	lookupID := created.GetLookup().GetLookupId()
	firstDigest := append([]byte(nil), created.GetLookup().GetContentSha256()...)
	initialResolved, err := service.catalog.GetResolved(t.Context(), lookupcatalog.GetRequest{
		TenantID: testTenant,
		OwnerID:  testOwner,
		LookupID: lookupID,
	})
	if err != nil || initialResolved.Asset.Ref.Version != 1 {
		t.Fatalf("initial physical version = %#v, %v", initialResolved.Asset.Ref, err)
	}

	createRequest.Definition.Name = "mutated"
	createRequest.CsvData[0] = 'X'
	created.Lookup.Definition.Name = "response-mutated"
	got, err := service.Get(t.Context(), scope, &opensplunkv1.GetLookupRequest{LookupId: lookupID})
	if err != nil || got.GetLookup().GetDefinition().GetName() != "services" {
		t.Fatalf("detached Get() = %+v, %v", got, err)
	}

	metadata := testDefinition("services")
	description := "metadata-only replacement"
	metadata.Description = &description
	replaced, err := service.Replace(t.Context(), scope, &opensplunkv1.ReplaceLookupRequest{
		LookupId: lookupID, ExpectedVersion: 1, Definition: metadata,
	})
	if err != nil {
		t.Fatalf("metadata Replace(): %v", err)
	}
	if replaced.GetLookup().GetVersion() != 2 || !proto.Equal(replaced.GetLookup().GetDefinition(), metadata) ||
		!bytesEqual(replaced.GetLookup().GetContentSha256(), firstDigest) {
		t.Fatalf("metadata replacement = %+v", replaced.GetLookup())
	}

	replacedCSV, err := service.Replace(t.Context(), scope, &opensplunkv1.ReplaceLookupRequest{
		LookupId: lookupID, ExpectedVersion: 2, Definition: metadata,
		CsvData: []byte("service_id,owner\napi,carol\n"),
	})
	if err != nil {
		t.Fatalf("CSV Replace(): %v", err)
	}
	if replacedCSV.GetLookup().GetVersion() != 3 || replacedCSV.GetLookup().GetRowCount() != 1 ||
		bytesEqual(replacedCSV.GetLookup().GetContentSha256(), firstDigest) {
		t.Fatalf("CSV replacement = %+v", replacedCSV.GetLookup())
	}
	replacedResolved, err := service.catalog.GetResolved(t.Context(), lookupcatalog.GetRequest{
		TenantID: testTenant,
		OwnerID:  testOwner,
		LookupID: lookupID,
	})
	if err != nil || replacedResolved.Asset.Ref.LookupAssetID != initialResolved.Asset.Ref.LookupAssetID ||
		replacedResolved.Asset.Ref.Version != 2 {
		t.Fatalf("replacement physical version = %#v, %v", replacedResolved.Asset.Ref, err)
	}
	historicalVersion := uint64(1)
	historical, err := service.Get(t.Context(), scope, &opensplunkv1.GetLookupRequest{LookupId: lookupID, Version: &historicalVersion})
	if err != nil || historical.GetLookup().GetRowCount() != 2 || !bytesEqual(historical.GetLookup().GetContentSha256(), firstDigest) {
		t.Fatalf("historical asset = %+v, %v", historical, err)
	}
	if _, err := service.Delete(t.Context(), scope, &opensplunkv1.DeleteLookupRequest{
		LookupId: lookupID, ExpectedVersion: 3, ConfirmationName: "services",
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("active deletion = %v", err)
	}

	disabled, err := service.SetState(t.Context(), scope, &opensplunkv1.SetLookupStateRequest{
		LookupId: lookupID, ExpectedVersion: 3, State: opensplunkv1.LookupState_LOOKUP_STATE_DISABLED,
	})
	if err != nil || disabled.GetLookup().GetVersion() != 4 || disabled.GetLookup().GetDisabledAt() == nil {
		t.Fatalf("disable = %+v, %v", disabled, err)
	}
	if _, err := service.SetState(t.Context(), scope, &opensplunkv1.SetLookupStateRequest{
		LookupId: lookupID, ExpectedVersion: 4, State: opensplunkv1.LookupState_LOOKUP_STATE_DELETED,
	}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("state route accepted deletion: %v", err)
	}
	deleted, err := service.Delete(t.Context(), scope, &opensplunkv1.DeleteLookupRequest{
		LookupId: lookupID, ExpectedVersion: 4, ConfirmationName: "services",
	})
	if err != nil || deleted.GetVersion() != 5 {
		t.Fatalf("Delete() = %+v, %v", deleted, err)
	}
	if _, err := service.Replace(t.Context(), scope, &opensplunkv1.ReplaceLookupRequest{
		LookupId: lookupID, ExpectedVersion: 5, Definition: metadata,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("deleted replacement = %v", err)
	}
}

func TestServiceCatalogConflictRollsBackPhysicalPublication(t *testing.T) {
	service, database := newTestServiceWithDatabase(t)
	scope := Scope{TenantID: testTenant, OwnerID: testOwner}
	request := &opensplunkv1.CreateLookupRequest{
		Definition: testDefinition("duplicate"),
		CsvData:    []byte("service_id,owner\na,alice\n"),
	}
	if _, err := service.Create(t.Context(), scope, request); err != nil {
		t.Fatalf("initial Create(): %v", err)
	}
	request.CsvData = []byte("service_id,owner\nb,bob\n")
	if _, err := service.Create(t.Context(), scope, request); !errors.Is(err, ErrConflict) {
		t.Fatalf("conflicting Create() error = %v", err)
	}
	var stages, identities, versions int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT staged_asset_count, asset_identity_count, published_version_count
		FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = ?`,
		testTenant,
	).Scan(&stages, &identities, &versions); err != nil {
		t.Fatalf("read lookup ledger: %v", err)
	}
	if stages != 0 || identities != 1 || versions != 1 {
		t.Fatalf("conflict leaked publication: stages %d identities %d versions %d", stages, identities, versions)
	}
}

func TestServiceDuplicateKeyAuthorityRollsBackPhysicalPublication(t *testing.T) {
	service, database := newTestServiceWithDatabase(t)
	_, err := service.Create(
		t.Context(),
		Scope{TenantID: testTenant, OwnerID: testOwner},
		&opensplunkv1.CreateLookupRequest{
			Definition: testDefinition("duplicate-keys"),
			CsvData:    []byte("service_id,owner\na,alice\na,bob\n"),
		},
	)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Create(duplicate key) error = %v", err)
	}
	var stages, identities, versions, storedBytes int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT staged_asset_count, asset_identity_count,
		       published_version_count, stored_content_bytes
		FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = ?`,
		testTenant,
	).Scan(&stages, &identities, &versions, &storedBytes); err != nil {
		t.Fatalf("read duplicate-key rollback ledger: %v", err)
	}
	if stages != 0 || identities != 0 || versions != 0 || storedBytes != 0 {
		t.Fatalf(
			"duplicate-key publication leaked state: stages %d identities %d versions %d bytes %d",
			stages,
			identities,
			versions,
			storedBytes,
		)
	}
}

func TestServicePreviewAndFilteredCursorPagination(t *testing.T) {
	service := newTestService(t)
	scope := Scope{TenantID: testTenant, OwnerID: testOwner}

	preview, err := service.Preview(t.Context(), scope, &opensplunkv1.PreviewLookupRequest{
		Definition:  testDefinition("preview"),
		CsvData:     []byte("service_id,owner\na,alice\nb,bob\n"),
		MaximumRows: uint32Pointer(1),
	})
	if err != nil || len(preview.GetRows()) != 1 || preview.GetTotalRows() != 2 || !preview.GetTruncated() || len(preview.GetContentSha256()) != 32 {
		t.Fatalf("Preview() = %+v, %v", preview, err)
	}
	invalid, err := service.Preview(t.Context(), scope, &opensplunkv1.PreviewLookupRequest{
		Definition: testDefinition("invalid"),
		CsvData:    []byte("service_id,owner\na,alice\na,bob\n"),
	})
	if err != nil || len(invalid.GetViolations()) != 1 || invalid.GetViolations()[0].GetCode() != "LOOKUP_DUPLICATE_KEY" {
		t.Fatalf("invalid Preview() = %+v, %v", invalid, err)
	}

	for _, name := range []string{"zulu", "alpha", "middle"} {
		_, err := service.Create(t.Context(), scope, &opensplunkv1.CreateLookupRequest{
			Definition: testDefinition(name), CsvData: []byte("service_id,owner\na,alice\n"),
		})
		if err != nil {
			t.Fatalf("create %s: %v", name, err)
		}
	}
	request := &opensplunkv1.ListLookupsRequest{
		Page:  &opensplunkv1.PageRequest{PageSize: uint32Pointer(1), IncludeTotalSize: true},
		AppId: stringPointer(testAppID),
		StateFilters: []opensplunkv1.LookupState{
			opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE,
			opensplunkv1.LookupState_LOOKUP_STATE_ACTIVE,
		},
		SortBy: opensplunkv1.LookupSortBy_LOOKUP_SORT_BY_NAME,
	}
	first, err := service.List(t.Context(), scope, request)
	if err != nil || len(first.GetLookups()) != 1 || first.GetLookups()[0].GetDefinition().GetName() != "alpha" ||
		first.GetPage().GetTotalSize() != 3 || !first.GetPage().GetTotalSizeExact() || first.GetPage().GetNextPageToken() == "" {
		t.Fatalf("first List() = %+v, %v", first, err)
	}
	token := first.GetPage().GetNextPageToken()
	request.Page.PageToken = &token
	second, err := service.List(t.Context(), scope, request)
	if err != nil || len(second.GetLookups()) != 1 || second.GetLookups()[0].GetDefinition().GetName() != "middle" {
		t.Fatalf("second List() = %+v, %v", second, err)
	}
	if _, err := service.Create(t.Context(), scope, &opensplunkv1.CreateLookupRequest{
		Definition: testDefinition("beta"), CsvData: []byte("service_id,owner\na,alice\n"),
	}); err != nil {
		t.Fatalf("create catalog mutation: %v", err)
	}
	if _, err := service.List(t.Context(), scope, request); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cursor replay after catalog mutation = %v", err)
	}
	differentText := "alpha"
	request.TextFilter = &differentText
	if _, err := service.List(t.Context(), scope, request); !errors.Is(err, ErrInvalid) {
		t.Fatalf("cursor replay with changed filter = %v", err)
	}
}

func TestServiceRejectsPartialDependenciesAndCrossOwnerReads(t *testing.T) {
	if _, err := New(Config{}); !errors.Is(err, ErrInvalid) {
		t.Fatalf("New(empty) = %v", err)
	}
	service := newTestService(t)
	created, err := service.Create(t.Context(), Scope{TenantID: testTenant, OwnerID: testOwner}, &opensplunkv1.CreateLookupRequest{
		Definition: testDefinition("private"), CsvData: []byte("service_id,owner\na,alice\n"),
	})
	if err != nil {
		t.Fatalf("Create(): %v", err)
	}
	_, err = service.Get(t.Context(), Scope{TenantID: testTenant, OwnerID: "other-owner"}, &opensplunkv1.GetLookupRequest{LookupId: created.GetLookup().GetLookupId()})
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-owner Get() = %v", err)
	}
}

func TestClassifyPhysicalVersionRaceAsConflict(t *testing.T) {
	if err := classify(lookupasset.ErrConflict); !errors.Is(err, ErrConflict) {
		t.Fatalf("classify(physical conflict) = %v", err)
	}
	if err := classify(lookupcatalog.ErrCapacity); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("classify(logical capacity) = %v", err)
	}
}

func TestServiceGetRejectsVersionsOutsideSQLiteAuthority(t *testing.T) {
	service := newTestService(t)
	version := uint64(math.MaxInt64) + 1
	_, err := service.Get(
		t.Context(),
		Scope{TenantID: testTenant, OwnerID: testOwner},
		&opensplunkv1.GetLookupRequest{LookupId: "lookup-1", Version: &version},
	)
	if !errors.Is(err, ErrInvalid) {
		t.Fatalf("Get(high-bit version) error = %v, want ErrInvalid", err)
	}
}

func newTestService(t *testing.T) *Service {
	t.Helper()
	service, _ := newTestServiceWithDatabase(t)
	return service
}

func newTestServiceWithDatabase(t *testing.T) (*Service, *control.DB) {
	t.Helper()
	database, err := control.Open(t.Context(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("control.Open(): %v", err)
	}
	t.Cleanup(func() { _ = database.Close() })
	if _, err := database.SQLDB().ExecContext(t.Context(), `INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES (?)`, testTenant); err != nil {
		t.Fatalf("provision test tenant: %v", err)
	}
	apps, err := control.NewAppCatalog(database, control.AppCatalogOptions{
		CursorKey: []byte("0123456789abcdef0123456789abcdef"),
		IDGenerator: func() (string, error) {
			return testAppID, nil
		},
	})
	if err != nil {
		t.Fatalf("control.NewAppCatalog(): %v", err)
	}
	if _, err := apps.CreateApp(
		t.Context(),
		control.AppAccessScope{TenantID: testTenant},
		control.AppDefinition{Slug: "lookup-service", DisplayName: "Lookup Service"},
	); err != nil {
		t.Fatalf("CreateApp(): %v", err)
	}
	assets, err := lookupasset.NewStore(database, lookupasset.StoreOptions{})
	if err != nil {
		t.Fatalf("lookupasset.NewStore(): %v", err)
	}
	catalog, err := lookupcatalog.New(database, assets, lookupcatalog.Options{})
	if err != nil {
		t.Fatalf("lookupcatalog.New(): %v", err)
	}
	service, err := New(Config{Assets: assets, Catalog: catalog, CursorKey: make([]byte, 32)})
	if err != nil {
		t.Fatalf("New(): %v", err)
	}
	return service, database
}

func testDefinition(name string) *opensplunkv1.LookupDefinition {
	return &opensplunkv1.LookupDefinition{
		AppId: testAppID, Name: name, SharingScope: opensplunkv1.SharingScope_SHARING_SCOPE_APP,
		KeyMappings:       []*opensplunkv1.LookupFieldMapping{{LookupField: "service_id", EventField: "service_id"}},
		OutputMappings:    []*opensplunkv1.LookupFieldMapping{{LookupField: "owner", EventField: "service_owner"}},
		OverwriteBehavior: opensplunkv1.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
	}
}

func uint32Pointer(value uint32) *uint32 { return &value }
func stringPointer(value string) *string { return &value }

func bytesEqual(left, right []byte) bool {
	return string(left) == string(right)
}
