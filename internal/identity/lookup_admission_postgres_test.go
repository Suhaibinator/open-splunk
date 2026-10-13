package identity

import (
	"context"
	"errors"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/lookupasset"
	"github.com/Suhaibinator/open-splunk/internal/lookupcatalog"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type nativeAdmissionSnapshotter struct{}

func (nativeAdmissionSnapshotter) VisibilityCutoff(context.Context) (uint64, error) { return 1, nil }

type nativeAdmissionExecutor struct{}

func (nativeAdmissionExecutor) Execute(context.Context, clickhouse.CompiledQuery, searchjobs.ResultSink) error {
	return nil
}

type nativeAdmissionLookupResolver struct {
	catalog *lookupcatalog.Catalog
	calls   atomic.Int32
}

func (r *nativeAdmissionLookupResolver) ResolveLookupAdmission(ctx context.Context, scope searchjobs.LookupAdmissionResolutionScope) (searchjobs.LookupAdmissionResolution, error) {
	r.calls.Add(1)
	resolved, err := r.catalog.ResolveAdmission(ctx, lookupcatalog.ResolveScope{OrganizationID: scope.OrganizationID, SiteAdministrator: scope.SiteAdministrator, TenantID: scope.TenantID, PrincipalID: scope.PrincipalID, AppID: scope.AppID, Names: scope.Names})
	if err != nil {
		return searchjobs.LookupAdmissionResolution{}, err
	}
	result := searchjobs.LookupAdmissionResolution{Explicit: []clickhouse.LookupResolution{}}
	key, _ := plan.ResolveField("service_id", spl.Range{})
	output, _ := plan.ResolveField("service_owner", spl.Range{})
	for _, item := range resolved.Explicit {
		contract := plan.Lookup{DefinitionName: item.Lookup.GetDefinition().GetName(), Keys: []plan.LookupKey{{LookupField: "service_id", EventField: key}}, Outputs: []plan.LookupOutput{{LookupField: "owner", EventField: output}}, WriteMode: plan.LookupWriteModeOverwrite}
		compiled, err := clickhouse.NewLookupResolutionFromVersionWithContract(contract, item.Lookup.GetLookupId(), item.Lookup.GetVersion(), item.Asset)
		if err != nil {
			return searchjobs.LookupAdmissionResolution{}, err
		}
		result.Explicit = append(result.Explicit, compiled)
	}
	return result, nil
}

// This helper extends the existing native runtime fixture, preserving real
// GoCommon membership resolution and scoped PostgreSQL lookup publication.
func runtimeNativeLookupAdmission(t *testing.T, runtime *Runtime, metadata *control.DB, policy *authorization.Service, site, owner, outsider authorization.Subject, org int64) {
	t.Helper()
	const tenant = "physical-tenant"
	apps, err := control.NewAppCatalog(metadata, control.AppCatalogOptions{CursorKey: []byte("native-lookup-admission-cursor-key")})
	if err != nil {
		t.Fatal(err)
	}
	app, err := apps.CreateApp(t.Context(), control.AppAccessScope{TenantID: tenant, OrganizationID: org}, control.AppDefinition{Slug: "native-lookup-admission", DisplayName: "Native lookup admission"})
	if err != nil {
		t.Fatal(err)
	}
	assets, err := lookupasset.NewStore(metadata, lookupasset.StoreOptions{})
	if err != nil {
		t.Fatal(err)
	}
	asset, err := lookupasset.ParseCSV(strings.NewReader("service_id,owner\napi,private-value\n"), lookupasset.DefaultLimits())
	if err != nil {
		t.Fatal(err)
	}
	stage, err := assets.StageCSV(t.Context(), lookupasset.StageRequest{TenantID: tenant, OwnerID: owner.OwnerID(), Asset: asset})
	if err != nil {
		t.Fatal(err)
	}
	version, err := assets.Publish(t.Context(), lookupasset.PublishRequest{TenantID: tenant, OwnerID: owner.OwnerID(), StageID: stage.StageID})
	if err != nil {
		t.Fatal(err)
	}
	lookups, err := lookupcatalog.New(metadata, assets, lookupcatalog.Options{})
	if err != nil {
		t.Fatal(err)
	}
	private, err := lookups.Create(t.Context(), lookupcatalog.CreateRequest{OrganizationID: org, TenantID: tenant, OwnerID: owner.OwnerID(), Asset: version, Definition: &opensplunk.LookupDefinition{AppId: app.ID, Name: "native_private", SharingScope: opensplunk.SharingScope_SHARING_SCOPE_PRIVATE, KeyMappings: []*opensplunk.LookupFieldMapping{{LookupField: "service_id", EventField: "service_id"}}, OutputMappings: []*opensplunk.LookupFieldMapping{{LookupField: "owner", EventField: "service_owner"}}, OverwriteBehavior: opensplunk.KnowledgeOverwriteBehavior_KNOWLEDGE_OVERWRITE_BEHAVIOR_REPLACE_EXISTING}})
	if err != nil {
		t.Fatal(err)
	}
	knowledge, err := knowledgecatalog.New(metadata, knowledgecatalog.Options{CursorKey: []byte("native-knowledge-admission-cursor")})
	if err != nil {
		t.Fatal(err)
	}
	resolver, err := knowledge.NewResolver(knowledgecatalog.ResolverOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lookupResolver := &nativeAdmissionLookupResolver{catalog: lookups}
	var ids atomic.Int32
	manager, err := searchjobs.New(searchjobs.Config{Executor: nativeAdmissionExecutor{}, Snapshotter: nativeAdmissionSnapshotter{}, Compiler: clickhouse.Compiler{Database: "open_splunk", Table: "events"}, KnowledgeResolver: resolver, LookupResolver: lookupResolver, CleanupInterval: -1, NewID: func() string { return "native-lookup-" + strconv.Itoa(int(ids.Add(1))) }, ResolveAdmissionScope: func(ctx context.Context, scope searchjobs.AccessScope) (searchjobs.AccessScope, error) {
		user, err := strconv.ParseUint(scope.OwnerID, 10, 64)
		if err != nil {
			return searchjobs.AccessScope{}, authorization.ErrUnauthorized
		}
		fresh, err := runtime.CurrentSubject(ctx, user)
		if err != nil {
			return searchjobs.AccessScope{}, err
		}
		if err := policy.AuthorizeOrganization(ctx, fresh, org, authorization.ViewerRole); err != nil {
			return searchjobs.AccessScope{}, err
		}
		scope.SiteAdministrator = fresh.IsSiteAdmin()
		return scope, nil
	}})
	if err != nil {
		t.Fatal(err)
	}
	defer manager.Close()
	rangeValue, err := searchtime.NewAbsoluteRange(time.Now().Add(-time.Hour), time.Now())
	if err != nil {
		t.Fatal(err)
	}
	request := searchjobs.CreateRequest{OrganizationID: strconv.FormatInt(org, 10), TenantID: tenant, OwnerID: outsider.OwnerID(), AppID: app.ID, SPL: "index=native-acl | lookup native_private service_id AS service_id OUTPUT owner AS service_owner | table service_owner", AuthorizedIndexes: []string{"native-acl"}, RequestedIndexes: []string{"native-acl"}, TimeRange: rangeValue}
	if _, err := manager.Create(t.Context(), request); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("native nonmember admission: %v", err)
	}
	if ids.Load() != 0 || lookupResolver.calls.Load() != 0 {
		t.Fatal("native nonmember touched private lookup source or allocated job")
	}
	request.OwnerID = owner.OwnerID()
	owned, err := manager.Create(t.Context(), request)
	if err != nil || owned.SitePrivilegeUsed {
		t.Fatalf("native member's own private lookup: %+v %v", owned, err)
	}
	request.OwnerID = site.OwnerID()
	job, err := manager.Create(t.Context(), request)
	if err != nil {
		t.Fatalf("native site private lookup admission: %v", err)
	}
	if !job.SitePrivilegeUsed || len(job.KnowledgeSnapshot.GetLookupAssets()) != 1 || job.KnowledgeSnapshot.GetLookupAssets()[0].GetLookupId() != private.GetLookupId() {
		t.Fatalf("site native source authority lost: %+v", job)
	}
}
