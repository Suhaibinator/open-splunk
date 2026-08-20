package knowledgepreview

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

type previewLookupResolverFunc func(
	context.Context,
	searchjobs.LookupAdmissionResolutionScope,
) (searchjobs.LookupAdmissionResolution, error)

func (resolve previewLookupResolverFunc) ResolveLookupAdmission(
	ctx context.Context,
	scope searchjobs.LookupAdmissionResolutionScope,
) (searchjobs.LookupAdmissionResolution, error) {
	return resolve(ctx, scope)
}

func TestProductionPreviewAdapterRestoresRetainedLookupAuthority(t *testing.T) {
	resolution := previewLookupResolution(t)
	resolver := previewLookupResolverFunc(func(
		ctx context.Context,
		scope searchjobs.LookupAdmissionResolutionScope,
	) (searchjobs.LookupAdmissionResolution, error) {
		if err := ctx.Err(); err != nil {
			return searchjobs.LookupAdmissionResolution{}, err
		}
		if scope.TenantID != previewTestTenant || scope.AppID != previewTestApp ||
			len(scope.Names) != 1 || scope.Names[0] != "service_catalog" {
			return searchjobs.LookupAdmissionResolution{}, errors.New(
				"unexpected preview lookup scope",
			)
		}
		return searchjobs.LookupAdmissionResolution{
			Explicit: []clickhouse.LookupResolution{resolution},
		}, nil
	})
	fixture := newPreviewFixtureForSearch(
		t,
		`index=main | lookup service_catalog service_id AS service OUTPUT owner AS service_owner | table status`,
		resolver,
	)
	execution, err := fixture.manager.CompletedExecutionSnapshotFor(
		context.Background(),
		fixture.access,
		previewTestJob,
	)
	if err != nil {
		t.Fatalf("CompletedExecutionSnapshotFor(): %v", err)
	}
	retained, err := execution.OpenRetainedKnowledgeExecution()
	if err != nil || retained == nil || !retained.CompiledQuery.HasLookupAuthority() {
		t.Fatalf("retained lookup execution = (%#v, %v)", retained, err)
	}

	compiled, err := (ProductionCompilerAdapter{Compiler: clickhouse.Compiler{
		Database: "open_splunk",
		Table:    "events",
	}}).CompilePreview(
		context.Background(),
		execution,
		retained.KnowledgePrelude,
	)
	if err != nil || !compiled.HasValidExecutionSeal() {
		t.Fatalf("CompilePreview(retained lookup) = seal %v, err %v", compiled.HasValidExecutionSeal(), err)
	}
	versions, ok := compiled.LookupAssetVersions()
	if !ok || len(versions) != 1 ||
		versions[0].LookupID() != "lookup-service-catalog" ||
		versions[0].AssetID() != "asset-service-catalog" {
		t.Fatalf("preview lookup provenance = %#v, %v", versions, ok)
	}
}

func previewLookupResolution(t *testing.T) clickhouse.LookupResolution {
	t.Helper()
	key, err := plan.ResolveField("service", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(service): %v", err)
	}
	output, err := plan.ResolveField("service_owner", spl.Range{})
	if err != nil {
		t.Fatalf("ResolveField(service_owner): %v", err)
	}
	contract := plan.Lookup{
		DefinitionName: "service_catalog",
		Keys: []plan.LookupKey{{
			LookupField: "service_id",
			EventField:  key,
		}},
		Outputs: []plan.LookupOutput{{
			LookupField: "owner",
			EventField:  output,
		}},
		WriteMode: plan.LookupWriteModeOverwrite,
	}
	resolution, err := clickhouse.NewLookupResolutionWithContract(
		contract,
		"lookup-service-catalog",
		1,
		previewTestTenant,
		"asset-service-catalog",
		1,
		uint64(len("service_id,owner\napi,platform\n")),
		sha256.Sum256([]byte("retained-preview-lookup")),
		[]string{"service_id", "owner"},
		[][]string{{"api", "platform"}},
	)
	if err != nil {
		t.Fatalf("NewLookupResolutionWithContract(): %v", err)
	}
	return resolution
}
