package searchjobs

import (
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/authorization"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
)

func TestAdmissionRefreshDeniesBeforeAnySourceResolution(t *testing.T) {
	var snapshots, resolutions, ids atomic.Int32
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		ResolveAdmissionScope: func(context.Context, AccessScope) (AccessScope, error) {
			return AccessScope{}, authorization.ErrForbidden
		},
		Snapshotter: snapshotterFunc(func(context.Context) (uint64, error) { snapshots.Add(1); return 1, nil }),
		KnowledgeResolver: knowledgeResolverFunc(func(context.Context, knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error) {
			resolutions.Add(1)
			return knowledgecatalog.Resolution{}, nil
		}),
		NewID: func() string { ids.Add(1); return "should-never-exist" },
	})
	request := validRequest()
	request.AppID = "app-one"
	request.OrganizationID = "1"
	if _, err := manager.Create(t.Context(), request); !errors.Is(err, authorization.ErrForbidden) {
		t.Fatalf("nonmember admission: %v", err)
	}
	if snapshots.Load() != 0 || resolutions.Load() != 0 || ids.Load() != 0 {
		t.Fatal("denied admission touched source authority or published identity")
	}
}

func TestAdmissionFreshSiteFlagSealsKnowledgeLookupAndJob(t *testing.T) {
	resolver, app := newEmptyKnowledgeResolver(t)
	var knowledgeSite, lookupSite atomic.Bool
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil }),
		ResolveAdmissionScope: func(_ context.Context, scope AccessScope) (AccessScope, error) {
			scope.SiteAdministrator = true
			return scope, nil
		},
		KnowledgeResolver: knowledgeResolverFunc(func(ctx context.Context, scope knowledgecatalog.ResolutionScope) (knowledgecatalog.Resolution, error) {
			knowledgeSite.Store(scope.SiteAdministrator)
			return resolver.Resolve(ctx, scope)
		}),
		LookupResolver: lookupResolverFunc(func(_ context.Context, scope LookupAdmissionResolutionScope) ([]clickhouse.LookupResolution, error) {
			lookupSite.Store(scope.SiteAdministrator)
			if scope.OrganizationID != 1 {
				t.Fatalf("lookup lost org: %+v", scope)
			}
			return nil, nil
		}),
	})
	request := validRequest()
	request.AppID = app
	request.OrganizationID = "1"
	job, err := manager.Create(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !knowledgeSite.Load() || !lookupSite.Load() || !job.SitePrivilegeUsed {
		t.Fatalf("fresh site authority lost: knowledge=%v lookup=%v job=%+v", knowledgeSite.Load(), lookupSite.Load(), job)
	}
}

func TestCallerCannotForgeSiteAdmissionWithoutRefresh(t *testing.T) {
	manager := newTestManager(t, Config{
		Executor: executorFunc(func(context.Context, clickhouse.CompiledQuery, ResultSink) error { return nil })})
	request := validRequest()
	request.siteAdministrator = true
	job, err := manager.Create(t.Context(), request)
	if err != nil {
		t.Fatal(err)
	}
	if job.SitePrivilegeUsed {
		t.Fatal("request supplied site authority")
	}
}
