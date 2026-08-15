package server

import (
	"context"
	"fmt"
	"math"
	"net/http"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
)

func knowledgeGraphHTTPAuthority(
	objectID string,
	version uint64,
) knowledgecatalog.CurrentRegistryAuthority {
	return knowledgecatalog.CurrentRegistryAuthority{
		TenantID:          knowledgeBoundaryTenantID,
		KnowledgeObjectID: objectID,
		CurrentVersion:    version,
		AppID:             knowledgeHTTPAppID,
		OwnerID:           knowledgeBoundaryOwnerID,
		ObjectType:        knowledgecatalog.ObjectTypeFieldAlias,
		SharingScope:      knowledgecatalog.SharingScopePrivate,
		State:             knowledgecatalog.StateDraft,
	}
}

func knowledgeGraphHTTPScopes() knowledgeScopes {
	apps := []string{knowledgeHTTPAppID}
	return knowledgeScopes{
		read: knowledgecatalog.ReadScope{
			TenantID:       knowledgeBoundaryTenantID,
			OwnerID:        knowledgeBoundaryOwnerID,
			ReadableAppIDs: append([]string(nil), apps...),
		},
		apps: apps,
	}
}

func knowledgeGraphConverterFixture(
	direction knowledgeGraphDirection,
) (knowledgecatalog.DependencyListRequest, knowledgecatalog.DependencyPage) {
	const (
		rootID  = "ko-http-root-1"
		otherID = "ko-http-other-1"
	)
	rootCurrent := knowledgeGraphHTTPAuthority(rootID, 3)
	otherCurrent := knowledgeGraphHTTPAuthority(otherID, 4)
	edge := knowledgecatalog.DependencyEdge{
		Role: knowledgecatalog.DependencyRoleFieldInput,
	}
	switch direction {
	case knowledgeGraphDependencies:
		edge.Source = knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: rootID, Version: 3}
		edge.Target = knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: otherID, Version: 2}
		edge.SourceCurrent = rootCurrent
		edge.TargetCurrent = otherCurrent
	case knowledgeGraphDependents:
		edge.Source = knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: otherID, Version: 4}
		edge.Target = knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: rootID, Version: 3}
		edge.SourceCurrent = otherCurrent
		edge.TargetCurrent = rootCurrent
	}
	return knowledgecatalog.DependencyListRequest{
			KnowledgeObjectID: rootID,
			PageSize:          2,
		}, knowledgecatalog.DependencyPage{
			Edges:           []knowledgecatalog.DependencyEdge{edge},
			ResolvedObject:  knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: rootID, Version: 3},
			ResolvedCurrent: rootCurrent,
			CatalogRevision: 10,
		}
}

func TestKnowledgeGraphHTTPServesHistoricalDependenciesAndCurrentDependents(
	t *testing.T,
) {
	t.Parallel()

	const (
		rootID   = "ko-http-root-1"
		targetID = "ko-http-target-1"
		sourceID = "ko-http-source-1"
	)
	rootCurrent := knowledgeGraphHTTPAuthority(rootID, 3)
	targetCurrent := knowledgeGraphHTTPAuthority(targetID, 4)
	targetCurrent.SharingScope = knowledgecatalog.SharingScopeGlobal
	targetCurrent.AppID = knowledgeHTTPOtherAppID
	sourceCurrent := knowledgeGraphHTTPAuthority(sourceID, 5)
	sourceCurrent.State = knowledgecatalog.StateDeleted
	dependenciesTotal := uint64(2)
	dependentsTotal := uint64(2)
	catalog := &knowledgeHTTPCatalog{
		dependenciesFn: func(
			_ context.Context,
			scope knowledgecatalog.ReadScope,
			request knowledgecatalog.DependencyListRequest,
		) (knowledgecatalog.DependencyPage, error) {
			if scope.TenantID != knowledgeBoundaryTenantID ||
				request.KnowledgeObjectID != rootID || request.Version == nil ||
				*request.Version != 2 || request.PageSize != 1 ||
				request.PageToken != "" || !request.IncludeTotal {
				t.Fatalf("dependencies scope=%+v request=%+v", scope, request)
			}
			return knowledgecatalog.DependencyPage{
				Edges: []knowledgecatalog.DependencyEdge{{
					Source:        knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: rootID, Version: 2},
					Target:        knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: targetID, Version: 2},
					Role:          knowledgecatalog.DependencyRoleFieldInput,
					SourceCurrent: rootCurrent,
					TargetCurrent: targetCurrent,
				}},
				ResolvedObject:  knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: rootID, Version: 2},
				ResolvedCurrent: rootCurrent,
				NextPageToken:   "short-page-next",
				TotalSize:       &dependenciesTotal,
				TotalSizeExact:  true,
				CatalogRevision: 10,
			}, nil
		},
		dependentsFn: func(
			_ context.Context,
			scope knowledgecatalog.ReadScope,
			request knowledgecatalog.DependencyListRequest,
		) (knowledgecatalog.DependencyPage, error) {
			if scope.TenantID != knowledgeBoundaryTenantID ||
				request.KnowledgeObjectID != rootID || request.Version == nil ||
				*request.Version != 2 || request.PageSize != 2 ||
				request.PageToken != "" || !request.IncludeTotal {
				t.Fatalf("dependents scope=%+v request=%+v", scope, request)
			}
			return knowledgecatalog.DependencyPage{
				Edges: []knowledgecatalog.DependencyEdge{{
					Source:        knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: sourceID, Version: 5},
					Target:        knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: rootID, Version: 2},
					Role:          knowledgecatalog.DependencyRoleFieldInput,
					SourceCurrent: sourceCurrent,
					TargetCurrent: rootCurrent,
				}},
				ResolvedObject:  knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: rootID, Version: 2},
				ResolvedCurrent: rootCurrent,
				NextPageToken:   "short-page-next",
				TotalSize:       &dependentsTotal,
				TotalSizeExact:  true,
				CatalogRevision: 10,
			}, nil
		},
	}
	appender := &knowledgeBoundaryAppender{}
	_, handler := newKnowledgeHTTPHandler(
		t,
		auth.BrowserRoleAdministrator,
		catalog,
		&knowledgeHTTPWriter{},
		knowledgeHTTPApps(),
		appender,
	)
	dependenciesPage := &opensplunkv1.PageRequest{
		PageSize:         new(uint32(1)),
		IncludeTotalSize: true,
	}
	dependenciesResponse := knowledgeHTTPPost(
		t,
		handler,
		knowledgeObjectsDependenciesPath,
		&opensplunkv1.ListKnowledgeObjectDependenciesRequest{
			KnowledgeObjectId: rootID,
			Version:           new(uint64(2)),
			Page:              proto.Clone(dependenciesPage).(*opensplunkv1.PageRequest),
		},
	)
	if dependenciesResponse.Code != http.StatusOK {
		t.Fatalf("dependencies status=%d body=%q", dependenciesResponse.Code, dependenciesResponse.Body.String())
	}
	dependencies := &opensplunkv1.ListKnowledgeObjectDependenciesResponse{}
	unmarshalResponse(t, dependenciesResponse, dependencies)
	if len(dependencies.GetDependencies()) != 1 ||
		dependencies.GetDependencies()[0].GetSource().GetVersion() != 2 ||
		dependencies.GetDependencies()[0].GetTarget().GetKnowledgeObjectId() != targetID ||
		dependencies.GetResolvedObject().GetVersion() != 2 ||
		dependencies.GetPage().GetNextPageToken() != "short-page-next" ||
		dependencies.GetPage().GetTotalSize() != 2 ||
		!dependencies.GetPage().GetTotalSizeExact() ||
		dependencies.GetTenantCatalogRevision() != 10 {
		t.Fatalf("dependencies response=%+v", dependencies)
	}

	dependentsResponse := knowledgeHTTPPost(
		t,
		handler,
		knowledgeObjectsDependentsPath,
		&opensplunkv1.ListKnowledgeObjectDependentsRequest{
			KnowledgeObjectId: rootID,
			Version:           new(uint64(2)),
			Page: &opensplunkv1.PageRequest{
				PageSize:         new(uint32(2)),
				IncludeTotalSize: true,
			},
		},
	)
	if dependentsResponse.Code != http.StatusOK {
		t.Fatalf("dependents status=%d body=%q", dependentsResponse.Code, dependentsResponse.Body.String())
	}
	dependents := &opensplunkv1.ListKnowledgeObjectDependentsResponse{}
	unmarshalResponse(t, dependentsResponse, dependents)
	if len(dependents.GetDependents()) != 1 ||
		dependents.GetDependents()[0].GetSource().GetKnowledgeObjectId() != sourceID ||
		dependents.GetDependents()[0].GetSource().GetVersion() != 5 ||
		dependents.GetDependents()[0].GetTarget().GetVersion() != 2 ||
		dependents.GetPage().GetNextPageToken() != "short-page-next" ||
		dependents.GetPage().GetTotalSize() != 2 ||
		dependents.GetResolvedObject().GetKnowledgeObjectId() != rootID {
		t.Fatalf("dependents response=%+v", dependents)
	}
	if calls := appender.snapshot(); len(calls) != 0 {
		t.Fatalf("successful graph attempts journaled=%+v", calls)
	}
}

func TestKnowledgeGraphConverterRejectsUntrustedPageAuthorityAndShape(
	t *testing.T,
) {
	t.Parallel()

	request, base := knowledgeGraphConverterFixture(knowledgeGraphDependencies)
	tests := []struct {
		name   string
		mutate func(*knowledgecatalog.DependencyPage)
	}{
		{name: "wrong tenant global root", mutate: func(page *knowledgecatalog.DependencyPage) {
			page.ResolvedCurrent.TenantID = "tenant-other"
			page.ResolvedCurrent.SharingScope = knowledgecatalog.SharingScopeGlobal
			page.Edges[0].SourceCurrent = page.ResolvedCurrent
		}},
		{name: "quarantined root", mutate: func(page *knowledgecatalog.DependencyPage) {
			page.ResolvedCurrent.State = knowledgecatalog.StateQuarantined
			page.Edges[0].SourceCurrent = page.ResolvedCurrent
		}},
		{name: "absent version does not resolve current", mutate: func(page *knowledgecatalog.DependencyPage) {
			page.ResolvedObject.Version = 2
			page.Edges[0].Source = page.ResolvedObject
		}},
		{name: "hidden target", mutate: func(page *knowledgecatalog.DependencyPage) {
			page.Edges[0].TargetCurrent.OwnerID = "owner-other"
		}},
		{name: "unsupported role", mutate: func(page *knowledgecatalog.DependencyPage) {
			page.Edges[0].Role = opensplunkv1.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_UNSPECIFIED
		}},
		{name: "exact self edge", mutate: func(page *knowledgecatalog.DependencyPage) {
			page.Edges[0].Target = page.Edges[0].Source
			page.Edges[0].TargetCurrent = page.Edges[0].SourceCurrent
		}},
		{name: "duplicate edge", mutate: func(page *knowledgecatalog.DependencyPage) {
			page.Edges = append(page.Edges, page.Edges[0])
		}},
		{name: "empty continuation", mutate: func(page *knowledgecatalog.DependencyPage) {
			page.Edges = nil
		}},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			page := base
			page.Edges = append([]knowledgecatalog.DependencyEdge(nil), base.Edges...)
			graphRequest := request
			if test.name == "empty continuation" {
				graphRequest.PageToken = "prior"
			}
			test.mutate(&page)
			if _, _, err := knowledgeGraphPageToProto(
				knowledgeGraphHTTPScopes(),
				graphRequest,
				page,
				knowledgeGraphDependencies,
			); err == nil {
				t.Fatal("invalid graph page accepted")
			}
		})
	}
}

func TestKnowledgeGraphConverterRejectsDirectionVersionAndPagingViolations(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name      string
		direction knowledgeGraphDirection
		mutate    func(*knowledgecatalog.DependencyListRequest, *knowledgecatalog.DependencyPage)
	}{
		{
			name: "dependent source is not current", direction: knowledgeGraphDependents,
			mutate: func(_ *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				page.Edges[0].Source.Version--
			},
		},
		{
			name: "dependent repeats source at another current version", direction: knowledgeGraphDependents,
			mutate: func(_ *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				repeated := page.Edges[0]
				repeated.Source.Version++
				repeated.SourceCurrent.CurrentVersion++
				page.Edges = append(page.Edges, repeated)
			},
		},
		{
			name: "edge belongs to the other direction", direction: knowledgeGraphDependents,
			mutate: func(request *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				dependencyRequest, dependencyPage := knowledgeGraphConverterFixture(knowledgeGraphDependencies)
				*request = dependencyRequest
				*page = dependencyPage
			},
		},
		{
			name: "edge fixed endpoint is not the resolved root", direction: knowledgeGraphDependencies,
			mutate: func(_ *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				page.Edges[0].Source.KnowledgeObjectID = "ko-http-wrong-root-1"
				page.Edges[0].SourceCurrent.KnowledgeObjectID = "ko-http-wrong-root-1"
			},
		},
		{
			name: "opposite current version exceeds revision", direction: knowledgeGraphDependencies,
			mutate: func(_ *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				page.Edges[0].TargetCurrent.CurrentVersion = page.CatalogRevision + 1
			},
		},
		{
			name: "opposite exact version exceeds revision", direction: knowledgeGraphDependencies,
			mutate: func(_ *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				page.Edges[0].Target.Version = page.CatalogRevision + 1
				page.Edges[0].TargetCurrent.CurrentVersion = page.CatalogRevision + 1
			},
		},
		{
			name: "resolved current and exact versions exceed revision", direction: knowledgeGraphDependencies,
			mutate: func(_ *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				page.ResolvedCurrent.CurrentVersion = page.CatalogRevision + 1
				page.ResolvedObject.Version = page.CatalogRevision + 1
				page.Edges[0].Source = page.ResolvedObject
				page.Edges[0].SourceCurrent = page.ResolvedCurrent
			},
		},
		{
			name: "unexpected total", direction: knowledgeGraphDependencies,
			mutate: func(_ *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				value := uint64(1)
				page.TotalSize = &value
				page.TotalSizeExact = true
			},
		},
		{
			name: "requested total absent", direction: knowledgeGraphDependencies,
			mutate: func(request *knowledgecatalog.DependencyListRequest, _ *knowledgecatalog.DependencyPage) {
				request.IncludeTotal = true
			},
		},
		{
			name: "requested total not exact", direction: knowledgeGraphDependencies,
			mutate: func(request *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				request.IncludeTotal = true
				value := uint64(1)
				page.TotalSize = &value
			},
		},
		{
			name: "continued total below cursor floor", direction: knowledgeGraphDependencies,
			mutate: func(request *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				request.IncludeTotal = true
				request.PageToken = "prior"
				page.NextPageToken = "next"
				value := uint64(2)
				page.TotalSize = &value
				page.TotalSizeExact = true
			},
		},
		{
			name: "terminal first total differs from rows", direction: knowledgeGraphDependencies,
			mutate: func(request *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				request.IncludeTotal = true
				value := uint64(2)
				page.TotalSize = &value
				page.TotalSizeExact = true
			},
		},
		{
			name: "outgoing total exceeds sealed edge cap", direction: knowledgeGraphDependencies,
			mutate: func(request *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				request.IncludeTotal = true
				page.NextPageToken = "next"
				value := uint64(knowledgecatalog.MaximumDependencyEdgesPerVersion + 1)
				page.TotalSize = &value
				page.TotalSizeExact = true
			},
		},
		{
			name: "incoming total exceeds identity cap", direction: knowledgeGraphDependents,
			mutate: func(request *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				request.IncludeTotal = true
				page.NextPageToken = "next"
				value := uint64(knowledgecatalog.MaximumObjectsPerTenant + 1)
				page.TotalSize = &value
				page.TotalSizeExact = true
			},
		},
		{
			name: "next token on empty page", direction: knowledgeGraphDependencies,
			mutate: func(_ *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				page.Edges = nil
				page.NextPageToken = "next"
			},
		},
		{
			name: "short outgoing page has next token", direction: knowledgeGraphDependencies,
			mutate: func(_ *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				page.NextPageToken = "next"
			},
		},
		{
			name: "next token repeats request token", direction: knowledgeGraphDependencies,
			mutate: func(request *knowledgecatalog.DependencyListRequest, page *knowledgecatalog.DependencyPage) {
				request.PageToken = "same"
				page.NextPageToken = "same"
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			request, page := knowledgeGraphConverterFixture(test.direction)
			page.Edges = append([]knowledgecatalog.DependencyEdge(nil), page.Edges...)
			test.mutate(&request, &page)
			if _, _, err := knowledgeGraphPageToProto(
				knowledgeGraphHTTPScopes(),
				request,
				page,
				test.direction,
			); err == nil {
				t.Fatal("invalid graph page accepted")
			}
		})
	}
}

func TestKnowledgeGraphConverterPinsMaximumResponseBelowTransportCap(
	t *testing.T,
) {
	t.Parallel()

	rootID := strings.Repeat("r", maximumKnowledgeObjectIDBytes)
	maximumVersion := uint64(math.MaxInt64)
	rootCurrent := knowledgeGraphHTTPAuthority(rootID, maximumVersion)
	rootCurrent.SharingScope = knowledgecatalog.SharingScopeGlobal
	edges := make([]knowledgecatalog.DependencyEdge, knowledgecatalog.MaximumPageSize)
	for index := range edges {
		targetID := strings.Repeat("t", maximumKnowledgeObjectIDBytes-5) +
			fmt.Sprintf("%05d", index)
		targetCurrent := knowledgeGraphHTTPAuthority(targetID, maximumVersion)
		targetCurrent.SharingScope = knowledgecatalog.SharingScopeGlobal
		edges[index] = knowledgecatalog.DependencyEdge{
			Source:        knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: rootID, Version: maximumVersion},
			Target:        knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: targetID, Version: maximumVersion},
			Role:          knowledgecatalog.DependencyRoleFieldInput,
			SourceCurrent: rootCurrent,
			TargetCurrent: targetCurrent,
		}
	}
	total := uint64(len(edges))
	converted, _, err := knowledgeGraphPageToProto(
		knowledgeGraphHTTPScopes(),
		knowledgecatalog.DependencyListRequest{
			KnowledgeObjectID: rootID,
			PageSize:          knowledgecatalog.MaximumPageSize,
			IncludeTotal:      true,
		},
		knowledgecatalog.DependencyPage{
			Edges:           edges,
			ResolvedObject:  knowledgecatalog.ObjectVersionIdentity{KnowledgeObjectID: rootID, Version: maximumVersion},
			ResolvedCurrent: rootCurrent,
			TotalSize:       &total,
			TotalSizeExact:  true,
			CatalogRevision: maximumVersion,
		},
		knowledgeGraphDependencies,
	)
	if err != nil {
		t.Fatalf("maximum graph page: %v", err)
	}
	response := &opensplunkv1.ListKnowledgeObjectDependenciesResponse{
		Dependencies:          converted.edges,
		Page:                  converted.page,
		TenantCatalogRevision: converted.revision,
		ResolvedObject:        converted.resolvedObject,
	}
	if size := proto.Size(response); size > maximumKnowledgeGraphResponseBytes || size < 70<<10 {
		t.Fatalf("maximum graph wire size=%d cap=%d", size, maximumKnowledgeGraphResponseBytes)
	}
}

func TestKnowledgeGraphHTTPPreflightsKnownFieldsBeforeScopeAndCatalog(
	t *testing.T,
) {
	t.Parallel()

	tests := []struct {
		name       string
		path       string
		request    proto.Message
		wantAction knowledgeattemptaudit.Action
	}{
		{
			name: "missing root", path: knowledgeObjectsDependenciesPath,
			request:    &opensplunkv1.ListKnowledgeObjectDependenciesRequest{},
			wantAction: knowledgeattemptaudit.ActionDependencies,
		},
		{
			name: "zero version", path: knowledgeObjectsDependenciesPath,
			request: &opensplunkv1.ListKnowledgeObjectDependenciesRequest{
				KnowledgeObjectId: "ko-http-root-1",
				Version:           new(uint64(0)),
			},
			wantAction: knowledgeattemptaudit.ActionDependencies,
		},
		{
			name: "page size", path: knowledgeObjectsDependentsPath,
			request: &opensplunkv1.ListKnowledgeObjectDependentsRequest{
				KnowledgeObjectId: "ko-http-root-1",
				Page: &opensplunkv1.PageRequest{
					PageSize: new(uint32(knowledgecatalog.MaximumPageSize + 1)),
				},
			},
			wantAction: knowledgeattemptaudit.ActionDependents,
		},
		{
			name: "control token", path: knowledgeObjectsDependenciesPath,
			request: &opensplunkv1.ListKnowledgeObjectDependenciesRequest{
				KnowledgeObjectId: "ko-http-root-1",
				Page: &opensplunkv1.PageRequest{
					PageToken: new("cursor\nsecret"),
				},
			},
			wantAction: knowledgeattemptaudit.ActionDependencies,
		},
		{
			name: "oversized token", path: knowledgeObjectsDependentsPath,
			request: &opensplunkv1.ListKnowledgeObjectDependentsRequest{
				KnowledgeObjectId: "ko-http-root-1",
				Page: &opensplunkv1.PageRequest{
					PageToken: new(strings.Repeat("x", maximumKnowledgePageTokenBytes+1)),
				},
			},
			wantAction: knowledgeattemptaudit.ActionDependents,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			catalog := &knowledgeHTTPCatalog{}
			apps := knowledgeHTTPApps()
			appender := &knowledgeBoundaryAppender{}
			_, handler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				catalog,
				&knowledgeHTTPWriter{},
				apps,
				appender,
			)
			response := knowledgeHTTPPost(t, handler, test.path, test.request)
			dependencies, dependents := catalog.graphCalls()
			attempts := appender.snapshot()
			if response.Code != http.StatusBadRequest || apps.callCount() != 0 ||
				dependencies != 0 || dependents != 0 || len(attempts) != 1 ||
				attempts[0].definition.Action != test.wantAction ||
				attempts[0].definition.Reason != knowledgeattemptaudit.ReasonInvalidDefinition ||
				attempts[0].definition.AuthorizedContext != nil {
				t.Fatalf("status=%d apps=%d graph=%d/%d attempts=%+v", response.Code, apps.callCount(), dependencies, dependents, attempts)
			}
		})
	}
}

func TestKnowledgeGraphHTTPErrorsBindOnlyAnAuthorizedRequestedRoot(
	t *testing.T,
) {
	t.Parallel()

	const rootID = "ko-http-root-1"
	root := knowledgeGraphHTTPAuthority(rootID, 3)
	authorized := knowledgecatalog.AuthorizedContext{
		AppID: root.AppID,
		Object: &knowledgecatalog.AuthorizedObject{
			KnowledgeObjectID: rootID,
			ObjectType:        root.ObjectType,
			Version:           root.CurrentVersion,
			SharingScope:      root.SharingScope,
		},
	}
	tests := []struct {
		name           string
		path           string
		request        proto.Message
		direction      knowledgeGraphDirection
		cause          error
		wantStatus     int
		wantReason     knowledgeattemptaudit.Reason
		wantAction     knowledgeattemptaudit.Action
		wantAuthorized bool
	}{
		{
			name: "not found strips forged context", path: knowledgeObjectsDependenciesPath,
			request:   &opensplunkv1.ListKnowledgeObjectDependenciesRequest{KnowledgeObjectId: rootID},
			direction: knowledgeGraphDependencies, cause: control.ErrNotFound,
			wantStatus: http.StatusNotFound, wantReason: knowledgeattemptaudit.ReasonNotFoundOrForbidden,
			wantAction: knowledgeattemptaudit.ActionDependencies,
		},
		{
			name: "post-authorization invalid cursor retains bound root", path: knowledgeObjectsDependentsPath,
			request:   &opensplunkv1.ListKnowledgeObjectDependentsRequest{KnowledgeObjectId: rootID},
			direction: knowledgeGraphDependents, cause: knowledgecatalog.ErrInvalidCursor,
			wantStatus: http.StatusBadRequest, wantReason: knowledgeattemptaudit.ReasonInvalidDefinition,
			wantAction: knowledgeattemptaudit.ActionDependents, wantAuthorized: true,
		},
		{
			name: "impossible invalid argument collapses", path: knowledgeObjectsDependenciesPath,
			request:   &opensplunkv1.ListKnowledgeObjectDependenciesRequest{KnowledgeObjectId: rootID},
			direction: knowledgeGraphDependencies, cause: control.ErrInvalidArgument,
			wantStatus: http.StatusServiceUnavailable, wantReason: knowledgeattemptaudit.ReasonServiceUnavailable,
			wantAction: knowledgeattemptaudit.ActionDependencies,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			serviceErr := knowledgecatalog.WithAuthorizedContext(test.cause, authorized)
			catalog := &knowledgeHTTPCatalog{}
			if test.direction == knowledgeGraphDependencies {
				catalog.dependenciesFn = func(context.Context, knowledgecatalog.ReadScope, knowledgecatalog.DependencyListRequest) (knowledgecatalog.DependencyPage, error) {
					return knowledgecatalog.DependencyPage{}, serviceErr
				}
			} else {
				catalog.dependentsFn = func(context.Context, knowledgecatalog.ReadScope, knowledgecatalog.DependencyListRequest) (knowledgecatalog.DependencyPage, error) {
					return knowledgecatalog.DependencyPage{}, serviceErr
				}
			}
			appender := &knowledgeBoundaryAppender{}
			_, handler := newKnowledgeHTTPHandler(
				t,
				auth.BrowserRoleAdministrator,
				catalog,
				&knowledgeHTTPWriter{},
				knowledgeHTTPApps(),
				appender,
			)
			response := knowledgeHTTPPost(t, handler, test.path, test.request)
			attempts := appender.snapshot()
			if response.Code != test.wantStatus || len(attempts) != 1 ||
				attempts[0].definition.Action != test.wantAction ||
				attempts[0].definition.Reason != test.wantReason ||
				(attempts[0].definition.AuthorizedContext != nil) != test.wantAuthorized {
				t.Fatalf("status=%d body=%q attempts=%+v", response.Code, response.Body.String(), attempts)
			}
		})
	}
}
