package server

import (
	"errors"
	"math"
	"net/http"
	"strings"
	"testing"

	"github.com/Suhaibinator/SRouter/pkg/router"
	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func TestKnowledgeDefinitionSanitizersRejectRepeatedShapeBeforeUnknownWalks(
	t *testing.T,
) {
	t.Parallel()

	newDefinition := func() *opensplunk.KnowledgeObjectDefinition {
		patterns := make(
			[]*opensplunk.KnowledgeSelectorPattern,
			knowledge.MaximumSelectorPatternsPerDimension+1,
		)
		for index := range patterns {
			patterns[index] = &opensplunk.KnowledgeSelectorPattern{}
		}
		// If reflection traversal ran first, this would produce the distinct
		// unsupported-fields error. The shape preflight must win instead.
		addKnowledgeHTTPUnknown(patterns[0])
		return &opensplunk.KnowledgeObjectDefinition{
			Selector: &opensplunk.KnowledgeSelector{IndexPatterns: patterns},
		}
	}
	tests := []struct {
		name     string
		sanitize func(*opensplunk.KnowledgeObjectDefinition) error
	}{
		{
			name: "create",
			sanitize: func(definition *opensplunk.KnowledgeObjectDefinition) error {
				_, err := sanitizeCreateKnowledgeObjectRequest(
					t.Context(),
					&opensplunk.CreateKnowledgeObjectRequest{Definition: definition},
				)
				return err
			},
		},
		{
			name: "update",
			sanitize: func(definition *opensplunk.KnowledgeObjectDefinition) error {
				_, err := sanitizeUpdateKnowledgeObjectRequest(
					t.Context(),
					&opensplunk.UpdateKnowledgeObjectRequest{Definition: definition},
				)
				return err
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			definition := newDefinition()
			err := test.sanitize(definition)
			var httpError *router.HTTPError
			if !errors.As(err, &httpError) || httpError.StatusCode != http.StatusBadRequest ||
				httpError.Message != "knowledge mutation definition exceeds its entry limit" {
				t.Fatalf("error=%T %v", err, err)
			}
			if len(definition.GetSelector().GetIndexPatterns()[0].ProtoReflect().GetUnknown()) == 0 {
				t.Fatal("preflight rejection traversed and sanitized the hostile definition")
			}
		})
	}
}

func TestValidateKnowledgeObjectSanitizerPreservesUnknownAuthorities(t *testing.T) {
	request := &opensplunk.ValidateKnowledgeObjectRequest{
		Definition: validateTestDefinition("unknowns"),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"name"}},
	}
	request.ProtoReflect().SetUnknown(validateTestVarintField(100, 1))
	request.UpdateMask.ProtoReflect().SetUnknown(validateTestVarintField(101, 2))
	request.Definition.GetFieldAlias().ProtoReflect().SetUnknown(validateTestVarintField(102, 3))
	got, err := sanitizeValidateKnowledgeObjectRequest(t.Context(), request)
	if err != nil || got != request || len(got.ProtoReflect().GetUnknown()) == 0 ||
		len(got.GetUpdateMask().ProtoReflect().GetUnknown()) == 0 ||
		len(got.GetDefinition().GetFieldAlias().ProtoReflect().GetUnknown()) == 0 {
		t.Fatalf("Validate sanitizer changed unknown authorities: %v / %v", got, err)
	}
}

func TestPreviewKnowledgeObjectRequestSanitizerPreservesUnknownAuthorities(t *testing.T) {
	request := &opensplunk.PreviewKnowledgeObjectRequest{
		Definition: validateTestDefinition("unknowns"),
		UpdateMask: &fieldmaskpb.FieldMask{Paths: []string{"field_alias"}},
	}
	request.ProtoReflect().SetUnknown(validateTestVarintField(100, 1))
	request.UpdateMask.ProtoReflect().SetUnknown(validateTestVarintField(101, 2))
	request.Definition.GetFieldAlias().ProtoReflect().SetUnknown(validateTestVarintField(102, 3))
	got, err := sanitizePreviewKnowledgeObjectRequest(t.Context(), request)
	if err != nil || got != request || len(got.ProtoReflect().GetUnknown()) == 0 ||
		len(got.GetUpdateMask().ProtoReflect().GetUnknown()) == 0 ||
		len(got.GetDefinition().GetFieldAlias().ProtoReflect().GetUnknown()) == 0 {
		t.Fatalf("Preview sanitizer changed unknown authorities: %v / %v", got, err)
	}
}

func TestSanitizeGetKnowledgeObjectRequestBoundsIdentityAndVersion(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		request *opensplunk.GetKnowledgeObjectRequest
		wantErr bool
	}{
		{
			name:    "valid identity",
			request: &opensplunk.GetKnowledgeObjectRequest{KnowledgeObjectId: "ko-1"},
		},
		{
			name: "valid version",
			request: &opensplunk.GetKnowledgeObjectRequest{
				KnowledgeObjectId: "ko-1",
				Version:           new(uint64(math.MaxInt64)),
			},
		},
		{
			name:    "empty identity",
			request: &opensplunk.GetKnowledgeObjectRequest{},
			wantErr: true,
		},
		{
			name: "untrimmed identity",
			request: &opensplunk.GetKnowledgeObjectRequest{
				KnowledgeObjectId: " ko-1 ",
			},
			wantErr: true,
		},
		{
			name: "control character identity",
			request: &opensplunk.GetKnowledgeObjectRequest{
				KnowledgeObjectId: "ko\x001",
			},
			wantErr: true,
		},
		{
			name: "oversized identity",
			request: &opensplunk.GetKnowledgeObjectRequest{
				KnowledgeObjectId: strings.Repeat("k", maximumKnowledgeObjectIDBytes+1),
			},
			wantErr: true,
		},
		{
			name: "zero version",
			request: &opensplunk.GetKnowledgeObjectRequest{
				KnowledgeObjectId: "ko-1",
				Version:           new(uint64(0)),
			},
			wantErr: true,
		},
		{
			name: "high bit version",
			request: &opensplunk.GetKnowledgeObjectRequest{
				KnowledgeObjectId: "ko-1",
				Version:           new(uint64(math.MaxInt64) + 1),
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			before := proto.Clone(test.request)
			got, err := sanitizeGetKnowledgeObjectRequest(t.Context(), test.request)
			assertKnowledgeSanitizerOutcome(
				t,
				got,
				test.request,
				before,
				err,
				test.wantErr,
				"knowledge get request is invalid",
			)
		})
	}
}

func TestSanitizeListKnowledgeObjectsRequestBoundsFiltersAndPaging(t *testing.T) {
	t.Parallel()

	overLimit := func(bytes int) *string {
		return new(" \t" + strings.Repeat("f", bytes+1) + "\r ")
	}
	tests := []struct {
		name    string
		request *opensplunk.ListKnowledgeObjectsRequest
		wantErr bool
	}{
		{
			name:    "empty request",
			request: &opensplunk.ListKnowledgeObjectsRequest{},
		},
		{
			name: "filters at maximum",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				ObjectTypeFilters:   make([]opensplunk.KnowledgeObjectType, 3),
				StateFilters:        make([]opensplunk.KnowledgeObjectState, 5),
				SharingScopeFilters: make([]opensplunk.SharingScope, 3),
				Page: &opensplunk.PageRequest{
					PageSize:  new(uint32(knowledgecatalog.MaximumPageSize)),
					PageToken: new(strings.Repeat("t", maximumKnowledgePageTokenBytes)),
				},
			},
		},
		{
			name: "object type filters above maximum",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				ObjectTypeFilters: make([]opensplunk.KnowledgeObjectType, 4),
			},
			wantErr: true,
		},
		{
			name: "state filters above maximum",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				StateFilters: make([]opensplunk.KnowledgeObjectState, 6),
			},
			wantErr: true,
		},
		{
			name: "sharing scope filters above maximum",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				SharingScopeFilters: make([]opensplunk.SharingScope, 4),
			},
			wantErr: true,
		},
		{
			name: "app filter above normalized limit",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				AppIdFilter: overLimit(maximumKnowledgeAppIDBytes),
			},
			wantErr: true,
		},
		{
			name: "owner filter above normalized limit",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				OwnerIdFilter: overLimit(maximumKnowledgeIdentityBytes),
			},
			wantErr: true,
		},
		{
			name: "text filter above normalized limit",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				TextFilter: overLimit(maximumKnowledgeIdentityBytes),
			},
			wantErr: true,
		},
		{
			name: "selector text filter above normalized limit",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				SelectorTextFilter: overLimit(maximumKnowledgeIdentityBytes),
			},
			wantErr: true,
		},
		{
			name: "page size above maximum",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				Page: &opensplunk.PageRequest{
					PageSize: new(uint32(knowledgecatalog.MaximumPageSize + 1)),
				},
			},
			wantErr: true,
		},
		{
			name: "page token above maximum",
			request: &opensplunk.ListKnowledgeObjectsRequest{
				Page: &opensplunk.PageRequest{
					PageToken: new(strings.Repeat("t", maximumKnowledgePageTokenBytes+1)),
				},
			},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			before := proto.Clone(test.request)
			got, err := sanitizeListKnowledgeObjectsRequest(t.Context(), test.request)
			assertKnowledgeSanitizerOutcome(
				t,
				got,
				test.request,
				before,
				err,
				test.wantErr,
				"knowledge list request is invalid",
			)
		})
	}
}

func TestKnowledgeGraphSanitizersBoundRootVersionAndPage(t *testing.T) {
	t.Parallel()

	type graphRequest struct {
		objectID string
		version  *uint64
		page     *opensplunk.PageRequest
	}
	tests := []struct {
		name    string
		request graphRequest
		wantErr bool
	}{
		{name: "valid root", request: graphRequest{objectID: "ko-root"}},
		{
			name: "valid version and page",
			request: graphRequest{
				objectID: "ko-root",
				version:  new(uint64(3)),
				page: &opensplunk.PageRequest{
					PageSize:  new(uint32(knowledgecatalog.MaximumPageSize)),
					PageToken: new("cursor"),
				},
			},
		},
		{name: "empty root", request: graphRequest{}, wantErr: true},
		{
			name:    "untrimmed root",
			request: graphRequest{objectID: "ko-root "},
			wantErr: true,
		},
		{
			name:    "zero version",
			request: graphRequest{objectID: "ko-root", version: new(uint64(0))},
			wantErr: true,
		},
		{
			name: "high bit version",
			request: graphRequest{
				objectID: "ko-root",
				version:  new(uint64(math.MaxInt64) + 1),
			},
			wantErr: true,
		},
		{
			name: "page size above maximum",
			request: graphRequest{objectID: "ko-root", page: &opensplunk.PageRequest{
				PageSize: new(uint32(knowledgecatalog.MaximumPageSize + 1)),
			}},
			wantErr: true,
		},
		{
			name: "control character page token",
			request: graphRequest{objectID: "ko-root", page: &opensplunk.PageRequest{
				PageToken: new("cursor\nsecret"),
			}},
			wantErr: true,
		},
		{
			name: "page token above maximum",
			request: graphRequest{objectID: "ko-root", page: &opensplunk.PageRequest{
				PageToken: new(strings.Repeat("t", maximumKnowledgePageTokenBytes+1)),
			}},
			wantErr: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			dependencies := &opensplunk.ListKnowledgeObjectDependenciesRequest{
				KnowledgeObjectId: test.request.objectID,
				Version:           test.request.version,
				Page:              test.request.page,
			}
			beforeDependencies := proto.Clone(dependencies)
			gotDependencies, dependenciesErr := sanitizeListKnowledgeObjectDependenciesRequest(
				t.Context(),
				dependencies,
			)
			assertKnowledgeSanitizerOutcome(
				t,
				gotDependencies,
				dependencies,
				beforeDependencies,
				dependenciesErr,
				test.wantErr,
				"knowledge dependencies request is invalid",
			)

			dependents := &opensplunk.ListKnowledgeObjectDependentsRequest{
				KnowledgeObjectId: test.request.objectID,
				Version:           test.request.version,
				Page:              test.request.page,
			}
			beforeDependents := proto.Clone(dependents)
			gotDependents, dependentsErr := sanitizeListKnowledgeObjectDependentsRequest(
				t.Context(),
				dependents,
			)
			assertKnowledgeSanitizerOutcome(
				t,
				gotDependents,
				dependents,
				beforeDependents,
				dependentsErr,
				test.wantErr,
				"knowledge dependents request is invalid",
			)
		})
	}
}

func TestSanitizePrepareKnowledgeObjectQuarantineRequestBoundsIdentity(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		objectID string
		wantErr  bool
	}{
		{name: "valid identity", objectID: "ko-root"},
		{name: "empty identity", wantErr: true},
		{name: "untrimmed identity", objectID: "\tko-root", wantErr: true},
		{
			name:     "oversized identity",
			objectID: strings.Repeat("k", maximumKnowledgeObjectIDBytes+1),
			wantErr:  true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			request := &opensplunk.PrepareKnowledgeObjectQuarantineRequest{
				KnowledgeObjectId: test.objectID,
			}
			before := proto.Clone(request)
			got, err := sanitizePrepareKnowledgeObjectQuarantineRequest(t.Context(), request)
			assertKnowledgeSanitizerOutcome(
				t,
				got,
				request,
				before,
				err,
				test.wantErr,
				"knowledge quarantine preparation request is invalid",
			)
		})
	}
}

// assertKnowledgeSanitizerOutcome pins the shared sanitizer contract: the same
// pointer comes back, an accepted request is unchanged, and a rejection is the
// exact bad-request message the handler used before the check moved.
func assertKnowledgeSanitizerOutcome(
	t *testing.T,
	got proto.Message,
	want proto.Message,
	before proto.Message,
	err error,
	wantErr bool,
	wantMessage string,
) {
	t.Helper()
	if got.ProtoReflect() != want.ProtoReflect() {
		t.Fatalf("sanitizer returned a different request: %v", got)
	}
	if !wantErr {
		if err != nil {
			t.Fatalf("sanitize valid request: %v", err)
		}
		if !proto.Equal(got, before) {
			t.Fatalf("sanitizer changed a valid request: %v", got)
		}
		return
	}
	var httpError *router.HTTPError
	if !errors.As(err, &httpError) ||
		httpError.StatusCode != http.StatusBadRequest ||
		httpError.Message != wantMessage {
		t.Fatalf("error=%T %v, want %q", err, err, wantMessage)
	}
}
