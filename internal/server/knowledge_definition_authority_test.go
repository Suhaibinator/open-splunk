package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net/http"
	"strings"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/auth"
	"github.com/Suhaibinator/open-splunk/internal/knowledge"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeattemptaudit"
	"github.com/Suhaibinator/open-splunk/internal/knowledgecatalog"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestKnowledgeCatalogDefinitionAuthorityPinsProtoSizeBoundary(t *testing.T) {
	t.Parallel()

	exactDefinition := knowledgeHTTPFutureDefinitionAtSize(
		t,
		knowledgedefinition.MaximumCanonicalBytes,
	)
	if size := proto.Size(exactDefinition); size != knowledgedefinition.MaximumCanonicalBytes {
		t.Fatalf(
			"exact definition proto.Size=%d, want %d",
			size,
			knowledgedefinition.MaximumCanonicalBytes,
		)
	}
	exact := knowledgeHTTPAuthorityObject(t, exactDefinition)
	exact.State = knowledgecatalog.StateDisabled
	exact.DisabledAt = new(exact.UpdatedAt)
	if !validKnowledgeCatalogDefinitionAuthority(exact) {
		t.Fatal("exact four-MiB catalog definition authority was rejected")
	}

	overDefinition := knowledgeHTTPFutureDefinitionAtSize(
		t,
		knowledgedefinition.MaximumCanonicalBytes+1,
	)
	if size := proto.Size(overDefinition); size != knowledgedefinition.MaximumCanonicalBytes+1 {
		t.Fatalf(
			"over definition proto.Size=%d, want %d",
			size,
			knowledgedefinition.MaximumCanonicalBytes+1,
		)
	}
	over := knowledgeHTTPAuthorityObject(t, overDefinition)
	over.State = knowledgecatalog.StateDisabled
	over.DisabledAt = new(over.UpdatedAt)
	if validKnowledgeCatalogDefinitionAuthority(over) {
		t.Fatal("catalog definition above four MiB was accepted")
	}

	unknownState := knowledgeHTTPObject()
	unknownState.State = knowledgecatalog.State("future-state")
	if validKnowledgeCatalogDefinitionAuthority(unknownState) {
		t.Fatal("recognized definition with unknown lifecycle state was accepted")
	}
}

func TestKnowledgeHTTPGetAndListValidateCatalogDefinitionAuthority(t *testing.T) {
	t.Parallel()

	empty := ""
	tests := []struct {
		name       string
		object     func(*testing.T) knowledgecatalog.Object
		wantStatus int
	}{
		{
			name:       "canonical recognized draft",
			object:     func(*testing.T) knowledgecatalog.Object { return knowledgeHTTPObject() },
			wantStatus: http.StatusOK,
		},
		{
			name: "canonical recognized active",
			object: func(*testing.T) knowledgecatalog.Object {
				object := knowledgeHTTPObject()
				object.State = knowledgecatalog.StateActive
				return object
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "recognized noncanonical metadata",
			object: func(t *testing.T) knowledgecatalog.Object {
				definition := knowledgeHTTPDefinition(
					opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				)
				definition.Description = &empty
				return knowledgeHTTPAuthorityObject(t, definition)
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "recognized nested unknown",
			object: func(t *testing.T) knowledgecatalog.Object {
				definition := knowledgeHTTPDefinition(
					opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				)
				addKnowledgeHTTPUnknown(definition.GetFieldAlias())
				return knowledgeHTTPAuthorityObject(t, definition)
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "recognized digest mismatch",
			object: func(*testing.T) knowledgecatalog.Object {
				object := knowledgeHTTPObject()
				object.DefinitionSHA256[0] ^= 0xff
				return object
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "definition over four MiB",
			object: func(t *testing.T) knowledgecatalog.Object {
				definition := knowledgeHTTPDefinition(
					opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
				)
				description := strings.Repeat(
					"x",
					knowledgedefinition.MaximumCanonicalBytes+1,
				)
				definition.Description = &description
				return knowledgeHTTPAuthorityObject(t, definition)
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "definition repeated shape exceeds traversal ceiling",
			object: func(*testing.T) knowledgecatalog.Object {
				object := knowledgeHTTPObject()
				object.Definition.Selector = &opensplunkv1.KnowledgeSelector{
					IndexPatterns: make(
						[]*opensplunkv1.KnowledgeSelectorPattern,
						knowledge.MaximumSelectorPatternsPerDimension+1,
					),
				}
				return object
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "canonical opaque draft",
			object: func(t *testing.T) knowledgecatalog.Object {
				return knowledgeHTTPFutureAuthorityObject(
					t,
					knowledgecatalog.StateDraft,
					false,
				)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "canonical opaque disabled",
			object: func(t *testing.T) knowledgecatalog.Object {
				return knowledgeHTTPFutureAuthorityObject(
					t,
					knowledgecatalog.StateDisabled,
					false,
				)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "canonical opaque deleted",
			object: func(t *testing.T) knowledgecatalog.Object {
				return knowledgeHTTPFutureAuthorityObject(
					t,
					knowledgecatalog.StateDeleted,
					false,
				)
			},
			wantStatus: http.StatusOK,
		},
		{
			name: "opaque active fails closed",
			object: func(t *testing.T) knowledgecatalog.Object {
				return knowledgeHTTPFutureAuthorityObject(
					t,
					knowledgecatalog.StateActive,
					false,
				)
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "opaque noncanonical metadata",
			object: func(t *testing.T) knowledgecatalog.Object {
				return knowledgeHTTPFutureAuthorityObject(
					t,
					knowledgecatalog.StateDisabled,
					true,
				)
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "opaque digest mismatch",
			object: func(t *testing.T) knowledgecatalog.Object {
				object := knowledgeHTTPFutureAuthorityObject(
					t,
					knowledgecatalog.StateDraft,
					false,
				)
				object.DefinitionSHA256[0] ^= 0xff
				return object
			},
			wantStatus: http.StatusServiceUnavailable,
		},
		{
			name: "quarantined authority remains absent",
			object: func(*testing.T) knowledgecatalog.Object {
				object := knowledgeHTTPObject()
				object.State = knowledgecatalog.StateQuarantined
				object.Definition = nil
				object.DefinitionSHA256 = nil
				object.QuarantinedAt = new(object.UpdatedAt)
				object.QuarantineReason = new("root_corruption")
				return object
			},
			wantStatus: http.StatusOK,
		},
	}

	for _, test := range tests {
		for _, operation := range []struct {
			name    string
			path    string
			request proto.Message
		}{
			{
				name: "get",
				path: knowledgeObjectsGetPath,
				request: &opensplunkv1.GetKnowledgeObjectRequest{
					KnowledgeObjectId: "ko-http-object-1",
				},
			},
			{
				name:    "list",
				path:    knowledgeObjectsListPath,
				request: &opensplunkv1.ListKnowledgeObjectsRequest{},
			},
		} {
			t.Run(test.name+"/"+operation.name, func(t *testing.T) {
				object := test.object(t)
				catalog := &knowledgeHTTPCatalog{
					getFn: func(
						context.Context,
						knowledgecatalog.ReadScope,
						string,
						*uint64,
					) (knowledgecatalog.Object, error) {
						return object, nil
					},
					listFn: func(
						context.Context,
						knowledgecatalog.ReadScope,
						knowledgecatalog.ListRequest,
					) (knowledgecatalog.ListPage, error) {
						return knowledgecatalog.ListPage{
							Objects:         []knowledgecatalog.Object{object},
							CatalogRevision: 7,
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
				response := knowledgeHTTPPost(
					t,
					handler,
					operation.path,
					operation.request,
				)
				attempts := appender.snapshot()
				if response.Code != test.wantStatus {
					t.Fatalf(
						"status=%d body=%q, want %d; attempts=%+v",
						response.Code,
						response.Body.String(),
						test.wantStatus,
						attempts,
					)
				}
				if test.wantStatus == http.StatusOK {
					if len(attempts) != 0 {
						t.Fatalf("successful response journaled attempts: %+v", attempts)
					}
					return
				}
				if response.Body.String() != knowledgeManagementUnavailableBody ||
					len(attempts) != 1 ||
					attempts[0].definition.Reason != knowledgeattemptaudit.ReasonServiceUnavailable ||
					attempts[0].definition.AuthorizedContext != nil {
					t.Fatalf(
						"body=%q attempts=%+v",
						response.Body.String(),
						attempts,
					)
				}
			})
		}
	}
}

func knowledgeHTTPAuthorityObject(
	t *testing.T,
	definition *opensplunkv1.KnowledgeObjectDefinition,
) knowledgecatalog.Object {
	t.Helper()
	object := knowledgeHTTPObject()
	object.Definition = definition
	object.AppID = definition.GetAppId()
	object.Name = definition.GetName()
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(definition)
	if err != nil {
		t.Fatalf("marshal catalog definition authority: %v", err)
	}
	digest := sha256.Sum256(encoded)
	object.DefinitionSHA256 = digest[:]
	return object
}

func knowledgeHTTPFutureAuthorityObject(
	t *testing.T,
	state knowledgecatalog.State,
	noncanonicalMetadata bool,
) knowledgecatalog.Object {
	t.Helper()
	metadata := knowledgeHTTPDefinition(
		opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
	)
	metadata.Body = nil
	if noncanonicalMetadata {
		empty := ""
		metadata.Description = &empty
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal future catalog metadata: %v", err)
	}
	encoded = protowire.AppendBytes(
		protowire.AppendTag(encoded, 13, protowire.BytesType),
		[]byte{0x08, 0x01},
	)
	definition := &opensplunkv1.KnowledgeObjectDefinition{}
	if err := (proto.UnmarshalOptions{
		DiscardUnknown: false,
		RecursionLimit: 32,
	}).Unmarshal(encoded, definition); err != nil {
		t.Fatalf("unmarshal future catalog definition: %v", err)
	}
	object := knowledgeHTTPAuthorityObject(t, definition)
	object.State = state
	switch state {
	case knowledgecatalog.StateDisabled:
		object.DisabledAt = new(object.UpdatedAt)
	case knowledgecatalog.StateDeleted:
		object.DeletedAt = new(object.UpdatedAt)
	}
	return object
}

func knowledgeHTTPFutureDefinitionAtSize(
	t *testing.T,
	target int,
) *opensplunkv1.KnowledgeObjectDefinition {
	t.Helper()
	metadata := knowledgeHTTPDefinition(
		opensplunkv1.SharingScope_SHARING_SCOPE_PRIVATE,
	)
	metadata.Body = nil
	known, err := (proto.MarshalOptions{Deterministic: true}).Marshal(metadata)
	if err != nil {
		t.Fatalf("marshal sized future metadata: %v", err)
	}
	tag := protowire.AppendTag(nil, 13, protowire.BytesType)
	payloadBytes := target - len(known) - len(tag)
	for {
		lengthBytes := len(protowire.AppendVarint(nil, uint64(payloadBytes)))
		next := target - len(known) - len(tag) - lengthBytes
		if next == payloadBytes {
			break
		}
		payloadBytes = next
	}
	encoded := protowire.AppendBytes(
		protowire.AppendTag(bytes.Clone(known), 13, protowire.BytesType),
		bytes.Repeat([]byte{0xa5}, payloadBytes),
	)
	if len(encoded) != target {
		t.Fatalf("constructed future definition=%d bytes, want %d", len(encoded), target)
	}
	definition := &opensplunkv1.KnowledgeObjectDefinition{}
	if err := (proto.UnmarshalOptions{
		DiscardUnknown: false,
		RecursionLimit: 32,
	}).Unmarshal(encoded, definition); err != nil {
		t.Fatalf("unmarshal sized future definition: %v", err)
	}
	return definition
}
