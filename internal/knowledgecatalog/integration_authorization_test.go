package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
)

func TestIntegrationHiddenCorruptionIsNotAReadOracle(t *testing.T) {
	tests := []struct {
		name    string
		corrupt func(*testing.T, *control.DB)
	}{
		{
			name: "definition digest mismatch",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
				mustExec(t, database, `UPDATE knowledge_definition_blobs
					SET definition_proto = X'00', definition_bytes = 1
					WHERE tenant_id = ? AND definition_digest = (
						SELECT definition_digest FROM knowledge_objects
						WHERE tenant_id = ? AND knowledge_object_id = 'ko-hidden-source'
					)`, testTenant, testTenant)
			},
		},
		{
			name: "same-byte projected description",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_list_projection_update_is_forbidden")
				mustExec(t, database, `UPDATE knowledge_object_list_projections
					SET description = 'xxxxxx'
					WHERE tenant_id = ? AND knowledge_object_id = 'ko-hidden-source'`, testTenant)
			},
		},
		{
			name: "same-byte projected selector",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_list_selector_update_is_forbidden")
				mustExec(t, database, `UPDATE knowledge_object_list_selector_patterns
					SET value = 'xxxxxx-*'
					WHERE tenant_id = ? AND knowledge_object_id = 'ko-hidden-source'
					  AND dimension = 'host'`, testTenant)
			},
		},
		{
			name: "dependency ordinal",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_dependency_update_is_forbidden")
				mustExec(t, database, `UPDATE knowledge_object_dependencies SET ordinal = 1
					WHERE tenant_id = ? AND source_object_id = 'ko-hidden-source'
					  AND source_object_version = 1`, testTenant)
			},
		},
		{
			name: "projection-only authorization escalation",
			corrupt: func(t *testing.T, database *control.DB) {
				dropTrigger(t, database, "knowledge_list_projection_update_is_forbidden")
				execWithForeignKeysDisabled(t, database, `UPDATE knowledge_object_list_projections
					SET owner_id = ?
					WHERE tenant_id = ? AND knowledge_object_id = 'ko-hidden-source'`, testOwner, testTenant)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			for index, name := range []string{"visible-alpha", "visible-zulu"} {
				description := "visible description"
				insertFixtureObject(t, database, fixtureObject{
					id:    "ko-" + name,
					owner: testOwner,
					versions: []fixtureVersion{{
						definition: aliasDefinition(testApp, name, SharingScopePrivate, &description, name+"-*"),
						state:      StateActive,
						mutation:   "create",
						timestamp:  int64(10 + index),
					}},
				})
			}
			insertFixtureObject(t, database, fixtureObject{
				id:    "ko-hidden-target",
				owner: "owner-b",
				versions: []fixtureVersion{{
					definition: dependencyExtractionDefinition(
						testApp, "hidden-target", SharingScopePrivate, nil, "target-*",
						dependencyFixtureInputField,
					),
					state:     StateActive,
					mutation:  "create",
					timestamp: 20,
				}},
			})
			hiddenDescription := "secret"
			insertFixtureObject(t, database, fixtureObject{
				id:    "ko-hidden-source",
				owner: "owner-b",
				versions: []fixtureVersion{{
					definition: dependencyAliasDefinition(
						testApp, "hidden-source", SharingScopePrivate, &hiddenDescription, "secret-*",
						dependencyFixtureInputField, "dependency_alias",
					),
					state:     StateActive,
					mutation:  "create",
					timestamp: 21,
					dependencies: []fixtureDependency{{
						targetObjectID: "ko-hidden-target",
						targetVersion:  1,
					}},
				}},
			})

			request := ListRequest{PageSize: 1, IncludeTotal: true}
			baselineFirst, err := store.List(context.Background(), testReadScope(), request)
			if err != nil {
				t.Fatalf("List(baseline first): %v", err)
			}
			continuation := request
			continuation.PageToken = baselineFirst.NextPageToken
			baselineSecond, err := store.List(context.Background(), testReadScope(), continuation)
			if err != nil {
				t.Fatalf("List(baseline second): %v", err)
			}
			if _, err := store.Get(context.Background(), testReadScope(), "ko-hidden-source", nil); !errors.Is(err, control.ErrNotFound) {
				t.Fatalf("Get(hidden baseline) error = %v, want ErrNotFound", err)
			}

			test.corrupt(t, database)
			afterFirst, err := store.List(context.Background(), testReadScope(), request)
			if err != nil {
				t.Fatalf("List(after hidden corruption first): %v", err)
			}
			afterSecond, err := store.List(context.Background(), testReadScope(), continuation)
			if err != nil {
				t.Fatalf("List(after hidden corruption continuation): %v", err)
			}
			integrationAssertPagesEqual(t, afterFirst, baselineFirst)
			integrationAssertPagesEqual(t, afterSecond, baselineSecond)
			for _, version := range []*uint64{nil, new(uint64(1))} {
				if _, err := store.Get(context.Background(), testReadScope(), "ko-hidden-source", version); !errors.Is(err, control.ErrNotFound) {
					t.Fatalf("Get(hidden after corruption, version=%v) error = %v, want ErrNotFound", version, err)
				}
			}
		})
	}
}

func integrationAssertPagesEqual(t *testing.T, got, want ListPage) {
	t.Helper()
	if got.NextPageToken != want.NextPageToken || got.TotalSizeExact != want.TotalSizeExact ||
		got.CatalogRevision != want.CatalogRevision || !reflect.DeepEqual(got.TotalSize, want.TotalSize) ||
		len(got.Objects) != len(want.Objects) {
		t.Fatalf("page scalar mismatch: got %#v, want %#v", got, want)
	}
	for index := range got.Objects {
		left, right := got.Objects[index], want.Objects[index]
		if left.KnowledgeObjectID != right.KnowledgeObjectID || left.TenantID != right.TenantID ||
			left.AppID != right.AppID || left.OwnerID != right.OwnerID || left.ObjectType != right.ObjectType ||
			left.Name != right.Name || left.Version != right.Version || left.SharingScope != right.SharingScope ||
			left.State != right.State || !bytes.Equal(left.DefinitionSHA256, right.DefinitionSHA256) ||
			!left.CreatedAt.Equal(right.CreatedAt) || !left.UpdatedAt.Equal(right.UpdatedAt) ||
			!reflect.DeepEqual(left.DisabledAt, right.DisabledAt) ||
			!reflect.DeepEqual(left.QuarantinedAt, right.QuarantinedAt) ||
			!reflect.DeepEqual(left.DeletedAt, right.DeletedAt) ||
			!reflect.DeepEqual(left.QuarantineReason, right.QuarantineReason) ||
			!proto.Equal(left.Definition, right.Definition) {
			t.Fatalf("page object %d mismatch: got %s, want %s", index, describeIntegrationObject(left), describeIntegrationObject(right))
		}
	}
}
