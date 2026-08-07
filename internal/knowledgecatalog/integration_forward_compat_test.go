package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
)

func TestIntegrationInactiveFutureBodiesRoundTripAndActiveFailsClosed(t *testing.T) {
	for _, state := range []State{StateDraft, StateDisabled, StateDeleted} {
		state := state
		t.Run(string(state), func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			name := "future-" + string(state)
			fixture := newIntegrationFutureDefinition(t, name, 0)
			insertIntegrationFutureObject(t, database, "ko-"+name, state, fixture, 20)

			got, err := store.Get(context.Background(), testReadScope(), "ko-"+name, nil)
			if err != nil {
				t.Fatalf("Get(%s future body): %v", state, err)
			}
			integrationAssertFutureObject(t, got, fixture, state)
			page, err := store.List(context.Background(), testReadScope(), ListRequest{
				StateFilters: []State{state},
			})
			if err != nil {
				t.Fatalf("List(%s future body): %v", state, err)
			}
			if len(page.Objects) != 1 {
				t.Fatalf("List(%s future body) objects = %d", state, len(page.Objects))
			}
			integrationAssertFutureObject(t, page.Objects[0], fixture, state)

			// Both the ordinary byte slice and protobuf unknown-field storage are
			// caller mutable, so a later read must reconstruct detached storage.
			got.DefinitionSHA256[0] ^= 0xff
			got.Definition.ProtoReflect().SetUnknown(nil)
			again, err := store.Get(context.Background(), testReadScope(), "ko-"+name, nil)
			if err != nil {
				t.Fatalf("Get(%s after caller mutation): %v", state, err)
			}
			integrationAssertFutureObject(t, again, fixture, state)
		})
	}

	t.Run("active", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		fixture := newIntegrationFutureDefinition(t, "future-active", 0)
		insertIntegrationFutureObject(t, database, "ko-future-active", StateActive, fixture, 10)
		if _, err := store.Get(context.Background(), testReadScope(), "ko-future-active", nil); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Get(active future body) error = %v, want ErrCorrupt", err)
		}
		if _, err := store.List(context.Background(), testReadScope(), ListRequest{}); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("List(active future body) error = %v, want ErrCorrupt", err)
		}
	})
}

func TestIntegrationHistoricalInactiveFutureBodySurvivesKnownReenable(t *testing.T) {
	database, store := newCatalogTestStore(t)
	fixture := newIntegrationFutureDefinition(t, "future-history", 0)
	insertIntegrationFutureObject(t, database, "ko-future-history", StateDisabled, fixture, 20)
	description := "body understood again by a newer publisher"
	staged, _ := stageIntegrationKnownPublication(
		t,
		database,
		"ko-future-history",
		aliasDefinition(testApp, "future-history", SharingScopePrivate, &description, "known-again-*"),
		StateActive,
		"enable",
		30,
	)
	if err := staged.Commit(); err != nil {
		t.Fatalf("commit known re-enable: %v", err)
	}

	current, err := store.Get(context.Background(), testReadScope(), "ko-future-history", nil)
	if err != nil {
		t.Fatalf("Get(re-enabled current): %v", err)
	}
	if current.Version != 3 || current.State != StateActive || current.Definition.GetDescription() != description ||
		len(integrationUnknownBody(current.Definition)) != 0 {
		t.Fatalf("re-enabled current = %s", describeIntegrationObject(current))
	}
	historicalVersion := uint64(2)
	historical, err := store.Get(
		context.Background(),
		testReadScope(),
		"ko-future-history",
		&historicalVersion,
	)
	if err != nil {
		t.Fatalf("Get(historical future body): %v", err)
	}
	integrationAssertFutureObject(t, historical, fixture, StateDisabled)
}

func TestIntegrationListCanonicalByteBudgetUsesPersistedAuthorities(t *testing.T) {
	database, store := newCatalogTestStore(t)
	const objectCount = 5
	definitionBytes := MaximumListResponseCanonicalDefinitionBytes/2 + 4*1024
	if definitionBytes > maximumDefinitionBytes ||
		definitionBytes*2 <= MaximumListResponseCanonicalDefinitionBytes ||
		definitionBytes*objectCount <= MaximumListFilterIntegrityDefinitionBytes {
		t.Fatalf("invalid integration budget fixture: %d bytes x %d", definitionBytes, objectCount)
	}
	fixtures := make([]integrationFutureDefinition, objectCount)
	for index := range fixtures {
		name := fmt.Sprintf("budget-%02d", index)
		fixtures[index] = newIntegrationFutureDefinition(t, name, definitionBytes)
		insertIntegrationFutureObject(
			t,
			database,
			"ko-"+name,
			StateDraft,
			fixtures[index],
			int64(100+index),
		)
	}

	request := ListRequest{PageSize: objectCount, IncludeTotal: true}
	seen := make(map[string]struct{}, objectCount)
	var firstObject Object
	for pageNumber := 0; ; pageNumber++ {
		page, err := store.List(context.Background(), testReadScope(), request)
		if err != nil {
			t.Fatalf("List(response-budget page %d): %v", pageNumber, err)
		}
		if len(page.Objects) != 1 || cap(page.Objects) != 1 || page.TotalSize == nil || *page.TotalSize != objectCount {
			t.Fatalf("response-budget page %d shape = %#v, len/cap=%d/%d", pageNumber, page, len(page.Objects), cap(page.Objects))
		}
		wantIndex := len(seen)
		integrationAssertFutureObject(t, page.Objects[0], fixtures[wantIndex], StateDraft)
		if _, duplicate := seen[page.Objects[0].KnowledgeObjectID]; duplicate {
			t.Fatalf("response-budget pagination repeated %q", page.Objects[0].KnowledgeObjectID)
		}
		seen[page.Objects[0].KnowledgeObjectID] = struct{}{}
		if pageNumber == 0 {
			firstObject = page.Objects[0]
		}
		if page.NextPageToken == "" {
			break
		}
		request.PageToken = page.NextPageToken
	}
	if len(seen) != objectCount {
		t.Fatalf("response-budget pagination returned %d objects, want %d", len(seen), objectCount)
	}

	// A scalar-only filter does not require decoding objects excluded by that
	// filter; it remains ordinary response-budget pagination.
	owner := testOwner
	scalarPage, err := store.List(context.Background(), testReadScope(), ListRequest{
		PageSize:      objectCount,
		OwnerIDFilter: &owner,
	})
	if err != nil || len(scalarPage.Objects) != 1 || scalarPage.NextPageToken == "" {
		t.Fatalf("List(scalar filter under response budget) = %#v, %v", scalarPage, err)
	}

	// Body-derived filtering validates excluded visible rows too. The storage
	// authority therefore caps the complete integrity sweep, not just response
	// materialization.
	matchingName := "budget-00"
	if _, err := store.List(context.Background(), testReadScope(), ListRequest{
		PageSize:   1,
		TextFilter: &matchingName,
	}); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("List(over-budget exclusion sweep) error = %v, want ErrCapacityExceeded", err)
	}

	firstObject.DefinitionSHA256[0] ^= 0xff
	firstObject.Definition.ProtoReflect().SetUnknown(nil)
	again, err := store.Get(context.Background(), testReadScope(), "ko-budget-00", nil)
	if err != nil {
		t.Fatalf("Get(detached large future body): %v", err)
	}
	integrationAssertFutureObject(t, again, fixtures[0], StateDraft)
}

func integrationAssertFutureObject(
	t *testing.T,
	object Object,
	fixture integrationFutureDefinition,
	state State,
) {
	t.Helper()
	wantVersion := uint64(1)
	if state == StateDisabled || state == StateDeleted {
		wantVersion = 2
	}
	if object.Version != wantVersion || object.State != state || object.Name != fixture.metadata.Name ||
		object.ObjectType != ObjectTypeFieldAlias || object.Definition == nil || object.Definition.GetBody() != nil ||
		!bytes.Equal(object.DefinitionSHA256, fixture.digest[:]) ||
		!bytes.Equal(integrationUnknownBody(object.Definition), fixture.bodyField) {
		t.Fatalf("future object = %s, unknown=%x", describeIntegrationObject(object), integrationUnknownBody(object.Definition))
	}
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(object.Definition)
	if err != nil {
		t.Fatalf("marshal returned future definition: %v", err)
	}
	if !bytes.Equal(encoded, fixture.bytes) {
		t.Fatalf("returned future definition changed stored bytes: got %d bytes, want %d", len(encoded), len(fixture.bytes))
	}
	switch state {
	case StateDisabled:
		if object.DisabledAt == nil || object.DeletedAt != nil {
			t.Fatalf("disabled future lifecycle = disabled:%v deleted:%v", object.DisabledAt, object.DeletedAt)
		}
	case StateDeleted:
		if object.DeletedAt == nil || object.DisabledAt != nil {
			t.Fatalf("deleted future lifecycle = disabled:%v deleted:%v", object.DisabledAt, object.DeletedAt)
		}
	default:
		if object.DisabledAt != nil || object.DeletedAt != nil {
			t.Fatalf("%s future lifecycle = disabled:%v deleted:%v", state, object.DisabledAt, object.DeletedAt)
		}
	}
}
