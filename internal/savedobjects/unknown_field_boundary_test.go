package savedobjects

import (
	"context"
	"errors"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/protostrict"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/known/fieldmaskpb"
)

func futureSavedField() protoreflect.RawFields {
	return protowire.AppendVarint(protowire.AppendTag(nil, 1000, protowire.VarintType), 1)
}

// TestCreateRejectsUnknownFieldsAtEveryReachableDepth proves the strict walker
// is still consulted on the clone (not the caller's message) for each nested
// message slot a saved-search definition can reach.
func TestCreateRejectsUnknownFieldsAtEveryReachableDepth(t *testing.T) {
	scope := AccessScope{OwnerID: "owner"}
	tests := map[string]func(*opensplunkv1.SavedSearchDefinition) protoreflect.Message{
		"root": func(definition *opensplunkv1.SavedSearchDefinition) protoreflect.Message {
			return definition.ProtoReflect()
		},
		"search": func(definition *opensplunkv1.SavedSearchDefinition) protoreflect.Message {
			return definition.Search.ProtoReflect()
		},
		"search time range": func(definition *opensplunkv1.SavedSearchDefinition) protoreflect.Message {
			definition.Search.TimeRange = &opensplunkv1.TimeRangeSpec{
				Earliest: stringPointer("-15m"), Latest: stringPointer("now"),
			}
			return definition.Search.TimeRange.ProtoReflect()
		},
	}
	for name, target := range tests {
		t.Run(name, func(t *testing.T) {
			_, store := openTestStore(t)
			definition := savedSearchDefinition("poisoned", "search")
			target(definition).SetUnknown(futureSavedField())
			if _, err := store.Create(context.Background(), scope, definition); !errors.Is(
				err, control.ErrInvalidArgument,
			) {
				t.Fatalf("Create() error = %v, want ErrInvalidArgument", err)
			}
			if definition.ProtoReflect().IsValid() && len(definition.GetName()) == 0 {
				t.Fatal("Create() mutated the caller's definition")
			}
		})
	}
}

// TestUpdateNeverPersistsUnknownFieldsRegardlessOfMask covers the masked-patch
// path: a mask that copies the poisoned field must reject, and a mask that
// skips it must still never persist unknown bytes.
func TestUpdateNeverPersistsUnknownFieldsRegardlessOfMask(t *testing.T) {
	ctx := context.Background()
	scope := AccessScope{OwnerID: "owner"}

	for name, test := range map[string]struct {
		mask       []string
		poisonRoot bool
		wantReject bool
	}{
		"mask copies the poisoned search":     {mask: []string{"search"}, wantReject: true},
		"full mask copies everything":         {mask: []string{"*"}, wantReject: true},
		"narrow mask skips the poisoned leaf": {mask: []string{"name"}, wantReject: false},
		"narrow mask with poisoned root":      {mask: []string{"name"}, poisonRoot: true, wantReject: false},
	} {
		t.Run(name, func(t *testing.T) {
			_, store := openTestStore(t)
			created, err := store.Create(ctx, scope, savedSearchDefinition("original", "search"))
			if err != nil {
				t.Fatalf("Create() error = %v", err)
			}

			patch := savedSearchDefinition("renamed", "search")
			patch.Search.ProtoReflect().SetUnknown(futureSavedField())
			if test.poisonRoot {
				patch.ProtoReflect().SetUnknown(futureSavedField())
			}
			updated, err := store.Update(
				ctx, scope, created.SavedSearchId, created.Version, patch,
				&fieldmaskpb.FieldMask{Paths: test.mask},
			)
			if test.wantReject {
				if !errors.Is(err, control.ErrInvalidArgument) {
					t.Fatalf("Update() error = %v, want ErrInvalidArgument", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("Update() error = %v", err)
			}
			if protostrict.ContainsUnknown(updated.Definition.ProtoReflect()) {
				t.Fatal("Update() smuggled unknown protobuf fields into the stored definition")
			}
			stored, err := store.Get(ctx, scope, created.SavedSearchId)
			if err != nil {
				t.Fatalf("Get() error = %v", err)
			}
			if protostrict.ContainsUnknown(stored.Definition.ProtoReflect()) {
				t.Fatal("stored saved search round-tripped unknown protobuf fields")
			}
			if stored.Definition.GetName() != "renamed" {
				t.Fatalf("stored name = %q, want renamed", stored.Definition.GetName())
			}
		})
	}
}

// TestCreateSeparatesTypedNilFromUnknownFields pins that a typed-nil search is
// reported as a missing field rather than as an unknown-field violation.
func TestCreateSeparatesTypedNilFromUnknownFields(t *testing.T) {
	_, store := openTestStore(t)
	definition := savedSearchDefinition("typed-nil", "search")
	definition.Search = (*opensplunkv1.SearchDefinition)(nil)
	if protostrict.ContainsUnknown(definition.ProtoReflect()) {
		t.Fatal("typed-nil singular field reported as an unknown field")
	}
	_, err := store.Create(context.Background(), AccessScope{OwnerID: "owner"}, definition)
	if !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Create() error = %v, want ErrInvalidArgument", err)
	}

	var typedNil *opensplunkv1.SavedSearchDefinition
	if _, err := store.Create(
		context.Background(), AccessScope{OwnerID: "owner"}, typedNil,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("Create(typed nil) error = %v, want ErrInvalidArgument", err)
	}
}
