package protostrict_test

import (
	"sync"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/protostrict"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/reflect/protodesc"
	"google.golang.org/protobuf/reflect/protoreflect"
	"google.golang.org/protobuf/types/descriptorpb"
	"google.golang.org/protobuf/types/dynamicpb"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func futureField() protoreflect.RawFields {
	return protowire.AppendVarint(protowire.AppendTag(nil, 1000, protowire.VarintType), 1)
}

// mapEdgeMessage builds a synthetic descriptor carrying both a scalar-valued
// map and a message-valued map, because no repository proto declares a map and
// the walker's two map branches are otherwise unreachable.
func mapEdgeMessage(t *testing.T) protoreflect.Message {
	t.Helper()
	stringField := descriptorpb.FieldDescriptorProto_TYPE_STRING
	messageField := descriptorpb.FieldDescriptorProto_TYPE_MESSAGE
	optional := descriptorpb.FieldDescriptorProto_LABEL_OPTIONAL
	repeated := descriptorpb.FieldDescriptorProto_LABEL_REPEATED
	entry := func(name, valueType string, kind descriptorpb.FieldDescriptorProto_Type) *descriptorpb.DescriptorProto {
		value := &descriptorpb.FieldDescriptorProto{
			Name: new("value"), Number: proto.Int32(2), Label: &optional, Type: &kind,
		}
		if valueType != "" {
			value.TypeName = new(valueType)
		}
		return &descriptorpb.DescriptorProto{
			Name: new(name),
			Field: []*descriptorpb.FieldDescriptorProto{
				{Name: new("key"), Number: proto.Int32(1), Label: &optional, Type: &stringField},
				value,
			},
			Options: &descriptorpb.MessageOptions{MapEntry: new(true)},
		}
	}
	file, err := protodesc.NewFile(&descriptorpb.FileDescriptorProto{
		Name: new("edge.proto"), Package: new("edge"),
		Syntax: new("proto3"),
		MessageType: []*descriptorpb.DescriptorProto{
			{Name: new("Inner")},
			{
				Name: new("Outer"),
				Field: []*descriptorpb.FieldDescriptorProto{
					{
						Name: new("scalars"), Number: proto.Int32(1), Label: &repeated,
						Type: &messageField, TypeName: new(".edge.Outer.ScalarsEntry"),
					},
					{
						Name: new("messages"), Number: proto.Int32(2), Label: &repeated,
						Type: &messageField, TypeName: new(".edge.Outer.MessagesEntry"),
					},
				},
				NestedType: []*descriptorpb.DescriptorProto{
					entry("ScalarsEntry", "", stringField),
					entry("MessagesEntry", ".edge.Inner", messageField),
				},
			},
		},
	}, nil)
	if err != nil {
		t.Fatalf("build synthetic file: %v", err)
	}
	return dynamicpb.NewMessage(file.Messages().ByName("Outer")).New()
}

func TestContainsUnknownWalksBothMapValueKinds(t *testing.T) {
	t.Parallel()
	outer := mapEdgeMessage(t)
	descriptor := outer.Descriptor()
	scalars := descriptor.Fields().ByName("scalars")
	messages := descriptor.Fields().ByName("messages")

	outer.Mutable(scalars).Map().Set(
		protoreflect.ValueOfString("k").MapKey(), protoreflect.ValueOfString("v"))
	inner := outer.NewField(messages).Map().NewValue()
	outer.Mutable(messages).Map().Set(protoreflect.ValueOfString("m").MapKey(), inner)
	if protostrict.ContainsUnknown(outer) {
		t.Fatalf("clean scalar-valued and message-valued maps rejected")
	}

	inner.Message().SetUnknown(futureField())
	if !protostrict.ContainsUnknown(outer) {
		t.Fatalf("unknown field inside a message-valued map entry was not detected")
	}
}

func TestContainsUnknownFindsUnknownOnlyInDeeplyNestedMapValue(t *testing.T) {
	t.Parallel()
	leaf := structpb.NewStringValue("leaf")
	tree, err := structpb.NewStruct(map[string]any{"a": map[string]any{"b": []any{}}})
	if err != nil {
		t.Fatal(err)
	}
	// struct -> map value -> struct -> map value -> list -> struct -> map value.
	depth := structpb.NewStructValue(&structpb.Struct{Fields: map[string]*structpb.Value{"c": leaf}})
	tree.Fields["a"].GetStructValue().Fields["b"].GetListValue().Values = []*structpb.Value{depth}
	if protostrict.ContainsUnknown(tree.ProtoReflect()) {
		t.Fatalf("clean deeply nested map tree rejected")
	}
	leaf.ProtoReflect().SetUnknown(futureField())
	if !protostrict.ContainsUnknown(tree.ProtoReflect()) {
		t.Fatalf("unknown field buried in a nested map value was not detected")
	}
}

func TestContainsUnknownRejectsNilElementsInsideCollections(t *testing.T) {
	t.Parallel()
	response := &opensplunk.ListSearchHistoryResponse{
		HistoryEntries: []*opensplunk.SearchHistoryEntry{{SearchJobId: "job-0"}, nil},
	}
	if !protostrict.ContainsUnknown(response.ProtoReflect()) {
		t.Fatalf("nil element inside a repeated message field was accepted")
	}

	// dynamicpb refuses to store a zero message, so reach the same walker branch
	// through a generated map whose Go value is a nil pointer.
	holed := &structpb.Struct{Fields: map[string]*structpb.Value{"a": nil}}
	if !protostrict.ContainsUnknown(holed.ProtoReflect()) {
		t.Fatalf("nil message inside a map value was accepted")
	}
}

func TestContainsUnknownTreatsUnsetTypedNilSingularFieldAsAbsent(t *testing.T) {
	t.Parallel()
	// A typed-nil singular field is simply unset on the wire, so the walker
	// accepts it; the consuming validator is what must reject the entry.
	entry := &opensplunk.SearchHistoryEntry{
		SearchJobId: "job-1",
		Definition:  (*opensplunk.SearchDefinition)(nil),
	}
	if protostrict.ContainsUnknown(entry.ProtoReflect()) {
		t.Fatalf("typed-nil singular field reported as unknown")
	}
	var top *opensplunk.SearchHistoryEntry
	if !protostrict.ContainsUnknown(top.ProtoReflect()) {
		t.Fatalf("typed-nil top-level message accepted")
	}
}

func TestContainsUnknownDetectsUnknownInsideOneofsAndWellKnownTypes(t *testing.T) {
	t.Parallel()
	duration := durationpb.New(0)
	duration.ProtoReflect().SetUnknown(futureField())
	oneof := &opensplunk.TypedValue{
		Kind: &opensplunk.TypedValue_DurationValue{DurationValue: duration},
	}
	if !protostrict.ContainsUnknown(oneof.ProtoReflect()) {
		t.Fatalf("unknown field inside a oneof message member was not detected")
	}

	stamp := timestamppb.New(time.Unix(0, 0).UTC())
	stamp.ProtoReflect().SetUnknown(futureField())
	entry := &opensplunk.SearchHistoryEntry{SearchJobId: "job-1", CreatedAt: stamp}
	if err := protostrict.RejectUnknownFields(entry.ProtoReflect(), "entry"); err == nil {
		t.Fatalf("unknown field inside google.protobuf.Timestamp was accepted")
	}
}

func TestContainsUnknownIsSafeForConcurrentReaders(t *testing.T) {
	t.Parallel()
	clean := &opensplunk.SearchHistoryEntry{
		SearchJobId: "job-1",
		Definition:  &opensplunk.SearchDefinition{Spl: "search index=main"},
	}
	dirty := proto.Clone(clean).(*opensplunk.SearchHistoryEntry)
	dirty.Definition.ProtoReflect().SetUnknown(futureField())
	var waiter sync.WaitGroup
	for range 16 {
		waiter.Go(func() {
			if protostrict.ContainsUnknown(clean.ProtoReflect()) ||
				!protostrict.ContainsUnknown(dirty.ProtoReflect()) {
				t.Error("concurrent walk disagreed with the sequential verdict")
			}
		})
	}
	waiter.Wait()
}
