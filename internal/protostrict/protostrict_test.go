package protostrict_test

import (
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/protostrict"
	"google.golang.org/protobuf/reflect/protoreflect"
)

func withUnknown(message protoreflect.Message) {
	// Field number 1000 (wire type 0, value 1) is not declared by any message
	// in this repository, so it survives round-trips as an unknown field.
	message.SetUnknown(protoreflect.RawFields{0xc0, 0x3e, 0x01})
}

func TestContainsUnknownAcceptsCleanMessage(t *testing.T) {
	entry := &opensplunkv1.SearchHistoryEntry{
		SearchJobId: "job-1",
		Definition:  &opensplunkv1.SearchDefinition{Spl: "search index=main"},
	}
	if protostrict.ContainsUnknown(entry.ProtoReflect()) {
		t.Fatalf("clean message reported as containing unknown fields")
	}
	if err := protostrict.RejectUnknownFields(entry.ProtoReflect(), "search-history entry"); err != nil {
		t.Fatalf("unexpected error for clean message: %v", err)
	}
}

func TestContainsUnknownDetectsRootUnknownField(t *testing.T) {
	entry := &opensplunkv1.SearchHistoryEntry{SearchJobId: "job-1"}
	withUnknown(entry.ProtoReflect())
	if !protostrict.ContainsUnknown(entry.ProtoReflect()) {
		t.Fatalf("root unknown field was not detected")
	}
}

func TestContainsUnknownDetectsNestedUnknownField(t *testing.T) {
	definition := &opensplunkv1.SearchDefinition{Spl: "search index=main"}
	withUnknown(definition.ProtoReflect())
	entry := &opensplunkv1.SearchHistoryEntry{SearchJobId: "job-1", Definition: definition}
	if !protostrict.ContainsUnknown(entry.ProtoReflect()) {
		t.Fatalf("nested unknown field was not detected")
	}
}

func TestContainsUnknownDetectsRepeatedElementUnknownField(t *testing.T) {
	nested := &opensplunkv1.SearchHistoryEntry{SearchJobId: "job-1"}
	withUnknown(nested.ProtoReflect())
	response := &opensplunkv1.ListSearchHistoryResponse{
		HistoryEntries: []*opensplunkv1.SearchHistoryEntry{
			{SearchJobId: "job-0"},
			nested,
		},
	}
	if !protostrict.ContainsUnknown(response.ProtoReflect()) {
		t.Fatalf("unknown field inside a repeated element was not detected")
	}
}

func TestContainsUnknownRejectsTypedNilMessage(t *testing.T) {
	var entry *opensplunkv1.SearchHistoryEntry
	if !protostrict.ContainsUnknown(entry.ProtoReflect()) {
		t.Fatalf("typed-nil message was accepted")
	}
}

func TestRejectUnknownFieldsUsesSubjectInMessage(t *testing.T) {
	entry := &opensplunkv1.SearchHistoryEntry{SearchJobId: "job-1"}
	withUnknown(entry.ProtoReflect())
	err := protostrict.RejectUnknownFields(entry.ProtoReflect(), "search-history entry")
	if err == nil || err.Error() != "search-history entry contains unknown protobuf fields" {
		t.Fatalf("unexpected error: %v", err)
	}
}
