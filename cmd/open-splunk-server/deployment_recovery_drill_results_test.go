//go:build linux

package main

import (
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"google.golang.org/protobuf/proto"
)

// recoveryDrillResultsEqual compares the durable result without comparing its
// process-issued public reference. The API handler signs snapshot references
// with a fresh random key at startup; restoring the same retained generation
// therefore issues a new reference. The drill separately exercises that ref.
func recoveryDrillResultsEqual(before, after *opensplunk.ResultPage) bool {
	if before.GetSnapshotRef() == "" || after.GetSnapshotRef() == "" {
		return false
	}
	durableBefore := proto.Clone(before).(*opensplunk.ResultPage)
	durableAfter := proto.Clone(after).(*opensplunk.ResultPage)
	durableBefore.SnapshotRef = ""
	durableAfter.SnapshotRef = ""
	return proto.Equal(durableBefore, durableAfter)
}

func TestRecoveryDrillResultsEqualAcrossReferenceRotation(t *testing.T) {
	before := recoveryDrillResultFixture()
	after := proto.Clone(before).(*opensplunk.ResultPage)
	after.SnapshotRef = "restored-process-reference"
	originalBefore := proto.Clone(before)
	originalAfter := proto.Clone(after)
	if !recoveryDrillResultsEqual(before, after) {
		t.Fatal("same durable result with a newly issued reference must compare equal")
	}
	if !proto.Equal(before, originalBefore) || !proto.Equal(after, originalAfter) {
		t.Fatal("result comparison mutated an input")
	}
}

func TestRecoveryDrillResultsEqualRejectsDurableChanges(t *testing.T) {
	mutations := map[string]func(*opensplunk.ResultPage){
		"schema identity": func(page *opensplunk.ResultPage) { page.Schema.SchemaId = "changed" },
		"schema revision": func(page *opensplunk.ResultPage) { page.Schema.Revision++ },
		"schema columns":  func(page *opensplunk.ResultPage) { page.Schema.Columns = nil },
		"row identity":    func(page *opensplunk.ResultPage) { page.Rows[0].RowId = "changed" },
		"row ordinal":     func(page *opensplunk.ResultPage) { page.Rows[0].Ordinal++ },
		"row bytes": func(page *opensplunk.ResultPage) {
			page.Rows[0].Cells[0].Kind = &opensplunk.TypedValue_StringValue{StringValue: "changed"}
		},
		"row count":         func(page *opensplunk.ResultPage) { page.Rows = nil },
		"page cursor":       func(page *opensplunk.ResultPage) { page.Page.NextPageToken = new("changed") },
		"page total":        func(page *opensplunk.ResultPage) { page.Page.TotalSize = new(uint64(2)) },
		"page exactness":    func(page *opensplunk.ResultPage) { page.Page.TotalSizeExact = false },
		"completion":        func(page *opensplunk.ResultPage) { page.SnapshotComplete = false },
		"unknown fields":    func(page *opensplunk.ResultPage) { page.ProtoReflect().SetUnknown([]byte{0x78, 0x01}) },
		"missing reference": func(page *opensplunk.ResultPage) { page.SnapshotRef = "" },
	}
	for name, mutate := range mutations {
		t.Run(name, func(t *testing.T) {
			before := recoveryDrillResultFixture()
			after := proto.Clone(before).(*opensplunk.ResultPage)
			after.SnapshotRef = "restored-process-reference"
			mutate(after)
			if recoveryDrillResultsEqual(before, after) || recoveryDrillResultsEqual(after, before) {
				t.Fatal("changed durable result or missing reference compared equal")
			}
		})
	}
	if recoveryDrillResultsEqual(nil, nil) || recoveryDrillResultsEqual(recoveryDrillResultFixture(), nil) || recoveryDrillResultsEqual(nil, recoveryDrillResultFixture()) {
		t.Fatal("missing result page compared equal")
	}
}

func recoveryDrillResultFixture() *opensplunk.ResultPage {
	return &opensplunk.ResultPage{
		Schema: &opensplunk.ResultSchema{SchemaId: "retained-schema", Revision: 1,
			Columns: []*opensplunk.ResultColumn{{FieldName: "_raw"}}},
		Rows: []*opensplunk.ResultRow{{RowId: "retained-row", Ordinal: 1,
			Cells: []*opensplunk.TypedValue{{Kind: &opensplunk.TypedValue_StringValue{StringValue: "recovery-event-one"}}}}},
		Page:             &opensplunk.PageResponse{TotalSize: new(uint64(1)), TotalSizeExact: true},
		SnapshotComplete: true,
		SnapshotRef:      "original-process-reference",
	}
}
