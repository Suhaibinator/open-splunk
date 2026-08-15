package server

import (
	"bytes"
	"context"
	"crypto/sha256"
	"net/http"
	"testing"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchhistory"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

func TestSearchJobProjectionRedactsDetachedKnowledgeObjectDisclosures(t *testing.T) {
	t.Parallel()

	job := completeJob("job-knowledge-summary")
	job.KnowledgeSnapshot = serverKnowledgeSnapshotSummary()
	wantRef := proto.Clone(job.KnowledgeSnapshot.Ref).(*opensplunkv1.KnowledgeSnapshotRef)

	converted, err := searchJobToProto(job, testNow)
	if err != nil {
		t.Fatalf("searchJobToProto() error = %v", err)
	}
	assertRedactedKnowledgeSnapshotSummary(t, converted.GetKnowledgeSnapshot(), wantRef)
	if job.KnowledgeSnapshot.Objects[0].GetAuthorizedObject().GetKnowledgeObjectId() != "extract-secret-id" ||
		job.KnowledgeSnapshot.Objects[1].GetAuthorizedObject().GetName() != "Secret Alias" ||
		job.KnowledgeSnapshot.LookupAssets[0].GetLookupId() != serverLookupLogicalID ||
		job.KnowledgeSnapshot.LookupAssets[0].GetAsset().GetLookupAssetId() != serverLookupPhysicalID {
		t.Fatal("search-job projection mutated manager-owned knowledge metadata")
	}
	converted.KnowledgeSnapshot.Ref.SnapshotSha256[0] ^= 0xff
	if !proto.Equal(job.KnowledgeSnapshot.Ref, wantRef) {
		t.Fatal("search-job projection aliases manager-owned reference bytes")
	}

	job.KnowledgeSnapshot = nil
	legacy, err := searchJobToProto(job, testNow)
	if err != nil || legacy.KnowledgeSnapshot != nil {
		t.Fatalf("legacy projection = (%+v, %v), want absent knowledge summary", legacy.GetKnowledgeSnapshot(), err)
	}
}

func TestKnowledgeSnapshotProjectionRejectsUnknownOversizedAndAmplifiedShapes(t *testing.T) {
	t.Parallel()

	unknown := serverKnowledgeSnapshotSummary()
	unknown.Objects[0].GetAuthorizedObject().ProtoReflect().SetUnknown(
		protowire.AppendVarint(protowire.AppendTag(nil, 100, protowire.VarintType), 1),
	)
	oversized := serverEmptyKnowledgeSnapshotSummary()
	oversized.ProtoReflect().SetUnknown(protowire.AppendBytes(
		protowire.AppendTag(nil, 100, protowire.BytesType),
		bytes.Repeat([]byte{0x7f}, knowledgesnapshot.MaximumSummaryBytes),
	))
	amplified := serverEmptyKnowledgeSnapshotSummary()
	amplified.Ref.ObjectCount = knowledgesnapshot.MaximumSummaryObjects + 1
	amplified.Objects = make([]*opensplunkv1.KnowledgeSnapshotObjectSummary, knowledgesnapshot.MaximumSummaryObjects+1)

	for name, summary := range map[string]*opensplunkv1.KnowledgeSnapshotSummary{
		"unknown":   unknown,
		"oversized": oversized,
		"amplified": amplified,
	} {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			job := completeJob("invalid-knowledge-summary")
			job.KnowledgeSnapshot = summary
			if converted, err := searchJobToProto(job, testNow); err == nil || converted != nil {
				t.Fatalf("searchJobToProto() = (%+v, %v), want fail-closed error", converted, err)
			}
		})
	}
}

func TestKnowledgeSnapshotProjectionPreservesTruncatedCanonicalPrefix(t *testing.T) {
	t.Parallel()

	input := serverKnowledgeSnapshotSummary()
	input.Ref.ObjectCount = knowledgesnapshot.MaximumSummaryObjects + 1
	for len(input.Objects) < knowledgesnapshot.MaximumSummaryObjects {
		object := proto.Clone(input.Objects[1]).(*opensplunkv1.KnowledgeSnapshotObjectSummary)
		object.ResolutionOrdinal = uint32(len(input.Objects))
		input.Objects = append(input.Objects, object)
	}
	input.ObjectsTruncated = true
	if err := knowledgesnapshot.ValidateSummary(input); err != nil {
		t.Fatalf("test summary is invalid: %v", err)
	}

	got, err := projectKnowledgeSnapshotSummary(input)
	if err != nil {
		t.Fatalf("projectKnowledgeSnapshotSummary() error = %v", err)
	}
	if !got.GetObjectsTruncated() || len(got.GetObjects()) != knowledgesnapshot.MaximumSummaryObjects ||
		got.GetRef().GetObjectCount() != knowledgesnapshot.MaximumSummaryObjects+1 {
		t.Fatalf("projected truncated prefix = %+v", got)
	}
	for index, object := range got.GetObjects() {
		if object.GetResolutionOrdinal() != uint32(index) || !object.GetRedacted() || object.GetAuthorizedObject() != nil {
			t.Fatalf("projected object %d = %+v", index, object)
		}
	}
	if len(got.GetLookupAssets()) != 0 || got.GetRef().GetLookupAssetCount() != 1 {
		t.Fatalf("projected lookup provenance = %+v", got)
	}
	if err := knowledgesnapshot.ValidateReference(got.GetRef()); err != nil {
		t.Fatalf("projected snapshot reference is invalid: %v", err)
	}
}

func TestSearchHistoryGetAndListRedactKnowledgeObjectDisclosures(t *testing.T) {
	entry := historyEntry(
		"history-knowledge", testNow, "search-app", "",
	)
	entry.KnowledgeSnapshot = serverKnowledgeSnapshotSummary()
	wantRef := proto.Clone(entry.KnowledgeSnapshot.Ref).(*opensplunkv1.KnowledgeSnapshotRef)
	store := &fakeSearchHistory{
		getFn: func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error) {
			return entry, nil
		},
		listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
			return searchhistory.ListResult{Entries: []*opensplunkv1.SearchHistoryEntry{entry}}, nil
		},
	}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI(),
	})

	getResponse := postProto(t, handler, "/api/v1/search/history/get", &opensplunkv1.GetSearchHistoryEntryRequest{SearchJobId: entry.SearchJobId})
	if getResponse.Code != http.StatusOK {
		t.Fatalf("get status = %d, body = %s", getResponse.Code, getResponse.Body.String())
	}
	var got opensplunkv1.GetSearchHistoryEntryResponse
	unmarshalResponse(t, getResponse, &got)
	assertRedactedKnowledgeSnapshotSummary(t, got.GetHistoryEntry().GetKnowledgeSnapshot(), wantRef)

	listResponse := postProto(t, handler, "/api/v1/search/history/list", &opensplunkv1.ListSearchHistoryRequest{})
	if listResponse.Code != http.StatusOK {
		t.Fatalf("list status = %d, body = %s", listResponse.Code, listResponse.Body.String())
	}
	var listed opensplunkv1.ListSearchHistoryResponse
	unmarshalResponse(t, listResponse, &listed)
	if len(listed.GetHistoryEntries()) != 1 {
		t.Fatalf("listed entries = %d, want 1", len(listed.GetHistoryEntries()))
	}
	assertRedactedKnowledgeSnapshotSummary(t, listed.GetHistoryEntries()[0].GetKnowledgeSnapshot(), wantRef)

	if entry.KnowledgeSnapshot.Objects[0].GetAuthorizedObject().GetKnowledgeObjectId() != "extract-secret-id" ||
		entry.KnowledgeSnapshot.Objects[1].GetAuthorizedObject().GetName() != "Secret Alias" ||
		entry.KnowledgeSnapshot.LookupAssets[0].GetLookupId() != serverLookupLogicalID ||
		entry.KnowledgeSnapshot.LookupAssets[0].GetAsset().GetLookupAssetId() != serverLookupPhysicalID {
		t.Fatal("history projection mutated store-owned knowledge metadata")
	}
}

func TestSearchHistoryGetAndListFailClosedOnInvalidKnowledgeDependencyOutput(t *testing.T) {
	invalid := historyEntry(
		"invalid-history-knowledge", testNow, "search-app", "",
	)
	invalid.KnowledgeSnapshot = serverEmptyKnowledgeSnapshotSummary()
	invalid.KnowledgeSnapshot.Ref.ObjectCount = knowledgesnapshot.MaximumSummaryObjects + 1
	invalid.KnowledgeSnapshot.Objects = make(
		[]*opensplunkv1.KnowledgeSnapshotObjectSummary,
		knowledgesnapshot.MaximumSummaryObjects+1,
	)
	store := &fakeSearchHistory{
		getFn: func(context.Context, searchhistory.AccessScope, string) (*opensplunkv1.SearchHistoryEntry, error) {
			return invalid, nil
		},
		listFn: func(context.Context, searchhistory.AccessScope, searchhistory.ListRequest) (searchhistory.ListResult, error) {
			return searchhistory.ListResult{Entries: []*opensplunkv1.SearchHistoryEntry{invalid}}, nil
		},
	}
	handler := newTestHandler(t, Config{
		SearchJobs: &fakeSearchJobs{}, Indexes: fakeIndexCatalog{}, SearchHistory: store, WebUI: testUI(),
	})

	getResponse := postProto(t, handler, "/api/v1/search/history/get", &opensplunkv1.GetSearchHistoryEntryRequest{SearchJobId: invalid.SearchJobId})
	if getResponse.Code != http.StatusInternalServerError {
		t.Fatalf("get status = %d, want 500; body = %s", getResponse.Code, getResponse.Body.String())
	}
	listResponse := postProto(t, handler, "/api/v1/search/history/list", &opensplunkv1.ListSearchHistoryRequest{})
	if listResponse.Code != http.StatusInternalServerError {
		t.Fatalf("list status = %d, want 500; body = %s", listResponse.Code, listResponse.Body.String())
	}
}

func assertRedactedKnowledgeSnapshotSummary(
	t *testing.T,
	got *opensplunkv1.KnowledgeSnapshotSummary,
	wantRef *opensplunkv1.KnowledgeSnapshotRef,
) {
	t.Helper()
	if got == nil || !proto.Equal(got.GetRef(), wantRef) || len(got.GetObjects()) != 2 || got.GetObjectsTruncated() {
		t.Fatalf("projected knowledge summary = %+v", got)
	}
	if len(got.GetLookupAssets()) != 0 || got.GetRef().GetLookupAssetCount() != 1 {
		t.Fatalf("projected lookup provenance = %+v", got)
	}
	for index, object := range got.GetObjects() {
		if !object.GetRedacted() || object.GetAuthorizedObject() != nil {
			t.Fatalf("object %d disclosure = %T, want redacted=true", index, object.GetDisclosure())
		}
	}
	if err := knowledgesnapshot.ValidateReference(got.GetRef()); err != nil {
		t.Fatalf("projected snapshot reference is invalid: %v", err)
	}
	if got.Objects[0].GetResolutionOrdinal() != 0 ||
		got.Objects[0].GetObjectType() != opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION ||
		got.Objects[0].GetStage() != opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION ||
		got.Objects[1].GetResolutionOrdinal() != 1 ||
		got.Objects[1].GetObjectType() != opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS ||
		got.Objects[1].GetStage() != opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS {
		t.Fatalf("projected object metadata = %+v", got.Objects)
	}
}

func serverKnowledgeSnapshotSummary() *opensplunkv1.KnowledgeSnapshotSummary {
	return &opensplunkv1.KnowledgeSnapshotSummary{
		Ref: &opensplunkv1.KnowledgeSnapshotRef{
			SnapshotSha256:               bytes.Repeat([]byte{0x42}, sha256.Size),
			TenantCatalogRevision:        7,
			TenantCatalogStateToken:      bytes.Repeat([]byte{0x73}, sha256.Size),
			ObjectCount:                  2,
			CompilerCompatibilityVersion: "0.1",
			LookupAssetCount:             1,
		},
		Objects: []*opensplunkv1.KnowledgeSnapshotObjectSummary{
			{
				ResolutionOrdinal: 0,
				ObjectType:        opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
				Stage:             opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
				Disclosure: &opensplunkv1.KnowledgeSnapshotObjectSummary_AuthorizedObject{
					AuthorizedObject: &opensplunkv1.KnowledgeSnapshotAuthorizedObjectSummary{
						KnowledgeObjectId: "extract-secret-id", Version: 11, Name: "Secret Extraction",
					},
				},
			},
			{
				ResolutionOrdinal: 1,
				ObjectType:        opensplunkv1.KnowledgeObjectType_KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
				Stage:             opensplunkv1.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
				Disclosure: &opensplunkv1.KnowledgeSnapshotObjectSummary_AuthorizedObject{
					AuthorizedObject: &opensplunkv1.KnowledgeSnapshotAuthorizedObjectSummary{
						KnowledgeObjectId: "alias-secret-id", Version: 12, Name: "Secret Alias",
					},
				},
			},
		},
		LookupAssets: []*opensplunkv1.KnowledgeSnapshotLookupAsset{
			serverLookupSnapshotAsset(),
		},
	}
}

const (
	serverLookupLogicalID  = "lookup-logical-secret-id"
	serverLookupPhysicalID = "lookup-physical-secret-id"
)

func serverLookupSnapshotAsset() *opensplunkv1.KnowledgeSnapshotLookupAsset {
	return &opensplunkv1.KnowledgeSnapshotLookupAsset{
		LookupId:      serverLookupLogicalID,
		LookupVersion: 13,
		Asset: &opensplunkv1.KnowledgeLookupAssetVersionReference{
			LookupAssetId: serverLookupPhysicalID,
			Version:       17,
			SizeBytes:     64,
			ContentSha256: bytes.Repeat([]byte{0x5a}, sha256.Size),
		},
	}
}

func serverEmptyKnowledgeSnapshotSummary() *opensplunkv1.KnowledgeSnapshotSummary {
	return &opensplunkv1.KnowledgeSnapshotSummary{Ref: &opensplunkv1.KnowledgeSnapshotRef{
		SnapshotSha256:               bytes.Repeat([]byte{0x42}, sha256.Size),
		TenantCatalogRevision:        7,
		TenantCatalogStateToken:      bytes.Repeat([]byte{0x73}, sha256.Size),
		CompilerCompatibilityVersion: "0.1",
	}}
}
