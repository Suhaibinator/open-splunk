package server

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/patterns"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

func TestPatternAPIPreservesExactRetainedMemberTypesAndTotals(t *testing.T) {
	query := patterns.MemberRequest{SearchJobID: "job", Generation: 7, PatternID: "opaque-id", PageSize: 2, IncludeTotal: true, Columns: []string{"count", "_raw"}}
	result := patterns.MemberResult{PatternID: "opaque-id", Generation: 7, TotalSize: new(uint64(2)), TotalSizeExact: true,
		SnapshotComplete: false,
		Schema:           searchjobs.Schema{Columns: []searchjobs.Column{{Name: "count", Kind: searchjobs.ValueKindUnsigned}, {Name: "_raw", Kind: searchjobs.ValueKindString}}},
		Rows: []searchjobs.ResultRow{
			{Ordinal: 4, Values: []searchjobs.Value{searchjobs.UnsignedValue(math.MaxUint64), searchjobs.StringValue("request 42")}},
			{Ordinal: 9001, Values: []searchjobs.Value{searchjobs.UnsignedValue(9007199254740993), searchjobs.StringValue("request 43")}},
		},
	}
	response, err := patternMembersToProto(context.Background(), result, query, "snapshot")
	if err != nil {
		t.Fatal(err)
	}
	bytes, err := proto.Marshal(response)
	if err != nil {
		t.Fatal(err)
	}
	decoded := new(opensplunk.ListSearchPatternMembersResponse)
	if err := proto.Unmarshal(bytes, decoded); err != nil {
		t.Fatal(err)
	}
	page := decoded.GetResultPage()
	if page.GetSnapshotComplete() || !page.GetPage().GetTotalSizeExact() || page.GetPage().GetTotalSize() != 2 || page.GetSnapshotRef() != "snapshot" {
		t.Fatalf("exact retained relation metadata = %v", page)
	}
	if page.Rows[0].Ordinal != 4 || page.Rows[1].Ordinal != 9001 || page.Rows[0].RowId != "job:4" || page.Rows[0].Cells[0].GetUint64Value() != math.MaxUint64 {
		t.Fatalf("member types/ordinals changed: %v", page.Rows)
	}
	query.Columns = []string{"_raw", "count"}
	if _, err := patternMembersToProto(context.Background(), result, query, "snapshot"); err == nil {
		t.Fatal("accepted reversed projection")
	}
}

func TestPatternAPIRejectsContradictoryCoverageAndPages(t *testing.T) {
	valid := func() patterns.ListResult {
		return patterns.ListResult{
			Patterns:   []patterns.Pattern{{ID: "id", Signature: "request <int>", EventCount: 2}},
			Generation: 7, EligibleEventCount: 2, ExcludedEventCount: 1, RetainedEventCount: 3,
			RetainedTruncated: true, TotalSize: new(uint64(1)), TotalSizeExact: true,
		}
	}
	query := patterns.ListRequest{Generation: 7, PageSize: 2, IncludeTotal: true}
	good, err := patternsToProto(context.Background(), valid(), query, "snapshot")
	if err != nil || good.GetEligibleEventCount() != 2 || !good.GetRetainedTruncated() {
		t.Fatalf("valid relation: %v / %v", good, err)
	}
	for name, mutate := range map[string]func(*patterns.ListResult){
		"wrong generation":    func(r *patterns.ListResult) { r.Generation++ },
		"lost excluded":       func(r *patterns.ListResult) { r.ExcludedEventCount = 0 },
		"overflow eligible":   func(r *patterns.ListResult) { r.EligibleEventCount = math.MaxUint64 },
		"incomplete relation": func(r *patterns.ListResult) { r.Patterns[0].EventCount = 1 },
		"false completion":    func(r *patterns.ListResult) { r.SnapshotComplete = true },
		"short continuation":  func(r *patterns.ListResult) { r.NextPageToken = "next"; r.TotalSize = new(uint64(2)) },
		"inexact total":       func(r *patterns.ListResult) { r.TotalSizeExact = false },
		"duplicate pattern":   func(r *patterns.ListResult) { r.Patterns = append(r.Patterns, r.Patterns[0]) },
	} {
		t.Run(name, func(t *testing.T) {
			value := valid()
			mutate(&value)
			if _, err := patternsToProto(context.Background(), value, query, "snapshot"); err == nil {
				t.Fatal("invalid relation accepted")
			}
		})
	}
	if _, err := patternPageToProto(1, 2, "", "next", new(uint64(3)), true, true, true); err != nil {
		t.Fatalf("byte-short member page rejected: %v", err)
	}
	if _, err := patternPageToProto(0, 2, "previous", "", new(uint64(3)), true, true, true); err == nil {
		t.Fatal("empty continuation accepted")
	}
}

func TestPatternAPISanitizersPreserveOpaqueIdentitiesAndProjection(t *testing.T) {
	input := &opensplunk.ListSearchPatternMembersRequest{SearchJobId: " job ", PatternId: " opaque ", SnapshotRef: " snapshot ",
		Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BALANCED,
		Page:        &opensplunk.PageRequest{PageSize: new(uint32(2)), PageToken: new(" cursor ")}, Columns: []string{" _raw", "Count"}}
	got, err := sanitizePatternMembers(context.Background(), input)
	if err != nil || got.SearchJobId != "job" || got.PatternId != " opaque " || got.SnapshotRef != " snapshot " || got.Page.GetPageToken() != " cursor " || got.Columns[0] != " _raw" {
		t.Fatalf("sanitizer rewrote exact identities: %v / %v", got, err)
	}
	for _, mutate := range []func(*opensplunk.ListSearchPatternMembersRequest){
		func(r *opensplunk.ListSearchPatternMembersRequest) { r.SnapshotRef = "" },
		func(r *opensplunk.ListSearchPatternMembersRequest) { r.PatternId = "" },
		func(r *opensplunk.ListSearchPatternMembersRequest) { r.Sensitivity = 99 },
		func(r *opensplunk.ListSearchPatternMembersRequest) { r.Page.PageSize = new(uint32(0)) },
		func(r *opensplunk.ListSearchPatternMembersRequest) { r.Columns = []string{"_raw", "_raw"} },
	} {
		value := proto.Clone(input).(*opensplunk.ListSearchPatternMembersRequest)
		mutate(value)
		if _, err := sanitizePatternMembers(context.Background(), value); err == nil {
			t.Fatal("invalid member request accepted")
		}
	}
}

type patternAPIFake struct {
	calls  int
	query  patterns.ListRequest
	result patterns.ListResult
	err    error
}

func (*patternAPIFake) MaximumPageSize() int { return patterns.MaximumPageSize }

func (fake *patternAPIFake) List(_ context.Context, _ searchjobs.AccessScope, query patterns.ListRequest) (patterns.ListResult, error) {
	fake.calls++
	fake.query = query
	return fake.result, fake.err
}
func (fake *patternAPIFake) Members(context.Context, searchjobs.AccessScope, patterns.MemberRequest) (patterns.MemberResult, error) {
	panic("unexpected member request")
}

type patternFailingWriter struct {
	header http.Header
	writes int
}

func (writer *patternFailingWriter) Header() http.Header { return writer.header }
func (*patternFailingWriter) WriteHeader(int)            {}
func (writer *patternFailingWriter) Write([]byte) (int, error) {
	writer.writes++
	return 0, errors.New("disconnected")
}

func TestPatternAPIValidatesSnapshotBeforeAnalysisAndReleasesSerialization(t *testing.T) {
	fake := &patternAPIFake{result: patterns.ListResult{Generation: 7, SnapshotComplete: true, TotalSize: new(uint64(0)), TotalSizeExact: true}}
	handler := &apiHandler{serializationGate: make(chan struct{}, 1)}
	api := &patternAPI{handler: handler, service: fake, parseSnapshot: func(job, ref string) (uint64, error) {
		if job != "job" || ref != "snapshot" {
			return 0, searchjobs.ErrInvalidCursor
		}
		return 7, nil
	}}
	input := &opensplunk.ListSearchPatternsRequest{SearchJobId: "job", SnapshotRef: "invalid", Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_PRECISE,
		Page: &opensplunk.PageRequest{IncludeTotalSize: true}}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
	if _, err := api.list(request, input); err == nil || fake.calls != 0 {
		t.Fatalf("unvalidated snapshot reached analysis: %v", err)
	} else {
		assertHTTPErrorStatus(t, err, http.StatusBadRequest)
	}
	input.SnapshotRef = "snapshot"
	response, err := api.list(request, input)
	if err != nil || fake.query.Generation != 7 || len(handler.serializationGate) != 1 {
		t.Fatalf("list/gate = %v / %d", err, len(handler.serializationGate))
	}
	writer := &patternFailingWriter{header: make(http.Header)}
	if err := newSerializedPatternsCodec().Encode(writer, response); err == nil || writer.writes != 1 || len(handler.serializationGate) != 0 {
		t.Fatalf("write failure retained permit: %v", err)
	}
	response, err = api.list(request, input)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	response.ctx = ctx
	writer = &patternFailingWriter{header: make(http.Header)}
	if err := newSerializedPatternsCodec().Encode(writer, response); err == nil || writer.writes != 0 || len(handler.serializationGate) != 0 {
		t.Fatalf("canceled encoding wrote or leaked: %v", err)
	}
	fake.result.Generation++
	if _, err := api.list(request, input); err == nil || len(handler.serializationGate) != 0 {
		t.Fatalf("invalid conversion leaked permit: %v", err)
	}
}

func TestPatternAPIMapsBoundedAndStaleFailures(t *testing.T) {
	for _, test := range []struct {
		err    error
		status int
	}{
		{patterns.ErrGenerationMismatch, http.StatusConflict}, {patterns.ErrInvalidCursor, http.StatusBadRequest},
		{patterns.ErrLimit, http.StatusUnprocessableEntity}, {patterns.ErrCapacity, http.StatusTooManyRequests},
		{patterns.ErrPatternNotFound, http.StatusNotFound}, {searchjobs.ErrExpired, http.StatusGone},
		{patterns.ErrClosed, http.StatusServiceUnavailable}, {context.Canceled, http.StatusRequestTimeout},
		{searchartifacts.ErrNotFound, http.StatusNotFound}, {searchartifacts.ErrExpired, http.StatusGone},
		{searchartifacts.ErrCapacity, http.StatusTooManyRequests}, {searchartifacts.ErrCorrupt, http.StatusConflict},
		{searchartifacts.ErrClosed, http.StatusServiceUnavailable}, {searchartifacts.ErrInvalidCursor, http.StatusBadRequest},
	} {
		assertHTTPErrorStatus(t, mapPatternsCallError(context.Background(), test.err), test.status)
	}
}

type patternMemberAPIFake struct {
	patternAPIFake
	memberResult patterns.MemberResult
	memberQuery  patterns.MemberRequest
}

func (fake *patternMemberAPIFake) Members(_ context.Context, _ searchjobs.AccessScope, query patterns.MemberRequest) (patterns.MemberResult, error) {
	fake.calls++
	fake.memberQuery = query
	return fake.memberResult, fake.err
}

func TestPatternMemberAPIHoldsPermitThroughWriteAndRejectsSnapshot(t *testing.T) {
	fake := &patternMemberAPIFake{memberResult: patterns.MemberResult{PatternID: "opaque", Generation: 7,
		Schema: searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw", Kind: searchjobs.ValueKindString}}},
		Rows:   []searchjobs.ResultRow{{Ordinal: 12, Values: []searchjobs.Value{searchjobs.StringValue("event")}}},
	}}
	handler := &apiHandler{serializationGate: make(chan struct{}, 1)}
	api := &patternAPI{handler: handler, service: fake, parseSnapshot: func(_, ref string) (uint64, error) {
		if ref != "snapshot" {
			return 0, searchjobs.ErrInvalidCursor
		}
		return 7, nil
	}}
	input := &opensplunk.ListSearchPatternMembersRequest{SearchJobId: "job", PatternId: "opaque", SnapshotRef: "invalid",
		Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BALANCED, Columns: []string{"_raw"}}
	request := httptest.NewRequestWithContext(t.Context(), http.MethodPost, "/", nil)
	_, err := api.members(request, input)
	assertHTTPErrorStatus(t, err, http.StatusBadRequest)
	if fake.calls != 0 {
		t.Fatal("invalid member snapshot reached analysis")
	}
	input.SnapshotRef = "snapshot"
	response, err := api.members(request, input)
	if err != nil || len(handler.serializationGate) != 1 || fake.memberQuery.Generation != 7 || fake.memberQuery.Columns[0] != "_raw" {
		t.Fatalf("member handler = %v", err)
	}
	writer := &patternFailingWriter{header: make(http.Header)}
	if err := newSerializedPatternMembersCodec().Encode(writer, response); err == nil || len(handler.serializationGate) != 0 {
		t.Fatalf("member write leaked permit: %v", err)
	}
	fake.memberResult.PatternID = "wrong"
	if _, err := api.members(request, input); err == nil || len(handler.serializationGate) != 0 {
		t.Fatalf("member mismatch leaked permit: %v", err)
	}
}

func TestPatternMemberSchemaIdentityBindsGenerationAndProjection(t *testing.T) {
	schema := searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw"}, {Name: "count"}}}
	first := patternMemberSchemaID("job", 7, schema)
	if first != patternMemberSchemaID("job", 7, schema) {
		t.Fatal("schema identity is unstable")
	}
	for _, other := range []string{
		patternMemberSchemaID("other", 7, schema),
		patternMemberSchemaID("job", 8, schema),
		patternMemberSchemaID("job", 7, searchjobs.Schema{Columns: []searchjobs.Column{schema.Columns[1], schema.Columns[0]}}),
		patternMemberSchemaID("job", 7, searchjobs.Schema{Columns: []searchjobs.Column{{Name: "_raw\x00count"}}}),
	} {
		if first == other {
			t.Fatal("distinct immutable schema identity collided")
		}
	}
}
