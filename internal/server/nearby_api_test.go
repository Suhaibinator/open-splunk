package server

import (
	"context"
	"errors"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/searchartifacts"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type nearbyTestSearchJobs struct {
	*fakeSearchJobs
	result searchjobs.NearbyContext
	err    error
	access searchjobs.AccessScope
	id     string
	gen    uint64
	row    uint64
	calls  int
}

func (jobs *nearbyTestSearchJobs) NearbyContextFor(
	_ context.Context,
	access searchjobs.AccessScope,
	id string,
	generation uint64,
	ordinal uint64,
) (searchjobs.NearbyContext, error) {
	jobs.calls++
	jobs.access = access
	jobs.id = id
	jobs.gen = generation
	jobs.row = ordinal
	return jobs.result, jobs.err
}

func TestResultSnapshotRefBindsJobGenerationAndProcessKey(t *testing.T) {
	first := &apiHandler{}
	second := &apiHandler{}
	for index := range first.searchArtifactCursorKey {
		first.searchArtifactCursorKey[index] = byte(index + 1)
		second.searchArtifactCursorKey[index] = byte(index + 2)
	}
	ref, err := first.resultSnapshotRef("job:with:colon", 42)
	if err != nil {
		t.Fatal(err)
	}
	if generation, err := first.parseResultSnapshotRef("job:with:colon", ref); err != nil || generation != 42 {
		t.Fatalf("snapshot round trip = %d, %v", generation, err)
	}
	tamperedSuffix := "A"
	if ref[len(ref)-1:] == tamperedSuffix {
		tamperedSuffix = "B"
	}
	for _, test := range []struct {
		name    string
		handler *apiHandler
		jobID   string
		ref     string
	}{
		{name: "other job", handler: first, jobID: "other", ref: ref},
		{name: "new process key", handler: second, jobID: "job:with:colon", ref: ref},
		{name: "tampered", handler: first, jobID: "job:with:colon", ref: ref[:len(ref)-1] + tamperedSuffix},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := test.handler.parseResultSnapshotRef(test.jobID, test.ref); !errors.Is(err, searchjobs.ErrInvalidCursor) {
				t.Fatalf("parseResultSnapshotRef() error = %v", err)
			}
		})
	}
}

func TestCompletedResultPagePublishesSnapshotRef(t *testing.T) {
	job := completeJob("snapshot-job")
	jobs := &fakeSearchJobs{getJob: job, resultsPage: searchjobs.ResultPage{
		Schema:    searchjobs.Schema{Columns: []searchjobs.Column{{Name: "message", Kind: searchjobs.ValueKindString}}},
		Rows:      []searchjobs.ResultRow{{Ordinal: 0, Values: []searchjobs.Value{searchjobs.StringValue("ok")}}},
		TotalRows: 1, Complete: true, Generation: 99,
	}}
	handler := nearbyTestAPIHandler(jobs)
	result, err := handler.getSearchResults(
		httptest.NewRequestWithContext(context.Background(), http.MethodPost, "/api/search/jobs/results", nil),
		&opensplunk.GetSearchResultsRequest{SearchJobId: job.ID},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer result.release()
	ref := result.message.GetResultPage().GetSnapshotRef()
	if ref == "" {
		t.Fatal("completed result page omitted snapshot reference")
	}
	if generation, err := handler.parseResultSnapshotRef(job.ID, ref); err != nil || generation != 99 {
		t.Fatalf("published snapshot = %d, %v", generation, err)
	}
}

func TestPrepareNearbyContextUsesFreshLiveAdmissionAndExactValues(t *testing.T) {
	decimal, err := searchjobs.DecimalValue("9007199254740993.000001")
	if err != nil {
		t.Fatal(err)
	}
	anchor := time.Date(2026, 9, 12, 6, 7, 8, 123456789, time.UTC)
	jobs := &nearbyTestSearchJobs{
		fakeSearchJobs: &fakeSearchJobs{},
		result: searchjobs.NearbyContext{
			AnchorTime: anchor, Earliest: anchor.Add(-5 * time.Minute), Latest: anchor.Add(5 * time.Minute),
			Index: "main", Host: "api", Source: "wild*'\\\n",
			Fields: []searchjobs.NearbyContextField{
				{Name: "large", Value: searchjobs.UnsignedValue(math.MaxUint64)},
				{Name: "decimal", Value: decimal, Suggested: true},
			},
		},
	}
	handler := nearbyTestAPIHandler(jobs)
	ref, err := handler.resultSnapshotRef("job:colon", 7)
	if err != nil {
		t.Fatal(err)
	}
	input := &opensplunk.PrepareNearbyContextRequest{
		SearchJobId: "job:colon", SnapshotRef: ref, RowId: "job:colon:17",
	}
	result, err := handler.prepareNearbyContext(
		httptest.NewRequestWithContext(context.Background(), http.MethodPost, nearbyContextPath, nil),
		input,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer result.release()
	response := result.message
	if jobs.calls != 1 || jobs.access != (searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "owner-1"}) ||
		jobs.id != "job:colon" || jobs.gen != 7 || jobs.row != 17 {
		t.Fatalf("live admission = calls %d access %+v id %q gen %d row %d", jobs.calls, jobs.access, jobs.id, jobs.gen, jobs.row)
	}
	if response.GetAnchorTime() != "2026-09-12T06:07:08.123456789Z" ||
		response.GetIndex() != "main" || response.GetSource() != "wild*'\\\n" {
		t.Fatalf("nearby response = %+v", response)
	}
	if got := response.GetFields()[0].GetValue().GetUint64Value(); got != math.MaxUint64 {
		t.Fatalf("uint64 = %d", got)
	}
	if got := response.GetFields()[1].GetValue().GetDecimalValue().GetValue(); got != "9007199254740993.000001" {
		t.Fatalf("decimal = %q", got)
	}
}

func TestPrepareNearbyContextHonorsDurableTerminalAuthority(t *testing.T) {
	job := completeJob("publication-failed")
	artifacts := &launchSearchArtifacts{record: searchartifacts.Record{
		Job: job, State: searchartifacts.StateInterrupted,
		Visibility:     searchartifacts.VisibilityPrivate,
		RetentionClass: searchartifacts.RetentionManual,
		Lifetime:       time.Hour, ExpiresAt: testNow.Add(time.Hour),
	}}
	jobs := &nearbyTestSearchJobs{fakeSearchJobs: &fakeSearchJobs{}, result: searchjobs.NearbyContext{}}
	handler := nearbyTestAPIHandler(jobs)
	handler.searchArtifacts = artifacts
	ref, err := handler.resultSnapshotRef(job.ID, 1)
	if err != nil {
		t.Fatal(err)
	}
	_, err = handler.prepareNearbyContext(
		httptest.NewRequestWithContext(context.Background(), http.MethodPost, nearbyContextPath, nil),
		&opensplunk.PrepareNearbyContextRequest{SearchJobId: job.ID, SnapshotRef: ref, RowId: job.ID + ":0"},
	)
	assertHTTPErrorStatus(t, err, http.StatusConflict)
	if jobs.calls != 0 {
		t.Fatal("live context was exposed after durable publication compensation")
	}
}

type nearbyDurableArtifacts struct {
	record       searchartifacts.Record
	lease        *nearbyDurableLease
	getModes     []searchartifacts.AccessMode
	acquireScope searchjobs.AccessScope
}

func (artifacts *nearbyDurableArtifacts) Get(
	_ context.Context,
	_ searchjobs.AccessScope,
	_ string,
	mode searchartifacts.AccessMode,
) (searchartifacts.Record, error) {
	artifacts.getModes = append(artifacts.getModes, mode)
	return artifacts.record, nil
}

func (*nearbyDurableArtifacts) ListPage(context.Context, searchjobs.AccessScope, searchartifacts.ListRequest) (searchartifacts.ListPage, error) {
	return searchartifacts.ListPage{}, nil
}

func (artifacts *nearbyDurableArtifacts) Acquire(
	_ context.Context,
	access searchjobs.AccessScope,
	_ string,
) (searchartifacts.ResultLease, error) {
	artifacts.acquireScope = access
	copy := *artifacts.lease
	return &copy, nil
}

func (*nearbyDurableArtifacts) ShareExpected(context.Context, searchjobs.AccessScope, string, uint64) (searchartifacts.Record, error) {
	return searchartifacts.Record{}, searchartifacts.ErrInvalid
}

func (*nearbyDurableArtifacts) UpdateSettingsExpected(context.Context, searchjobs.AccessScope, string, searchartifacts.Settings, uint64) (searchartifacts.Record, error) {
	return searchartifacts.Record{}, searchartifacts.ErrInvalid
}

type nearbyDurableLease struct {
	schema     searchjobs.Schema
	row        searchjobs.ResultRow
	generation uint64
	seek       uint64
	read       bool
}

func (lease *nearbyDurableLease) Schema() searchjobs.Schema { return lease.schema }
func (lease *nearbyDurableLease) RowCount() uint64          { return lease.row.Ordinal + 1 }
func (*nearbyDurableLease) RowCountExact() bool             { return true }
func (*nearbyDurableLease) ResultsTruncated() bool          { return false }
func (lease *nearbyDurableLease) Generation() uint64        { return lease.generation }
func (lease *nearbyDurableLease) Seek(_ context.Context, ordinal uint64) error {
	lease.seek = ordinal
	return nil
}
func (lease *nearbyDurableLease) Next(context.Context) (searchjobs.ResultRow, bool, error) {
	if lease.read || lease.seek != lease.row.Ordinal {
		return searchjobs.ResultRow{}, false, nil
	}
	lease.read = true
	return lease.row, true, nil
}
func (*nearbyDurableLease) Close() error { return nil }

func TestPrepareNearbyContextReadsDurableGenerationAfterRestart(t *testing.T) {
	job := completeJob("durable-nearby")
	job.NearbyEventProvenance = &searchjobs.NearbyEventProvenance{
		Version:   searchjobs.NearbyEventProvenanceVersion,
		TimeIndex: 0, IndexIndex: 1, HostIndex: 2, SourceIndex: 3,
	}
	anchor := time.Date(2026, 9, 12, 7, 8, 9, 987654321, time.UTC)
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "index", Kind: searchjobs.ValueKindString},
		{Name: "host", Kind: searchjobs.ValueKindString},
		{Name: "source", Kind: searchjobs.ValueKindString},
	}}
	artifacts := &nearbyDurableArtifacts{
		record: searchartifacts.Record{
			Job: job, State: searchartifacts.StateCompleted,
			Visibility:     searchartifacts.VisibilityEveryone,
			RetentionClass: searchartifacts.RetentionShared,
			Lifetime:       time.Hour, ExpiresAt: testNow.Add(time.Hour), ArtifactPresent: true,
		},
		lease: &nearbyDurableLease{
			schema: schema, generation: 41,
			row: searchjobs.ResultRow{Ordinal: 5, Values: []searchjobs.Value{
				searchjobs.TimeValue(anchor), searchjobs.StringValue("shared"),
				searchjobs.StringValue("worker"), searchjobs.StringValue("restart.log"),
			}},
		},
	}
	handler := nearbyTestAPIHandler(&nearbyTestSearchJobs{
		fakeSearchJobs: &fakeSearchJobs{}, err: searchjobs.ErrNotFound,
	})
	handler.searchArtifacts = artifacts
	handler.ownerID = "viewer"
	ref, err := handler.resultSnapshotRef(job.ID, 41)
	if err != nil {
		t.Fatal(err)
	}
	result, err := handler.prepareNearbyContext(
		httptest.NewRequestWithContext(context.Background(), http.MethodPost, nearbyContextPath, nil),
		&opensplunk.PrepareNearbyContextRequest{
			SearchJobId: job.ID, SnapshotRef: ref, RowId: job.ID + ":5",
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	defer result.release()
	if result.message.GetAnchorTime() != "2026-09-12T07:08:09.987654321Z" || result.message.GetIndex() != "shared" {
		t.Fatalf("durable nearby response = %+v", result.message)
	}
	if artifacts.acquireScope != (searchjobs.AccessScope{TenantID: "tenant-1", OwnerID: "viewer"}) ||
		len(artifacts.getModes) != 2 || artifacts.getModes[0] != searchartifacts.AccessRefresh ||
		artifacts.getModes[1] != searchartifacts.AccessInspect {
		t.Fatalf("durable admissions = scope %+v modes %v", artifacts.acquireScope, artifacts.getModes)
	}
}

func TestPrepareNearbyContextRejectsStaleDurableIdentityAndLegacyMetadata(t *testing.T) {
	job := completeJob("durable-rejected")
	job.NearbyEventProvenance = &searchjobs.NearbyEventProvenance{
		Version:   searchjobs.NearbyEventProvenanceVersion,
		TimeIndex: 0, IndexIndex: 1, HostIndex: 2, SourceIndex: 3,
	}
	schema := searchjobs.Schema{Columns: []searchjobs.Column{
		{Name: "_time", Kind: searchjobs.ValueKindTime},
		{Name: "index", Kind: searchjobs.ValueKindString},
		{Name: "host", Kind: searchjobs.ValueKindString},
		{Name: "source", Kind: searchjobs.ValueKindString},
	}}
	row := searchjobs.ResultRow{Ordinal: 5, Values: []searchjobs.Value{
		searchjobs.TimeValue(testNow), searchjobs.StringValue("main"),
		searchjobs.StringValue("host"), searchjobs.StringValue("source"),
	}}
	for _, test := range []struct {
		name       string
		generation uint64
		rowID      string
		legacy     bool
	}{
		{name: "wrong generation", generation: 42, rowID: job.ID + ":5"},
		{name: "out of range", generation: 41, rowID: job.ID + ":6"},
		{name: "legacy provenance", generation: 41, rowID: job.ID + ":5", legacy: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			recordJob := job
			if test.legacy {
				recordJob.NearbyEventProvenance = nil
			}
			artifacts := &nearbyDurableArtifacts{
				record: searchartifacts.Record{
					Job: recordJob, State: searchartifacts.StateCompleted,
					Visibility:     searchartifacts.VisibilityPrivate,
					RetentionClass: searchartifacts.RetentionManual,
					Lifetime:       time.Hour, ExpiresAt: testNow.Add(time.Hour), ArtifactPresent: true,
				},
				lease: &nearbyDurableLease{schema: schema, row: row, generation: 41},
			}
			handler := nearbyTestAPIHandler(&nearbyTestSearchJobs{
				fakeSearchJobs: &fakeSearchJobs{}, err: searchjobs.ErrNotFound,
			})
			handler.searchArtifacts = artifacts
			ref, err := handler.resultSnapshotRef(job.ID, test.generation)
			if err != nil {
				t.Fatal(err)
			}
			_, err = handler.prepareNearbyContext(
				httptest.NewRequestWithContext(context.Background(), http.MethodPost, nearbyContextPath, nil),
				&opensplunk.PrepareNearbyContextRequest{
					SearchJobId: job.ID, SnapshotRef: ref, RowId: test.rowID,
				},
			)
			assertHTTPErrorStatus(t, err, http.StatusConflict)
		})
	}
}

func TestPrepareNearbyContextHonorsCancellationBeforeAdmissionAndSerialization(t *testing.T) {
	jobs := &nearbyTestSearchJobs{
		fakeSearchJobs: &fakeSearchJobs{},
		result: searchjobs.NearbyContext{
			AnchorTime: testNow, Earliest: testNow.Add(-5 * time.Minute),
			Latest: testNow.Add(5 * time.Minute), Index: "main",
		},
	}
	handler := nearbyTestAPIHandler(jobs)
	ref, err := handler.resultSnapshotRef("job", 1)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = handler.prepareNearbyContext(
		httptest.NewRequestWithContext(ctx, http.MethodPost, nearbyContextPath, nil),
		&opensplunk.PrepareNearbyContextRequest{SearchJobId: "job", SnapshotRef: ref, RowId: "job:0"},
	)
	if !errors.Is(err, context.Canceled) || jobs.calls != 0 {
		t.Fatalf("canceled admission = error %v, live calls %d", err, jobs.calls)
	}

	ctx, cancel = context.WithCancel(context.Background())
	cancel()
	_, err = handler.serializedNearbyContext(ctx, &opensplunk.PrepareNearbyContextRequest{}, jobs.result)
	if !errors.Is(err, context.Canceled) || len(handler.serializationGate) != 0 {
		t.Fatalf("canceled serialization = error %v, permits %d", err, len(handler.serializationGate))
	}
}

func TestNearbyRequestRejectsNoncanonicalIdentity(t *testing.T) {
	for _, test := range []struct {
		name  string
		input *opensplunk.PrepareNearbyContextRequest
	}{
		{name: "missing job", input: &opensplunk.PrepareNearbyContextRequest{SnapshotRef: "ref", RowId: "job:0"}},
		{name: "spaced ref", input: &opensplunk.PrepareNearbyContextRequest{SearchJobId: "job", SnapshotRef: " ref", RowId: "job:0"}},
		{name: "oversized row", input: &opensplunk.PrepareNearbyContextRequest{SearchJobId: "job", SnapshotRef: "ref", RowId: strings.Repeat("x", maximumNearbyRowIDBytes+1)}},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := sanitizePrepareNearbyContextRequest(context.Background(), test.input); err == nil {
				t.Fatal("sanitizePrepareNearbyContextRequest() accepted invalid identity")
			}
		})
	}
	for _, rowID := range []string{"job:00", "job:+1", "job:-1", "other:0", "job:"} {
		if _, err := parseNearbyRowID("job", rowID); err == nil {
			t.Fatalf("parseNearbyRowID(%q) succeeded", rowID)
		}
	}
}

func TestNearbyResponseCodecEnforcesEightMiBLimit(t *testing.T) {
	released := 0
	response := &opensplunk.PrepareNearbyContextResponse{Fields: []*opensplunk.NearbyContextField{{
		FieldName: "large",
		Value: &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{
			StringValue: strings.Repeat("x", maximumNearbyContextResponseBytes),
		}},
	}}}
	err := newSerializedNearbyContextCodec().Encode(
		httptest.NewRecorder(),
		&serializedNearbyContextResponse{
			message: response, ctx: context.Background(), release: func() { released++ },
		},
	)
	if err == nil || !strings.Contains(err.Error(), "byte limit") || released != 1 {
		t.Fatalf("Encode() error = %v, released = %d", err, released)
	}
}

func nearbyTestAPIHandler(jobs SearchJobs) *apiHandler {
	handler := &apiHandler{
		jobs: jobs, tenantID: "tenant-1", ownerID: "owner-1",
		serializationGate: make(chan struct{}, 1),
	}
	for index := range handler.searchArtifactCursorKey {
		handler.searchArtifactCursorKey[index] = byte(index + 1)
	}
	return handler
}
