package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/patterns"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type snapshotAdmissionExports struct {
	fakeExports
	replayJob   exportjobs.Job
	replayFound bool
	replayCalls int
	keyedCalls  int
	request     exportjobs.CreateRequest
}

func (exports *snapshotAdmissionExports) ReplayIdempotent(_ context.Context, _ searchjobs.AccessScope, _ requestidempotency.Intent) (exportjobs.Job, bool, error) {
	exports.replayCalls++
	return exports.replayJob, exports.replayFound, nil
}

func (exports *snapshotAdmissionExports) CreateIdempotent(ctx context.Context, access searchjobs.AccessScope, request exportjobs.CreateRequest, _ requestidempotency.Intent) (exportjobs.Job, bool, error) {
	exports.keyedCalls++
	job, err := exports.Create(ctx, access, request)
	return job, false, err
}

func TestPatternExportResolvesSnapshotOnlyForFreshAdmission(t *testing.T) {
	for _, keyed := range []bool{false, true} {
		t.Run(map[bool]string{false: "unkeyed", true: "keyed"}[keyed], func(t *testing.T) {
			exports := &snapshotAdmissionExports{}
			handler := nearbyTestAPIHandler(&fakeSearchJobs{})
			handler.exports = exports
			handler.now = func() time.Time { return testNow }
			exports.createFn = func(_ context.Context, access searchjobs.AccessScope, request exportjobs.CreateRequest) (exportjobs.Job, error) {
				assertExportScope(t, access, "tenant-1", "owner-1")
				exports.request = request
				job := testExportJob("export", exportjobs.FormatCSV, exportjobs.StateQueued)
				job.SourceKind, job.Pattern = request.SourceKind, request.Pattern
				return job, nil
			}
			ref, err := handler.resultSnapshotRef("search-1", 42)
			if err != nil {
				t.Fatal(err)
			}
			definition := csvExportDefinition("search-1")
			definition.Source = &opensplunk.ExportDefinition_PatternMembers{PatternMembers: &opensplunk.PatternMemberExportSource{SearchJobId: "search-1", SnapshotRef: ref, Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_BALANCED, PatternId: "member-id"}}
			input := &opensplunk.CreateExportJobRequest{Definition: definition}
			if keyed {
				input.ClientRequestId = new("logical-export-action")
			}
			ctx, err := audit.WithActor(context.Background(), audit.Actor{Kind: audit.ActorKindBrowser, ID: "administrator", Role: audit.ActorRoleAdministrator})
			if err != nil {
				t.Fatal(err)
			}
			request := httptest.NewRequestWithContext(ctx, http.MethodPost, "/", nil)
			if _, err := handler.createExportJob(request, input); err != nil {
				t.Fatal(err)
			}
			if exports.request.Pattern == nil || exports.request.Pattern.Generation != 42 || exports.request.Pattern.SnapshotRef != ref || exports.request.Pattern.PatternID != "member-id" {
				t.Fatalf("resolved source = %+v", exports.request.Pattern)
			}
			if keyed && (exports.replayCalls != 1 || exports.keyedCalls != 1) {
				t.Fatalf("receipt admission calls = %d/%d", exports.replayCalls, exports.keyedCalls)
			}
			definition.GetPatternMembers().SnapshotRef = "invalid"
			_, err = handler.createExportJob(request, input)
			assertHTTPErrorStatus(t, err, http.StatusBadRequest)
			if exports.createCalls != 1 {
				t.Fatal("invalid snapshot reached export creation")
			}
		})
	}
}

func TestPatternExportReceiptSurvivesPublicSnapshotKeyRotation(t *testing.T) {
	job := testExportJob("accepted-export", exportjobs.FormatCSV, exportjobs.StateCompleted)
	job.SourceKind = exportjobs.SourcePatternMembers
	job.Pattern = &patterns.ExportRequest{SearchJobID: job.SearchJobID, SnapshotRef: "prior-process-reference", Generation: 42, Sensitivity: patterns.Precise, PatternID: "accepted-member"}
	exports := &snapshotAdmissionExports{replayJob: job, replayFound: true}
	handler := nearbyTestAPIHandler(&fakeSearchJobs{})
	handler.exports = exports
	handler.now = func() time.Time { return testNow }
	definition := csvExportDefinition(job.SearchJobID)
	definition.Source = &opensplunk.ExportDefinition_PatternMembers{PatternMembers: &opensplunk.PatternMemberExportSource{SearchJobId: job.SearchJobID, SnapshotRef: job.Pattern.SnapshotRef, Sensitivity: opensplunk.PatternSensitivity_PATTERN_SENSITIVITY_PRECISE, PatternId: job.Pattern.PatternID}}
	ctx, err := audit.WithActor(context.Background(), audit.Actor{Kind: audit.ActorKindBrowser, ID: "administrator", Role: audit.ActorRoleAdministrator})
	if err != nil {
		t.Fatal(err)
	}
	response, err := handler.createExportJob(httptest.NewRequestWithContext(ctx, http.MethodPost, "/", nil), &opensplunk.CreateExportJobRequest{Definition: definition, ClientRequestId: new("accepted-export-action")})
	if err != nil {
		t.Fatal(err)
	}
	if !response.GetReplayed() || response.GetExportJob().GetExportJobId() != job.ID || exports.replayCalls != 1 || exports.keyedCalls != 0 || exports.createCalls != 0 {
		t.Fatalf("replay dispatched work: response %v, calls %+v", response, exports)
	}
}
