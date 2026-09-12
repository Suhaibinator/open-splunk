package server

import (
	"context"
	"errors"
	"net/http/httptest"
	"testing"
	"time"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/audit"
	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"google.golang.org/protobuf/proto"
)

type receiptExports struct {
	fakeExports
	replay func(context.Context, searchjobs.AccessScope, requestidempotency.Intent) (exportjobs.Job, bool, error)
	admit  func(context.Context, searchjobs.AccessScope, exportjobs.CreateRequest, requestidempotency.Intent) (exportjobs.Job, bool, error)
}

func (service *receiptExports) ReplayIdempotent(ctx context.Context, access searchjobs.AccessScope, intent requestidempotency.Intent) (exportjobs.Job, bool, error) {
	return service.replay(ctx, access, intent)
}
func (service *receiptExports) CreateIdempotent(ctx context.Context, access searchjobs.AccessScope, request exportjobs.CreateRequest, intent requestidempotency.Intent) (exportjobs.Job, bool, error) {
	return service.admit(ctx, access, request, intent)
}
func exportReceiptContext(t *testing.T) context.Context {
	t.Helper()
	ctx, err := audit.WithActor(context.Background(), audit.Actor{Kind: audit.ActorKindBrowser, ID: "stable-user", Role: audit.ActorRoleUser})
	if err != nil {
		t.Fatal(err)
	}
	return ctx
}
func TestExportReceiptPrecedesCreateOnlyConversionAndReturnsCurrentMetadata(t *testing.T) {
	input := &opensplunk.CreateExportJobRequest{ClientRequestId: new("logical-export-request")}
	current := testExportJob("accepted-export", exportjobs.FormatCSV, exportjobs.StateCanceled)
	current.Version = 9
	calls := 0
	service := &receiptExports{replay: func(_ context.Context, scope searchjobs.AccessScope, intent requestidempotency.Intent) (exportjobs.Job, bool, error) {
		calls++
		if scope.TenantID != "tenant" || scope.OwnerID != "owner" || intent.ActorID != "stable-user" || intent.Route != requestidempotency.RouteCreateExportJob {
			t.Fatalf("incorrect replay authority: %+v %+v", scope, intent)
		}
		canonical := proto.Clone(input).(*opensplunk.CreateExportJobRequest)
		canonical.ClientRequestId = nil
		expected, err := requestidempotency.NewIntent("tenant", "browser", "stable-user", requestidempotency.RouteCreateExportJob, input.GetClientRequestId(), canonical)
		if err != nil || intent != expected {
			t.Fatalf("intent not sealed from caller input: %+v %v", intent, err)
		}
		return current, true, nil
	}}
	handler := &apiHandler{exports: service, tenantID: "tenant", ownerID: "owner", now: func() time.Time { return testNow }}
	request := httptest.NewRequestWithContext(exportReceiptContext(t), "POST", "/api/search/exports/create", nil)
	response, err := handler.createExportJob(request, input)
	if err != nil || !response.GetReplayed() || response.GetExportJob().GetStateVersion() != 9 || response.GetExportJob().GetState() != opensplunk.ExportJobState_EXPORT_JOB_STATE_CANCELED || response.GetExportJob().GetExportJobId() != current.ID {
		t.Fatalf("replay=%v %v", response, err)
	}
	if calls != 1 || service.createCalls != 0 {
		t.Fatal("replay invoked create")
	}
	// Missing authenticated identity never reaches receipt lookup.
	if _, err := handler.createExportJob(httptest.NewRequestWithContext(t.Context(), "POST", "/", nil), input); err == nil || calls != 1 {
		t.Fatalf("unauthenticated receipt disclosure: %v calls=%d", err, calls)
	}
}
func TestExportReceiptConflictDoesNotReachAdmission(t *testing.T) {
	service := &receiptExports{replay: func(context.Context, searchjobs.AccessScope, requestidempotency.Intent) (exportjobs.Job, bool, error) {
		return exportjobs.Job{}, true, requestidempotency.ErrConflict
	}}
	handler := &apiHandler{exports: service, tenantID: "tenant", ownerID: "owner"}
	request := httptest.NewRequestWithContext(exportReceiptContext(t), "POST", "/", nil)
	_, err := handler.createExportJob(request, &opensplunk.CreateExportJobRequest{ClientRequestId: new("logical-export-request")})
	if err == nil || err.Error() != mapRequestIdempotencyError(requestidempotency.ErrConflict).Error() {
		t.Fatalf("conflict=%v", err)
	}
}
func TestExportAcceptanceWinsConcurrentRequestCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(exportReceiptContext(t))
	defer cancel()
	accepted := testExportJob("accepted-export", exportjobs.FormatCSV, exportjobs.StateQueued)
	service := &receiptExports{
		replay: func(context.Context, searchjobs.AccessScope, requestidempotency.Intent) (exportjobs.Job, bool, error) {
			return exportjobs.Job{}, false, nil
		},
		admit: func(_ context.Context, _ searchjobs.AccessScope, request exportjobs.CreateRequest, _ requestidempotency.Intent) (exportjobs.Job, bool, error) {
			if request.SearchJobID != "search-1" {
				return exportjobs.Job{}, false, errors.New("wrong source")
			}
			cancel()
			return accepted, false, nil
		},
	}
	handler := &apiHandler{exports: service, tenantID: "tenant", ownerID: "owner", now: func() time.Time { return testNow }}
	response, err := handler.createExportJob(httptest.NewRequestWithContext(ctx, "POST", "/", nil), &opensplunk.CreateExportJobRequest{ClientRequestId: new("logical-export-request"), Definition: csvExportDefinition("search-1")})
	if err != nil || response.GetReplayed() || response.GetExportJob().GetExportJobId() != accepted.ID {
		t.Fatalf("committed cancellation=%v %v", response, err)
	}
}
