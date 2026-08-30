package server

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type recordingTrustedSearchAdmission struct {
	request TrustedSearchAdmissionRequest
	job     searchjobs.Job
}

func (admission *recordingTrustedSearchAdmission) AdmitTrustedSearch(_ context.Context, request TrustedSearchAdmissionRequest) (searchjobs.Job, error) {
	admission.request = request
	return admission.job, nil
}

func TestInteractiveCreateUsesSharedTrustedAdmission(t *testing.T) {
	t.Parallel()
	job := completeJobForApp("job-trusted", "search")
	admission := &recordingTrustedSearchAdmission{job: job}
	handler := &apiHandler{
		trustedSearchAdmission: admission,
		ownerID:                "owner-1",
		tenantID:               "tenant-1",
		now:                    func() time.Time { return testNow },
	}
	request := createRequest("-1h", "now", "main")
	request.Definition.AppId = new("search")
	response, err := handler.createSearchJob(newAPIRequest(t.Context()), request)
	if err != nil {
		t.Fatalf("createSearchJob() error = %v", err)
	}
	if response.GetSearchJob().GetSearchJobId() != job.ID {
		t.Fatalf("created job = %q, want %q", response.GetSearchJob().GetSearchJobId(), job.ID)
	}
	got := admission.request
	if got.OwnerID != "owner-1" || got.TenantID != "tenant-1" || got.AppID != "search" ||
		len(got.IndexScope) != 1 || got.IndexScope[0] != "main" || got.Source.Origin != searchjobs.JobOriginAdHoc ||
		got.TimeRange.Earliest() != testNow.Add(-time.Hour) || got.TimeRange.Latest() != testNow {
		t.Fatalf("trusted admission request = %+v", got)
	}
}

func newAPIRequest(ctx context.Context) *http.Request {
	return httptest.NewRequestWithContext(ctx, http.MethodPost, "/api/search/jobs/create", nil)
}

var _ TrustedSearchAdmission = (*recordingTrustedSearchAdmission)(nil)
