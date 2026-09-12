package server

import (
	"context"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/requestidempotency"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

type racingReceiptSearchJobs struct {
	fakeSearchJobs
	current searchjobs.Job
}

func (jobs *racingReceiptSearchJobs) ReplayIdempotent(context.Context, searchjobs.AccessScope, requestidempotency.Intent) (searchjobs.Job, bool, error) {
	return searchjobs.Job{}, false, nil
}

func (jobs *racingReceiptSearchJobs) CreateIdempotent(context.Context, searchjobs.CreateRequest, requestidempotency.Intent) (searchjobs.Job, bool, error) {
	return jobs.current, true, nil
}

type racingReceiptTrustedAdmission struct {
	recordingTrustedSearchAdmission
}

func (admission *racingReceiptTrustedAdmission) ReplayTrustedSearch(context.Context, searchjobs.AccessScope, requestidempotency.Intent) (searchjobs.Job, bool, error) {
	return searchjobs.Job{}, false, nil
}

func (admission *racingReceiptTrustedAdmission) AdmitTrustedSearchIdempotent(context.Context, TrustedSearchAdmissionRequest, requestidempotency.Intent) (searchjobs.Job, bool, error) {
	return admission.job, true, nil
}

func TestSearchAdmissionRaceReturnsCurrentReplay(t *testing.T) {
	for _, trusted := range []bool{false, true} {
		name := "direct"
		if trusted {
			name = "trusted"
		}
		t.Run(name, func(t *testing.T) {
			current := completeJobForApp("accepted-search", "accepted-app")
			current.Version = 9
			jobs := &racingReceiptSearchJobs{current: current}
			handler := &apiHandler{
				jobs: jobs, ownerID: "owner-1", tenantID: "tenant-1",
				now:     func() time.Time { return testNow },
				indexes: fakeIndexCatalog{indexes: []control.Index{{State: control.IndexStateActive, Definition: control.IndexDefinition{Name: "main", SearchEnabled: true}}}},
			}
			if trusted {
				admission := &racingReceiptTrustedAdmission{}
				admission.job = current
				handler.trustedSearchAdmission = admission
			}
			input := createRequest("-1h", "now", "main")
			input.ClientRequestId = new("logical-search-request")
			// A dynamic default/source may have changed since the racing winner accepted.
			input.Definition.AppId = new("resolved-app")
			response, err := handler.createSearchJob(newAPIRequest(exportReceiptContext(t)), input)
			if err != nil || !response.GetReplayed() || response.GetSearchJob().GetSearchJobId() != current.ID || response.GetSearchJob().GetStateVersion() != current.Version || response.GetSearchJob().GetDefinition().GetAppId() != current.AppID {
				t.Fatalf("racing replay = %v, %v", response, err)
			}
		})
	}
}
