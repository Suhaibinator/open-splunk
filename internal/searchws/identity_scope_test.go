package searchws

import (
	"context"
	"errors"
	"net/http"
	"testing"

	exportjobs "github.com/Suhaibinator/open-splunk/internal/export"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
)

func TestDynamicIdentitySeparatesJournalsAndRevalidatesReplay(t *testing.T) {
	job := scopedSearchJob("shared-job")
	job.State = searchjobs.StateCompleted
	job.FinishedAt = job.StartedAt
	job.OrganizationID = "7"
	reader := newMutableSearchSnapshots(job)
	allowed := true
	checks := 0
	service, err := New(Config{
		Searches: adaptSynchronousSearchSnapshots(reader), Exports: notFoundExportSnapshots{},
		AccessForRequest: func(*http.Request) (searchjobs.AccessScope, error) { return searchjobs.AccessScope{}, nil },
		AuthorizeJob: func(_ context.Context, scope searchjobs.AccessScope, actual searchjobs.Job) error {
			checks++
			if !allowed || scope.OwnerID != "owner" || actual.OrganizationID != "7" {
				return errors.New("revoked")
			}
			return nil
		},
		AuthorizeExport: func(context.Context, searchjobs.AccessScope, exportjobs.Job) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	defer service.Close(context.Background())
	scope := searchjobs.AccessScope{TenantID: "tenant", OwnerID: "owner", OrganizationID: "7"}
	key := targetKey{kind: targetKindSearch, id: job.ID, access: scope}
	target, err := service.resolveTarget(context.Background(), key)
	if err != nil {
		t.Fatal(err)
	}
	defer service.releaseResolvedTarget(target)
	if checks == 0 {
		t.Fatal("initial projection did not check current permissions")
	}
	allowed = false
	if err := target.refreshForSubscription(context.Background(), 0, false); !errors.Is(err, errTargetNotFound) {
		t.Fatalf("cached terminal replay survived revocation: %v", err)
	}
	if checks < 2 {
		t.Fatal("terminal replay reused its prior grant")
	}
	otherScope := scope
	otherScope.OwnerID = "another-user"
	if _, err := service.resolveTarget(context.Background(), targetKey{kind: targetKindSearch, id: job.ID, access: otherScope}); !errors.Is(err, errTargetNotFound) {
		t.Fatalf("another user reused first user's journal: %v", err)
	}
}

func TestDynamicIdentityRequiresBothResourceAuthorizers(t *testing.T) {
	_, err := New(Config{Searches: adaptSynchronousSearchSnapshots(stubSearchSnapshots{}), Exports: stubExportSnapshots{}, AccessForRequest: func(*http.Request) (searchjobs.AccessScope, error) { return searchjobs.AccessScope{}, nil }})
	if err == nil {
		t.Fatal("dynamic user access accepted without resource authorization")
	}
}
