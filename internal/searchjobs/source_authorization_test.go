package searchjobs

import (
	"context"
	"errors"
	"slices"
	"sync/atomic"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
)

func TestKnowledgeInventoryAuthorizationUsesSealedSourceSnapshot(t *testing.T) {
	for _, revoke := range []bool{false, true} {
		name := "allowed"
		if revoke {
			name = "revoked before inventory"
		}
		t.Run(name, func(t *testing.T) {
			resolver, appID := newCalculatedFieldKnowledgeResolver(t)
			executor := &statsWildcardTestExecutor{inventoryFields: []string{"knowledge_latency"}}
			var inventoryChecks atomic.Int64
			manager := newTestManager(t, Config{
				Executor: executor, KnowledgeResolver: resolver, CleanupInterval: -1,
				NewID: sequenceIDs("private-source-authority"),
				AuthorizeJob: func(_ context.Context, access AccessScope, job Job) error {
					if job.OwnerID != access.OwnerID || job.OrganizationID != "1" || job.AppID != appID ||
						!slices.Equal(job.EffectiveIndexes, []string{"main"}) ||
						len(job.KnowledgeObjectIDs) != 1 ||
						knowledgesnapshot.ValidateObjectIDs(job.KnowledgeSnapshot, job.KnowledgeObjectIDs) != nil {
						return errors.New("source authorization received incomplete private authority")
					}
					if job.ID == "" && job.State == StatePlanning {
						inventoryChecks.Add(1)
						if revoke {
							return errors.New("private knowledge source was revoked")
						}
					}
					return nil
				},
			})
			request := validRequest()
			request.AppID = appID
			request.SPL = `index=main | stats sum(knowledge_*)`
			created, err := manager.Create(t.Context(), request)
			if revoke {
				if !errors.Is(err, ErrSourceForbidden) {
					t.Fatalf("revoked admission error = %v, want source authorization failure", err)
				}
				assertEmptyManagerAdmissionState(t, manager)
			} else {
				if err != nil {
					t.Fatal(err)
				}
				waitForState(t, manager, created.ID, StateCompleted)
			}
			inventory, execution := executor.calls()
			want := 1
			if revoke {
				want = 0
			}
			if inventoryChecks.Load() != 1 || inventory != want || execution != want {
				t.Fatalf("checks/inventory/execution = %d/%d/%d, want 1/%d/%d", inventoryChecks.Load(), inventory, execution, want, want)
			}
		})
	}
}
