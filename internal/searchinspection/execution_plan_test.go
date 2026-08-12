package searchinspection

import (
	"slices"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchsnapshot"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

// buildInspectionAuthoredPlan keeps projection-only fixtures outside the
// manager-attested retained-execution boundary. Production inspection always
// receives a Manager-minted ExecutionSnapshot and uses BuildExecutionPlan;
// tests that only exercise authored projection use the explicit Job rebuild.
func buildInspectionAuthoredPlan(
	snapshot searchjobs.ExecutionSnapshot,
) (*plan.Query, error) {
	return searchsnapshot.BuildPlan(searchjobs.Job{
		ID:               snapshot.ID,
		OwnerID:          snapshot.OwnerID,
		TenantID:         snapshot.TenantID,
		AppID:            snapshot.AppID,
		SPL:              snapshot.SPL,
		EffectiveIndexes: slices.Clone(snapshot.EffectiveIndexes),
		Earliest:         snapshot.Earliest,
		Latest:           snapshot.Latest,
		CreatedAt:        snapshot.SearchStart,
		TimeRange: searchtime.Intent{
			Timezone: snapshot.SearchTimezone,
		},
		IndexTimeCutoff:  snapshot.IndexTimeCutoff,
		VisibilityCutoff: snapshot.VisibilityCutoff,
	})
}
