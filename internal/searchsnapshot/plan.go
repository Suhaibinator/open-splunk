// Package searchsnapshot reconstructs trusted logical plans from immutable
// search-job metadata. It is the single re-execution boundary for derived
// analyses and exports: caller-provided SQL and mutable authorization state
// never enter this package.
package searchsnapshot

import (
	"errors"
	"slices"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/spl"
)

// BuildPlan parses the original SPL and rebuilds its logical plan against the
// exact tenant, index, time, and storage-visibility snapshot retained by job.
// EffectiveIndexes is already the authorization intersection selected by the
// original plan, so it deliberately supplies both scope inputs here. Reusing
// RequestedIndexes could widen or otherwise change a completed search.
func BuildPlan(job searchjobs.Job) (*plan.Query, error) {
	return buildPlan(planSnapshot{
		spl:              job.SPL,
		tenantID:         job.TenantID,
		effectiveIndexes: job.EffectiveIndexes,
		earliest:         job.Earliest,
		latest:           job.Latest,
		indexTimeCutoff:  job.IndexTimeCutoff,
		visibilityCutoff: job.VisibilityCutoff,
	})
}

// BuildExecutionPlan rebuilds a logical plan from Manager's lightweight,
// completed execution snapshot without acquiring or copying result rows.
func BuildExecutionPlan(snapshot searchjobs.ExecutionSnapshot) (*plan.Query, error) {
	return buildPlan(planSnapshot{
		spl:              snapshot.SPL,
		tenantID:         snapshot.TenantID,
		effectiveIndexes: snapshot.EffectiveIndexes,
		earliest:         snapshot.Earliest,
		latest:           snapshot.Latest,
		indexTimeCutoff:  snapshot.IndexTimeCutoff,
		visibilityCutoff: snapshot.VisibilityCutoff,
	})
}

type planSnapshot struct {
	spl              string
	tenantID         string
	effectiveIndexes []string
	earliest         time.Time
	latest           time.Time
	indexTimeCutoff  time.Time
	visibilityCutoff uint64
}

func buildPlan(snapshot planSnapshot) (*plan.Query, error) {
	parsed, err := spl.Parse(snapshot.spl)
	if err != nil {
		return nil, err
	}
	visibilityCutoff := snapshot.visibilityCutoff
	indexes := slices.Clone(snapshot.effectiveIndexes)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          snapshot.tenantID,
		AuthorizedIndexes: indexes,
		RequestedIndexes:  slices.Clone(indexes),
		Earliest:          snapshot.earliest,
		Latest:            snapshot.latest,
		IndexTimeCutoff:   snapshot.indexTimeCutoff,
		VisibilityCutoff:  &visibilityCutoff,
	})
	if err != nil {
		return nil, err
	}
	wantIndexes := slices.Clone(indexes)
	slices.Sort(wantIndexes)
	wantIndexes = slices.Compact(wantIndexes)
	if !slices.Equal(logical.EffectiveIndexes, wantIndexes) {
		return nil, errors.New("rebuild immutable search plan: effective index scope changed")
	}
	return logical, nil
}
