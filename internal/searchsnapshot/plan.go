// Package searchsnapshot reconstructs trusted logical plans from immutable
// search-job metadata. It is the single re-execution boundary for derived
// analyses and exports: caller-provided SQL and mutable authorization state
// never enter this package.
package searchsnapshot

import (
	"slices"

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
	parsed, err := spl.Parse(job.SPL)
	if err != nil {
		return nil, err
	}
	visibilityCutoff := job.VisibilityCutoff
	indexes := slices.Clone(job.EffectiveIndexes)
	return plan.Build(parsed, plan.Scope{
		TenantID:          job.TenantID,
		AuthorizedIndexes: indexes,
		RequestedIndexes:  slices.Clone(indexes),
		Earliest:          job.Earliest,
		Latest:            job.Latest,
		IndexTimeCutoff:   job.IndexTimeCutoff,
		VisibilityCutoff:  &visibilityCutoff,
	})
}
