// Package searchsnapshot reconstructs trusted logical plans from immutable
// search-job metadata. It is the single re-execution boundary for derived
// analyses and exports: caller-provided SQL and mutable authorization state
// never enter this package.
package searchsnapshot

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
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
		searchJobID:      job.ID,
		tenantID:         job.TenantID,
		effectiveIndexes: job.EffectiveIndexes,
		earliest:         job.Earliest,
		latest:           job.Latest,
		searchStart:      job.CreatedAt,
		searchTimezone:   job.TimeRange.Timezone,
		indexTimeCutoff:  job.IndexTimeCutoff,
		visibilityCutoff: job.VisibilityCutoff,
	})
}

// BuildExecutionPlan rebuilds a logical plan from Manager's lightweight,
// completed execution snapshot without acquiring or copying result rows.
func BuildExecutionPlan(snapshot searchjobs.ExecutionSnapshot) (*plan.Query, error) {
	prelude, preludePresent, err := snapshot.OpenRetainedKnowledgePrelude()
	if err != nil {
		return nil, fmt.Errorf("rebuild immutable search plan: open retained knowledge prelude: %w", err)
	}
	expansion, expansionPresent, err := snapshot.OpenRetainedStatsWildcardExpansion()
	if err != nil {
		return nil, fmt.Errorf("rebuild immutable search plan: open retained stats wildcard expansion: %w", err)
	}
	logical, err := buildPlan(planSnapshot{
		spl:                           snapshot.SPL,
		searchJobID:                   snapshot.ID,
		tenantID:                      snapshot.TenantID,
		effectiveIndexes:              snapshot.EffectiveIndexes,
		earliest:                      snapshot.Earliest,
		latest:                        snapshot.Latest,
		searchStart:                   snapshot.SearchStart,
		searchTimezone:                snapshot.SearchTimezone,
		indexTimeCutoff:               snapshot.IndexTimeCutoff,
		visibilityCutoff:              snapshot.VisibilityCutoff,
		statsWildcardExpansion:        expansion,
		statsWildcardExpansionPresent: expansionPresent,
	})
	if err != nil {
		return nil, err
	}
	if !preludePresent {
		return logical, nil
	}
	logical, err = plan.InjectKnowledgePrelude(logical, prelude)
	if err != nil {
		return nil, fmt.Errorf("rebuild immutable search plan: inject retained knowledge prelude: %w", err)
	}
	return logical, nil
}

// BuildExecutionPlanWithKnowledgePrelude rebuilds the exact immutable event
// scope retained by a Manager-minted execution and substitutes one complete
// backend-neutral knowledge program. It deliberately delegates compilation to
// the ordinary compiler path and cannot create executable authority by itself.
func BuildExecutionPlanWithKnowledgePrelude(
	snapshot searchjobs.ExecutionSnapshot,
	program knowledgeprogram.Program,
) (*plan.Query, error) {
	retained, err := snapshot.OpenRetainedKnowledgeExecution()
	if err != nil {
		return nil, fmt.Errorf(
			"rebuild immutable search plan: open retained knowledge execution: %w",
			err,
		)
	}
	if retained == nil || program.IsZero() {
		return nil, errors.New(
			"rebuild immutable search plan: retained or candidate knowledge authority is unavailable",
		)
	}
	expansion, expansionPresent, err := snapshot.OpenRetainedStatsWildcardExpansion()
	if err != nil {
		return nil, fmt.Errorf(
			"rebuild immutable search plan: open retained stats wildcard expansion: %w",
			err,
		)
	}
	if expansionPresent && !program.Equal(retained.KnowledgePrelude) {
		return nil, errors.New(
			"rebuild immutable search plan: candidate knowledge changes retained stats wildcard inventory",
		)
	}
	logical, err := buildPlan(planSnapshot{
		spl:                           snapshot.SPL,
		searchJobID:                   snapshot.ID,
		tenantID:                      snapshot.TenantID,
		effectiveIndexes:              snapshot.EffectiveIndexes,
		earliest:                      snapshot.Earliest,
		latest:                        snapshot.Latest,
		searchStart:                   snapshot.SearchStart,
		searchTimezone:                snapshot.SearchTimezone,
		indexTimeCutoff:               snapshot.IndexTimeCutoff,
		visibilityCutoff:              snapshot.VisibilityCutoff,
		statsWildcardExpansion:        expansion,
		statsWildcardExpansionPresent: expansionPresent,
	})
	if err != nil {
		return nil, err
	}
	logical, err = plan.InjectKnowledgePrelude(logical, program)
	if err != nil {
		return nil, fmt.Errorf(
			"rebuild immutable search plan: inject candidate knowledge prelude: %w",
			err,
		)
	}
	return logical, nil
}

type planSnapshot struct {
	spl                           string
	searchJobID                   string
	tenantID                      string
	effectiveIndexes              []string
	earliest                      time.Time
	latest                        time.Time
	searchStart                   time.Time
	searchTimezone                string
	indexTimeCutoff               time.Time
	visibilityCutoff              uint64
	statsWildcardExpansion        plan.StatsWildcardExpansion
	statsWildcardExpansionPresent bool
}

func buildPlan(snapshot planSnapshot) (*plan.Query, error) {
	parsed, err := spl.Parse(snapshot.spl)
	if err != nil {
		return nil, err
	}
	visibilityCutoff := snapshot.visibilityCutoff
	indexes := slices.Clone(snapshot.effectiveIndexes)
	scope := plan.Scope{
		TenantID:          snapshot.tenantID,
		AuthorizedIndexes: indexes,
		RequestedIndexes:  slices.Clone(indexes),
		SearchJobID:       snapshot.searchJobID,
		Earliest:          snapshot.earliest,
		Latest:            snapshot.latest,
		SearchStart:       snapshot.searchStart,
		SearchTimezone:    snapshot.searchTimezone,
		IndexTimeCutoff:   snapshot.indexTimeCutoff,
		VisibilityCutoff:  &visibilityCutoff,
	}
	var logical *plan.Query
	if snapshot.statsWildcardExpansionPresent {
		if snapshot.statsWildcardExpansion.IsZero() {
			return nil, errors.New("rebuild immutable search plan: stats wildcard expansion is incomplete")
		}
		logical, err = plan.BuildWithStatsWildcardExpansion(
			parsed,
			scope,
			snapshot.statsWildcardExpansion,
		)
	} else {
		logical, err = plan.Build(parsed, scope)
	}
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
