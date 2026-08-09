package searchanalysis

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/searchtime"
)

type searchAnalysisSnapshotExecutor struct{}

func (searchAnalysisSnapshotExecutor) Execute(
	_ context.Context,
	query clickhouse.CompiledQuery,
	sink searchjobs.ResultSink,
) error {
	fields := slices.Clone(query.OutputFields)
	if len(fields) == 0 {
		fields = []string{"_raw"}
	}
	columns := make([]searchjobs.Column, len(fields))
	for index, field := range fields {
		columns[index] = searchjobs.Column{
			Name: field,
			Kind: searchjobs.ValueKindString,
		}
	}
	return sink.SetSchema(searchjobs.Schema{Columns: columns})
}

type searchAnalysisSnapshotter uint64

func (snapshotter searchAnalysisSnapshotter) VisibilityCutoff(context.Context) (uint64, error) {
	return uint64(snapshotter), nil
}

// sealSearchAnalysisSnapshot converts a test's explicit public scope into the
// same private manager-attested ExecutionSnapshot accepted by production
// completed-search analysis. Tests that deliberately change a signed field
// must call this helper again instead of mutating the sealed value in place.
func sealSearchAnalysisSnapshot(
	template searchjobs.ExecutionSnapshot,
) (searchjobs.ExecutionSnapshot, error) {
	timezone := template.SearchTimezone
	if timezone == "" {
		timezone = "UTC"
	}
	resolved, err := searchtime.Resolve(
		template.Earliest.UTC().Format(time.RFC3339Nano),
		template.Latest.UTC().Format(time.RFC3339Nano),
		&timezone,
		template.SearchStart,
	)
	if err != nil {
		return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
			"resolve search-analysis snapshot time range: %w",
			err,
		)
	}
	retention := template.ExpiresAt.Sub(template.SearchStart)
	if retention <= 0 {
		retention = time.Hour
	}
	manager, err := searchjobs.New(searchjobs.Config{
		Executor:        searchAnalysisSnapshotExecutor{},
		Snapshotter:     searchAnalysisSnapshotter(template.VisibilityCutoff),
		RetentionTTL:    retention,
		CleanupInterval: -1,
		Now:             func() time.Time { return template.SearchStart },
		NewID:           func() string { return template.ID },
	})
	if err != nil {
		return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
			"create search-analysis snapshot manager: %w",
			err,
		)
	}
	defer func() { _ = manager.Close() }()
	request := searchjobs.CreateRequest{
		SPL:               template.SPL,
		OwnerID:           template.OwnerID,
		TenantID:          template.TenantID,
		AppID:             template.AppID,
		AuthorizedIndexes: slices.Clone(template.EffectiveIndexes),
		RequestedIndexes:  slices.Clone(template.EffectiveIndexes),
		TimeRange:         resolved,
	}
	created, err := manager.Create(context.Background(), request)
	if err != nil {
		return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
			"create search-analysis snapshot: %w",
			err,
		)
	}
	access := searchjobs.AccessScope{
		TenantID: template.TenantID,
		OwnerID:  template.OwnerID,
	}
	deadline := time.Now().Add(2 * time.Second)
	for {
		job, getErr := manager.GetFor(access, created.ID)
		if getErr != nil {
			return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
				"get search-analysis snapshot job: %w",
				getErr,
			)
		}
		switch job.State {
		case searchjobs.StateCompleted:
			snapshot, snapshotErr := manager.CompletedExecutionSnapshotFor(
				context.Background(),
				access,
				created.ID,
			)
			if snapshotErr != nil {
				return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
					"open search-analysis execution snapshot: %w",
					snapshotErr,
				)
			}
			return snapshot, nil
		case searchjobs.StateFailed, searchjobs.StateCanceled:
			return searchjobs.ExecutionSnapshot{}, fmt.Errorf(
				"search-analysis snapshot job ended in %v",
				job.State,
			)
		}
		if time.Now().After(deadline) {
			return searchjobs.ExecutionSnapshot{}, errors.New(
				"timed out minting search-analysis execution snapshot",
			)
		}
		time.Sleep(time.Millisecond)
	}
}
