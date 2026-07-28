package gradethiscorpus

import (
	"errors"
	"fmt"
	"slices"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"
)

const (
	// InspectionLoadRowsPerCohort keeps every decoy part below the default
	// 8,192-row MergeTree granule while making one accidental cohort read
	// unmistakable in exact progress counters.
	InspectionLoadRowsPerCohort uint64 = 7_500
	InspectionLoadTotalRows            = 4 * InspectionLoadRowsPerCohort

	maximumInspectionTenantBytes = 128
)

// InspectionLoadCohortID identifies one independent pruning dimension around
// the exact twenty-event compatibility target.
type InspectionLoadCohortID string

const (
	InspectionLoadSameScopeOutOfTime InspectionLoadCohortID = "same-scope-out-of-time"
	InspectionLoadForeignTenant      InspectionLoadCohortID = "foreign-tenant"
	InspectionLoadForeignIndex       InspectionLoadCohortID = "foreign-index"
	InspectionLoadAdjacentPartition  InspectionLoadCohortID = "adjacent-partition"
)

// InspectionLoadCohort describes one deterministic homogeneous ClickHouse
// part. Payloads intentionally resemble target events; exactly one scope or
// time dimension keeps each cohort outside the canonical search result.
type InspectionLoadCohort struct {
	ID        InspectionLoadCohortID
	TenantID  string
	IndexName string
	EventTime time.Time
	Rows      uint64
}

// InspectionLoadCohorts returns four detached cohorts totaling 30,000 decoy
// events. The target profile remains the sole semantic oracle.
func InspectionLoadCohorts(
	targetTenant string,
) ([]InspectionLoadCohort, error) {
	if !validInspectionTenant(targetTenant) {
		return nil, errors.New(
			"GradeThis inspection target tenant is invalid",
		)
	}
	return []InspectionLoadCohort{
		{
			ID:        InspectionLoadSameScopeOutOfTime,
			TenantID:  strings.Clone(targetTenant),
			IndexName: IndexName,
			EventTime: baseTime.Add(30 * time.Minute),
			Rows:      InspectionLoadRowsPerCohort,
		},
		{
			ID:        InspectionLoadForeignTenant,
			TenantID:  targetTenant + "-inspection-foreign",
			IndexName: IndexName,
			EventTime: baseTime.Add(5 * time.Minute),
			Rows:      InspectionLoadRowsPerCohort,
		},
		{
			ID:        InspectionLoadForeignIndex,
			TenantID:  strings.Clone(targetTenant),
			IndexName: IndexName + "-inspection-foreign",
			EventTime: baseTime.Add(5 * time.Minute),
			Rows:      InspectionLoadRowsPerCohort,
		},
		{
			ID:        InspectionLoadAdjacentPartition,
			TenantID:  strings.Clone(targetTenant),
			IndexName: IndexName,
			EventTime: baseTime.Add(-24 * time.Hour),
			Rows:      InspectionLoadRowsPerCohort,
		},
	}, nil
}

// ValidateInspectionLoad proves that cohorts remain the exact detached
// pruning profile paired with the immutable v0.1 fixture.
func ValidateInspectionLoad(
	profile Profile,
	targetTenant string,
	cohorts []InspectionLoadCohort,
) error {
	if err := Validate(profile); err != nil {
		return fmt.Errorf(
			"GradeThis inspection target profile: %w",
			err,
		)
	}
	want, err := InspectionLoadCohorts(targetTenant)
	if err != nil {
		return err
	}
	if !slices.Equal(cohorts, want) {
		return errors.New(
			"GradeThis inspection load cohorts do not match the pinned profile",
		)
	}
	var rows uint64
	for _, cohort := range cohorts {
		if cohort.Rows > ^uint64(0)-rows {
			return errors.New(
				"GradeThis inspection load row count overflowed",
			)
		}
		rows += cohort.Rows
	}
	if rows != InspectionLoadTotalRows {
		return fmt.Errorf(
			"GradeThis inspection load contains %d rows, want %d",
			rows,
			InspectionLoadTotalRows,
		)
	}
	return nil
}

func validInspectionTenant(value string) bool {
	if value == "" ||
		value != strings.TrimSpace(value) ||
		len(value) > maximumInspectionTenantBytes ||
		!utf8.ValidString(value) {
		return false
	}
	for _, character := range value {
		if unicode.IsControl(character) {
			return false
		}
	}
	return true
}
