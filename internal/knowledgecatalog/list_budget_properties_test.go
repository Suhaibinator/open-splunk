package knowledgecatalog

import (
	"errors"
	"fmt"
	"math"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func validateListDefinitionBudget(maximumBytes int64, groups ...[]projectionRecord) error {
	if maximumBytes < maximumDefinitionBytes {
		return fmt.Errorf("%w: knowledge catalog list definition budget is invalid", ErrCorrupt)
	}
	var total int64
	for _, records := range groups {
		for _, record := range records {
			charge, err := listDefinitionCharge(record)
			if err != nil {
				return err
			}
			if total > maximumBytes-charge {
				return fmt.Errorf("%w: knowledge catalog list definition budget exceeded", control.ErrCapacityExceeded)
			}
			total += charge
		}
	}
	return nil
}

func TestListDefinitionBudgetBoundaryArithmetic(t *testing.T) {
	t.Parallel()

	maximum := int64(MaximumListResponseCanonicalDefinitionBytes)
	oneMaximum := projectionRecord{State: StateActive, DefinitionBytes: maximumDefinitionBytes}
	if err := validateListDefinitionBudget(maximum, []projectionRecord{oneMaximum}); err != nil {
		t.Fatalf("one maximum definition at exact response budget: %v", err)
	}
	if err := validateListDefinitionBudget(maximum-1, []projectionRecord{oneMaximum}); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("undersized configured budget error = %v, want ErrCorrupt", err)
	}
	if err := validateListDefinitionBudget(maximum, []projectionRecord{
		{State: StateActive, DefinitionBytes: maximumDefinitionBytes},
		{State: StateActive, DefinitionBytes: 1},
	}); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("exact budget + 1 error = %v, want ErrCapacityExceeded", err)
	}
	if err := validateListDefinitionBudget(math.MaxInt64, []projectionRecord{
		oneMaximum, oneMaximum, oneMaximum,
	}); err != nil {
		t.Fatalf("large valid budget arithmetic overflowed: %v", err)
	}

	for _, test := range []struct {
		name   string
		record projectionRecord
		want   error
	}{
		{name: "zero active", record: projectionRecord{State: StateActive}, want: ErrCorrupt},
		{name: "negative active", record: projectionRecord{State: StateActive, DefinitionBytes: -1}, want: ErrCorrupt},
		{name: "active over object maximum", record: projectionRecord{State: StateActive, DefinitionBytes: maximumDefinitionBytes + 1}, want: ErrCorrupt},
		{name: "quarantined zero", record: projectionRecord{State: StateQuarantined}},
		{name: "quarantined nonzero", record: projectionRecord{State: StateQuarantined, DefinitionBytes: 1}, want: ErrCorrupt},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			err := validateListDefinitionBudget(maximum, []projectionRecord{test.record})
			if test.want == nil && err != nil {
				t.Fatalf("error = %v, want success", err)
			}
			if test.want != nil && !errors.Is(err, test.want) {
				t.Fatalf("error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestBoundedListResponseRecordsExactFitOverflowAndSentinel(t *testing.T) {
	t.Parallel()

	half := int64(MaximumListResponseCanonicalDefinitionBytes / 2)
	exactDefinitions := []projectionRecord{
		{State: StateActive, DefinitionBytes: half},
		{State: StateActive, DefinitionBytes: half},
	}
	returned, more, err := boundedListResponseRecords(exactDefinitions, 2)
	if err != nil || more || len(returned) != 2 || cap(returned) != 2 {
		t.Fatalf("exact definition budget = len/cap %d/%d, more=%t, err=%v", len(returned), cap(returned), more, err)
	}

	overDefinitions := []projectionRecord{
		{State: StateActive, DefinitionBytes: half + 1},
		{State: StateActive, DefinitionBytes: half + 1},
	}
	returned, more, err = boundedListResponseRecords(overDefinitions, 2)
	if err != nil || !more || len(returned) != 1 || cap(returned) != 1 {
		t.Fatalf("over definition budget = len/cap %d/%d, more=%t, err=%v", len(returned), cap(returned), more, err)
	}

	dependencyFit := int(MaximumListResponseDependencies / maximumDependenciesPerVersion)
	dependencyRecords := make([]projectionRecord, dependencyFit+1)
	for index := range dependencyRecords {
		dependencyRecords[index] = projectionRecord{
			State: StateActive, DefinitionBytes: 1, DependencyCount: maximumDependenciesPerVersion,
		}
	}
	returned, more, err = boundedListResponseRecords(dependencyRecords, MaximumPageSize)
	if err != nil || !more || len(returned) != dependencyFit || cap(returned) != dependencyFit {
		t.Fatalf("dependency boundary = len/cap %d/%d want %d, more=%t, err=%v",
			len(returned), cap(returned), dependencyFit, more, err)
	}

	// The page-size+1 sentinel is pagination evidence only. Its body and
	// dependency authorities must not be inspected or retained in the returned
	// slice, even when they are corrupt.
	corruptSentinels := []projectionRecord{
		{State: StateActive, DefinitionBytes: 1},
		{State: StateActive, DefinitionBytes: 0, DependencyCount: -1},
	}
	returned, more, err = boundedListResponseRecords(corruptSentinels, 1)
	if err != nil || !more || len(returned) != 1 || cap(returned) != 1 {
		t.Fatalf("unhydrated corrupt sentinel = len/cap %d/%d, more=%t, err=%v", len(returned), cap(returned), more, err)
	}
	corruptSentinels[0] = projectionRecord{State: StateActive, DefinitionBytes: 0}
	if _, _, err := boundedListResponseRecords(corruptSentinels, 1); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("corrupt returned record error = %v, want ErrCorrupt", err)
	}

	for _, test := range []struct {
		name     string
		records  []projectionRecord
		pageSize int
	}{
		{name: "zero page", pageSize: 0},
		{name: "over maximum page", pageSize: MaximumPageSize + 1},
		{name: "more than one sentinel", pageSize: 1, records: make([]projectionRecord, 3)},
	} {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, _, err := boundedListResponseRecords(test.records, test.pageSize); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestListHydrationBudgetEachDimensionExactAndOver(t *testing.T) {
	t.Parallel()

	minimum := listHydrationBudget{
		definitionBytes:    maximumDefinitionBytes,
		projectionBytes:    maximumDescriptionBytes + (8 << 10),
		selectorPatterns:   maximumSelectorPatterns,
		selectorValueBytes: 8 << 10,
		dependencies:       maximumDependenciesPerVersion,
	}
	exact := projectionRecord{
		State:              StateActive,
		DefinitionBytes:    maximumDefinitionBytes,
		ProjectionBytes:    minimum.projectionBytes,
		IndexSelectorCount: maximumSelectorPatterns,
		SelectorValueBytes: minimum.selectorValueBytes,
		DependencyCount:    maximumDependenciesPerVersion,
	}
	if err := validateListHydrationBudget([]projectionRecord{exact}, minimum); err != nil {
		t.Fatalf("all dimensions at exact minimum budget: %v", err)
	}

	invalidBudgets := []struct {
		name   string
		mutate func(*listHydrationBudget)
	}{
		{name: "definition", mutate: func(b *listHydrationBudget) { b.definitionBytes-- }},
		{name: "projection", mutate: func(b *listHydrationBudget) { b.projectionBytes-- }},
		{name: "selector rows", mutate: func(b *listHydrationBudget) { b.selectorPatterns-- }},
		{name: "selector bytes", mutate: func(b *listHydrationBudget) { b.selectorValueBytes-- }},
		{name: "dependencies", mutate: func(b *listHydrationBudget) { b.dependencies-- }},
	}
	for _, test := range invalidBudgets {
		test := test
		t.Run("invalid budget/"+test.name, func(t *testing.T) {
			t.Parallel()
			budget := minimum
			test.mutate(&budget)
			if err := validateListHydrationBudget(nil, budget); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("error = %v, want ErrCorrupt", err)
			}
		})
	}

	overflows := []struct {
		name    string
		records []projectionRecord
	}{
		{
			name: "definition bytes",
			records: []projectionRecord{
				{State: StateActive, DefinitionBytes: maximumDefinitionBytes},
				{State: StateActive, DefinitionBytes: 1},
			},
		},
		{
			name: "projection bytes",
			records: []projectionRecord{
				{State: StateActive, DefinitionBytes: 1, ProjectionBytes: minimum.projectionBytes},
				{State: StateActive, DefinitionBytes: 1, ProjectionBytes: 1},
			},
		},
		{
			name: "selector rows",
			records: []projectionRecord{
				{State: StateActive, DefinitionBytes: 1, IndexSelectorCount: maximumSelectorPatterns},
				{State: StateActive, DefinitionBytes: 1, HostSelectorCount: 1},
			},
		},
		{
			name: "selector bytes",
			records: []projectionRecord{
				{State: StateActive, DefinitionBytes: 1, SelectorValueBytes: minimum.selectorValueBytes},
				{State: StateActive, DefinitionBytes: 1, SelectorValueBytes: 1},
			},
		},
		{
			name: "dependencies",
			records: []projectionRecord{
				{State: StateActive, DefinitionBytes: 1, DependencyCount: maximumDependenciesPerVersion},
				{State: StateActive, DefinitionBytes: 1, DependencyCount: 1},
			},
		},
	}
	for _, test := range overflows {
		test := test
		t.Run("overflow/"+test.name, func(t *testing.T) {
			t.Parallel()
			if err := validateListHydrationBudget(test.records, minimum); !errors.Is(err, control.ErrCapacityExceeded) {
				t.Fatalf("error = %v, want ErrCapacityExceeded", err)
			}
		})
	}
}
