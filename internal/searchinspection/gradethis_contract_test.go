package searchinspection

import (
	"errors"
	"fmt"
	"reflect"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/queryexec"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/gradethiscorpus"
)

const gradeThisInspectionPrivateSentinel = "private-inspection-secret-7f2c"

func TestGradeThisSummarizeInspectionPlan(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name    string
		mutate  func(*queryexec.ExplainPlan)
		wantErr error
	}{
		{name: "valid"},
		{
			name: "physical columns are canonicalized",
			mutate: func(plan *queryexec.ExplainPlan) {
				slices.Reverse(plan.Reads[0].Columns)
			},
		},
		{
			name: "zero reads",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads = nil
			},
			wantErr: errGradeThisInspectionReadCount,
		},
		{
			name: "multiple reads",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads = append(plan.Reads, queryexec.ExplainRead{})
			},
			wantErr: errGradeThisInspectionReadCount,
		},
		{
			name: "private physical column",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Columns = append(
					plan.Reads[0].Columns,
					gradeThisInspectionPrivateSentinel,
				)
			},
			wantErr: errGradeThisInspectionColumns,
		},
		{
			name: "missing MinMax",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes = removeGradeThisInspectionIndex(
					plan.Reads[0].Indexes,
					"MinMax",
				)
			},
			wantErr: errGradeThisInspectionMinMax,
		},
		{
			name: "duplicate MinMax",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes = append(
					plan.Reads[0].Indexes,
					plan.Reads[0].Indexes[0],
				)
			},
			wantErr: errGradeThisInspectionMinMax,
		},
		{
			name: "named MinMax",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[0].Name =
					gradeThisInspectionPrivateSentinel
			},
			wantErr: errGradeThisInspectionMinMax,
		},
		{
			name: "private MinMax key",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[0].Keys = []string{
					gradeThisInspectionPrivateSentinel,
				}
			},
			wantErr: errGradeThisInspectionMinMax,
		},
		{
			name: "nil MinMax keys",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[0].Keys = nil
			},
			wantErr: errGradeThisInspectionMinMax,
		},
		{
			name: "missing partition",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes = removeGradeThisInspectionIndex(
					plan.Reads[0].Indexes,
					"Partition",
				)
			},
			wantErr: errGradeThisInspectionPartition,
		},
		{
			name: "duplicate partition",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes = append(
					plan.Reads[0].Indexes,
					plan.Reads[0].Indexes[1],
				)
			},
			wantErr: errGradeThisInspectionPartition,
		},
		{
			name: "named partition",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[1].Name =
					gradeThisInspectionPrivateSentinel
			},
			wantErr: errGradeThisInspectionPartition,
		},
		{
			name: "private partition key",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[1].Keys = []string{
					gradeThisInspectionPrivateSentinel,
				}
			},
			wantErr: errGradeThisInspectionPartition,
		},
		{
			name: "nil partition keys",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[1].Keys = nil
			},
			wantErr: errGradeThisInspectionPartition,
		},
		{
			name: "missing primary key",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes = removeGradeThisInspectionIndex(
					plan.Reads[0].Indexes,
					"PrimaryKey",
				)
			},
			wantErr: errGradeThisInspectionPrimaryKey,
		},
		{
			name: "duplicate primary key",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes = append(
					plan.Reads[0].Indexes,
					plan.Reads[0].Indexes[2],
				)
			},
			wantErr: errGradeThisInspectionPrimaryKey,
		},
		{
			name: "named primary key",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[2].Name =
					gradeThisInspectionPrivateSentinel
			},
			wantErr: errGradeThisInspectionPrimaryKey,
		},
		{
			name: "private primary key",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[2].Keys = []string{
					gradeThisInspectionPrivateSentinel,
				}
			},
			wantErr: errGradeThisInspectionPrimaryKey,
		},
		{
			name: "nil primary keys",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[2].Keys = nil
			},
			wantErr: errGradeThisInspectionPrimaryKey,
		},
		{
			name: "reordered primary keys",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[2].Keys = []string{
					"index_name",
					"tenant_id",
					"event_time",
				}
			},
			wantErr: errGradeThisInspectionPrimaryKey,
		},
		{
			name: "missing visibility skip",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes = removeGradeThisInspectionIndex(
					plan.Reads[0].Indexes,
					"Skip",
				)
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name: "duplicate visibility skip",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes = append(
					plan.Reads[0].Indexes,
					plan.Reads[0].Indexes[3],
				)
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name: "private skip name",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[3].Name =
					gradeThisInspectionPrivateSentinel
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name: "trace skip rejected",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[3].Name = "idx_trace_id"
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name: "raw text skip rejected",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[3].Name = "idx_raw_text"
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name: "private skip key",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[3].Keys = []string{
					gradeThisInspectionPrivateSentinel,
				}
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name: "unexpected index type",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes = append(
					plan.Reads[0].Indexes,
					queryexec.ExplainIndex{
						Type: gradeThisInspectionPrivateSentinel,
					},
				)
			},
			wantErr: errGradeThisInspectionIndex,
		},
	}
	tests = append(
		tests,
		gradeThisInspectionCounterMutationTests(
			"MinMax",
			0,
			errGradeThisInspectionMinMax,
		)...,
	)
	tests = append(
		tests,
		gradeThisInspectionCounterMutationTests(
			"partition",
			1,
			errGradeThisInspectionPartition,
		)...,
	)
	tests = append(
		tests,
		gradeThisInspectionCounterMutationTests(
			"primary key",
			2,
			errGradeThisInspectionPrimaryKey,
		)...,
	)
	tests = append(
		tests,
		gradeThisInspectionCounterMutationTests(
			"visibility skip",
			3,
			errGradeThisInspectionSkip,
		)...,
	)

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			plan := validGradeThisInspectionPlan(
				gradethiscorpus.SearchFollowTrace,
			)
			if test.mutate != nil {
				test.mutate(&plan)
			}
			got, err := gradeThisSummarizeInspectionPlan(plan)
			if test.wantErr == nil {
				if err != nil {
					t.Fatal(err)
				}
				want := validGradeThisInspectionSummary(
					gradethiscorpus.SearchFollowTrace,
				)
				if !reflect.DeepEqual(got, want) {
					t.Fatalf("plan summary = %#v, want %#v", got, want)
				}
			} else if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			rendered := fmt.Sprintf("%#v %v", got, err)
			if strings.Contains(
				rendered,
				gradeThisInspectionPrivateSentinel,
			) {
				t.Fatal("plan validation retained private metadata")
			}
		})
	}
}

func TestGradeThisValidateInspectionSummaryExactCorpus(t *testing.T) {
	t.Parallel()

	searches := gradethiscorpus.Searches()
	if len(searches) != len(gradeThisInspectionExpectedColumns) {
		t.Fatal("GradeThis physical-plan contract does not cover the corpus")
	}
	for _, search := range searches {
		search := search
		t.Run(string(search.ID), func(t *testing.T) {
			t.Parallel()

			plan := validGradeThisInspectionPlan(search.ID)
			summary, err := gradeThisSummarizeInspectionPlan(plan)
			if err != nil {
				t.Fatal(err)
			}
			if err := gradeThisValidateInspectionSummary(
				search.ID,
				summary,
			); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestGradeThisValidateInspectionSummaryRejectsDrift(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name     string
		searchID gradethiscorpus.SearchID
		mutate   func(*gradeThisInspectionPlanSummary)
		wantErr  error
	}{
		{
			name:     "unknown search",
			searchID: "unknown-search",
			wantErr:  errGradeThisInspectionSearch,
		},
		{
			name:     "missing physical column",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.columns = summary.columns[1:]
			},
			wantErr: errGradeThisInspectionColumns,
		},
		{
			name:     "extra allowed physical column",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.columns = append(summary.columns, "body")
				slices.Sort(summary.columns)
			},
			wantErr: errGradeThisInspectionColumns,
		},
		{
			name:     "reordered physical columns",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				slices.Reverse(summary.columns)
			},
			wantErr: errGradeThisInspectionColumns,
		},
		{
			name:     "MinMax counters",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.minMax.selectedGranules++
			},
			wantErr: errGradeThisInspectionMinMax,
		},
		{
			name:     "partition counters",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.partition.selectedParts++
			},
			wantErr: errGradeThisInspectionPartition,
		},
		{
			name:     "primary key counters",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.primaryKey.initialGranules++
			},
			wantErr: errGradeThisInspectionPrimaryKey,
		},
		{
			name:     "missing visibility skip",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.skips = nil
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name:     "duplicate visibility skip",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.skips = append(
					summary.skips,
					gradeThisInspectionSkip("idx_visibility_seq"),
				)
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name:     "private skip name",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.skips[0].name =
					gradeThisInspectionPrivateSentinel
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name:     "private skip key",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.skips[0].keys = []string{
					gradeThisInspectionPrivateSentinel,
				}
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name:     "skip counters",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.skips[0].counts.selectedParts++
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name:     "field names skip on non-server search",
			searchID: gradethiscorpus.SearchResponses,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.skips = append(
					[]gradeThisInspectionSkipEvidence{
						gradeThisInspectionSkip("idx_field_names"),
					},
					summary.skips...,
				)
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name:     "server search missing field names skip",
			searchID: gradethiscorpus.SearchServerErrors,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.skips = summary.skips[1:]
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name:     "trace skip rejected",
			searchID: gradethiscorpus.SearchFollowTrace,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.skips[0].name = "idx_trace_id"
			},
			wantErr: errGradeThisInspectionSkip,
		},
		{
			name:     "raw text skip rejected",
			searchID: gradethiscorpus.SearchRawErrorFragment,
			mutate: func(summary *gradeThisInspectionPlanSummary) {
				summary.skips[0].name = "idx_raw_text"
			},
			wantErr: errGradeThisInspectionSkip,
		},
	}

	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			summary := validGradeThisInspectionSummary(test.searchID)
			if test.mutate != nil {
				test.mutate(&summary)
			}
			err := gradeThisValidateInspectionSummary(
				test.searchID,
				summary,
			)
			if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if strings.Contains(
				fmt.Sprint(err),
				gradeThisInspectionPrivateSentinel,
			) {
				t.Fatal("plan validation error disclosed private metadata")
			}
		})
	}
}

func gradeThisInspectionCounterMutationTests(
	prefix string,
	index int,
	wantErr error,
) []struct {
	name    string
	mutate  func(*queryexec.ExplainPlan)
	wantErr error
} {
	return []struct {
		name    string
		mutate  func(*queryexec.ExplainPlan)
		wantErr error
	}{
		{
			name: prefix + " initial parts",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[index].InitialParts++
			},
			wantErr: wantErr,
		},
		{
			name: prefix + " selected parts",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[index].SelectedParts++
			},
			wantErr: wantErr,
		},
		{
			name: prefix + " initial granules",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[index].InitialGranules++
			},
			wantErr: wantErr,
		},
		{
			name: prefix + " selected granules",
			mutate: func(plan *queryexec.ExplainPlan) {
				plan.Reads[0].Indexes[index].SelectedGranules++
			},
			wantErr: wantErr,
		},
	}
}

func validGradeThisInspectionPlan(
	searchID gradethiscorpus.SearchID,
) queryexec.ExplainPlan {
	columns := slices.Clone(gradeThisInspectionExpectedColumns[searchID])
	slices.Reverse(columns)
	indexes := []queryexec.ExplainIndex{
		{
			Type:             "MinMax",
			Keys:             []string{"event_time"},
			InitialParts:     5,
			SelectedParts:    3,
			InitialGranules:  5,
			SelectedGranules: 3,
		},
		{
			Type:             "Partition",
			Keys:             []string{"toYYYYMM(event_time)"},
			InitialParts:     3,
			SelectedParts:    3,
			InitialGranules:  3,
			SelectedGranules: 3,
		},
		{
			Type: "PrimaryKey",
			Keys: []string{
				"tenant_id",
				"index_name",
				"event_time",
			},
			InitialParts:     3,
			SelectedParts:    1,
			InitialGranules:  3,
			SelectedGranules: 1,
		},
		{
			Type:             "Skip",
			Name:             "idx_visibility_seq",
			InitialParts:     1,
			SelectedParts:    1,
			InitialGranules:  1,
			SelectedGranules: 1,
		},
	}
	if searchID == gradethiscorpus.SearchServerErrors {
		indexes = append(indexes, queryexec.ExplainIndex{
			Type:             "Skip",
			Name:             "idx_field_names",
			InitialParts:     1,
			SelectedParts:    1,
			InitialGranules:  1,
			SelectedGranules: 1,
		})
	}
	return queryexec.ExplainPlan{
		NodeTypes: []string{"ReadFromMergeTree"},
		Reads: []queryexec.ExplainRead{{
			Columns: columns,
			Indexes: indexes,
		}},
	}
}

func validGradeThisInspectionSummary(
	searchID gradethiscorpus.SearchID,
) gradeThisInspectionPlanSummary {
	columns := slices.Clone(gradeThisInspectionExpectedColumns[searchID])
	skips := []gradeThisInspectionSkipEvidence{
		gradeThisInspectionSkip("idx_visibility_seq"),
	}
	if searchID == gradethiscorpus.SearchServerErrors {
		skips = append(
			[]gradeThisInspectionSkipEvidence{
				gradeThisInspectionSkip("idx_field_names"),
			},
			skips...,
		)
	}
	return gradeThisInspectionPlanSummary{
		columns:    columns,
		minMax:     gradeThisInspectionMinMaxCounts(),
		partition:  gradeThisInspectionPartitionCounts(),
		primaryKey: gradeThisInspectionPrimaryKeyCounts(),
		skips:      skips,
	}
}

func removeGradeThisInspectionIndex(
	indexes []queryexec.ExplainIndex,
	indexType string,
) []queryexec.ExplainIndex {
	result := make([]queryexec.ExplainIndex, 0, len(indexes))
	for _, index := range indexes {
		if index.Type != indexType {
			result = append(result, index)
		}
	}
	return result
}
