package queryexec

import (
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/gradethiscorpus"
)

const gradeThisInspectionPrivateSentinel = "private-inspection-secret-7f2c"

func TestGradeThisValidateInspectionCorpusEvidence(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name   string
		mutate func(
			[]gradethiscorpus.Search,
			map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
		) (
			[]gradethiscorpus.Search,
			map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
		)
		wantErr error
	}{
		{name: "minimum progress bounds"},
		{
			name: "maximum progress bounds",
			mutate: gradeThisMutateAllInspectionEvidence(
				func(evidence *gradeThisInspectionEvidence) {
					evidence.scannedRows =
						gradeThisMaximumInspectionScannedRows
					evidence.scannedBytes =
						gradeThisMaximumInspectionScannedBytes
				},
			),
		},
		{
			name: "nine searches",
			mutate: func(
				searches []gradethiscorpus.Search,
				evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) (
				[]gradethiscorpus.Search,
				map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) {
				return searches[:len(searches)-1], evidence
			},
			wantErr: errGradeThisInspectionCorpus,
		},
		{
			name: "eleven searches",
			mutate: func(
				searches []gradethiscorpus.Search,
				evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) (
				[]gradethiscorpus.Search,
				map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) {
				return append(searches, searches[0]), evidence
			},
			wantErr: errGradeThisInspectionCorpus,
		},
		{
			name: "duplicate search ID",
			mutate: func(
				searches []gradethiscorpus.Search,
				evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) (
				[]gradethiscorpus.Search,
				map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) {
				searches[len(searches)-1].ID = searches[0].ID
				return searches, evidence
			},
			wantErr: errGradeThisInspectionCorpus,
		},
		{
			name: "empty search ID",
			mutate: func(
				searches []gradethiscorpus.Search,
				evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) (
				[]gradethiscorpus.Search,
				map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) {
				searches[len(searches)-1].ID = ""
				return searches, evidence
			},
			wantErr: errGradeThisInspectionCorpus,
		},
		{
			name: "missing evidence",
			mutate: func(
				searches []gradethiscorpus.Search,
				evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) (
				[]gradethiscorpus.Search,
				map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) {
				delete(evidence, searches[0].ID)
				return searches, evidence
			},
			wantErr: errGradeThisInspectionCorpus,
		},
		{
			name: "extra evidence",
			mutate: func(
				searches []gradethiscorpus.Search,
				evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) (
				[]gradethiscorpus.Search,
				map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) {
				privateID := gradethiscorpus.SearchID(
					gradeThisInspectionPrivateSentinel,
				)
				evidence[privateID] = gradeThisInspectionEvidence{
					searchID:     privateID,
					scannedRows:  1,
					scannedBytes: 1,
				}
				return searches, evidence
			},
			wantErr: errGradeThisInspectionCorpus,
		},
		{
			name: "same-size foreign evidence",
			mutate: func(
				searches []gradethiscorpus.Search,
				evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) (
				[]gradethiscorpus.Search,
				map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) {
				delete(evidence, searches[0].ID)
				privateID := gradethiscorpus.SearchID(
					gradeThisInspectionPrivateSentinel,
				)
				evidence[privateID] = gradeThisInspectionEvidence{
					searchID:     privateID,
					scannedRows:  1,
					scannedBytes: 1,
				}
				return searches, evidence
			},
			wantErr: errGradeThisInspectionCorpus,
		},
		{
			name: "embedded search ID mismatch",
			mutate: func(
				searches []gradethiscorpus.Search,
				evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) (
				[]gradethiscorpus.Search,
				map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
			) {
				observation := evidence[searches[0].ID]
				observation.searchID = gradethiscorpus.SearchID(
					gradeThisInspectionPrivateSentinel,
				)
				evidence[searches[0].ID] = observation
				return searches, evidence
			},
			wantErr: errGradeThisInspectionCorpus,
		},
		{
			name: "zero scanned rows",
			mutate: gradeThisMutateFirstInspectionEvidence(
				func(evidence *gradeThisInspectionEvidence) {
					evidence.scannedRows = 0
				},
			),
			wantErr: errGradeThisInspectionRows,
		},
		{
			name: "over-limit scanned rows",
			mutate: gradeThisMutateFirstInspectionEvidence(
				func(evidence *gradeThisInspectionEvidence) {
					evidence.scannedRows =
						gradeThisMaximumInspectionScannedRows + 1
				},
			),
			wantErr: errGradeThisInspectionRows,
		},
		{
			name: "zero scanned bytes",
			mutate: gradeThisMutateFirstInspectionEvidence(
				func(evidence *gradeThisInspectionEvidence) {
					evidence.scannedBytes = 0
				},
			),
			wantErr: errGradeThisInspectionBytes,
		},
		{
			name: "over-limit scanned bytes",
			mutate: gradeThisMutateFirstInspectionEvidence(
				func(evidence *gradeThisInspectionEvidence) {
					evidence.scannedBytes =
						gradeThisMaximumInspectionScannedBytes + 1
				},
			),
			wantErr: errGradeThisInspectionBytes,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			searches, evidence :=
				gradeThisValidInspectionCorpusEvidence()
			if test.mutate != nil {
				searches, evidence = test.mutate(searches, evidence)
			}
			err := gradeThisValidateInspectionCorpusEvidence(
				searches,
				evidence,
			)
			if test.wantErr == nil {
				if err != nil {
					t.Fatal(err)
				}
			} else if !errors.Is(err, test.wantErr) {
				t.Fatalf("error = %v, want %v", err, test.wantErr)
			}
			if strings.Contains(
				fmt.Sprint(err),
				gradeThisInspectionPrivateSentinel,
			) {
				t.Fatal("corpus validation disclosed private metadata")
			}
		})
	}
}

func gradeThisValidInspectionCorpusEvidence() (
	[]gradethiscorpus.Search,
	map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
) {
	searches := gradethiscorpus.Searches()
	evidence := make(
		map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
		len(searches),
	)
	for _, search := range searches {
		evidence[search.ID] = gradeThisInspectionEvidence{
			searchID:     search.ID,
			scannedRows:  1,
			scannedBytes: 1,
		}
	}
	return searches, evidence
}

func gradeThisMutateFirstInspectionEvidence(
	mutate func(*gradeThisInspectionEvidence),
) func(
	[]gradethiscorpus.Search,
	map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
) (
	[]gradethiscorpus.Search,
	map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
) {
	return func(
		searches []gradethiscorpus.Search,
		evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
	) (
		[]gradethiscorpus.Search,
		map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
	) {
		observation := evidence[searches[0].ID]
		mutate(&observation)
		evidence[searches[0].ID] = observation
		return searches, evidence
	}
}

func gradeThisMutateAllInspectionEvidence(
	mutate func(*gradeThisInspectionEvidence),
) func(
	[]gradethiscorpus.Search,
	map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
) (
	[]gradethiscorpus.Search,
	map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
) {
	return func(
		searches []gradethiscorpus.Search,
		evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
	) (
		[]gradethiscorpus.Search,
		map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
	) {
		for id, observation := range evidence {
			mutate(&observation)
			evidence[id] = observation
		}
		return searches, evidence
	}
}
