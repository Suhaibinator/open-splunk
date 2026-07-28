package queryexec

import (
	"context"
	"errors"
	"testing"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/Suhaibinator/open-splunk/internal/searchjobs"
	"github.com/Suhaibinator/open-splunk/internal/testsupport/gradethiscorpus"
)

const (
	// A selected decoy part contains 7,500 rows. This deliberately lower
	// ceiling therefore proves that native progress did not include even one
	// complete poison cohort while allowing modest implementation variance
	// around the twenty canonical events.
	gradeThisMaximumInspectionScannedRows  = uint64(100)
	gradeThisMaximumInspectionScannedBytes = uint64(
		4 << 20,
	)
)

type gradeThisInspectionEvidence struct {
	searchID     gradethiscorpus.SearchID
	scannedRows  uint64
	scannedBytes uint64
}

var (
	errGradeThisInspectionRows = errors.New(
		"GradeThis inspection scanned-row evidence is outside its bound",
	)
	errGradeThisInspectionBytes = errors.New(
		"GradeThis inspection scanned-byte evidence is outside its bound",
	)
	errGradeThisInspectionCorpus = errors.New(
		"GradeThis inspection corpus evidence is incomplete",
	)
)

func gradeThisStopInspectionMerges(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()
	if err := gradethiscorpus.StopInspectionMerges(
		ctx,
		connection,
	); err != nil {
		t.Fatal(err)
	}
}

func gradeThisStoreInspectionLoad(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	profile gradethiscorpus.Profile,
) {
	t.Helper()
	if err := gradethiscorpus.StoreInspectionLoad(
		ctx,
		connection,
		profile,
		"tenant",
	); err != nil {
		t.Fatal(err)
	}
}

func gradeThisAssertInspectionPartLayout(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()
	layout, err := gradethiscorpus.ReadInspectionPartLayout(
		ctx,
		connection,
	)
	if err != nil {
		t.Fatal(err)
	}
	profile := gradethiscorpus.Fixture()
	if err := gradethiscorpus.ValidateInspectionPartLayout(
		profile,
		layout,
	); err != nil {
		t.Fatal(err)
	}
	t.Logf("GradeThis inspection part layout: %#v", layout)
}

func gradeThisCaptureInspectionEvidence(
	t *testing.T,
	searchID gradethiscorpus.SearchID,
	jobID string,
	terminal searchjobs.Job,
) gradeThisInspectionEvidence {
	t.Helper()
	evidence := gradeThisInspectionEvidence{
		searchID:     searchID,
		scannedRows:  terminal.ScannedRows,
		scannedBytes: terminal.ScannedBytes,
	}
	if terminal.ID != jobID {
		t.Fatalf(
			"GradeThis inspection completed the wrong job for %q",
			searchID,
		)
	}
	if err := gradeThisValidateInspectionProgress(evidence); err != nil {
		t.Fatalf("validate GradeThis progress for %q: %v", searchID, err)
	}
	t.Logf(
		"GradeThis load evidence: search=%s scanned_rows=%d scanned_bytes=%d",
		searchID,
		evidence.scannedRows,
		evidence.scannedBytes,
	)
	return evidence
}

func gradeThisAssertInspectionCorpusEvidence(
	t *testing.T,
	searches []gradethiscorpus.Search,
	evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
) {
	t.Helper()
	if err := gradeThisValidateInspectionCorpusEvidence(
		searches,
		evidence,
	); err != nil {
		t.Fatal(err)
	}
}

func gradeThisValidateInspectionCorpusEvidence(
	searches []gradethiscorpus.Search,
	evidence map[gradethiscorpus.SearchID]gradeThisInspectionEvidence,
) error {
	canonical := gradethiscorpus.Searches()
	if len(searches) != len(canonical) ||
		len(evidence) != len(canonical) {
		return errGradeThisInspectionCorpus
	}
	canonicalIDs := make(
		map[gradethiscorpus.SearchID]struct{},
		len(canonical),
	)
	for _, search := range canonical {
		canonicalIDs[search.ID] = struct{}{}
	}
	seen := make(map[gradethiscorpus.SearchID]struct{}, len(searches))
	for _, search := range searches {
		if search.ID == "" {
			return errGradeThisInspectionCorpus
		}
		if _, canonicalID := canonicalIDs[search.ID]; !canonicalID {
			return errGradeThisInspectionCorpus
		}
		if _, duplicate := seen[search.ID]; duplicate {
			return errGradeThisInspectionCorpus
		}
		seen[search.ID] = struct{}{}
		observation, ok := evidence[search.ID]
		if !ok || observation.searchID != search.ID {
			return errGradeThisInspectionCorpus
		}
		if err := gradeThisValidateInspectionProgress(observation); err != nil {
			return err
		}
	}
	return nil
}

func gradeThisValidateInspectionProgress(
	evidence gradeThisInspectionEvidence,
) error {
	if evidence.scannedRows == 0 ||
		evidence.scannedRows > gradeThisMaximumInspectionScannedRows {
		return errGradeThisInspectionRows
	}
	if evidence.scannedBytes == 0 ||
		evidence.scannedBytes > gradeThisMaximumInspectionScannedBytes {
		return errGradeThisInspectionBytes
	}
	return nil
}
