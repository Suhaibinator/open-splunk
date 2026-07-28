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
	gradeThisInspectionDatabase = "open_splunk"
	gradeThisInspectionTable    = "events"

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

type gradeThisInspectionPart struct {
	partition string
	parts     uint64
	rows      uint64
	marks     uint64
	maxLevel  uint64
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
	if err := connection.Exec(
		ctx,
		"SYSTEM STOP MERGES "+
			gradeThisInspectionDatabase+"."+
			gradeThisInspectionTable,
	); err != nil {
		t.Fatalf("stop GradeThis inspection merges: %v", err)
	}
	gradeThisAssertNoInspectionMerges(t, ctx, connection)
}

func gradeThisStoreInspectionLoad(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
	profile gradethiscorpus.Profile,
) {
	t.Helper()
	cohorts, err := gradethiscorpus.InspectionLoadCohorts("tenant")
	if err != nil {
		t.Fatal(err)
	}
	if err := gradethiscorpus.ValidateInspectionLoad(
		profile,
		"tenant",
		cohorts,
	); err != nil {
		t.Fatal(err)
	}
	for _, cohort := range cohorts {
		if err := connection.Exec(
			ctx,
			gradeThisInspectionInsertSQL,
			string(cohort.ID),
			cohort.TenantID,
			cohort.IndexName,
			cohort.EventTime,
			profile.IndexTime,
			profile.TraceID,
			string(cohort.ID),
			cohort.Rows,
		); err != nil {
			t.Fatalf(
				"insert GradeThis inspection cohort %q: %v",
				cohort.ID,
				err,
			)
		}
	}
}

const gradeThisInspectionInsertSQL = `
INSERT INTO open_splunk.events
(
	event_id,
	tenant_id,
	index_name,
	event_time,
	index_time,
	collected_at,
	event_time_source,
	host,
	source,
	sourcetype,
	service,
	severity,
	level,
	body,
	raw,
	raw_encoding,
	trace_id,
	span_id,
	fields,
	field_names,
	field_types,
	field_metadata_version,
	collector_id,
	batch_id,
	batch_sequence,
	expires_at,
	visibility_seq
)
SELECT
	concat('gradethis-inspection-', ?, '-', toString(number)),
	?,
	?,
	addNanoseconds(toDateTime64(?, 9, 'UTC'), toInt64(number)),
	toDateTime64(?, 3, 'UTC'),
	CAST(NULL AS Nullable(DateTime64(9, 'UTC'))),
	toUInt8(0),
	'gradethis-inspection',
	'inspection-load',
	'go:zap:json',
	CAST('gradethis' AS Nullable(String)),
	toUInt8(4),
	CAST('ERROR' AS Nullable(String)),
	CAST('Request metrics' AS Nullable(String)),
	concat(
		char(123),
		'"duration":"9999ms","error":"connection refused",',
		'"layer":"load","level":"ERROR","logger":"load-decoy",',
		'"message":"Request metrics","path":"/api/v1/decoy",',
		'"status":599,"sequence":',
		toString(number),
		char(125)
	),
	toUInt8(1),
	CAST(? AS Nullable(String)),
	CAST(NULL AS Nullable(String)),
	CAST(
		concat(
			char(123),
			'"duration":"9999ms","layer":"load","logger":"load-decoy",',
			'"path":"/api/v1/decoy","status":599',
			char(125)
		)
		AS JSON
	),
	['duration', 'layer', 'logger', 'path', 'status'],
	CAST([2, 2, 2, 2, 3] AS Array(UInt8)),
	toUInt8(1),
	'gradethis-inspection',
	concat('gradethis-inspection-', ?),
	number + 1,
	toDateTime64('2100-01-01 00:00:00', 3, 'UTC'),
	toUInt64(1)
FROM numbers(?)`

func gradeThisAssertInspectionPartLayout(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()
	gradeThisAssertNoInspectionMerges(t, ctx, connection)
	rows, err := connection.Query(
		ctx,
		`SELECT
			partition,
			toUInt64(count()),
			toUInt64(sum(rows)),
			toUInt64(sum(marks)),
			toUInt64(max(level))
		FROM system.parts
		WHERE
			database = ?
			AND table = ?
			AND active
		GROUP BY partition
		ORDER BY partition`,
		gradeThisInspectionDatabase,
		gradeThisInspectionTable,
	)
	if err != nil {
		t.Fatalf("read GradeThis inspection part layout: %v", err)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			t.Errorf(
				"close GradeThis inspection part layout: %v",
				closeErr,
			)
		}
	}()

	got := make([]gradeThisInspectionPart, 0, 2)
	for rows.Next() {
		var part gradeThisInspectionPart
		if err := rows.Scan(
			&part.partition,
			&part.parts,
			&part.rows,
			&part.marks,
			&part.maxLevel,
		); err != nil {
			t.Fatalf("scan GradeThis inspection part layout: %v", err)
		}
		got = append(got, part)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate GradeThis inspection part layout: %v", err)
	}

	profile := gradethiscorpus.Fixture()
	want := []gradeThisInspectionPart{
		{
			partition: profile.BaseTime.AddDate(0, 0, -1).
				Format("200601"),
			parts:    1,
			rows:     gradethiscorpus.InspectionLoadRowsPerCohort,
			marks:    2,
			maxLevel: 0,
		},
		{
			partition: profile.BaseTime.Format("200601"),
			parts:     4,
			rows: uint64(len(profile.Events)) +
				3*gradethiscorpus.InspectionLoadRowsPerCohort,
			marks:    8,
			maxLevel: 0,
		},
	}
	if len(got) != len(want) {
		t.Fatalf(
			"GradeThis inspection partitions = %#v, want %#v",
			got,
			want,
		)
	}
	for index := range want {
		if got[index].partition != want[index].partition ||
			got[index].parts != want[index].parts ||
			got[index].rows != want[index].rows ||
			got[index].marks != want[index].marks ||
			got[index].maxLevel != want[index].maxLevel {
			t.Fatalf(
				"GradeThis inspection partition %d = %#v, want %#v",
				index,
				got[index],
				want[index],
			)
		}
	}
	t.Logf("GradeThis inspection part layout: %#v", got)
}

func gradeThisAssertNoInspectionMerges(
	t *testing.T,
	ctx context.Context,
	connection clickhousedriver.Conn,
) {
	t.Helper()
	var active uint64
	if err := connection.QueryRow(
		ctx,
		`SELECT toUInt64(count())
		FROM system.merges
		WHERE database = ? AND table = ?`,
		gradeThisInspectionDatabase,
		gradeThisInspectionTable,
	).Scan(&active); err != nil {
		t.Fatalf("read GradeThis active merges: %v", err)
	}
	if active != 0 {
		t.Fatalf("GradeThis active merges = %d, want 0", active)
	}
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
