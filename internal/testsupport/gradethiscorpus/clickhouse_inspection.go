package gradethiscorpus

import (
	"context"
	"errors"
	"fmt"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
)

const (
	inspectionDatabase = "open_splunk"
	inspectionTable    = "events"
)

// InspectionPart describes one active ClickHouse part in the deterministic
// GradeThis inspection fixture.
type InspectionPart struct {
	Partition string
	Parts     uint64
	Rows      uint64
	Marks     uint64
	MaxLevel  uint64
}

// StopInspectionMerges freezes the disposable inspection table before fixture
// insertion so part and granule evidence remains reproducible.
func StopInspectionMerges(
	ctx context.Context,
	connection clickhousedriver.Conn,
) error {
	if ctx == nil || connection == nil {
		return errors.New(
			"GradeThis inspection context and ClickHouse connection are required",
		)
	}
	if err := connection.Exec(
		ctx,
		"SYSTEM STOP MERGES "+inspectionDatabase+"."+inspectionTable,
	); err != nil {
		return fmt.Errorf("stop GradeThis inspection merges: %w", err)
	}
	var active uint64
	if err := connection.QueryRow(
		ctx,
		`SELECT toUInt64(count())
		FROM system.merges
		WHERE database = ? AND table = ?`,
		inspectionDatabase,
		inspectionTable,
	).Scan(&active); err != nil {
		return fmt.Errorf("read GradeThis active merges: %w", err)
	}
	if active != 0 {
		return errors.New("GradeThis inspection table still has active merges")
	}
	return nil
}

// StoreInspectionLoad inserts the four independently poisoned 7,500-row
// cohorts as four distinct parts.
func StoreInspectionLoad(
	ctx context.Context,
	connection clickhousedriver.Conn,
	profile Profile,
	targetTenant string,
) error {
	if ctx == nil || connection == nil {
		return errors.New(
			"GradeThis inspection context and ClickHouse connection are required",
		)
	}
	cohorts, err := InspectionLoadCohorts(targetTenant)
	if err != nil {
		return err
	}
	if err := ValidateInspectionLoad(
		profile,
		targetTenant,
		cohorts,
	); err != nil {
		return err
	}
	for _, cohort := range cohorts {
		if err := storeInspectionPart(
			ctx,
			connection,
			string(cohort.ID),
			cohort.TenantID,
			cohort.IndexName,
			cohort.EventTime,
			profile,
			cohort.Rows,
		); err != nil {
			return fmt.Errorf(
				"insert GradeThis inspection cohort %q: %w",
				cohort.ID,
				err,
			)
		}
	}
	return nil
}

func storeInspectionPart(
	ctx context.Context,
	connection clickhousedriver.Conn,
	tag string,
	tenantID string,
	indexName string,
	eventTime time.Time,
	profile Profile,
	rows uint64,
) error {
	return connection.Exec(
		ctx,
		inspectionInsertSQL,
		tag,
		tenantID,
		indexName,
		eventTime,
		profile.IndexTime,
		profile.TraceID,
		tag,
		rows,
	)
}

// ReadInspectionPartLayout returns active parts in partition order.
func ReadInspectionPartLayout(
	ctx context.Context,
	connection clickhousedriver.Conn,
) (layout []InspectionPart, resultErr error) {
	if ctx == nil || connection == nil {
		return nil, errors.New(
			"GradeThis inspection context and ClickHouse connection are required",
		)
	}
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
		inspectionDatabase,
		inspectionTable,
	)
	if err != nil {
		return nil, fmt.Errorf(
			"read GradeThis inspection part layout: %w",
			err,
		)
	}
	defer func() {
		if closeErr := rows.Close(); closeErr != nil {
			resultErr = errors.Join(
				resultErr,
				fmt.Errorf(
					"close GradeThis inspection part layout: %w",
					closeErr,
				),
			)
		}
	}()

	layout = make([]InspectionPart, 0, 2)
	for rows.Next() {
		var part InspectionPart
		if err := rows.Scan(
			&part.Partition,
			&part.Parts,
			&part.Rows,
			&part.Marks,
			&part.MaxLevel,
		); err != nil {
			return nil, fmt.Errorf(
				"scan GradeThis inspection part layout: %w",
				err,
			)
		}
		layout = append(layout, part)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf(
			"iterate GradeThis inspection part layout: %w",
			err,
		)
	}
	return layout, nil
}

// ValidateInspectionPartLayout pins the five independent level-zero parts and
// their exact mark geometry on the repository-pinned ClickHouse image.
func ValidateInspectionPartLayout(
	profile Profile,
	layout []InspectionPart,
) error {
	if err := Validate(profile); err != nil {
		return fmt.Errorf("GradeThis inspection target profile: %w", err)
	}
	want := []InspectionPart{
		{
			Partition: profile.BaseTime.Add(-24 * time.Hour).
				Format("200601"),
			Parts:    1,
			Rows:     InspectionLoadRowsPerCohort,
			Marks:    2,
			MaxLevel: 0,
		},
		{
			Partition: profile.BaseTime.Format("200601"),
			Parts:     4,
			Rows: uint64(len(profile.Events)) +
				3*InspectionLoadRowsPerCohort,
			Marks:    8,
			MaxLevel: 0,
		},
	}
	if len(layout) != len(want) {
		return errors.New(
			"GradeThis inspection part layout has the wrong partition count",
		)
	}
	for index := range want {
		if layout[index] != want[index] {
			return errors.New(
				"GradeThis inspection part layout does not match its pinned geometry",
			)
		}
	}
	return nil
}

const inspectionInsertSQL = `
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
	ingest_source_kind,
	ingest_source_id,
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
		'"message":"Request metrics","path":"/api/decoy",',
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
			'"path":"/api/decoy","status":599',
			char(125)
		)
		AS JSON
	),
	['duration', 'layer', 'logger', 'path', 'status'],
	CAST([2, 2, 2, 2, 3] AS Array(UInt8)),
	toUInt8(1),
	'gradethis-inspection',
	toUInt8(1),
	'gradethis-inspection',
	concat('gradethis-inspection-', ?),
	number + 1,
	toDateTime64('2100-01-01 00:00:00', 3, 'UTC'),
	toUInt64(1)
FROM numbers(?)`
