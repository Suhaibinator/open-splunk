package knowledgecatalog

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/pgtest"
	"gorm.io/gorm"
)

func TestProjectionIntegrityReadStopsAtAggregateBudgetPlusOne(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	token, visits := newProjectionVisitCounter(t, database)
	query := database.GORMDB().Raw(`
		WITH RECURSIVE sequence(value) AS (
			SELECT 1
			UNION ALL
			SELECT value + 1 FROM sequence WHERE value < ?
		)
		SELECT
			? AS tenant_id,
			'ko-stream-' || lpad((value)::text,5,'0') || '' AS knowledge_object_id,
			1 AS object_version,
			? AS app_id,
			? AS owner_id,
			'field_alias' AS object_type,
			'stream-' || lpad((value)::text,5,'0') || '' AS name,
			'private' AS sharing_scope,
			'active' AS state,
			1 AS description_present,
			repeat('x', (?)::integer) AS description,
			0 AS index_selector_count,
			0 AS host_selector_count,
			0 AS source_selector_count,
			0 AS sourcetype_selector_count,
			0 AS selector_value_bytes,
			0 AS canonical_selector_bytes,
			CAST(? AS BIGINT) AS projection_bytes,
			CAST(? AS BIGINT) AS seal_projection_bytes,
			0 AS seal_canonical_selector_bytes,
			decode(repeat('00', (32)::int), 'hex') AS definition_digest,
			1 AS definition_bytes,
			0 AS dependency_count,
			1 AS created_at_unix_micro,
			1 AS updated_at_unix_micro,
			NULL AS disabled_at_unix_micro,
			NULL AS quarantined_at_unix_micro,
			NULL AS deleted_at_unix_micro,
			NULL AS quarantine_reason,
			octet_length('ko-stream-' || lpad((value)::text,5,'0') || '') AS width_knowledge_object_id_bytes,
			length(convert_to((?)::text,'UTF8')) AS width_tenant_id_bytes,
			length(convert_to((?)::text,'UTF8')) AS width_app_id_bytes,
			length(convert_to((?)::text,'UTF8')) AS width_owner_id_bytes,
			length('field_alias') AS width_object_type_bytes,
			octet_length('stream-' || lpad((value)::text,5,'0') || '') AS width_name_bytes,
			length('private') AS width_sharing_scope_bytes,
			length('active') AS width_state_bytes,
			32 AS width_digest_bytes,
			0 AS width_quarantine_reason_bytes,
			CAST(? AS BIGINT) AS width_description_bytes,
			0 AS width_selector_value_bytes,
			CAST(? AS BIGINT) AS width_projection_bytes,
			CAST(? AS BIGINT) AS width_seal_projection_bytes,
			0 AS width_canonical_selector_bytes,
			0 AS width_seal_canonical_selector_bytes
		FROM sequence
		WHERE `+projectionVisitFunction+`(?, value) = 1
	`,
		maximumObjectsPerTenant,
		testTenant,
		testApp,
		testOwner,
		maximumDescriptionBytes,
		maximumDescriptionBytes,
		maximumDescriptionBytes,
		testTenant,
		testApp,
		testOwner,
		maximumDescriptionBytes,
		maximumDescriptionBytes,
		maximumDescriptionBytes,
		token,
	)

	_, err := readProjectionRecordsBounded(
		query,
		maximumObjectsPerTenant,
		MaximumListFilterIntegrityProjectionBytes,
	)
	if !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("stream aggregate error = %v, want ErrCapacityExceeded", err)
	}
	wantVisits := int64(MaximumListFilterIntegrityProjectionBytes/maximumDescriptionBytes + 1)
	if got := visits.Load(); got < wantVisits || got > wantVisits+1 {
		t.Fatalf("streamed projection visits = %d, want %d or %d", got, wantVisits, wantVisits+1)
	}
	if visits.Load() >= maximumObjectsPerTenant {
		t.Fatalf("streamed projection read drained all %d rows", maximumObjectsPerTenant)
	}
}

func TestBaseProjectionQueryNeverReturnsOversizedRawPayload(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	description := "safe projection"
	insertFixtureObject(t, database, fixtureObject{
		id: "ko-oversized-projection", owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(
				testApp,
				"oversized-projection",
				SharingScopePrivate,
				&description,
				"oversized-*",
			),
			state: StateDraft, mutation: "create", timestamp: 10,
		}},
	})
	dropTableTriggers(t, database, "knowledge_object_list_projections")
	connection, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("acquire projection-corruption connection: %v", err)
	}
	defer connection.Close()
	if _, err := pgtest.Checks(context.Background(), connection, false); err != nil {
		t.Fatalf("disable projection checks: %v", err)
	}
	// Suspend the description btree only to stage a payload that PostgreSQL
	// normally refuses to index; the guarded read must clip the TOAST value.
	if _, err := connection.ExecContext(t.Context(), `DROP INDEX knowledge_list_projection_description_idx`); err != nil {
		t.Fatal(err)
	}
	const sentinel = "SECRET-PROJECTION-PAYLOAD"
	const corruptBytes = 8 << 20
	if _, err := connection.ExecContext(context.Background(), `
		UPDATE knowledge_object_list_projections
		SET description = $1 || repeat('x', ($2)::integer), description_present = 1
		WHERE tenant_id = $3 AND knowledge_object_id = $4 AND object_version = 1
	`, sentinel, corruptBytes-len(sentinel), testTenant, "ko-oversized-projection"); err != nil {
		t.Fatalf("inject oversized projection description: %v", err)
	}
	if _, err := pgtest.Checks(context.Background(), connection, true); err != nil {
		t.Fatalf("restore projection checks: %v", err)
	}

	normalized, err := normalizeListRequest(testReadScope(), ListRequest{})
	if err != nil {
		t.Fatalf("normalize projection guard request: %v", err)
	}
	query := applyListFilters(baseProjectionQuery(database.GORMDB()), normalized).
		Where("projection.knowledge_object_id = ?", "ko-oversized-projection")
	var guarded []projectionReadRecord
	if err := query.Session(&gorm.Session{}).Limit(2).Find(&guarded).Error; err != nil {
		t.Fatalf("read SQL-guarded projection: %v", err)
	}
	if len(guarded) != 1 || guarded[0].DescriptionBytes != corruptBytes ||
		guarded[0].Record.Description != "" {
		t.Fatalf("guarded oversized projection = %#v", guarded)
	}

	_, err = readProjectionRecordsBounded(query, 2, MaximumListFilterIntegrityProjectionBytes)
	if !errors.Is(err, ErrCorrupt) {
		t.Fatalf("oversized projection error = %v, want ErrCorrupt", err)
	}
	if strings.Contains(err.Error(), sentinel) {
		t.Fatalf("oversized projection error leaked raw payload: %v", err)
	}
}

func dropTableTriggers(t *testing.T, database *control.DB, table string) {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(t.Context(), `
		SELECT t.tgname FROM pg_trigger t JOIN pg_class r ON r.oid=t.tgrelid JOIN pg_namespace n ON n.oid=r.relnamespace WHERE n.nspname=current_schema() AND r.relname=$1 AND NOT t.tgisinternal AND t.tgname<>'a000_control_write_lock' ORDER BY t.tgname
	`, table)
	if err != nil {
		t.Fatalf("enumerate %s triggers: %v", table, err)
	}
	var names []string
	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			_ = rows.Close()
			t.Fatalf("scan %s trigger: %v", table, err)
		}
		names = append(names, name)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		t.Fatalf("read %s triggers: %v", table, err)
	}
	if err := rows.Close(); err != nil {
		t.Fatalf("close %s trigger enumeration: %v", table, err)
	}
	for _, name := range names {
		if _, err := database.SQLDB().ExecContext(t.Context(), `DROP TRIGGER "`+strings.ReplaceAll(name, `"`, `""`)+`" ON "`+strings.ReplaceAll(table, `"`, `""`)+`"`); err != nil {
			t.Fatal(err)
		}
	}
}
