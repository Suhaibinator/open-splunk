package clickhouse

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	sqldriver "database/sql/driver"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"maps"
	"net"
	"path/filepath"
	"reflect"
	"slices"
	"strings"
	"testing"
	"time"

	clickhousedriver "github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"google.golang.org/protobuf/types/known/durationpb"
	"google.golang.org/protobuf/types/known/timestamppb"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/eventfields"
	"github.com/Suhaibinator/open-splunk/internal/ingest"
	"github.com/Suhaibinator/open-splunk/internal/visibility"
)

func TestStoreNativeBatchContractAndEventOrder(t *testing.T) {
	t.Parallel()
	indexTime := time.Date(2026, 7, 21, 3, 4, 6, 987654321, time.FixedZone("offset", -7*60*60))
	committedAt := time.Date(2026, 7, 21, 10, 4, 8, 999999999, time.FixedZone("commit", 2*60*60))
	conn := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	retention := &fakeRetentionProvider{periods: map[string]time.Duration{"main": 72 * time.Hour}}
	store := mustTestStore(t, conn, retention)
	store.clock = func() time.Time { return committedAt }

	first := testStoredEvent("event-2", "main", indexTime)
	first.Event.Raw = []byte{0xff, 0, 'r', 'a', 'w'}
	first.Event.Service = new("")
	first.Event.Level = nil
	first.Event.Message = new("")
	first.Event.TraceId = nil
	first.Event.SpanId = new("")
	second := testStoredEvent("event-1", "main", indexTime)
	sequence := uint64(19)
	input := ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "batch", BatchSequence: sequence,
		SourceBatchSHA256: testSourceBatchDigest("batch"),
		ReceivedAt:        indexTime,
		Events:            []*ingest.StoredEvent{first, second},
	}
	result, err := store.Store(context.Background(), input)
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if conn.prepareCalls != 1 || conn.query != eventsInsertSQL || strings.Contains(conn.query, "?") {
		t.Fatalf("native prepare contract calls=%d query=%q", conn.prepareCalls, conn.query)
	}
	sequencer := store.visibility.(*fakeVisibilitySequencer)
	if len(sequencer.reserveKeys) != 1 || sequencer.reserveKeys[0] != deduplicationToken(input) || !slices.Equal(sequencer.committed, []uint64{1}) {
		t.Fatalf("visibility reserve/commit = %v / %v", sequencer.reserveKeys, sequencer.committed)
	}
	wantSettings := map[string]any{
		"async_insert": uint8(0), "wait_for_async_insert": uint8(1),
		"insert_deduplication_token":                                             deduplicationToken(input),
		"input_format_json_read_numbers_as_strings":                              uint8(0),
		"input_format_json_read_bools_as_numbers":                                uint8(0),
		"input_format_json_read_bools_as_strings":                                uint8(0),
		"input_format_json_infer_array_of_dynamic_from_array_of_different_types": uint8(1),
		"input_format_try_infer_dates":                                           uint8(0),
		"input_format_try_infer_datetimes":                                       uint8(0),
	}
	for name, want := range wantSettings {
		if got := conn.settings[name]; !reflect.DeepEqual(got, want) {
			t.Errorf("setting %s = %#v (%T), want %#v", name, got, got, want)
		}
	}
	if len(conn.batch.rows) != 2 {
		t.Fatalf("rows = %d", len(conn.batch.rows))
	}
	if got := []string{conn.batch.rows[0][0].(string), conn.batch.rows[1][0].(string)}; !slices.Equal(got, []string{"event-2", "event-1"}) {
		t.Fatalf("event order = %v", got)
	}
	if got, ok := conn.batch.rows[0][14].([]byte); !ok || !slices.Equal(got, first.Event.Raw) {
		t.Fatalf("raw = %#v (%T), want byte-safe []byte", conn.batch.rows[0][14], conn.batch.rows[0][14])
	}
	for _, column := range []int{3, 4, 5, eventExpiresAtColumn} {
		value, ok := conn.batch.rows[0][column].(time.Time)
		if !ok || value.Location() != time.UTC {
			t.Errorf("time column %d = %#v (%T), want UTC time.Time", column, conn.batch.rows[0][column], conn.batch.rows[0][column])
		}
	}
	assertOptionalString(t, conn.batch.rows[0][10], true)
	assertOptionalString(t, conn.batch.rows[0][12], false)
	assertOptionalString(t, conn.batch.rows[0][13], true)
	assertOptionalString(t, conn.batch.rows[0][16], false)
	assertOptionalString(t, conn.batch.rows[0][17], true)
	wantIndexTime := indexTime.UTC().Truncate(time.Millisecond)
	if got := conn.batch.rows[0][4]; got != wantIndexTime {
		t.Fatalf("index_time = %v, want %v", got, wantIndexTime)
	}
	if got := conn.batch.rows[0][eventExpiresAtColumn]; got != wantIndexTime.Add(72*time.Hour) {
		t.Fatalf("expires_at = %v", got)
	}
	if got := conn.batch.rows[0][eventVisibilitySequenceColumn]; got != uint64(1) {
		t.Fatalf("visibility_seq = %#v, want 1", got)
	}
	if got, ok := conn.batch.rows[0][27].([]uint8); !ok || !slices.Equal(got, []uint8{uint8(eventfields.StoredValueTypeUint64)}) {
		t.Fatalf("field_types = %#v (%T), want [uint64]", conn.batch.rows[0][27], conn.batch.rows[0][27])
	}
	if got := conn.batch.rows[0][28]; got != eventfields.CurrentFieldMetadataVersion {
		t.Fatalf("field_metadata_version = %#v, want 1", got)
	}
	if got := eventInsertColumns[eventVisibilitySequenceColumn:]; !slices.Equal(got, []string{"visibility_seq", "field_types", "field_metadata_version"}) {
		t.Fatalf("appended insert columns = %#v", got)
	}
	for _, position := range []struct {
		index int
		name  string
	}{
		{index: eventIndexNameColumn, name: "index_name"},
		{index: eventIndexTimeColumn, name: "index_time"},
		{index: eventExpiresAtColumn, name: "expires_at"},
		{index: eventVisibilitySequenceColumn, name: "visibility_seq"},
	} {
		if got := eventInsertColumns[position.index]; got != position.name {
			t.Errorf("event insert column %d = %q, want %q", position.index, got, position.name)
		}
	}
	if conn.batch.sendCalls != 1 || conn.batch.abortCalls != 0 || conn.batch.closeCalls != 1 {
		t.Fatalf("batch lifecycle send=%d abort=%d close=%d", conn.batch.sendCalls, conn.batch.abortCalls, conn.batch.closeCalls)
	}
	if result.Accepted != 2 || result.Duplicate != 0 || result.AcknowledgedThrough != nil {
		t.Fatalf("result = %+v", result)
	}
	if result.CommittedAt != committedAt.UTC().Truncate(time.Microsecond) {
		t.Fatalf("committed_at = %v", result.CommittedAt)
	}
	if !slices.Equal(retention.calls, []string{"tenant/main"}) {
		t.Fatalf("retention calls = %v", retention.calls)
	}
}

func TestStageDurablyQueuesHECWithoutSynchronousClickHouseWrite(t *testing.T) {
	t.Parallel()
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	sequencer := &fakeVisibilitySequencer{
		reservation: visibility.Reservation{
			Sequence:            17,
			HECRequestSequence:  4,
			HECAcknowledgmentID: 9,
		},
	}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	batch := validStoreBatch()
	batch.Source = ingest.HECSource("ingestion-token-record")
	batch.CollectorID = ""
	batch.Events[0].Source = batch.Source
	batch.Events[0].CollectorID = ""
	batch.HECAdmission = &ingest.HECStageAdmission{
		TokenID:               "ingestion-token-record",
		TokenVersion:          3,
		RequestID:             batch.BatchID,
		AcknowledgmentEnabled: true,
		Channel:               "channel-a",
		CreatedAt:             batch.ReceivedAt,
	}

	staged, err := store.Stage(context.Background(), batch)
	if err != nil {
		t.Fatalf("Stage: %v", err)
	}
	if staged.State != ingest.StoredBatchPending || staged.VisibilitySequence != 17 ||
		staged.HECRequestSequence != 4 || staged.HECAcknowledgmentID != 9 ||
		!reflect.DeepEqual(staged.Outcome, ingest.StoreResult{}) {
		t.Fatalf("staged result = %+v", staged)
	}
	if connection.prepareCalls != 0 || len(connection.batch.rows) != 0 {
		t.Fatalf("Stage wrote ClickHouse synchronously: prepares=%d rows=%d", connection.prepareCalls, len(connection.batch.rows))
	}
	if !slices.Equal(sequencer.released, []uint64{17}) || len(sequencer.reservation.Outbox) == 0 {
		t.Fatalf("durable stage release/outbox = %v/%d", sequencer.released, len(sequencer.reservation.Outbox))
	}
	if len(sequencer.reserveRequests) != 1 || sequencer.reserveRequests[0].HECAdmission == nil ||
		sequencer.reserveRequests[0].HECAdmission.TokenID != "ingestion-token-record" ||
		sequencer.reserveRequests[0].HECAdmission.TokenVersion != 3 ||
		sequencer.reserveRequests[0].HECAdmission.AcknowledgmentChannel != "channel-a" ||
		sequencer.reserveRequests[0].StoredRowCount != 1 ||
		sequencer.reserveRequests[0].DecodedEventBytes != decodedEventBytes(batch) {
		t.Fatalf("staged HEC admission = %+v", sequencer.reserveRequests)
	}

	if err := store.ReconcilePending(context.Background()); err != nil {
		t.Fatalf("ReconcilePending: %v", err)
	}
	if connection.prepareCalls != 1 || len(connection.batch.rows) != 1 ||
		!slices.Equal(sequencer.committed, []uint64{17}) {
		t.Fatalf("reconciliation prepares=%d rows=%d commits=%v", connection.prepareCalls, len(connection.batch.rows), sequencer.committed)
	}
	telemetry := store.HECReconciliationTelemetry()
	if !telemetry.Available || telemetry.Successes != 1 || telemetry.Retries != 0 ||
		telemetry.Ambiguities != 0 {
		t.Fatalf("HEC reconciliation telemetry = %+v", telemetry)
	}
}

func TestStageAcceptsIndependentHECRequestsFromOneToken(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	nowMicros := time.Date(2026, time.July, 21, 0, 0, 0, 0, time.UTC).UnixMicro()
	tokenDigest := sha256.Sum256([]byte("HEC independent request token"))
	for _, statement := range []struct {
		query     string
		arguments []any
	}{
		{`
			INSERT INTO indexes (
				index_id, version, name, display_name, ingestion_enabled,
				search_enabled, state, created_at_unix_micro, updated_at_unix_micro
			) VALUES ('hec-index', 1, 'main', 'Main', 1, 1, 'active', ?, ?)`, []any{nowMicros, nowMicros}},
		{`
			INSERT INTO ingestion_tokens (
				ingestion_token_id, version, name, description,
				token_prefix, token_digest, state,
				created_at_unix_micro, updated_at_unix_micro,
				expires_at_unix_micro, revoked_at_unix_micro,
				last_used_at_unix_micro, bound_collector_id,
				max_ingest_events_per_second,
				max_ingest_uncompressed_bytes_per_second, purpose
			) VALUES (
				'hec-token', 1, 'HEC', '', 'hectest0', ?, 'active',
				?, ?, NULL, NULL, NULL, NULL, 0, 0, 'hec'
			)`, []any{tokenDigest[:], nowMicros, nowMicros}},
		{`
			INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
			VALUES ('hec-token', 'hec-index')`, nil},
		{`
			INSERT INTO ingestion_token_hec_profiles (
				ingestion_token_id, default_index_id, default_host,
				default_source, default_sourcetype, indexer_acknowledgment
			) VALUES ('hec-token', 'hec-index', NULL, NULL, NULL, 0)`, nil},
	} {
		if _, err := controlDB.SQLDB().ExecContext(ctx, statement.query, statement.arguments...); err != nil {
			t.Fatalf("seed HEC authority: %v", err)
		}
	}

	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	request := func(id string) ingest.StoreBatch {
		batch := validStoreBatch()
		batch.Source = ingest.HECSource("hec-token")
		batch.CollectorID = ""
		batch.BatchID = id
		batch.BatchSequence = 1
		batch.SourceBatchSHA256 = testSourceBatchDigest(id)
		batch.ReceivedAt = time.Date(2026, time.July, 21, 1, 2, 3, 0, time.UTC)
		batch.Events[0].Source = batch.Source
		batch.Events[0].CollectorID = ""
		batch.Events[0].BatchID = id
		batch.Events[0].Event.EventId = id + "-0"
		batch.HECAdmission = &ingest.HECStageAdmission{
			TokenID:      "hec-token",
			TokenVersion: 1,
			RequestID:    id,
			CreatedAt:    batch.ReceivedAt,
			AuthorizedIndexes: []ingest.HECIndexAuthority{{
				Name: "main", Version: 1,
			}},
		}
		return batch
	}
	first, err := store.Stage(ctx, request("hec-request-one"))
	if err != nil {
		t.Fatalf("Stage(first HEC request): %v", err)
	}
	second, err := store.Stage(ctx, request("hec-request-two"))
	if err != nil {
		t.Fatalf("Stage(second HEC request): %v", err)
	}
	if first.HECRequestSequence != 1 || second.HECRequestSequence != 2 ||
		first.VisibilitySequence == 0 || second.VisibilitySequence <= first.VisibilitySequence {
		t.Fatalf("independent HEC staging = first %+v second %+v", first, second)
	}
	if connection.prepareCalls != 0 {
		t.Fatalf("HEC Stage wrote ClickHouse synchronously %d times", connection.prepareCalls)
	}
}

func TestStoreAssignsCommitOrderedVisibilityAndCapturesCutoff(t *testing.T) {
	t.Parallel()
	conn := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	sequencer := &fakeVisibilitySequencer{reservation: visibility.Reservation{Sequence: 10}, cutoff: 10}
	store := mustTestStoreWithVisibility(t, conn, fixedRetention(time.Hour), sequencer)

	if _, err := store.Store(context.Background(), validStoreBatch()); err != nil {
		t.Fatalf("Store: %v", err)
	}
	if got := conn.batch.rows[0][eventVisibilitySequenceColumn]; got != uint64(10) {
		t.Fatalf("stored visibility = %#v, want 10", got)
	}
	cutoff, err := store.VisibilityCutoff(context.Background())
	if err != nil {
		t.Fatalf("VisibilityCutoff: %v", err)
	}
	if cutoff != 10 || sequencer.cutoffCalls != 1 || !slices.Equal(sequencer.committed, []uint64{10}) {
		t.Fatalf("cutoff=%d calls=%d committed=%v", cutoff, sequencer.cutoffCalls, sequencer.committed)
	}
}

func TestVisibilityLookupFailureIsClassified(t *testing.T) {
	t.Parallel()
	sequencerErr := errors.New("control database unavailable")
	conn := &fakeStoreConnection{}
	sequencer := &fakeVisibilitySequencer{reserveErr: sequencerErr, cutoffErr: sequencerErr}
	store := mustTestStoreWithVisibility(t, conn, fixedRetention(time.Hour), sequencer)

	if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
		t.Fatalf("Store error = %v, want transient visibility lookup failure", err)
	}
	if _, err := store.VisibilityCutoff(context.Background()); !isTransient(err) {
		t.Fatalf("VisibilityCutoff error = %v, want transient failure", err)
	}
}

func TestVisibilityIdentityConflictUsesTerminalIngestError(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{lookupErr: visibility.ErrConflict}
	store := mustTestStoreWithVisibility(t, &fakeStoreConnection{}, fixedRetention(time.Hour), sequencer)

	_, err := store.Store(context.Background(), validStoreBatch())
	var conflict *ingest.DurableIdentityConflictError
	if !errors.As(err, &conflict) || isTransient(err) {
		t.Fatalf("Store error = %v, want terminal DurableIdentityConflictError", err)
	}
}

func TestVisibilityFinalizationDeadlineIsRetryable(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{
		reservation: visibility.Reservation{Sequence: 1},
		commitErr:   context.DeadlineExceeded,
	}
	store := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{}}, fixedRetention(time.Hour), sequencer)
	if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
		t.Fatalf("Store error = %v, want retryable finalization failure", err)
	}
}

func TestStorePreservesAmbiguousReservationAndRecognizesCommittedRetry(t *testing.T) {
	t.Parallel()
	batch := validStoreBatch()
	write := &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF}
	connection := &fakeStoreConnection{batch: write}
	sequencer := &fakeVisibilitySequencer{reservation: visibility.Reservation{Sequence: 7}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)

	if _, err := store.Store(context.Background(), batch); !isTransient(err) {
		t.Fatalf("ambiguous Store error = %v", err)
	}
	if !slices.Equal(sequencer.released, []uint64{7}) || len(sequencer.committed) != 0 {
		t.Fatalf("ambiguous insert lease lifecycle: release=%v commit=%v", sequencer.released, sequencer.committed)
	}

	connection.batch = &fakeWriteBatch{}
	batch.ReceivedAt = batch.ReceivedAt.Add(time.Hour)
	batch.Events[0].IndexTime = batch.ReceivedAt
	if _, err := store.Store(context.Background(), batch); err != nil {
		t.Fatalf("retry Store: %v", err)
	}
	if got := connection.batch.rows[0][eventVisibilitySequenceColumn]; got != uint64(7) {
		t.Fatalf("retry visibility = %#v, want stable 7", got)
	}
	if got := connection.batch.rows[0][4]; got != time.Date(2026, 7, 21, 1, 2, 3, 0, time.UTC) {
		t.Fatalf("retry index time = %v, want first-attempt time", got)
	}
	if !slices.Equal(sequencer.committed, []uint64{7}) {
		t.Fatalf("committed = %v", sequencer.committed)
	}

	connection.prepareCalls = 0
	result, err := store.Store(context.Background(), batch)
	if err != nil {
		t.Fatalf("committed retry: %v", err)
	}
	if result.Accepted != 0 || result.Duplicate != 1 || connection.prepareCalls != 0 {
		t.Fatalf("committed retry result=%+v prepareCalls=%d", result, connection.prepareCalls)
	}
}

func TestStoreRebuildsFreshReservationAfterObservedPendingIsAbandoned(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })

	stale := validStoreBatch()
	stale.Events[0].Event.Raw = []byte(`{"message":"stale-normalization"}`)
	fresh := validStoreBatch()
	fresh.Events[0].Event.Raw = []byte(`{"message":"fresh-normalization"}`)
	seedStore := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{},
		fixedRetention(time.Hour),
		sequencer,
	)
	rows, err := seedStore.rowsForBatch(ctx, stale, nil)
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := encodeReservationMetadata(rows, stale)
	if err != nil {
		t.Fatal(err)
	}
	outbox, err := encodeStoreOutbox(stale)
	if err != nil {
		t.Fatal(err)
	}
	payloadDigest, err := storePayloadDigest(stale)
	if err != nil {
		t.Fatal(err)
	}
	const staleAttemptID = "stale-normalization-owner"
	pending, err := sequencer.Reserve(ctx, visibility.ReserveRequest{
		BatchKey:          deduplicationToken(stale),
		SequenceKey:       sequenceIdentityKey(stale),
		AttemptID:         staleAttemptID,
		IndexTime:         stale.ReceivedAt,
		PayloadSHA256:     payloadDigest,
		Metadata:          metadata,
		Outbox:            outbox,
		StoredRowCount:    uint32(len(rows)),
		DecodedEventBytes: decodedEventBytes(stale),
	})
	if err != nil {
		t.Fatal(err)
	}

	tracing := &abandonBeforeExistingReserveSequencer{
		Sequencer: sequencer,
		sequence:  pending.Sequence,
		owner:     staleAttemptID,
	}
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), tracing)
	result, err := store.Store(ctx, fresh)
	if err != nil {
		t.Fatalf("Store after pending abandonment: %v", err)
	}
	if result.Accepted != 1 || result.Duplicate != 0 || result.BatchRejection != nil {
		t.Fatalf("Store result = %+v, want one newly accepted event", result)
	}
	if len(tracing.requests) != 2 {
		t.Fatalf("Reserve requests = %d, want one acquisition plus one bounded fallback", len(tracing.requests))
	}
	acquire, allocate := tracing.requests[0], tracing.requests[1]
	if !acquire.ExistingOnly || !acquire.IndexTime.IsZero() ||
		acquire.Metadata != nil || acquire.Outbox != nil {
		t.Fatalf("first Reserve request = %+v, want identity-only existing acquisition", acquire)
	}
	if allocate.ExistingOnly || allocate.AttemptID != acquire.AttemptID ||
		!allocate.IndexTime.Equal(fresh.ReceivedAt) || len(allocate.Metadata) == 0 || len(allocate.Outbox) == 0 {
		t.Fatalf("fallback Reserve request = %+v, want full fresh allocation with reused clean attempt", allocate)
	}
	replayed, err := decodeStoreOutbox(allocate.Outbox)
	if err != nil {
		t.Fatalf("decode fallback outbox: %v", err)
	}
	if got := replayed.Events[0].Event.GetRaw(); !slices.Equal(got, fresh.Events[0].Event.GetRaw()) ||
		slices.Equal(got, stale.Events[0].Event.GetRaw()) {
		t.Fatalf("fallback outbox raw = %q, want fresh %q and not stale %q", got, fresh.Events[0].Event.GetRaw(), stale.Events[0].Event.GetRaw())
	}
	if connection.prepareCalls != 1 || connection.batch.sendCalls != 1 || len(connection.batch.rows) != 1 {
		t.Fatalf(
			"ClickHouse work = prepares %d sends %d rows %d, want 1/1/1",
			connection.prepareCalls,
			connection.batch.sendCalls,
			len(connection.batch.rows),
		)
	}
	if got := connection.batch.rows[0][eventVisibilitySequenceColumn]; got != pending.Sequence+1 {
		t.Fatalf("fresh visibility sequence = %#v, want %d", got, pending.Sequence+1)
	}
	var total, abandoned, committed int
	if err := controlDB.SQLDB().QueryRowContext(ctx, `
		SELECT count(*),
		       count(*) FILTER (WHERE state = 'abandoned'),
		       count(*) FILTER (WHERE state = 'committed')
		FROM ingest_visibility_reservations`).Scan(&total, &abandoned, &committed); err != nil {
		t.Fatal(err)
	}
	if total != 2 || abandoned != 1 || committed != 1 {
		t.Fatalf("visibility rows = total %d abandoned %d committed %d, want 2/1/1", total, abandoned, committed)
	}
}

func TestStoreBoundsRepeatedReservationGoneAsServerBusy(t *testing.T) {
	t.Parallel()
	sequencer := &alwaysGoneVisibilitySequencer{
		fakeVisibilitySequencer: &fakeVisibilitySequencer{
			reservation:    visibility.Reservation{Sequence: 1},
			hasReservation: true,
		},
	}
	connection := &fakeStoreConnection{}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)

	_, err := store.Store(context.Background(), validStoreBatch())
	assertTransient(
		t,
		err,
		opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY,
	)
	if _, ok := errors.AsType[*ingest.StoredBatchGoneError](err); ok {
		t.Fatalf("ordinary Store leaked StoredBatchGoneError: %v", err)
	}
	if !errors.Is(err, visibility.ErrReservationGone) {
		t.Fatalf("Store error = %v, want preserved ErrReservationGone cause", err)
	}
	if len(sequencer.requests) != 2 || !sequencer.requests[0].ExistingOnly ||
		sequencer.requests[1].ExistingOnly {
		t.Fatalf("bounded Reserve requests = %+v, want existing-only then one fresh allocation", sequencer.requests)
	}
	if connection.prepareCalls != 0 {
		t.Fatalf("exhausted fallback prepared ClickHouse %d times", connection.prepareCalls)
	}
}

func TestServerOwnedReconcilerResolvesGapAndAdvancesCommittedFrontier(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })

	firstBatch := validStoreBatch()
	firstConnection := &fakeStoreConnection{batch: &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF}}
	first := mustTestStoreWithVisibility(t, firstConnection, fixedRetention(72*time.Hour), sequencer)
	if _, err := first.Store(ctx, firstBatch); !isTransient(err) {
		t.Fatalf("ambiguous first Store error = %v, want transient", err)
	}

	secondBatch := validStoreBatch()
	secondBatch.BatchID = "later-batch"
	secondBatch.BatchSequence = 2
	secondBatch.SourceBatchSHA256 = testSourceBatchDigest("later-batch")
	secondBatch.Events[0].BatchID = secondBatch.BatchID
	secondConnection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	second := mustTestStoreWithVisibility(t, secondConnection, fixedRetention(time.Hour), sequencer)
	if _, err := second.Store(ctx, secondBatch); !errors.Is(err, visibility.ErrAmbiguousBarrier) || !isTransient(err) {
		t.Fatalf("later Store behind ambiguous send error = %v, want transient barrier", err)
	}
	if secondConnection.prepareCalls != 0 {
		t.Fatalf("later Store reached ClickHouse %d times behind ambiguous send", secondConnection.prepareCalls)
	}

	firstConnection.batch = &fakeWriteBatch{}
	if err := first.ReconcilePending(ctx); err != nil {
		t.Fatalf("ReconcilePending: %v", err)
	}
	telemetry := first.HECReconciliationTelemetry()
	if !telemetry.Available || telemetry.Successes != 1 || telemetry.Retries != 0 ||
		telemetry.Ambiguities != 1 {
		t.Fatalf("ambiguous reconciliation telemetry = %+v", telemetry)
	}
	if len(firstConnection.batch.rows) != 1 || firstConnection.batch.rows[0][eventVisibilitySequenceColumn] != uint64(1) {
		t.Fatalf("reconciled rows = %#v, want original sequence 1", firstConnection.batch.rows)
	}
	if got := firstConnection.batch.rows[0][14]; !slices.Equal(got.([]byte), firstBatch.Events[0].Event.Raw) {
		t.Fatalf("reconciled raw = %#v, want persisted original", got)
	}
	if cutoff, err := first.VisibilityCutoff(ctx); err != nil || cutoff != 1 {
		t.Fatalf("frontier after server reconciliation = %d, err=%v, want 1", cutoff, err)
	}
	if _, err := second.Store(ctx, secondBatch); err != nil {
		t.Fatalf("later Store after reconciliation: %v", err)
	}
	if cutoff, err := first.VisibilityCutoff(ctx); err != nil || cutoff != 2 {
		t.Fatalf("frontier after later Store = %d, err=%v, want 2", cutoff, err)
	}
	state, result, err := first.LookupBatch(ctx, ingest.StoreBatchIdentity{
		TenantID: firstBatch.TenantID, CollectorID: firstBatch.CollectorID,
		BatchID: firstBatch.BatchID, BatchSequence: firstBatch.BatchSequence,
		SourceBatchSHA256: firstBatch.SourceBatchSHA256,
	})
	if err != nil || state != ingest.StoredBatchCommitted || result.Duplicate != 1 {
		t.Fatalf("committed lookup state=%v result=%+v err=%v", state, result, err)
	}
}

func TestBackgroundReconcilerDrainsOutboxWithoutCollectorRetry(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	if staged, err := store.Stage(ctx, validStoreBatch()); err != nil ||
		staged.State != ingest.StoredBatchPending {
		t.Fatalf("Stage = %+v error=%v, want pending durable outbox", staged, err)
	}

	store.retryAfter = time.Millisecond
	store.startReconciler()
	deadline := time.Now().Add(5 * time.Second)
	for {
		cutoff, cutoffErr := store.VisibilityCutoff(ctx)
		if cutoffErr != nil {
			t.Fatal(cutoffErr)
		}
		if cutoff == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("background reconciler did not commit the durable outbox")
		}
		time.Sleep(time.Millisecond)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if len(connection.batch.rows) != 1 || connection.batch.rows[0][eventVisibilitySequenceColumn] != uint64(1) {
		t.Fatalf("background replay rows = %#v", connection.batch.rows)
	}
}

func TestBackgroundReconcilerCountsScheduledRetries(t *testing.T) {
	t.Parallel()
	pruned := make(chan struct{}, 2)
	sequencer := &fakeVisibilitySequencer{
		pruneErrors: []error{io.ErrUnexpectedEOF, nil},
		pruneNotify: pruned,
	}
	store := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{},
		fixedRetention(time.Hour),
		sequencer,
	)
	store.retryAfter = time.Millisecond
	store.startReconciler()
	defer store.Close()

	for attempt := range 2 {
		select {
		case <-pruned:
		case <-time.After(5 * time.Second):
			t.Fatalf("background reconciliation attempt %d did not run", attempt+1)
		}
	}
	deadline := time.Now().Add(5 * time.Second)
	for {
		telemetry := store.HECReconciliationTelemetry()
		if telemetry.Available && telemetry.Retries == 1 {
			if telemetry.Successes != 0 || telemetry.Ambiguities != 0 {
				t.Fatalf("retry reconciliation telemetry = %+v", telemetry)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("retry reconciliation telemetry did not settle: %+v", telemetry)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestReconcilePendingAdmissionIsContextCancelable(t *testing.T) {
	t.Parallel()
	store := mustTestStore(t, &fakeStoreConnection{}, fixedRetention(time.Hour))
	<-store.reconcileSlot
	defer func() { store.reconcileSlot <- struct{}{} }()

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- store.ReconcilePending(ctx) }()
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("ReconcilePending error = %v, want context canceled", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("ReconcilePending did not stop while waiting for its process-local slot")
	}
}

func TestCloseCancelsAndWaitsForManualReconciliationBeforeClosingConnection(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{reservation: visibility.Reservation{Sequence: 1}}
	store := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{batch: &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF}},
		fixedRetention(time.Hour),
		sequencer,
	)
	if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
		t.Fatalf("seed pending Store error = %v, want transient", err)
	}
	gate := &gatedStoreConnection{
		entered:          make(chan struct{}),
		resume:           make(chan struct{}),
		exited:           make(chan struct{}),
		closed:           make(chan struct{}),
		closedBeforeExit: make(chan struct{}),
	}
	store.connection = gate
	reconcileDone := make(chan error, 1)
	go func() { reconcileDone <- store.ReconcilePending(context.Background()) }()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("manual reconciliation did not reach ClickHouse")
	}

	closeDone := make(chan error, 1)
	go func() { closeDone <- store.Close() }()
	select {
	case <-gate.exited:
	case <-time.After(5 * time.Second):
		t.Fatal("Close did not cancel manual reconciliation")
	}
	if err := <-reconcileDone; !errors.Is(err, ErrStoreClosed) {
		t.Fatalf("manual reconciliation error = %v, want ErrStoreClosed", err)
	}
	select {
	case <-gate.closed:
	case <-time.After(5 * time.Second):
		t.Fatal("connection did not close after manual reconciliation stopped")
	}
	select {
	case <-gate.closedBeforeExit:
		t.Fatal("connection closed before manual reconciliation left ClickHouse")
	default:
	}
	if err := <-closeDone; err != nil {
		t.Fatal(err)
	}
}

func TestReconcilePendingPrunesAtClickHouseDeduplicationHorizon(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{}
	store := mustTestStoreWithVisibility(t, &fakeStoreConnection{}, fixedRetention(time.Hour), sequencer)
	if err := store.ReconcilePending(context.Background()); err != nil {
		t.Fatal(err)
	}
	wantRetention := visibility.TerminalRetention{
		Committed:             10_000,
		Rejected:              10_000,
		RejectedMetadataBytes: 256 << 20,
	}
	if sequencer.pruneRetention != wantRetention || sequencer.pruneLimit != visibilityPruneBatch {
		t.Fatalf("prune policy = retain %+v limit %d", sequencer.pruneRetention, sequencer.pruneLimit)
	}
}

func TestReconcilePendingPrunesAfterReplayFailureWithoutMaskingIt(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{
		reservation: visibility.Reservation{Sequence: 1},
	}
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
		t.Fatalf("seed pending Store error = %v, want transient", err)
	}
	sequencer.reservation.PreviouslyReserved = true
	sequencer.pruneErr = visibility.ErrPendingCapacity
	connection.batch = &fakeWriteBatch{}
	connection.prepareErr = io.ErrUnexpectedEOF

	err := store.ReconcilePending(context.Background())
	if !errors.Is(err, io.ErrUnexpectedEOF) || !errors.Is(err, visibility.ErrPendingCapacity) {
		t.Fatalf("ReconcilePending error = %v, want replay and prune causes", err)
	}
	var transient *ingest.TransientStoreError
	if !errors.As(err, &transient) ||
		transient.Reason != opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE {
		t.Fatalf("joined retry = %#v, want primary replay classification", transient)
	}
	if sequencer.pruneCalls != 1 {
		t.Fatalf("normal replay failure prune calls = %d, want 1", sequencer.pruneCalls)
	}

	sequencer.pruneCalls = 0
	err = store.reconcilePending(context.Background(), true)
	if !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("exclusive replay error = %v, want original replay failure", err)
	}
	if sequencer.pruneCalls != 0 {
		t.Fatalf("exclusive proveDrained pruned %d times", sequencer.pruneCalls)
	}
}

func TestReconcilePendingBoundsTerminalPruneDuringPersistentReplayFailure(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	connection := &fakeStoreConnection{batch: &fakeWriteBatch{sendErr: io.ErrUnexpectedEOF}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	if _, err := store.Store(ctx, validStoreBatch()); !isTransient(err) {
		t.Fatalf("seed pending Store error = %v, want transient", err)
	}

	const rejectedRows = durableBatchRejectWindow + visibilityPruneBatch + 1
	lastSequence := int64(rejectedRows + 1)
	tx, err := controlDB.SQLDB().BeginTx(ctx, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
		WITH RECURSIVE sequences(sequence) AS (
			SELECT 2
			UNION ALL
			SELECT sequence + 1 FROM sequences WHERE sequence < ?
		)
		INSERT INTO ingest_batch_identities
			(batch_key, sequence_key, payload_sha256, first_visibility_seq, created_at_unix_micro)
		SELECT printf('retention-reject-%d', sequence),
		       printf('retention-sequence-%d', sequence),
		       zeroblob(32), sequence, sequence
		FROM sequences`, lastSequence); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
		WITH RECURSIVE sequences(sequence) AS (
			SELECT 2
			UNION ALL
			SELECT sequence + 1 FROM sequences WHERE sequence < ?
		)
		INSERT INTO ingest_visibility_reservations
			(sequence, batch_key, state, phase, attempt_id, index_time_unix_milli,
			 metadata, outbox, outbox_sha256, stored_row_count, decoded_event_bytes,
			 created_at_unix_micro, committed_at_unix_micro)
		SELECT sequence, printf('retention-reject-%d', sequence),
		       'rejected', 'final', '', 0, X'', X'', X'', 0, 0, sequence, sequence
		FROM sequences`, lastSequence); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if _, err := tx.ExecContext(ctx, `
		UPDATE ingest_visibility_state SET last_assigned = ? WHERE singleton = 1`,
		lastSequence,
	); err != nil {
		_ = tx.Rollback()
		t.Fatal(err)
	}
	if err := tx.Commit(); err != nil {
		t.Fatal(err)
	}

	connection.batch = &fakeWriteBatch{}
	connection.prepareErr = io.ErrUnexpectedEOF
	err = store.ReconcilePending(ctx)
	if !errors.Is(err, io.ErrUnexpectedEOF) || !isTransient(err) {
		t.Fatalf("ReconcilePending error = %v, want primary transient replay failure", err)
	}
	var rejected, pending int
	var pendingAttempt string
	if err := controlDB.SQLDB().QueryRowContext(ctx, `
		SELECT count(*) FILTER (WHERE state = 'rejected'),
		       count(*) FILTER (WHERE state = 'reserved'),
		       coalesce(max(attempt_id) FILTER (WHERE state = 'reserved'), '')
		FROM ingest_visibility_reservations`).Scan(&rejected, &pending, &pendingAttempt); err != nil {
		t.Fatal(err)
	}
	if rejected != rejectedRows-visibilityPruneBatch || pending != 1 || pendingAttempt != "" {
		t.Fatalf(
			"post-failure ledger = rejected %d pending %d attempt %q, want %d/1/empty",
			rejected,
			pending,
			pendingAttempt,
			rejectedRows-visibilityPruneBatch,
		)
	}
}

func TestTerminalReservationsScheduleBoundedMaintenance(t *testing.T) {
	t.Parallel()
	pruned := make(chan struct{}, 2)
	sequencer := &fakeVisibilitySequencer{pruneNotify: pruned}
	store := mustTestStoreWithVisibility(t, &fakeStoreConnection{}, fixedRetention(time.Hour), sequencer)
	store.startReconciler()
	defer store.Close()

	select {
	case <-pruned: // startup maintenance
	case <-time.After(5 * time.Second):
		t.Fatal("startup maintenance did not run")
	}
	store.terminalCount.Store(visibilityPruneBatch - 1)
	store.noteTerminalReservation()
	select {
	case <-pruned:
	case <-time.After(5 * time.Second):
		t.Fatal("terminal reservation threshold did not schedule pruning")
	}
}

func TestTerminalRejectionBytesScheduleConcurrentBoundedMaintenance(t *testing.T) {
	t.Parallel()
	store := &Store{reconcileWake: make(chan struct{}, 1)}

	const workers = 257
	metadataBytes := uint64(durableBatchRejectPruneWakeBytes/32 + 1)
	start := make(chan struct{})
	done := make(chan struct{}, workers)
	for range workers {
		go func() {
			<-start
			store.noteTerminalRejection(metadataBytes)
			done <- struct{}{}
		}()
	}
	close(start)
	for range workers {
		<-done
	}

	wantBytes := (uint64(workers) * metadataBytes) % durableBatchRejectPruneWakeBytes
	if got := store.rejectionWakeBytes.Load(); got != wantBytes {
		t.Fatalf("concurrent rejected metadata bytes = %d, want %d", got, wantBytes)
	}
	if got := store.terminalCount.Load(); got != workers {
		t.Fatalf("concurrent terminal count = %d, want %d", got, workers)
	}
	select {
	case <-store.reconcileWake:
	default:
		t.Fatal("rejected metadata crossed its byte interval without scheduling maintenance")
	}
}

func TestStoreReleasesPreviouslyAmbiguousAttemptOnPreSendRetryFailure(t *testing.T) {
	t.Parallel()
	connection := &fakeStoreConnection{prepareErr: io.ErrUnexpectedEOF}
	sequencer := &fakeVisibilitySequencer{reservation: visibility.Reservation{
		Sequence: 12, PreviouslyReserved: true, MayHaveReachedStorage: true,
	}}
	store := mustTestStoreWithVisibility(t, connection, fixedRetention(time.Hour), sequencer)
	if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
		t.Fatalf("Store error = %v, want transient", err)
	}
	if !slices.Equal(sequencer.released, []uint64{12}) {
		t.Fatalf("previously ambiguous attempt releases = %v, want [12]", sequencer.released)
	}
}

func TestStoreNeverAbandonsRecoveredUnsentOutbox(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{reservation: visibility.Reservation{
		Sequence: 13, PreviouslyReserved: true,
	}}
	store := mustTestStoreWithVisibility(
		t,
		&fakeStoreConnection{prepareErr: errors.New("permanent schema mismatch")},
		fixedRetention(time.Hour),
		sequencer,
	)
	if _, err := store.Store(context.Background(), validStoreBatch()); err == nil || isTransient(err) {
		t.Fatalf("Store error = %v, want original permanent failure", err)
	}
	if !slices.Equal(sequencer.released, []uint64{13}) || len(sequencer.abandoned) != 0 {
		t.Fatalf("recovered lifecycle abandon=%v release=%v", sequencer.abandoned, sequencer.released)
	}
}

func TestStoreRetainsNewUnsentOutboxOnTransientPreSendFailure(t *testing.T) {
	t.Parallel()
	sequencer := &fakeVisibilitySequencer{reservation: visibility.Reservation{Sequence: 4}}
	store := mustTestStoreWithVisibility(t, &fakeStoreConnection{prepareErr: io.ErrUnexpectedEOF}, fixedRetention(time.Hour), sequencer)
	if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
		t.Fatalf("Store error = %v, want transient", err)
	}
	if !slices.Equal(sequencer.released, []uint64{4}) || len(sequencer.abandoned) != 0 {
		t.Fatalf("unsent transient lifecycle abandon=%v release=%v", sequencer.abandoned, sequencer.released)
	}
}

func TestStoreFailedMarkSendingResolvesUnsentOrAmbiguousPhase(t *testing.T) {
	t.Parallel()
	t.Run("deduplication barrier preserves outbox", func(t *testing.T) {
		sequencer := &fakeVisibilitySequencer{
			reservation: visibility.Reservation{Sequence: 4}, markErr: visibility.ErrAmbiguousBarrier,
		}
		store := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{}}, fixedRetention(time.Hour), sequencer)
		if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
			t.Fatalf("Store error = %v, want transient", err)
		}
		if !slices.Equal(sequencer.released, []uint64{4}) || len(sequencer.abandoned) != 0 {
			t.Fatalf("barrier lifecycle abandon=%v release=%v", sequencer.abandoned, sequencer.released)
		}
	})
	t.Run("recovered unsent failure preserves outbox", func(t *testing.T) {
		sequencer := &fakeVisibilitySequencer{
			reservation: visibility.Reservation{Sequence: 7, PreviouslyReserved: true},
			markErr:     context.Canceled,
		}
		store := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{}}, fixedRetention(time.Hour), sequencer)
		if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
			t.Fatalf("Store error = %v, want transient", err)
		}
		if !slices.Equal(sequencer.released, []uint64{7}) || len(sequencer.abandoned) != 0 {
			t.Fatalf("recovered lifecycle abandon=%v release=%v", sequencer.abandoned, sequencer.released)
		}
	})
	t.Run("unsent is abandoned", func(t *testing.T) {
		sequencer := &fakeVisibilitySequencer{
			reservation: visibility.Reservation{Sequence: 5}, markErr: context.DeadlineExceeded,
		}
		store := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{}}, fixedRetention(time.Hour), sequencer)
		if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
			t.Fatalf("Store error = %v, want transient", err)
		}
		if !slices.Equal(sequencer.abandoned, []uint64{5}) || len(sequencer.released) != 0 {
			t.Fatalf("lifecycle abandon=%v release=%v", sequencer.abandoned, sequencer.released)
		}
	})
	t.Run("ambiguous is released", func(t *testing.T) {
		sequencer := &fakeVisibilitySequencer{
			reservation: visibility.Reservation{Sequence: 6}, markErr: context.DeadlineExceeded,
			abandonErr: visibility.ErrAttemptLease,
		}
		store := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{}}, fixedRetention(time.Hour), sequencer)
		if _, err := store.Store(context.Background(), validStoreBatch()); !isTransient(err) {
			t.Fatalf("Store error = %v, want transient", err)
		}
		if !slices.Equal(sequencer.released, []uint64{6}) {
			t.Fatalf("ambiguous release=%v, want [6]", sequencer.released)
		}
	})
}

func TestStoreAttemptLeaseFencesConcurrentWriters(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })

	gate := &gatedStoreConnection{
		entered: make(chan struct{}),
		resume:  make(chan struct{}),
		err:     io.ErrUnexpectedEOF,
	}
	first := mustTestStoreWithVisibility(t, gate, fixedRetention(time.Hour), sequencer)
	firstDone := make(chan error, 1)
	go func() {
		_, storeErr := first.Store(ctx, validStoreBatch())
		firstDone <- storeErr
	}()
	select {
	case <-gate.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("first Store did not reach Prepare")
	}

	secondConnection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	second := mustTestStoreWithVisibility(t, secondConnection, fixedRetention(time.Hour), sequencer)
	if _, err := second.Store(ctx, validStoreBatch()); !errors.Is(err, visibility.ErrAttemptInProgress) {
		t.Fatalf("same batch while first attempt active error = %v, want ErrAttemptInProgress", err)
	}
	if secondConnection.prepareCalls != 0 {
		t.Fatalf("fenced same batch reached ClickHouse Prepare %d times", secondConnection.prepareCalls)
	}

	different := validStoreBatch()
	different.BatchID = "different-batch"
	different.BatchSequence = 2
	different.SourceBatchSHA256 = testSourceBatchDigest("different-batch")
	different.Events[0].BatchID = different.BatchID
	if _, err := second.Store(ctx, different); err != nil {
		t.Fatalf("different batch behind first attempt: %v", err)
	}
	if secondConnection.prepareCalls != 1 || secondConnection.batch.rows[0][eventVisibilitySequenceColumn] != uint64(2) {
		t.Fatalf("independent batch prepare=%d sequence=%#v, want one insert at sequence 2", secondConnection.prepareCalls, secondConnection.batch.rows[0][eventVisibilitySequenceColumn])
	}
	if cutoff, err := second.VisibilityCutoff(ctx); err != nil || cutoff != 0 {
		t.Fatalf("cutoff behind unresolved sequence 1 = %d, err=%v", cutoff, err)
	}

	close(gate.resume)
	select {
	case firstErr := <-firstDone:
		if !isTransient(firstErr) {
			t.Fatalf("first Store error = %v, want transient", firstErr)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("first Store did not resolve its unsent reservation")
	}

	retryConnection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	retry := mustTestStoreWithVisibility(t, retryConnection, fixedRetention(time.Hour), sequencer)
	if _, err := retry.Store(ctx, validStoreBatch()); err != nil {
		t.Fatalf("retry Store: %v", err)
	}
	if got := retryConnection.batch.rows[0][eventVisibilitySequenceColumn]; got != uint64(1) {
		t.Fatalf("retry visibility sequence = %#v, want durable outbox sequence 1", got)
	}
	cutoff, err := retry.VisibilityCutoff(ctx)
	if err != nil || cutoff != 2 {
		t.Fatalf("cutoff after retry = %d, want 2, err=%v", cutoff, err)
	}
}

func TestStorePermanentPreSendFailureDoesNotBlockLaterBatch(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	bad := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{
		appendErr: errors.New("deterministic native conversion failure"),
	}}, fixedRetention(time.Hour), sequencer)
	if _, err := bad.Store(ctx, validStoreBatch()); err == nil || isTransient(err) {
		t.Fatalf("bad Store error = %v, want permanent", err)
	}
	cutoff, err := bad.VisibilityCutoff(ctx)
	if err != nil || cutoff != 1 {
		t.Fatalf("cutoff after abandoned unsent sequence = %d, err=%v", cutoff, err)
	}

	nextBatch := validStoreBatch()
	nextBatch.BatchID = "next-batch"
	nextBatch.BatchSequence = 2
	nextBatch.Events[0].BatchID = nextBatch.BatchID
	nextConnection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	next := mustTestStoreWithVisibility(t, nextConnection, fixedRetention(time.Hour), sequencer)
	if _, err := next.Store(ctx, nextBatch); err != nil {
		t.Fatalf("next Store: %v", err)
	}
	if got := nextConnection.batch.rows[0][eventVisibilitySequenceColumn]; got != uint64(2) {
		t.Fatalf("next visibility sequence = %#v, want 2", got)
	}
}

func TestStoreRetryUsesPersistedRetentionBeforeLivePolicy(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	batch := validStoreBatch()
	firstProvider := &fakeRetentionProvider{periods: map[string]time.Duration{"main": 72 * time.Hour}}
	first := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{
		sendErr: io.ErrUnexpectedEOF,
	}}, firstProvider, sequencer)
	if _, err := first.Store(ctx, batch); !isTransient(err) {
		t.Fatalf("ambiguous Store error = %v, want transient", err)
	}

	unavailablePolicy := &fakeRetentionProvider{err: errors.New("index policy was removed")}
	retryConnection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	retry := mustTestStoreWithVisibility(t, retryConnection, unavailablePolicy, sequencer)
	retryBatch := validStoreBatch()
	retryBatch.ReceivedAt = retryBatch.ReceivedAt.Add(time.Hour)
	retryBatch.Events[0].IndexTime = retryBatch.ReceivedAt
	retryBatch.Events[0].Event.Raw = []byte(`{"message":"new redaction policy"}`)
	if _, err := retry.Store(ctx, retryBatch); err != nil {
		t.Fatalf("pending retry with unavailable policy: %v", err)
	}
	if len(unavailablePolicy.calls) != 0 {
		t.Fatalf("pending retry consulted live retention: %v", unavailablePolicy.calls)
	}
	if got := retryConnection.batch.rows[0][eventExpiresAtColumn]; got != batch.ReceivedAt.UTC().Add(72*time.Hour) {
		t.Fatalf("retried expires_at = %v, want persisted retention", got)
	}

	committedPolicy := &fakeRetentionProvider{err: errors.New("still unavailable")}
	committed := mustTestStoreWithVisibility(t, &fakeStoreConnection{}, committedPolicy, sequencer)
	result, err := committed.Store(ctx, batch)
	if err != nil || result.Duplicate != 1 {
		t.Fatalf("committed retry result=%+v err=%v", result, err)
	}
	if len(committedPolicy.calls) != 0 {
		t.Fatalf("committed retry consulted live retention: %v", committedPolicy.calls)
	}
}

func TestStoreRetryRejectsUnsupportedRetentionMetadata(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	controlDB, err := control.Open(ctx, filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = controlDB.Close() })
	sequencer, err := visibility.NewSQLite(ctx, controlDB)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = sequencer.Close() })
	batch := validStoreBatch()
	first := mustTestStoreWithVisibility(t, &fakeStoreConnection{batch: &fakeWriteBatch{
		sendErr: io.ErrUnexpectedEOF,
	}}, fixedRetention(time.Millisecond), sequencer)
	if _, err := first.Store(ctx, batch); !isTransient(err) {
		t.Fatalf("initial ambiguous Store error = %v, want transient", err)
	}

	var current []byte
	if err := controlDB.SQLDB().QueryRowContext(ctx, `
		SELECT metadata
		FROM ingest_visibility_reservations
		WHERE sequence = 1`).Scan(&current); err != nil {
		t.Fatalf("read current reservation metadata: %v", err)
	}
	if len(current) < 5 || current[4] != reservationMetadataVersion {
		t.Fatalf("new reservation metadata version = %v, want %d", current, reservationMetadataVersion)
	}
	unaligned := rewriteSingleIndexReservationMetadata(
		t,
		current,
		reservationMetadataVersion,
		1500*time.Microsecond,
	)
	if _, err := decodeReservationMetadata(unaligned); err == nil ||
		!strings.Contains(err.Error(), "retention duration") {
		t.Fatalf("unaligned metadata error = %v, want invalid retention duration", err)
	}
	unsupported := rewriteSingleIndexReservationMetadata(
		t,
		current,
		reservationMetadataVersion+1,
		time.Millisecond,
	)
	if _, err := decodeReservationMetadata(unsupported); err == nil ||
		!strings.Contains(err.Error(), "fresh ingestion state") {
		t.Fatalf("unsupported metadata error = %v, want fresh-state rejection", err)
	}
	if _, err := controlDB.SQLDB().ExecContext(ctx, `
		UPDATE ingest_visibility_reservations
		SET metadata = ?
		WHERE sequence = 1`, unsupported); err != nil {
		t.Fatalf("install unsupported reservation metadata: %v", err)
	}

	unavailablePolicy := &fakeRetentionProvider{err: errors.New("unsupported retry consulted live policy")}
	retryConnection := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	retry := mustTestStoreWithVisibility(t, retryConnection, unavailablePolicy, sequencer)
	if _, err := retry.Store(ctx, batch); err == nil ||
		!strings.Contains(err.Error(), "fresh ingestion state") {
		t.Fatalf("retry unsupported reservation error = %v, want fresh-state rejection", err)
	}
	if len(unavailablePolicy.calls) != 0 {
		t.Fatalf("unsupported retry consulted live retention: %v", unavailablePolicy.calls)
	}
}

func TestConvertTypedObjectPreservesTypesTagsAndEscapedNames(t *testing.T) {
	t.Parallel()
	timestamp := time.Date(2026, 7, 21, 3, 4, 5, 123456789, time.UTC)
	object := typedObjectValue(
		typedField("unsigned", typedUint(^uint64(0))),
		typedField("signed", typedSint(-1<<63)),
		typedField("ratio", typedDouble(1.25)),
		typedField("ok", typedBool(true)),
		typedField("nothing", typedNull()),
		typedField("text", typedString("2026-07-21T03:04:05Z")),
		typedField("literal.dot", typedString("literal")),
		typedField("percent%2Ekey", typedString("percent")),
		typedField("nested", typedObject(typedField("slash\\key", typedString("kept")), typedField("nil", typedNull()))),
		typedField("mixed", typedList(typedSint(1), typedString("two"), typedBool(true), typedNull(), typedObject(typedField("inside.dot", typedUint(3))))),
		typedField("bytes", typedBytes([]byte{0, 0xff, 0x10})),
		typedField("timestamp", typedTimestamp(timestamp)),
		typedField("duration", typedDuration(3*time.Second+4*time.Nanosecond)),
		typedField("decimal", typedDecimal("-12345678901234567890.00100e+12")),
		typedField("empty_list", typedList()),
		typedField("empty_object", typedObject()),
	)
	document, names, types, err := convertTypedObject(object)
	if err != nil {
		t.Fatalf("convertTypedObject: %v", err)
	}
	wantNames := []string{
		"bytes", "decimal", "duration", "empty_list", "empty_object", "literal\\.dot", "mixed", "nested.nil", "nested.slash\\\\key",
		"nothing", "ok", "percent%2Ekey", "ratio", "signed", "text", "timestamp", "unsigned",
	}
	if !slices.IsSorted(names) || !slices.Equal(names, wantNames) {
		t.Fatalf("field_names = %#v, want %#v", names, wantNames)
	}
	wantTypes := []uint8{
		uint8(eventfields.StoredValueTypeBytes),
		uint8(eventfields.StoredValueTypeDecimal),
		uint8(eventfields.StoredValueTypeDuration),
		uint8(eventfields.StoredValueTypeList),
		uint8(eventfields.StoredValueTypeObject),
		uint8(eventfields.StoredValueTypeString),
		uint8(eventfields.StoredValueTypeList),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeString),
		uint8(eventfields.StoredValueTypeNull),
		uint8(eventfields.StoredValueTypeBool),
		uint8(eventfields.StoredValueTypeString),
		uint8(eventfields.StoredValueTypeDouble),
		uint8(eventfields.StoredValueTypeSint64),
		uint8(eventfields.StoredValueTypeString),
		uint8(eventfields.StoredValueTypeTimestamp),
		uint8(eventfields.StoredValueTypeUint64),
	}
	if !slices.Equal(types, wantTypes) {
		t.Fatalf("field_types = %#v, want %#v for field_names %#v", types, wantTypes, names)
	}
	assertJSONPath(t, document, "signed", int64(-1<<63))
	assertJSONPath(t, document, "unsigned", ^uint64(0))
	assertJSONPath(t, document, "ratio", 1.25)
	ratioValue, ratioExists := document.ValueAtPath("ratio")
	ratioDynamic, ratioTyped := ratioValue.(clickhousedriver.Dynamic)
	if !ratioExists || !ratioTyped || ratioDynamic.Type() != "Float64" {
		t.Fatalf("ratio did not retain forced Float64 type: %#v", ratioValue)
	}
	assertJSONPath(t, document, "ok", true)
	assertJSONPath(t, document, "nothing", nil)
	assertJSONPath(t, document, "text", "2026-07-21T03:04:05Z")
	assertJSONPath(t, document, "literal%2Edot", "literal")
	assertJSONPath(t, document, "percent%252Ekey", "percent")
	assertJSONPath(t, document, "nested.slash\\key", "kept")
	assertJSONPath(t, document, "nested.nil", nil)

	value, _ := document.ValueAtPath("mixed")
	mixed, ok := value.(clickhousedriver.Dynamic)
	if !ok || mixed.Type() != "Array(Dynamic)" {
		t.Fatalf("mixed = %#v (%T)", value, value)
	}
	items, ok := mixed.Any().([]clickhousedriver.Dynamic)
	if !ok || len(items) != 5 || items[0].Any() != int64(1) || items[1].Any() != "two" || !items[3].Nil() {
		t.Fatalf("mixed payload = %#v", mixed.Any())
	}
	itemObject, ok := items[4].Any().(map[string]clickhousedriver.Dynamic)
	if !ok || itemObject["inside.dot"].Any() != uint64(3) {
		t.Fatalf("list object = %#v", items[4].Any())
	}
	assertTagged(t, document, "bytes", "bytes/v1", "AP8Q")
	assertTagged(t, document, "timestamp", "timestamp/v1", "2026-07-21T03:04:05.123456789Z")
	assertTagged(t, document, "duration", "duration/v1", "3:4")
	assertTagged(t, document, "decimal", "decimal/v1", "-12345678901234567890.00100e+12")
	assertDynamicType(t, document, "empty_list", "Array(Dynamic)")
	assertDynamicType(t, document, "empty_object", "Map(String, Dynamic)")

	nativeColumn, err := column.Type("JSON(max_dynamic_paths=256, max_dynamic_types=16)").Column("fields", &column.ServerContext{
		VersionMajor: 26, VersionMinor: 3, VersionPatch: 17, Timezone: time.UTC,
	})
	if err != nil {
		t.Fatalf("construct native JSON column: %v", err)
	}
	if err := nativeColumn.AppendRow(document); err != nil {
		t.Fatalf("native JSON driver rejected converted value: %v", err)
	}
}

func TestConvertTypedObjectRejectsAggregateFieldMetadataOverLimit(t *testing.T) {
	t.Parallel()

	parentName := strings.Repeat(".", eventfields.MaximumDynamicPathSegmentBytes)
	leaves := make([]*opensplunk.TypedObjectField, 140)
	for index := range leaves {
		leaves[index] = typedField(fmt.Sprintf("leaf%04d", index), typedString("value"))
	}
	object := typedObjectValue(leaves...)
	for range eventfields.MaximumDynamicPathSegments - 1 {
		object = typedObjectValue(typedField(parentName, typedObject(object.Fields...)))
	}

	if _, _, _, err := convertTypedObject(object); err == nil ||
		!strings.Contains(err.Error(), "stored field-name metadata") {
		t.Fatalf("convertTypedObject(amplified metadata) error = %v", err)
	}
}

func TestStoredValueTypeCodesMatchProtobufValueType(t *testing.T) {
	t.Parallel()

	pairs := []struct {
		stored eventfields.StoredValueType
		wire   opensplunk.ValueType
	}{
		{eventfields.StoredValueTypeNull, opensplunk.ValueType_VALUE_TYPE_NULL},
		{eventfields.StoredValueTypeString, opensplunk.ValueType_VALUE_TYPE_STRING},
		{eventfields.StoredValueTypeSint64, opensplunk.ValueType_VALUE_TYPE_SINT64},
		{eventfields.StoredValueTypeUint64, opensplunk.ValueType_VALUE_TYPE_UINT64},
		{eventfields.StoredValueTypeDouble, opensplunk.ValueType_VALUE_TYPE_DOUBLE},
		{eventfields.StoredValueTypeBool, opensplunk.ValueType_VALUE_TYPE_BOOL},
		{eventfields.StoredValueTypeBytes, opensplunk.ValueType_VALUE_TYPE_BYTES},
		{eventfields.StoredValueTypeTimestamp, opensplunk.ValueType_VALUE_TYPE_TIMESTAMP},
		{eventfields.StoredValueTypeDuration, opensplunk.ValueType_VALUE_TYPE_DURATION},
		{eventfields.StoredValueTypeList, opensplunk.ValueType_VALUE_TYPE_LIST},
		{eventfields.StoredValueTypeObject, opensplunk.ValueType_VALUE_TYPE_OBJECT},
		{eventfields.StoredValueTypeDecimal, opensplunk.ValueType_VALUE_TYPE_DECIMAL},
	}
	for _, pair := range pairs {
		if uint8(pair.stored) != uint8(pair.wire) {
			t.Errorf("stored type %d != wire type %d", pair.stored, pair.wire)
		}
	}
}

func TestConvertNilTypedObjectReturnsVersionableEmptyMetadata(t *testing.T) {
	t.Parallel()

	document, names, types, err := convertTypedObject(nil)
	if err != nil || document == nil || names == nil || types == nil || len(names) != 0 || len(types) != 0 {
		t.Fatalf("convertTypedObject(nil) document=%#v names=%#v types=%#v err=%v", document, names, types, err)
	}
}

func TestTypedValueToNativeRejectsDurationOutsideResultRange(t *testing.T) {
	t.Parallel()

	value := &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_DurationValue{
		DurationValue: &durationpb.Duration{Seconds: 9_223_372_037},
	}}
	if _, err := typedValueToNative(value); err == nil || !strings.Contains(err.Error(), "duration") {
		t.Fatalf("typedValueToNative(out-of-range duration) error = %v", err)
	}
}

func TestConvertTypedObjectAvoidsDottedPathCollisions(t *testing.T) {
	t.Parallel()
	object := typedObjectValue(
		typedField("a.b", typedString("literal dot")),
		typedField("a%2Eb", typedString("escape-looking")),
		typedField("a", typedObject(typedField("b", typedString("nested")))),
	)
	document, names, types, err := convertTypedObject(object)
	if err != nil {
		t.Fatal(err)
	}
	assertJSONPath(t, document, "a%2Eb", "literal dot")
	assertJSONPath(t, document, "a%252Eb", "escape-looking")
	assertJSONPath(t, document, "a.b", "nested")
	if !slices.Equal(names, []string{"a%2Eb", "a.b", "a\\.b"}) {
		t.Fatalf("field_names = %#v", names)
	}
	if !slices.Equal(types, []uint8{2, 2, 2}) {
		t.Fatalf("field_types = %#v", types)
	}
}

func TestDeduplicationTokenStableAndLengthFramed(t *testing.T) {
	t.Parallel()
	base := ingest.StoreBatch{TenantID: "tenant", CollectorID: "collector", BatchID: "batch", BatchSequence: 1}
	first := deduplicationToken(base)
	if deduplicationToken(base) != first || !strings.HasPrefix(first, "open-splunk-ingest-v1-") || len(first) != len("open-splunk-ingest-v1-")+64 {
		t.Fatalf("unstable or malformed token %q", first)
	}
	changed := []ingest.StoreBatch{
		{TenantID: "tenant2", CollectorID: "collector", BatchID: "batch"},
		{TenantID: "tenant", CollectorID: "collector2", BatchID: "batch"},
		{TenantID: "tenant", CollectorID: "collector", BatchID: "batch2"},
	}
	for _, candidate := range changed {
		if deduplicationToken(candidate) == first {
			t.Fatalf("token collision for %+v", candidate)
		}
	}
	a := deduplicationToken(ingest.StoreBatch{TenantID: "ab", CollectorID: "c", BatchID: "d"})
	b := deduplicationToken(ingest.StoreBatch{TenantID: "a", CollectorID: "bc", BatchID: "d"})
	if a == b {
		t.Fatal("unframed tuple collision")
	}
}

func TestStorePayloadDigestUsesOriginalCollectorBatchIdentity(t *testing.T) {
	t.Parallel()
	base := validStoreBatch()
	first, err := storePayloadDigest(base)
	if err != nil {
		t.Fatalf("storePayloadDigest(base): %v", err)
	}
	// These values are server-derived or policy-derived and may legitimately
	// differ when the exact collector batch is retried on another stream.
	retry := validStoreBatch()
	retry.ReceivedAt = retry.ReceivedAt.Add(time.Hour)
	retry.Events[0].Event.Raw = []byte(`{"message":"redacted differently"}`)
	second, err := storePayloadDigest(retry)
	if err != nil {
		t.Fatalf("storePayloadDigest(retry): %v", err)
	}
	if first != second {
		t.Fatal("server-derived retry differences changed the durable source digest")
	}
	retry.SourceBatchSHA256 = testSourceBatchDigest("different-wire-batch")
	changed, err := storePayloadDigest(retry)
	if err != nil {
		t.Fatalf("storePayloadDigest(changed source): %v", err)
	}
	if changed == first {
		t.Fatal("different collector wire batch reused the same payload digest")
	}
}

func TestHECSourceUsesIndependentDurableIdentityAndStorageProvenance(t *testing.T) {
	t.Parallel()
	native := validStoreBatch()
	hec := validStoreBatch()
	hec.Source = ingest.HECSource("ingestion-token-record")
	hec.CollectorID = ""
	hec.Events[0].Source = hec.Source
	hec.Events[0].CollectorID = ""

	if deduplicationToken(hec) == deduplicationToken(native) ||
		sequenceIdentityKey(hec) == sequenceIdentityKey(native) {
		t.Fatal("HEC and native identities collided")
	}
	if !strings.HasPrefix(deduplicationToken(hec), "open-splunk-ingest-hec-v1-") {
		t.Fatalf("HEC deduplication identity = %q", deduplicationToken(hec))
	}
	independent := hec
	independent.BatchID = "second-independent-request"
	if sequenceIdentityKey(independent) == sequenceIdentityKey(hec) {
		t.Fatal("independent HEC request IDs reused one visibility sequence identity")
	}
	nativeDigest, err := storePayloadDigest(native)
	if err != nil {
		t.Fatal(err)
	}
	hecDigest, err := storePayloadDigest(hec)
	if err != nil {
		t.Fatal(err)
	}
	if nativeDigest == hecDigest {
		t.Fatal("HEC and native payload identities collided")
	}

	store := mustTestStore(t, &fakeStoreConnection{}, fixedRetention(time.Hour))
	rows, err := store.rowsForBatch(context.Background(), hec, nil)
	if err != nil {
		t.Fatalf("rowsForBatch: %v", err)
	}
	if got := rows[0][20]; got != "" {
		t.Fatalf("collector_id = %#v, want empty", got)
	}
	if got := rows[0][21]; got != uint8(ingest.IngestionSourceKindHEC) {
		t.Fatalf("ingest_source_kind = %#v", got)
	}
	if got := rows[0][22]; got != "ingestion-token-record" {
		t.Fatalf("ingest_source_id = %#v", got)
	}
}

func TestStoreRetentionLookupIsCachedPerIndex(t *testing.T) {
	t.Parallel()
	conn := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	provider := &fakeRetentionProvider{periods: map[string]time.Duration{"main": time.Hour, "audit": 30 * 24 * time.Hour}}
	store := mustTestStore(t, conn, provider)
	base := time.Date(2026, 7, 21, 1, 2, 3, 456789123, time.UTC)
	_, err := store.Store(context.Background(), ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "batch", BatchSequence: 7,
		SourceBatchSHA256: testSourceBatchDigest("batch"),
		ReceivedAt:        base,
		Events: []*ingest.StoredEvent{
			testStoredEvent("one", "main", base),
			testStoredEvent("two", "audit", base),
			testStoredEvent("three", "main", base),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(provider.calls, []string{"tenant/main", "tenant/audit"}) {
		t.Fatalf("retention calls = %v", provider.calls)
	}
	wants := []time.Time{
		base.Truncate(time.Millisecond).Add(time.Hour),
		base.Truncate(time.Millisecond).Add(30 * 24 * time.Hour),
		base.Truncate(time.Millisecond).Add(time.Hour),
	}
	for i, want := range wants {
		if got := conn.batch.rows[i][eventExpiresAtColumn]; got != want {
			t.Errorf("row %d expires_at = %v, want %v", i, got, want)
		}
	}
}

func TestStoreUsesAdmittedRetentionSnapshotWithoutLivePolicyLookup(t *testing.T) {
	t.Parallel()
	conn := &fakeStoreConnection{batch: &fakeWriteBatch{}}
	provider := &fakeRetentionProvider{err: errors.New("live retention must not be consulted")}
	store := mustTestStore(t, conn, provider)
	base := time.Date(2026, 7, 21, 1, 2, 3, 456789123, time.UTC)
	_, err := store.Store(context.Background(), ingest.StoreBatch{
		TenantID: "tenant", CollectorID: "collector", BatchID: "batch", BatchSequence: 7,
		SourceBatchSHA256: testSourceBatchDigest("batch"),
		ReceivedAt:        base,
		RetentionByIndex: map[string]time.Duration{
			"audit": 30 * 24 * time.Hour,
			"main":  time.Hour,
		},
		Events: []*ingest.StoredEvent{
			testStoredEvent("one", "main", base),
			testStoredEvent("two", "audit", base),
			testStoredEvent("three", "main", base),
		},
	})
	if err != nil {
		t.Fatalf("Store: %v", err)
	}
	if len(provider.calls) != 0 {
		t.Fatalf("live retention calls = %v", provider.calls)
	}
	wants := []time.Time{
		base.Truncate(time.Millisecond).Add(time.Hour),
		base.Truncate(time.Millisecond).Add(30 * 24 * time.Hour),
		base.Truncate(time.Millisecond).Add(time.Hour),
	}
	for index, want := range wants {
		if got := conn.batch.rows[index][eventExpiresAtColumn]; got != want {
			t.Errorf("row %d expires_at = %v, want %v", index, got, want)
		}
	}
}

func TestStoreRejectsIncompleteOrExcessAdmittedRetentionSnapshot(t *testing.T) {
	t.Parallel()
	base := validStoreBatch()
	base.RetentionByIndex = map[string]time.Duration{}

	tests := []struct {
		name      string
		retention map[string]time.Duration
	}{
		{name: "missing accepted index", retention: map[string]time.Duration{}},
		{name: "unaccepted index", retention: map[string]time.Duration{
			"main": time.Hour, "other": time.Hour,
		}},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			batch := base
			batch.RetentionByIndex = test.retention
			provider := &fakeRetentionProvider{err: errors.New("live retention must not be consulted")}
			conn := &fakeStoreConnection{batch: &fakeWriteBatch{}}
			store := mustTestStore(t, conn, provider)
			if _, err := store.Store(context.Background(), batch); err == nil {
				t.Fatal("Store succeeded")
			}
			if len(provider.calls) != 0 || conn.prepareCalls != 0 {
				t.Fatalf("side effects: retention=%v prepare=%d", provider.calls, conn.prepareCalls)
			}
		})
	}
}

func TestRowsForBatchRejectsExcessAdmittedRetentionBeforeEventFieldConversion(t *testing.T) {
	t.Parallel()
	batch := validStoreBatch()
	batch.RetentionByIndex = map[string]time.Duration{
		"main":  time.Hour,
		"other": time.Hour,
	}
	batch.Events[0].Event.Fields = typedObjectValue(nil)
	provider := &fakeRetentionProvider{err: errors.New("live retention must not be consulted")}
	store := &Store{retention: provider}

	if _, err := store.rowsForBatch(context.Background(), batch, nil); err == nil ||
		!strings.Contains(err.Error(), "more indexes than accepted events") {
		t.Fatalf("rowsForBatch error = %v, want admitted snapshot cardinality error", err)
	}
	if len(provider.calls) != 0 {
		t.Fatalf("live retention calls = %v", provider.calls)
	}
}

func TestRowsForBatchRejectsOversizedAdmittedRetentionBeforeEventFieldConversion(t *testing.T) {
	t.Parallel()
	batch := validStoreBatch()
	count := int(ingest.HardMaxBatchEvents) + 1
	batch.Events = make([]*ingest.StoredEvent, count)
	batch.Events[0] = testStoredEvent("event", "main", batch.ReceivedAt)
	batch.Events[0].Event.Fields = typedObjectValue(nil)
	batch.RetentionByIndex = make(map[string]time.Duration, count)
	batch.RetentionByIndex["main"] = time.Hour
	for index := 1; index < count; index++ {
		batch.RetentionByIndex[fmt.Sprintf("index-%04d", index)] = time.Hour
	}
	provider := &fakeRetentionProvider{err: errors.New("live retention must not be consulted")}
	store := &Store{retention: provider}

	if _, err := store.rowsForBatch(context.Background(), batch, nil); err == nil ||
		!strings.Contains(err.Error(), "exceeds hard batch event limit") {
		t.Fatalf("rowsForBatch error = %v, want admitted snapshot hard-limit error", err)
	}
	if len(provider.calls) != 0 {
		t.Fatalf("live retention calls = %v", provider.calls)
	}
}

func TestReservationMetadataBoundsMatchIndexScope(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 21, 1, 2, 3, 0, time.UTC)
	rows := make([][]any, maxDurableBatchEvents)
	for index := range rows {
		name := fmt.Sprintf("%03d-%s", index, strings.Repeat("x", 251))
		row := make([]any, len(eventInsertColumns))
		row[eventIndexNameColumn] = name
		row[eventIndexTimeColumn] = base
		row[eventExpiresAtColumn] = base.Add(time.Hour)
		rows[index] = row
	}
	metadata, err := encodeReservationMetadata(rows, ingest.StoreBatch{
		BatchSequence:      1,
		OriginalEventCount: uint32(len(rows)),
	})
	if err != nil {
		t.Fatalf("encode maximum-length index scope: %v", err)
	}
	decoded, err := decodeReservationMetadata(metadata)
	if err != nil || len(decoded.RetentionByIndex) != maxDurableBatchEvents {
		t.Fatalf("decode maximum index scope: count=%d err=%v", len(decoded.RetentionByIndex), err)
	}
	if len(metadata) > visibility.MaxMetadataBytes {
		t.Fatalf("metadata size = %d, limit = %d", len(metadata), visibility.MaxMetadataBytes)
	}
}

func TestStoreRejectsTooManyIndexesBeforeVisibilityReservation(t *testing.T) {
	t.Parallel()
	base := time.Date(2026, 7, 21, 1, 2, 3, 0, time.UTC)
	batch := validStoreBatch()
	batch.Events = make([]*ingest.StoredEvent, maxDurableBatchEvents+1)
	for index := range batch.Events {
		batch.Events[index] = testStoredEvent(fmt.Sprintf("event-%03d", index), fmt.Sprintf("index-%03d", index), base)
	}
	batch.OriginalEventCount = uint32(len(batch.Events))
	sequencer := &fakeVisibilitySequencer{reservation: visibility.Reservation{Sequence: 1}}
	store := mustTestStoreWithVisibility(t, &fakeStoreConnection{}, fixedRetention(time.Hour), sequencer)
	if _, err := store.Store(context.Background(), batch); err == nil || !strings.Contains(err.Error(), "unique index count") {
		t.Fatalf("Store error = %v, want unique-index limit", err)
	}
	if len(sequencer.reserveKeys) != 0 {
		t.Fatalf("invalid metadata created visibility reservations: %v", sequencer.reserveKeys)
	}
}

func TestStoreClassifiesErrorsAndReleasesBatch(t *testing.T) {
	valid := validStoreBatch()
	tests := []struct {
		name       string
		prepareErr error
		sendErr    error
		wantReason opensplunk.RetryBatchReason
		permanent  bool
	}{
		{name: "network", prepareErr: &net.OpError{Op: "dial", Net: "tcp", Err: io.EOF}, wantReason: opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE},
		{name: "pool busy", prepareErr: clickhousedriver.ErrAcquireConnTimeout, wantReason: opensplunk.RetryBatchReason_RETRY_BATCH_REASON_SERVER_BUSY},
		{name: "bad connection", prepareErr: sqldriver.ErrBadConn, wantReason: opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE},
		{name: "rate limited", prepareErr: &clickhousedriver.Exception{Code: 364, Name: "RECEIVED_ERROR_TOO_MANY_REQUESTS"}, wantReason: opensplunk.RetryBatchReason_RETRY_BATCH_REASON_RATE_LIMITED},
		{name: "send EOF", sendErr: io.ErrUnexpectedEOF, wantReason: opensplunk.RetryBatchReason_RETRY_BATCH_REASON_STORAGE_UNAVAILABLE},
		{name: "schema", prepareErr: &clickhousedriver.Exception{Code: 60, Name: "UNKNOWN_TABLE"}, permanent: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			batch := &fakeWriteBatch{sendErr: test.sendErr}
			conn := &fakeStoreConnection{batch: batch, prepareErr: test.prepareErr}
			store := mustTestStore(t, conn, fixedRetention(time.Hour))
			_, err := store.Store(context.Background(), valid)
			if err == nil {
				t.Fatal("Store succeeded")
			}
			if test.permanent {
				if isTransient(err) {
					t.Fatalf("permanent error wrapped as transient: %v", err)
				}
			} else {
				assertTransient(t, err, test.wantReason)
			}
			if test.sendErr != nil && (batch.sendCalls != 1 || batch.abortCalls != 1 || batch.closeCalls != 1) {
				t.Fatalf("send lifecycle send=%d abort=%d close=%d", batch.sendCalls, batch.abortCalls, batch.closeCalls)
			}
		})
	}

	t.Run("append permanent", func(t *testing.T) {
		batch := &fakeWriteBatch{appendErr: errors.New("bad native value")}
		store := mustTestStore(t, &fakeStoreConnection{batch: batch}, fixedRetention(time.Hour))
		_, err := store.Store(context.Background(), valid)
		if err == nil || isTransient(err) || batch.abortCalls != 1 || batch.closeCalls != 1 || batch.sendCalls != 0 {
			t.Fatalf("err=%v send=%d abort=%d close=%d", err, batch.sendCalls, batch.abortCalls, batch.closeCalls)
		}
	})
	t.Run("context cancellation", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		store := mustTestStore(t, &fakeStoreConnection{prepareErr: context.Canceled}, fixedRetention(time.Hour))
		_, err := store.Store(ctx, valid)
		if !errors.Is(err, context.Canceled) || isTransient(err) {
			t.Fatalf("error = %v", err)
		}
	})
}

func TestStoreRejectsInvalidInputsBeforePrepare(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		batch     ingest.StoreBatch
		retention RetentionProvider
	}{
		{name: "empty", batch: ingest.StoreBatch{TenantID: "tenant", CollectorID: "collector", BatchID: "batch"}, retention: fixedRetention(time.Hour)},
		{name: "missing tenant", batch: ingest.StoreBatch{CollectorID: "collector", BatchID: "batch", Events: []*ingest.StoredEvent{testStoredEvent("e", "main", time.Now())}}, retention: fixedRetention(time.Hour)},
		{name: "nil event", batch: ingest.StoreBatch{TenantID: "tenant", CollectorID: "collector", BatchID: "batch", Events: []*ingest.StoredEvent{nil}}, retention: fixedRetention(time.Hour)},
		{name: "zero retention", batch: validStoreBatch(), retention: fixedRetention(0)},
		{name: "sub-millisecond retention", batch: validStoreBatch(), retention: fixedRetention(time.Nanosecond)},
	}
	outOfRangeExpiry := validStoreBatch()
	outOfRangeExpiry.ReceivedAt = MaximumSearchTime()
	outOfRangeExpiry.Events[0].IndexTime = outOfRangeExpiry.ReceivedAt
	tests = append(tests, struct {
		name      string
		batch     ingest.StoreBatch
		retention RetentionProvider
	}{name: "out-of-range expiration", batch: outOfRangeExpiry, retention: fixedRetention(time.Millisecond)})
	mismatch := validStoreBatch()
	mismatch.Events[0].TenantID = "other"
	tests = append(tests, struct {
		name      string
		batch     ingest.StoreBatch
		retention RetentionProvider
	}{name: "metadata mismatch", batch: mismatch, retention: fixedRetention(time.Hour)})
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			conn := &fakeStoreConnection{batch: &fakeWriteBatch{}}
			store := mustTestStore(t, conn, test.retention)
			if _, err := store.Store(context.Background(), test.batch); err == nil {
				t.Fatal("Store succeeded")
			}
			if conn.prepareCalls != 0 {
				t.Fatalf("prepare calls = %d", conn.prepareCalls)
			}
		})
	}
}

func TestEventExpirationEnforcesStoragePrecisionAndRange(t *testing.T) {
	t.Parallel()

	offset := time.FixedZone("event-expiration", -7*60*60)
	indexTime := time.Date(2026, 7, 26, 1, 2, 3, 987_654_321, offset)
	want := indexTime.UTC().Truncate(time.Millisecond).Add(time.Millisecond)
	got, err := eventExpiration(indexTime, time.Millisecond)
	if err != nil || !got.Equal(want) || got.Location() != time.UTC {
		t.Fatalf("eventExpiration() = %v, %v; want %v in UTC", got, err, want)
	}

	got, err = eventExpiration(MinimumSearchTime(), time.Millisecond)
	if err != nil || !got.Equal(MinimumSearchTime().Add(time.Millisecond)) {
		t.Fatalf(
			"eventExpiration(minimum) = %v, %v; want %v",
			got,
			err,
			MinimumSearchTime().Add(time.Millisecond),
		)
	}

	got, err = eventExpiration(MaximumSearchTime().Add(-time.Millisecond), time.Millisecond)
	if err != nil || !got.Equal(MaximumSearchTime()) {
		t.Fatalf("eventExpiration(maximum) = %v, %v; want %v", got, err, MaximumSearchTime())
	}

	tests := []struct {
		name      string
		indexTime time.Time
		retention time.Duration
	}{
		{name: "zero retention", indexTime: indexTime, retention: 0},
		{name: "negative retention", indexTime: indexTime, retention: -time.Millisecond},
		{name: "sub-millisecond retention", indexTime: indexTime, retention: time.Nanosecond},
		{name: "index time below range", indexTime: MinimumSearchTime().Add(-time.Millisecond), retention: time.Millisecond},
		{name: "index time above range", indexTime: MaximumSearchTime().Add(time.Millisecond), retention: time.Millisecond},
		{name: "expiration above range", indexTime: MaximumSearchTime(), retention: time.Millisecond},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if _, err := eventExpiration(test.indexTime, test.retention); err == nil {
				t.Fatal("eventExpiration succeeded")
			}
		})
	}
}

func rewriteSingleIndexReservationMetadata(
	t *testing.T,
	metadata []byte,
	version byte,
	retention time.Duration,
) []byte {
	t.Helper()

	rewritten := slices.Clone(metadata)
	payloadLength := len(rewritten) - sha256.Size
	if payloadLength < 5+8+8+8 || string(rewritten[:4]) != "OSVM" {
		t.Fatalf("reservation metadata shape = %x", rewritten)
	}
	if count := binary.BigEndian.Uint64(rewritten[5:13]); count != 1 {
		t.Fatalf("reservation metadata index count = %d, want 1", count)
	}
	nameLength := binary.BigEndian.Uint64(rewritten[13:21])
	retentionOffset := uint64(21) + nameLength
	if retentionOffset+8 > uint64(payloadLength) {
		t.Fatalf("reservation metadata name length = %d exceeds payload", nameLength)
	}
	rewritten[4] = version
	binary.BigEndian.PutUint64(
		rewritten[int(retentionOffset):int(retentionOffset)+8],
		uint64(retention),
	)
	checksum := sha256.Sum256(rewritten[:payloadLength])
	copy(rewritten[payloadLength:], checksum[:])
	return rewritten
}

func TestReservationMetadataRejectsSubMillisecondRetention(t *testing.T) {
	t.Parallel()

	indexTime := time.Date(2026, 7, 26, 1, 2, 3, 0, time.UTC)
	row := make([]any, len(eventInsertColumns))
	row[eventIndexNameColumn] = "main"
	row[eventIndexTimeColumn] = indexTime
	row[eventExpiresAtColumn] = indexTime.Add(time.Nanosecond)
	_, err := encodeReservationMetadata([][]any{row}, ingest.StoreBatch{
		BatchSequence:      1,
		OriginalEventCount: 1,
	})
	if err == nil {
		t.Fatal("encodeReservationMetadata accepted sub-millisecond retention")
	}
}

func TestStoreRejectsReservedDynamicRootsFromDirectStoredEvents(t *testing.T) {
	t.Parallel()

	store := mustTestStore(t, &fakeStoreConnection{}, fixedRetention(time.Hour))
	names := append(eventfields.ReservedDynamicRootNames(), "__Os_Private")
	for _, canonical := range names {
		for _, name := range []string{canonical, strings.ToUpper(canonical)} {
			t.Run(name, func(t *testing.T) {
				batch := validStoreBatch()
				batch.Events[0].Event.Fields = typedObjectValue(typedField(name, typedString("forged")))
				if _, err := store.rowsForBatch(context.Background(), batch, nil); err == nil || !strings.Contains(err.Error(), "reserved event metadata") {
					t.Fatalf("rowsForBatch(%q) error = %v", name, err)
				}
			})
		}
	}

	batch := validStoreBatch()
	batch.Events[0].Event.Fields = typedObjectValue(typedField("nested", typedObject(typedField("service", typedString("allowed")))))
	if _, err := store.rowsForBatch(context.Background(), batch, nil); err != nil {
		t.Fatalf("nested canonical spelling must remain an ordinary dynamic path: %v", err)
	}
}

func TestConfigAndConnectionLifecycle(t *testing.T) {
	t.Parallel()
	config := DefaultConfig()
	if !slices.Equal(config.Addresses, []string{"127.0.0.1:9000"}) || config.Database != "open_splunk" || config.Table != "events" {
		t.Fatalf("DefaultConfig = %+v", config)
	}
	tlsConfig := &tls.Config{ServerName: "clickhouse.example", MinVersion: tls.VersionTLS13}
	config.TLS = tlsConfig
	options, normalized, err := config.clickHouseOptions()
	if err != nil {
		t.Fatal(err)
	}
	if options.Protocol != clickhousedriver.Native || options.TLS == tlsConfig || options.TLS.ServerName != tlsConfig.ServerName ||
		options.Compression == nil || options.Compression.Method != clickhousedriver.CompressionLZ4 || normalized.RetryAfter <= 0 {
		t.Fatalf("unsafe options/config: %+v / %+v", options, normalized)
	}
	plaintext := DefaultConfig()
	plaintext.Addresses = []string{"per-clickhouse:9000"}
	plaintextOptions, _, err := plaintext.clickHouseOptions()
	if err != nil {
		t.Fatalf("plaintext Docker hostname: %v", err)
	}
	if plaintextOptions.TLS != nil ||
		!slices.Equal(plaintextOptions.Addr, plaintext.Addresses) {
		t.Fatalf("plaintext Docker hostname options = %+v", plaintextOptions)
	}
	invalid := DefaultConfig()
	invalid.Addresses = []string{"not-a-host-port"}
	if _, _, err := invalid.clickHouseOptions(); err == nil {
		t.Fatal("invalid address accepted")
	}
	invalid = DefaultConfig()
	invalid.Addresses = []string{"127.0.0.1:9000", "127.0.0.1:9001"}
	if _, _, err := invalid.clickHouseOptions(); err == nil {
		t.Fatal("multiple ClickHouse addresses accepted in single-node mode")
	}
	invalid = DefaultConfig()
	invalid.Password = "very-secret"
	invalid.Table = "events; DROP"
	if _, _, err := invalid.clickHouseOptions(); err == nil || strings.Contains(err.Error(), invalid.Password) {
		t.Fatalf("unsafe config error = %v", err)
	}
	conn := &fakeStoreConnection{batch: &fakeWriteBatch{}, pingErr: errors.New("ping failed"), closeErr: errors.New("close failed")}
	store := mustTestStore(t, conn, fixedRetention(time.Hour))
	if err := store.Ping(context.Background()); !errors.Is(err, conn.pingErr) {
		t.Fatalf("Ping = %v", err)
	}
	if err := store.Close(); !errors.Is(err, conn.closeErr) {
		t.Fatalf("Close = %v", err)
	}
	if err := store.Close(); !errors.Is(err, conn.closeErr) || conn.closeCalls != 1 {
		t.Fatalf("second Close = %v, connection close calls = %d", err, conn.closeCalls)
	}
}

type fakeStoreConnection struct {
	prepareCalls      int
	closeCalls        int
	query             string
	settings          clickhousedriver.Settings
	prepareErr        error
	batch             *fakeWriteBatch
	pingErr, closeErr error
	closeStarted      chan struct{}
	closeRelease      <-chan struct{}
}

func (c *fakeStoreConnection) prepare(_ context.Context, query string, settings clickhousedriver.Settings) (writeBatch, error) {
	c.prepareCalls++
	c.query = query
	c.settings = make(clickhousedriver.Settings, len(settings))
	maps.Copy(c.settings, settings)
	if c.prepareErr != nil {
		return nil, c.prepareErr
	}
	if c.batch == nil {
		c.batch = &fakeWriteBatch{}
	}
	return c.batch, nil
}
func (c *fakeStoreConnection) exec(
	context.Context,
	string,
	clickhousedriver.Settings,
	clickhousedriver.Parameters,
	string,
) error {
	return errors.New("unexpected ClickHouse Exec")
}
func (c *fakeStoreConnection) queryRow(
	context.Context,
	string,
	clickhousedriver.Parameters,
) storeQueryRow {
	return fakeErrorStoreQueryRow{
		err: errors.New("unexpected ClickHouse QueryRow"),
	}
}
func (c *fakeStoreConnection) Ping(context.Context) error { return c.pingErr }
func (c *fakeStoreConnection) Close() error {
	c.closeCalls++
	if c.closeStarted != nil {
		close(c.closeStarted)
	}
	if c.closeRelease != nil {
		<-c.closeRelease
	}
	return c.closeErr
}

type gatedStoreConnection struct {
	entered          chan struct{}
	resume           chan struct{}
	exited           chan struct{}
	closed           chan struct{}
	closedBeforeExit chan struct{}
	err              error
}

func (connection *gatedStoreConnection) prepare(ctx context.Context, _ string, _ clickhousedriver.Settings) (writeBatch, error) {
	close(connection.entered)
	if connection.exited != nil {
		defer close(connection.exited)
	}
	select {
	case <-connection.resume:
		return nil, connection.err
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (connection *gatedStoreConnection) exec(
	context.Context,
	string,
	clickhousedriver.Settings,
	clickhousedriver.Parameters,
	string,
) error {
	return errors.New("unexpected ClickHouse Exec")
}

func (connection *gatedStoreConnection) queryRow(
	context.Context,
	string,
	clickhousedriver.Parameters,
) storeQueryRow {
	return fakeErrorStoreQueryRow{
		err: errors.New("unexpected ClickHouse QueryRow"),
	}
}

type fakeErrorStoreQueryRow struct {
	err error
}

func (row fakeErrorStoreQueryRow) Scan(...any) error {
	return row.err
}

func (*gatedStoreConnection) Ping(context.Context) error { return nil }
func (connection *gatedStoreConnection) Close() error {
	if connection.closedBeforeExit != nil && connection.exited != nil {
		select {
		case <-connection.exited:
		default:
			close(connection.closedBeforeExit)
		}
	}
	if connection.closed != nil {
		close(connection.closed)
	}
	return nil
}

type fakeWriteBatch struct {
	rows                              [][]any
	appendErr, sendErr, closeErr      error
	sendCalls, abortCalls, closeCalls int
}

func (b *fakeWriteBatch) Append(values ...any) error {
	if b.appendErr != nil {
		return b.appendErr
	}
	b.rows = append(b.rows, append([]any(nil), values...))
	return nil
}
func (b *fakeWriteBatch) Send() error  { b.sendCalls++; return b.sendErr }
func (b *fakeWriteBatch) Abort() error { b.abortCalls++; return nil }
func (b *fakeWriteBatch) Close() error { b.closeCalls++; return b.closeErr }

type fakeRetentionProvider struct {
	periods map[string]time.Duration
	calls   []string
	err     error
}

func (p *fakeRetentionProvider) RetentionForIndex(_ context.Context, tenant, index string) (time.Duration, error) {
	p.calls = append(p.calls, tenant+"/"+index)
	if p.err != nil {
		return 0, p.err
	}
	return p.periods[index], nil
}

func fixedRetention(period time.Duration) RetentionProvider {
	return RetentionProviderFunc(func(context.Context, string, string) (time.Duration, error) { return period, nil })
}
func mustTestStore(t *testing.T, conn storeConnection, retention RetentionProvider) *Store {
	t.Helper()
	return mustTestStoreWithVisibility(t, conn, retention, &fakeVisibilitySequencer{reservation: visibility.Reservation{Sequence: 1}})
}
func mustTestStoreWithVisibility(t *testing.T, conn storeConnection, retention RetentionProvider, sequencer visibility.Sequencer) *Store {
	t.Helper()
	store, err := newStore(conn, "open_splunk", "events", retention, sequencer, time.Now, time.Second)
	if err != nil {
		t.Fatalf("newStore: %v", err)
	}
	return store
}

type fakeVisibilitySequencer struct {
	reservation     visibility.Reservation
	hasReservation  bool
	lookupErr       error
	reserveErr      error
	commitErr       error
	releaseErr      error
	markErr         error
	abandonErr      error
	pendingUsage    *visibility.PendingUsage
	pendingErr      error
	acquireBlocked  bool
	cutoff          uint64
	cutoffErr       error
	reserveKeys     []string
	reserveRequests []visibility.ReserveRequest
	committed       []uint64
	released        []uint64
	marked          []uint64
	abandoned       []uint64
	lookupCalls     int
	cutoffCalls     int
	acquireCalls    int
	pendingCalls    int
	pruneRetention  visibility.TerminalRetention
	pruneLimit      uint32
	pruneDeleted    uint32
	pruneErr        error
	pruneErrors     []error
	pruneCalls      int
	pruneNotify     chan<- struct{}
}

type abandonBeforeExistingReserveSequencer struct {
	visibility.Sequencer
	sequence  uint64
	owner     string
	abandoned bool
	requests  []visibility.ReserveRequest
}

func (sequencer *abandonBeforeExistingReserveSequencer) Reserve(
	ctx context.Context,
	request visibility.ReserveRequest,
) (visibility.Reservation, error) {
	captured := request
	captured.Metadata = slices.Clone(request.Metadata)
	captured.Outbox = slices.Clone(request.Outbox)
	sequencer.requests = append(sequencer.requests, captured)
	if !sequencer.abandoned {
		sequencer.abandoned = true
		if err := sequencer.Abandon(ctx, sequencer.sequence, sequencer.owner); err != nil {
			return visibility.Reservation{}, err
		}
	}
	return sequencer.Sequencer.Reserve(ctx, request)
}

type alwaysGoneVisibilitySequencer struct {
	*fakeVisibilitySequencer
	requests []visibility.ReserveRequest
}

func (sequencer *alwaysGoneVisibilitySequencer) Reserve(
	_ context.Context,
	request visibility.ReserveRequest,
) (visibility.Reservation, error) {
	captured := request
	captured.Metadata = slices.Clone(request.Metadata)
	captured.Outbox = slices.Clone(request.Outbox)
	sequencer.requests = append(sequencer.requests, captured)
	return visibility.Reservation{}, visibility.ErrReservationGone
}

func (sequencer *fakeVisibilitySequencer) Lookup(_ context.Context, _, _ string, _ [32]byte) (visibility.Reservation, bool, error) {
	sequencer.lookupCalls++
	return sequencer.reservation, sequencer.hasReservation, sequencer.lookupErr
}

func (sequencer *fakeVisibilitySequencer) Reserve(_ context.Context, request visibility.ReserveRequest) (visibility.Reservation, error) {
	sequencer.reserveKeys = append(sequencer.reserveKeys, request.BatchKey)
	captured := request
	if request.HECAdmission != nil {
		cloned := *request.HECAdmission
		captured.HECAdmission = &cloned
	}
	sequencer.reserveRequests = append(sequencer.reserveRequests, captured)
	reservation := sequencer.reservation
	if len(sequencer.reserveKeys) > 1 && !reservation.AlreadyCommitted {
		reservation.PreviouslyReserved = true
	}
	if reservation.Sequence == 0 {
		reservation.Sequence = 1
	}
	if reservation.IndexTime.IsZero() {
		reservation.IndexTime = request.IndexTime.UTC().Truncate(time.Millisecond)
	}
	if reservation.Metadata == nil {
		reservation.Metadata = slices.Clone(request.Metadata)
	}
	if reservation.Outbox == nil {
		reservation.Outbox = slices.Clone(request.Outbox)
	}
	if reservation.BatchKey == "" {
		reservation.BatchKey = request.BatchKey
	}
	if reservation.SequenceKey == "" {
		reservation.SequenceKey = request.SequenceKey
	}
	if reservation.PayloadSHA256 == ([sha256.Size]byte{}) {
		reservation.PayloadSHA256 = request.PayloadSHA256
	}
	sequencer.reservation = reservation
	if sequencer.reserveErr == nil {
		sequencer.hasReservation = true
	}
	return reservation, sequencer.reserveErr
}
func (sequencer *fakeVisibilitySequencer) Reject(_ context.Context, request visibility.RejectRequest) (visibility.Reservation, error) {
	if sequencer.hasReservation {
		return sequencer.reservation, sequencer.reserveErr
	}
	reservation := visibility.Reservation{
		BatchKey:      request.BatchKey,
		SequenceKey:   request.SequenceKey,
		Sequence:      1,
		Rejected:      true,
		NewlyRejected: true,
		IndexTime:     request.IndexTime.UTC().Truncate(time.Millisecond),
		PayloadSHA256: request.PayloadSHA256,
		Metadata:      slices.Clone(request.Metadata),
		RejectedAt:    request.RejectedAt.UTC().Truncate(time.Microsecond),
	}
	sequencer.reservation = reservation
	if sequencer.reserveErr == nil {
		sequencer.hasReservation = true
	}
	return reservation, sequencer.reserveErr
}
func (sequencer *fakeVisibilitySequencer) AcquirePending(_ context.Context, _ string) (visibility.Reservation, bool, error) {
	sequencer.acquireCalls++
	if sequencer.acquireBlocked || !sequencer.hasReservation ||
		sequencer.reservation.AlreadyCommitted || sequencer.reservation.Rejected {
		return visibility.Reservation{}, false, nil
	}
	return sequencer.reservation, true, nil
}
func (sequencer *fakeVisibilitySequencer) PendingUsage(context.Context) (visibility.PendingUsage, error) {
	sequencer.pendingCalls++
	if sequencer.pendingErr != nil {
		return visibility.PendingUsage{}, sequencer.pendingErr
	}
	if sequencer.pendingUsage != nil {
		return *sequencer.pendingUsage, nil
	}
	if !sequencer.hasReservation || sequencer.reservation.AlreadyCommitted || sequencer.reservation.Rejected {
		return visibility.PendingUsage{}, nil
	}
	return visibility.PendingUsage{
		Reservations: 1,
		OutboxBytes:  uint64(len(sequencer.reservation.Outbox)),
	}, nil
}
func (sequencer *fakeVisibilitySequencer) MarkSending(_ context.Context, sequence uint64, _ string) error {
	sequencer.marked = append(sequencer.marked, sequence)
	if sequencer.markErr == nil {
		sequencer.reservation.MayHaveReachedStorage = true
	}
	return sequencer.markErr
}
func (sequencer *fakeVisibilitySequencer) Commit(_ context.Context, sequence uint64, _ string, committedAt time.Time) error {
	sequencer.committed = append(sequencer.committed, sequence)
	if sequencer.commitErr == nil {
		sequencer.reservation.AlreadyCommitted = true
		sequencer.reservation.CommittedAt = committedAt
		sequencer.reservation.Outbox = nil
	}
	return sequencer.commitErr
}
func (sequencer *fakeVisibilitySequencer) Release(_ context.Context, sequence uint64, _ string) error {
	sequencer.released = append(sequencer.released, sequence)
	return sequencer.releaseErr
}
func (sequencer *fakeVisibilitySequencer) Abandon(_ context.Context, sequence uint64, _ string) error {
	sequencer.abandoned = append(sequencer.abandoned, sequence)
	if sequencer.abandonErr == nil {
		sequencer.hasReservation = false
	}
	return sequencer.abandonErr
}
func (sequencer *fakeVisibilitySequencer) Cutoff(context.Context) (uint64, error) {
	sequencer.cutoffCalls++
	return sequencer.cutoff, sequencer.cutoffErr
}
func (sequencer *fakeVisibilitySequencer) PruneTerminal(_ context.Context, retention visibility.TerminalRetention, limit uint32) (uint32, error) {
	sequencer.pruneCalls++
	sequencer.pruneRetention = retention
	sequencer.pruneLimit = limit
	if sequencer.pruneNotify != nil {
		sequencer.pruneNotify <- struct{}{}
	}
	if len(sequencer.pruneErrors) >= sequencer.pruneCalls {
		return sequencer.pruneDeleted, sequencer.pruneErrors[sequencer.pruneCalls-1]
	}
	return sequencer.pruneDeleted, sequencer.pruneErr
}
func validStoreBatch() ingest.StoreBatch {
	now := time.Date(2026, 7, 21, 1, 2, 3, 0, time.UTC)
	return ingest.StoreBatch{TenantID: "tenant", CollectorID: "collector", BatchID: "batch", BatchSequence: 1,
		OriginalEventCount: 1,
		SourceBatchSHA256:  testSourceBatchDigest("batch"),
		ReceivedAt:         now,
		Events:             []*ingest.StoredEvent{testStoredEvent("event", "main", now)}}
}
func testSourceBatchDigest(label string) [sha256.Size]byte {
	return sha256.Sum256([]byte("source-event-batch:" + label))
}
func testStoredEvent(id, index string, indexTime time.Time) *ingest.StoredEvent {
	eventTime := time.Date(2026, 7, 21, 3, 4, 5, 123456789, time.FixedZone("event-offset", 5*60*60))
	return &ingest.StoredEvent{
		TenantID: "tenant", CollectorID: "collector", BatchID: "batch", IndexTime: indexTime,
		Event: &opensplunk.LogEvent{
			EventId: id, IndexName: index, EventTime: timestamppb.New(eventTime), CollectedAt: timestamppb.New(eventTime.Add(-time.Second)),
			EventTimeSource: opensplunk.EventTimeSource_EVENT_TIME_SOURCE_PARSED,
			Host:            "host", Source: "app.log", Sourcetype: "go:zap:json", Severity: opensplunk.LogSeverity_LOG_SEVERITY_INFO,
			Raw: []byte("{\"message\":\"hello\"}"), RawEncoding: opensplunk.RawEncoding_RAW_ENCODING_UTF8,
			Fields: typedObjectValue(typedField("status", typedUint(200))),
		},
	}
}

func assertOptionalString(t *testing.T, value any, present bool) {
	t.Helper()
	if !present {
		if value != nil {
			t.Fatalf("optional = %#v, want nil", value)
		}
		return
	}
	pointer, ok := value.(*string)
	if !ok || pointer == nil || *pointer != "" {
		t.Fatalf("optional = %#v (%T), want empty string", value, value)
	}
}
func assertJSONPath(t *testing.T, document *clickhousedriver.JSON, path string, want any) {
	t.Helper()
	got, ok := document.ValueAtPath(path)
	if dynamic, isDynamic := got.(clickhousedriver.Dynamic); isDynamic {
		if dynamic.Nil() {
			got = nil
		} else {
			got = dynamic.Any()
		}
	}
	if !ok || !reflect.DeepEqual(got, want) {
		t.Fatalf("path %q = %#v (%T), want %#v", path, got, got, want)
	}
}
func assertTagged(t *testing.T, document *clickhousedriver.JSON, path, wantType, wantValue string) {
	t.Helper()
	value, ok := document.ValueAtPath(path)
	dynamic, dynamicOK := value.(clickhousedriver.Dynamic)
	if !ok || !dynamicOK || dynamic.Type() != "Map(String, String)" {
		t.Fatalf("tag %q = %#v (%T)", path, value, value)
	}
	tag, ok := dynamic.Any().(map[string]string)
	if !ok || len(tag) != 2 || tag[extendedTypeKey] != wantType || tag[extendedValueKey] != wantValue {
		t.Fatalf("tag %q payload = %#v", path, dynamic.Any())
	}
}
func assertDynamicType(t *testing.T, document *clickhousedriver.JSON, path, want string) {
	t.Helper()
	value, ok := document.ValueAtPath(path)
	dynamic, dynamicOK := value.(clickhousedriver.Dynamic)
	if !ok || !dynamicOK || dynamic.Type() != want {
		t.Fatalf("dynamic type at %q = %#v (%T), want %q", path, value, value, want)
	}
}
func assertTransient(t *testing.T, err error, reason opensplunk.RetryBatchReason) {
	t.Helper()
	var transient *ingest.TransientStoreError
	if !errors.As(err, &transient) || transient.Reason != reason || transient.RetryAfter <= 0 {
		t.Fatalf("error = %v, want transient reason %v", err, reason)
	}
}
func isTransient(err error) bool {
	var transient *ingest.TransientStoreError
	return errors.As(err, &transient)
}

func typedField(name string, value *opensplunk.TypedValue) *opensplunk.TypedObjectField {
	return &opensplunk.TypedObjectField{Name: name, Value: value}
}
func typedObjectValue(fields ...*opensplunk.TypedObjectField) *opensplunk.TypedObject {
	return &opensplunk.TypedObject{Fields: fields}
}
func typedNull() *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_NullValue{NullValue: opensplunk.NullValue_NULL_VALUE_NULL}}
}
func typedString(v string) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_StringValue{StringValue: v}}
}
func typedSint(v int64) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_Sint64Value{Sint64Value: v}}
}
func typedUint(v uint64) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_Uint64Value{Uint64Value: v}}
}
func typedDouble(v float64) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_DoubleValue{DoubleValue: v}}
}
func typedBool(v bool) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_BoolValue{BoolValue: v}}
}
func typedBytes(v []byte) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_BytesValue{BytesValue: v}}
}
func typedTimestamp(v time.Time) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_TimestampValue{TimestampValue: timestamppb.New(v)}}
}
func typedDuration(v time.Duration) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_DurationValue{DurationValue: durationpb.New(v)}}
}
func typedDecimal(v string) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_DecimalValue{DecimalValue: &opensplunk.DecimalValue{Value: v}}}
}
func typedList(v ...*opensplunk.TypedValue) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_ListValue{ListValue: &opensplunk.TypedValueList{Values: v}}}
}
func typedObject(fields ...*opensplunk.TypedObjectField) *opensplunk.TypedValue {
	return &opensplunk.TypedValue{Kind: &opensplunk.TypedValue_ObjectValue{ObjectValue: typedObjectValue(fields...)}}
}
