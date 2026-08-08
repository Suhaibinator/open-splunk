package searchhistory

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
	"gorm.io/gorm"
)

var errTestSearchAttemptAuditAppend = errors.New("test search-attempt audit append failure")

type searchAttemptAuditTestRecord struct {
	SearchJobID string `gorm:"column:search_job_id;primaryKey"`
	TenantID    string `gorm:"column:tenant_id"`
	OwnerID     string `gorm:"column:owner_id"`
	OccurredAt  int64  `gorm:"column:occurred_at_unix_micro"`
}

func (searchAttemptAuditTestRecord) TableName() string {
	return "search_attempt_audit_test_events"
}

type recordedSearchAttemptAudit struct {
	tenantID  string
	event     SearchAttemptAuditEvent
	rowFound  bool
	row       pendingHistoryRecord
	insideSQL bool
}

type recordingSearchAttemptAuditAppender struct {
	mu      sync.Mutex
	calls   []recordedSearchAttemptAudit
	fail    bool
	persist bool
}

var _ SearchAttemptAuditAppender = (*recordingSearchAttemptAuditAppender)(nil)

func (appender *recordingSearchAttemptAuditAppender) AppendSearchAttemptInTransaction(
	ctx context.Context,
	tx *gorm.DB,
	tenantID string,
	event SearchAttemptAuditEvent,
) error {
	if ctx == nil || tx == nil || tx.Statement == nil {
		return errors.New("test search-attempt audit appender received an invalid transaction")
	}
	_, insideSQLTransaction := tx.Statement.ConnPool.(*sql.Tx)
	if !insideSQLTransaction {
		return errors.New("test search-attempt audit appender was not called inside a SQL transaction")
	}

	recorded := recordedSearchAttemptAudit{
		tenantID: strings.Clone(tenantID),
		event: SearchAttemptAuditEvent{
			OccurredAt:        event.OccurredAt,
			SearchJobID:       strings.Clone(event.SearchJobID),
			OwnerID:           strings.Clone(event.OwnerID),
			KnowledgeSnapshot: event.KnowledgeSnapshot,
		},
		insideSQL: true,
	}
	rowErr := tx.Where("search_job_id = ?", event.SearchJobID).Take(&recorded.row).Error
	switch {
	case rowErr == nil:
		recorded.rowFound = true
		recorded.row.EntryProto = slices.Clone(recorded.row.EntryProto)
		recorded.row.EntrySHA256 = slices.Clone(recorded.row.EntrySHA256)
	case errors.Is(rowErr, gorm.ErrRecordNotFound):
	default:
		return rowErr
	}

	appender.mu.Lock()
	appender.calls = append(appender.calls, recorded)
	appender.mu.Unlock()
	if appender.persist {
		created := tx.Create(&searchAttemptAuditTestRecord{
			SearchJobID: event.SearchJobID,
			TenantID:    tenantID,
			OwnerID:     event.OwnerID,
			OccurredAt:  event.OccurredAt.UnixMicro(),
		})
		if created.Error != nil {
			return created.Error
		}
	}
	if appender.fail {
		return errTestSearchAttemptAuditAppend
	}
	return nil
}

func (appender *recordingSearchAttemptAuditAppender) snapshot() []recordedSearchAttemptAudit {
	appender.mu.Lock()
	defer appender.mu.Unlock()
	result := slices.Clone(appender.calls)
	for index := range result {
		result[index].row.EntryProto = slices.Clone(result[index].row.EntryProto)
		result[index].row.EntrySHA256 = slices.Clone(result[index].row.EntrySHA256)
		result[index].event.KnowledgeSnapshot = cloneKnowledgeSnapshotRef(
			result[index].event.KnowledgeSnapshot,
		)
	}
	return result
}

func TestSearchAttemptAuditAppendsOnceAfterPendingInsertInSameTransaction(t *testing.T) {
	database, _ := openTestStore(t, Options{})
	createSearchAttemptAuditTestTable(t, database)
	appender := &recordingSearchAttemptAuditAppender{persist: true}
	store := newSearchAttemptAuditStore(t, database, appender, true)
	ctx := context.Background()
	scope := AccessScope{TenantID: " tenant-a ", OwnerID: " owner-a "}
	created := time.Date(2026, time.August, 4, 12, 34, 56, 987_654_321, time.FixedZone("test", -7*60*60))
	input := pendingHistoryEntry(" job-a ", "index=main", created)
	input.KnowledgeSnapshot = &opensplunkv1.KnowledgeSnapshotSummary{
		Ref: &opensplunkv1.KnowledgeSnapshotRef{
			SnapshotSha256:               bytes.Repeat([]byte{0x42}, 32),
			TenantCatalogRevision:        7,
			TenantCatalogStateToken:      bytes.Repeat([]byte{0x73}, 32),
			ObjectCount:                  0,
			CompilerCompatibilityVersion: "0.1",
		},
	}
	original := proto.Clone(input).(*opensplunkv1.SearchHistoryEntry)

	admitted, err := store.BeginAttempt(ctx, scope, input)
	if err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}
	wantOccurredAt := time.UnixMicro(created.UnixMicro()).UTC()
	calls := appender.snapshot()
	if len(calls) != 1 {
		t.Fatalf("audit calls = %d, want 1", len(calls))
	}
	call := calls[0]
	if call.tenantID != "tenant-a" || call.event.SearchJobID != "job-a" ||
		call.event.OwnerID != "owner-a" || !call.event.OccurredAt.Equal(wantOccurredAt) ||
		!proto.Equal(call.event.KnowledgeSnapshot, original.GetKnowledgeSnapshot().GetRef()) ||
		!call.insideSQL || !call.rowFound {
		t.Fatalf("audit call = %#v", call)
	}
	input.KnowledgeSnapshot.Ref.SnapshotSha256[0] ^= 0xff
	input.KnowledgeSnapshot.Ref.TenantCatalogStateToken[0] ^= 0xff
	if detached := appender.snapshot()[0].event.KnowledgeSnapshot; !proto.Equal(detached, original.GetKnowledgeSnapshot().GetRef()) {
		t.Fatal("audit appender received caller-owned knowledge snapshot storage")
	}
	if call.row.TenantID != "tenant-a" || call.row.OwnerID != "owner-a" ||
		call.row.SearchJobID != "job-a" || call.row.CreatedAtUnixMicro != wantOccurredAt.UnixMicro() {
		t.Fatalf("pending row observed by appender = %#v", call.row)
	}
	if got := admitted.GetCreatedAt().AsTime(); !got.Equal(wantOccurredAt) {
		t.Fatalf("admitted created_at = %s, want %s", got, wantOccurredAt)
	}
	assertSearchAttemptAuditTestRows(t, database, 1)

	if _, err := store.BeginAttempt(ctx, scope, original); err != nil {
		t.Fatalf("idempotent BeginAttempt() error = %v", err)
	}
	changed := proto.Clone(original).(*opensplunkv1.SearchHistoryEntry)
	changed.Definition.Spl = "index=other"
	if _, err := store.BeginAttempt(ctx, scope, changed); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("changed BeginAttempt() error = %v, want ErrVersionConflict", err)
	}
	if _, err := store.BeginAttempt(ctx, AccessScope{TenantID: "tenant-a", OwnerID: "other"}, original); !errors.Is(err, control.ErrAlreadyExists) {
		t.Fatalf("cross-owner BeginAttempt() error = %v, want ErrAlreadyExists", err)
	}
	if calls := appender.snapshot(); len(calls) != 1 {
		t.Fatalf("audit calls after retries and conflicts = %d, want 1", len(calls))
	}
	assertSearchAttemptAuditTestRows(t, database, 1)
}

func TestSearchAttemptAuditFailureRollsBackPendingAndAuditRows(t *testing.T) {
	database, _ := openTestStore(t, Options{})
	createSearchAttemptAuditTestTable(t, database)
	appender := &recordingSearchAttemptAuditAppender{fail: true, persist: true}
	store := newSearchAttemptAuditStore(t, database, appender, true)
	entry := pendingHistoryEntry("job-rollback", "index=main", time.Now().UTC())

	if _, err := store.BeginAttempt(
		context.Background(),
		AccessScope{TenantID: "tenant", OwnerID: "owner"},
		entry,
	); !errors.Is(err, errTestSearchAttemptAuditAppend) {
		t.Fatalf("BeginAttempt() error = %v, want audit append failure", err)
	}
	if calls := appender.snapshot(); len(calls) != 1 || !calls[0].rowFound {
		t.Fatalf("audit calls = %#v, want one call after pending visibility", calls)
	}
	var pendingRows int64
	if err := database.GORMDB().Model(&pendingHistoryRecord{}).Count(&pendingRows).Error; err != nil {
		t.Fatal(err)
	}
	if pendingRows != 0 {
		t.Fatalf("pending rows after audit failure = %d, want 0", pendingRows)
	}
	assertSearchAttemptAuditTestRows(t, database, 0)
}

func TestSearchAttemptAuditTerminalJobIDReuseAppendsNothing(t *testing.T) {
	database, _ := openTestStore(t, Options{})
	createSearchAttemptAuditTestTable(t, database)
	appender := &recordingSearchAttemptAuditAppender{persist: true}
	store := newSearchAttemptAuditStore(t, database, appender, true)
	ctx := context.Background()
	scope := AccessScope{TenantID: "tenant-terminal", OwnerID: "owner-terminal"}
	created := time.Date(2026, time.August, 5, 10, 11, 12, 345_678_000, time.UTC)
	pending := pendingHistoryEntry("job-terminal-reuse", "index=main", created)

	if _, err := store.BeginAttempt(ctx, scope, pending); err != nil {
		t.Fatalf("BeginAttempt() error = %v", err)
	}
	terminal := historyEntry(
		pending.SearchJobId,
		pending.Definition.Spl,
		"search",
		opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
		created,
	)
	if _, err := store.CompleteAttempt(ctx, scope, terminal); err != nil {
		t.Fatalf("CompleteAttempt() error = %v", err)
	}

	if _, err := store.BeginAttempt(ctx, scope, pending); !errors.Is(
		err,
		control.ErrVersionConflict,
	) {
		t.Fatalf(
			"BeginAttempt(terminal job ID) error = %v, want ErrVersionConflict",
			err,
		)
	}
	if calls := appender.snapshot(); len(calls) != 1 {
		t.Fatalf("audit calls after terminal job ID reuse = %d, want 1", len(calls))
	}
	assertSearchAttemptAuditTestRows(t, database, 1)

	var pendingRows, terminalRows int64
	if err := database.GORMDB().Model(&pendingHistoryRecord{}).Count(&pendingRows).Error; err != nil {
		t.Fatalf("count pending rows: %v", err)
	}
	if err := database.GORMDB().Model(&historyRecord{}).Count(&terminalRows).Error; err != nil {
		t.Fatalf("count terminal rows: %v", err)
	}
	if pendingRows != 0 || terminalRows != 1 {
		t.Fatalf(
			"history rows after terminal job ID reuse = pending %d/terminal %d, want 0/1",
			pendingRows,
			terminalRows,
		)
	}
}

func TestNewValidatesSearchAttemptAuditConfiguration(t *testing.T) {
	database, _ := openTestStore(t, Options{})
	validAppender := &recordingSearchAttemptAuditAppender{}
	var typedNil *recordingSearchAttemptAuditAppender

	for name, options := range map[string]Options{
		"required without appender": {
			CursorKey: testCursorKey, RequireSearchAttemptAudit: true,
		},
		"typed nil optional appender": {
			CursorKey: testCursorKey, AuditAppender: typedNil,
		},
		"typed nil required appender": {
			CursorKey: testCursorKey, AuditAppender: typedNil, RequireSearchAttemptAudit: true,
		},
	} {
		t.Run(name, func(t *testing.T) {
			store, err := New(database, options)
			if store != nil || !errors.Is(err, control.ErrInvalidArgument) {
				t.Fatalf("New() = (%v, %v), want nil/ErrInvalidArgument", store, err)
			}
		})
	}

	optional, err := New(database, Options{CursorKey: testCursorKey})
	if err != nil || optional == nil || optional.searchAttemptAuditAppender != nil {
		t.Fatalf("New(optional unaudited) = (%v, %v)", optional, err)
	}
	required, err := New(database, Options{
		CursorKey: testCursorKey, AuditAppender: validAppender, RequireSearchAttemptAudit: true,
	})
	if err != nil || required == nil || required.searchAttemptAuditAppender != validAppender {
		t.Fatalf("New(required audited) = (%v, %v)", required, err)
	}
}

func TestSearchAttemptAuditConcurrentExactRetriesAppendOnce(t *testing.T) {
	database, _ := openTestStore(t, Options{})
	createSearchAttemptAuditTestTable(t, database)
	appender := &recordingSearchAttemptAuditAppender{persist: true}
	store := newSearchAttemptAuditStore(t, database, appender, true)
	entry := pendingHistoryEntry("job-concurrent", "index=main", time.Now().UTC())
	scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}

	const workers = 12
	start := make(chan struct{})
	errorsByWorker := make(chan error, workers)
	var wait sync.WaitGroup
	for range workers {
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			_, err := store.BeginAttempt(context.Background(), scope, entry)
			errorsByWorker <- err
		}()
	}
	close(start)
	wait.Wait()
	close(errorsByWorker)
	for err := range errorsByWorker {
		if err != nil {
			t.Fatalf("concurrent BeginAttempt() error = %v", err)
		}
	}
	if calls := appender.snapshot(); len(calls) != 1 {
		t.Fatalf("concurrent audit calls = %d, want 1", len(calls))
	}
	assertSearchAttemptAuditTestRows(t, database, 1)
}

func TestSearchAttemptAuditConcurrentConflictingAdmissionsAppendOnce(t *testing.T) {
	database, _ := openTestStore(t, Options{})
	createSearchAttemptAuditTestTable(t, database)
	appender := &recordingSearchAttemptAuditAppender{persist: true}
	store := newSearchAttemptAuditStore(t, database, appender, true)
	scope := AccessScope{TenantID: "tenant-conflict", OwnerID: "owner-conflict"}
	created := time.Date(2026, time.August, 5, 13, 14, 15, 456_789_000, time.UTC)
	entries := []*opensplunkv1.SearchHistoryEntry{
		pendingHistoryEntry("job-concurrent-conflict", "index=main", created),
		pendingHistoryEntry("job-concurrent-conflict", "index=other", created),
	}

	type admissionResult struct {
		entry *opensplunkv1.SearchHistoryEntry
		err   error
	}
	start := make(chan struct{})
	results := make(chan admissionResult, len(entries))
	var wait sync.WaitGroup
	for _, entry := range entries {
		entry := entry
		wait.Add(1)
		go func() {
			defer wait.Done()
			<-start
			admitted, err := store.BeginAttempt(
				context.Background(),
				scope,
				entry,
			)
			results <- admissionResult{entry: admitted, err: err}
		}()
	}
	close(start)
	wait.Wait()
	close(results)

	var admitted, conflicted int
	var winner *opensplunkv1.SearchHistoryEntry
	for result := range results {
		switch {
		case result.err == nil:
			admitted++
			winner = result.entry
		case errors.Is(result.err, control.ErrVersionConflict):
			conflicted++
		default:
			t.Fatalf("concurrent conflicting BeginAttempt() error = %v", result.err)
		}
	}
	if admitted != 1 || conflicted != 1 || winner == nil {
		t.Fatalf(
			"concurrent conflicting admissions = %d admitted/%d conflicted, want 1/1",
			admitted,
			conflicted,
		)
	}
	if calls := appender.snapshot(); len(calls) != 1 {
		t.Fatalf("concurrent conflicting audit calls = %d, want 1", len(calls))
	}
	assertSearchAttemptAuditTestRows(t, database, 1)

	var pending pendingHistoryRecord
	if err := database.GORMDB().Where(
		"search_job_id = ?",
		"job-concurrent-conflict",
	).Take(&pending).Error; err != nil {
		t.Fatalf("read winning pending admission: %v", err)
	}
	persisted, err := pendingAttemptFromRecord(pending)
	if err != nil {
		t.Fatalf("decode winning pending admission: %v", err)
	}
	if persisted.entry.GetDefinition().GetSpl() != winner.GetDefinition().GetSpl() {
		t.Fatalf(
			"persisted winning SPL = %q, admitted winner = %q",
			persisted.entry.GetDefinition().GetSpl(),
			winner.GetDefinition().GetSpl(),
		)
	}
}

func newSearchAttemptAuditStore(
	t *testing.T,
	database *control.DB,
	appender SearchAttemptAuditAppender,
	require bool,
) *Store {
	t.Helper()
	store, err := New(database, Options{
		CursorKey:                 testCursorKey,
		AuditAppender:             appender,
		RequireSearchAttemptAudit: require,
	})
	if err != nil {
		t.Fatalf("New() error = %v", err)
	}
	return store
}

func createSearchAttemptAuditTestTable(t *testing.T, database *control.DB) {
	t.Helper()
	if err := database.GORMDB().Exec(`
		CREATE TABLE search_attempt_audit_test_events (
			search_job_id TEXT PRIMARY KEY NOT NULL,
			tenant_id TEXT NOT NULL,
			owner_id TEXT NOT NULL,
			occurred_at_unix_micro INTEGER NOT NULL
		) STRICT
	`).Error; err != nil {
		t.Fatalf("create search-attempt audit test table: %v", err)
	}
}

func assertSearchAttemptAuditTestRows(t *testing.T, database *control.DB, want int64) {
	t.Helper()
	var got int64
	if err := database.GORMDB().Model(&searchAttemptAuditTestRecord{}).Count(&got).Error; err != nil {
		t.Fatalf("count search-attempt audit test rows: %v", err)
	}
	if got != want {
		t.Fatalf("search-attempt audit test rows = %d, want %d", got, want)
	}
}
