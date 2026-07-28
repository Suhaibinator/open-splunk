package searchhistory

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"google.golang.org/protobuf/proto"
)

func TestPersistedTerminalCorruptionIsNotCallerInvalidArgument(t *testing.T) {
	t.Run("checksum-valid semantic corruption", func(t *testing.T) {
		database, store := openTestStore(t, Options{})
		ctx := context.Background()
		scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
		created := time.Now().UTC().Add(-time.Minute)
		entry := historyEntry(
			"terminal-semantic-corruption",
			"index=main",
			"search",
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			created,
		)
		if _, err := store.Record(ctx, scope, entry); err != nil {
			t.Fatal(err)
		}
		rewriteStoredHistoryProto(t, database, &historyRecord{}, entry.SearchJobId, func(stored *opensplunkv1.SearchHistoryEntry) {
			stored.Definition.Spl = " "
		})

		_, getErr := store.Get(ctx, scope, entry.SearchJobId)
		assertPersistedCorruption(t, "Get", getErr)
		_, listErr := store.List(ctx, scope, ListRequest{})
		assertPersistedCorruption(t, "List", listErr)
		_, completeErr := store.CompleteAttempt(ctx, scope, entry)
		assertPersistedCorruption(t, "CompleteAttempt terminal duplicate", completeErr)

		invalidRequest := historyEntry(
			"terminal-invalid-request",
			" ",
			"search",
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			created,
		)
		if _, err := store.Record(ctx, scope, invalidRequest); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("Record(invalid request) error = %v, want ErrInvalidArgument", err)
		}
		canceled, cancel := context.WithCancel(ctx)
		cancel()
		if _, err := store.Get(canceled, scope, entry.SearchJobId); !errors.Is(err, context.Canceled) {
			t.Fatalf("Get(canceled) error = %v, want context.Canceled", err)
		}
	})

	t.Run("persisted scope corruption", func(t *testing.T) {
		database, store := openTestStore(t, Options{})
		ctx := context.Background()
		scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
		created := time.Now().UTC().Add(-time.Minute)
		terminal := historyEntry(
			"terminal-scope-corruption",
			"index=main",
			"search",
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			created,
		)
		if _, err := store.Record(ctx, scope, terminal); err != nil {
			t.Fatal(err)
		}
		update := database.GORMDB().Model(&historyRecord{}).
			Where("search_job_id = ?", terminal.SearchJobId).
			Update("owner_id", " ")
		if update.Error != nil || update.RowsAffected != 1 {
			t.Fatalf("corrupt terminal scope = (%d, %v)", update.RowsAffected, update.Error)
		}

		pending := pendingHistoryEntry(terminal.SearchJobId, terminal.Definition.Spl, created)
		_, beginErr := store.BeginAttempt(ctx, scope, pending)
		assertPersistedCorruption(t, "BeginAttempt terminal scope", beginErr)

		var record historyRecord
		read := database.GORMDB().Where("search_job_id = ?", terminal.SearchJobId).Take(&record)
		if read.Error != nil {
			t.Fatal(read.Error)
		}
		_, conversionErr := historyEntryFromRecord(record)
		assertPersistedCorruption(t, "terminal record conversion", conversionErr)
	})
}

func TestPersistedPendingCorruptionIsNotCallerInvalidArgument(t *testing.T) {
	t.Run("checksum-valid semantic corruption", func(t *testing.T) {
		database, store := openTestStore(t, Options{})
		ctx := context.Background()
		scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
		created := time.Now().UTC().Add(-time.Minute)
		pending := pendingHistoryEntry("pending-semantic-corruption", "index=main", created)
		if _, err := store.BeginAttempt(ctx, scope, pending); err != nil {
			t.Fatal(err)
		}
		rewriteStoredHistoryProto(
			t,
			database,
			&pendingHistoryRecord{},
			pending.SearchJobId,
			func(stored *opensplunkv1.SearchHistoryEntry) {
				stored.Definition.Spl = " "
			},
		)

		_, beginErr := store.BeginAttempt(ctx, scope, pending)
		assertPersistedCorruption(t, "BeginAttempt", beginErr)
		terminal := historyEntry(
			pending.SearchJobId,
			pending.Definition.Spl,
			"search",
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			created,
		)
		_, completeErr := store.CompleteAttempt(ctx, scope, terminal)
		assertPersistedCorruption(t, "CompleteAttempt", completeErr)
		_, recoverErr := store.RecoverInterrupted(ctx, scope)
		assertPersistedCorruption(t, "RecoverInterrupted", recoverErr)

		invalidRequest := pendingHistoryEntry("pending-invalid-request", " ", created)
		if _, err := store.BeginAttempt(ctx, scope, invalidRequest); !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf("BeginAttempt(invalid request) error = %v, want ErrInvalidArgument", err)
		}
	})

	t.Run("persisted scope corruption", func(t *testing.T) {
		database, store := openTestStore(t, Options{})
		ctx := context.Background()
		scope := AccessScope{TenantID: "tenant", OwnerID: "owner"}
		created := time.Now().UTC().Add(-time.Minute)
		pending := pendingHistoryEntry("pending-scope-corruption", "index=main", created)
		if _, err := store.BeginAttempt(ctx, scope, pending); err != nil {
			t.Fatal(err)
		}
		update := database.GORMDB().Model(&pendingHistoryRecord{}).
			Where("search_job_id = ?", pending.SearchJobId).
			Update("owner_id", " ")
		if update.Error != nil || update.RowsAffected != 1 {
			t.Fatalf("corrupt pending scope = (%d, %v)", update.RowsAffected, update.Error)
		}

		_, beginErr := store.BeginAttempt(ctx, scope, pending)
		assertPersistedCorruption(t, "BeginAttempt pending scope", beginErr)
		terminal := historyEntry(
			pending.SearchJobId,
			pending.Definition.Spl,
			"search",
			opensplunkv1.SearchJobState_SEARCH_JOB_STATE_COMPLETED,
			created,
		)
		_, completeErr := store.CompleteAttempt(ctx, scope, terminal)
		assertPersistedCorruption(t, "CompleteAttempt pending scope", completeErr)

		var record pendingHistoryRecord
		read := database.GORMDB().Where("search_job_id = ?", pending.SearchJobId).Take(&record)
		if read.Error != nil {
			t.Fatal(read.Error)
		}
		_, conversionErr := pendingAttemptFromRecord(record)
		assertPersistedCorruption(t, "pending record conversion", conversionErr)
	})
}

func rewriteStoredHistoryProto(
	t *testing.T,
	database *control.DB,
	model any,
	searchJobID string,
	mutate func(*opensplunkv1.SearchHistoryEntry),
) {
	t.Helper()
	var stored struct {
		EntryProto []byte
	}
	read := database.GORMDB().Model(model).
		Select("entry_proto").
		Where("search_job_id = ?", searchJobID).
		Take(&stored)
	if read.Error != nil {
		t.Fatal(read.Error)
	}
	entry := new(opensplunkv1.SearchHistoryEntry)
	if err := (proto.UnmarshalOptions{DiscardUnknown: false}).Unmarshal(stored.EntryProto, entry); err != nil {
		t.Fatal(err)
	}
	mutate(entry)
	encoded, err := (proto.MarshalOptions{Deterministic: true}).Marshal(entry)
	if err != nil {
		t.Fatal(err)
	}
	checksum := sha256.Sum256(encoded)
	update := database.GORMDB().Model(model).
		Where("search_job_id = ?", searchJobID).
		Updates(map[string]any{
			"entry_proto":  encoded,
			"entry_sha256": checksum[:],
		})
	if update.Error != nil || update.RowsAffected != 1 {
		t.Fatalf("rewrite stored history proto = (%d, %v)", update.RowsAffected, update.Error)
	}
}

func assertPersistedCorruption(t *testing.T, operation string, err error) {
	t.Helper()
	if err == nil {
		t.Fatalf("%s error = nil, want persisted-corruption error", operation)
	}
	if errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("%s error = %v, must not retain ErrInvalidArgument", operation, err)
	}
}
