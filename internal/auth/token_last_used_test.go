package auth

import (
	"context"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestRecordCollectorTokenUseProjectsMonotonicObservation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	createdAt := time.Date(2026, 7, 28, 10, 0, 0, 123_456_789, time.UTC)
	store.now = func() time.Time { return createdAt }
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "last-used",
		AllowedIndexNames: []string{"main"},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	if !issued.Token.LastUsedAt.IsZero() {
		t.Fatalf("new token LastUsedAt = %v, want zero", issued.Token.LastUsedAt)
	}
	originalVersion := issued.Token.Version
	originalUpdatedAt := issued.Token.UpdatedAt

	// A backwards wall-clock correction must not falsely classify a valid
	// credential as inactive. Clamp the first observation to the durable
	// creation boundary so the use remains visible without violating the
	// schema invariant.
	rolledBackAcceptedAt := createdAt.Add(-time.Hour)
	if err := store.RecordCollectorTokenUse(ctx, issued.Token.ID, rolledBackAcceptedAt); err != nil {
		t.Fatalf("RecordCollectorTokenUse(clock rollback): %v", err)
	}
	afterRollback, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(clock rollback): %v", err)
	}
	if !afterRollback.LastUsedAt.Equal(databaseTime(createdAt)) {
		t.Fatalf(
			"clock-rollback LastUsedAt = %v, want creation boundary %v",
			afterRollback.LastUsedAt,
			databaseTime(createdAt),
		)
	}
	if afterRollback.Version != originalVersion ||
		!afterRollback.UpdatedAt.Equal(originalUpdatedAt) {
		t.Fatalf(
			"clock rollback changed administrative CAS metadata: %#v",
			afterRollback,
		)
	}

	firstAcceptedAt := createdAt.Add(5*time.Minute + 987*time.Nanosecond)
	if err := store.RecordCollectorTokenUse(ctx, issued.Token.ID, firstAcceptedAt); err != nil {
		t.Fatalf("RecordCollectorTokenUse(first): %v", err)
	}
	firstAcceptedAt = databaseTime(firstAcceptedAt)
	got, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(first): %v", err)
	}
	if !got.LastUsedAt.Equal(firstAcceptedAt) {
		t.Fatalf("LastUsedAt = %v, want %v", got.LastUsedAt, firstAcceptedAt)
	}
	if got.Version != originalVersion || !got.UpdatedAt.Equal(originalUpdatedAt) {
		t.Fatalf(
			"recording use changed administrative CAS metadata: version=%d updated_at=%v, want %d/%v",
			got.Version,
			got.UpdatedAt,
			originalVersion,
			originalUpdatedAt,
		)
	}

	for label, observedAt := range map[string]time.Time{
		"older": firstAcceptedAt.Add(-time.Minute),
		"equal": firstAcceptedAt,
	} {
		t.Run(label, func(t *testing.T) {
			if err := store.RecordCollectorTokenUse(ctx, issued.Token.ID, observedAt); err != nil {
				t.Fatalf("RecordCollectorTokenUse(%s): %v", label, err)
			}
			current, err := store.GetCollectorToken(ctx, issued.Token.ID)
			if err != nil {
				t.Fatalf("GetCollectorToken(%s): %v", label, err)
			}
			if !current.LastUsedAt.Equal(firstAcceptedAt) {
				t.Fatalf("%s observation moved LastUsedAt to %v, want %v", label, current.LastUsedAt, firstAcceptedAt)
			}
			if current.Version != originalVersion || !current.UpdatedAt.Equal(originalUpdatedAt) {
				t.Fatalf("%s observation changed administrative CAS metadata: %#v", label, current)
			}
		})
	}

	newestAcceptedAt := databaseTime(firstAcceptedAt.Add(time.Minute))
	if err := store.RecordCollectorTokenUse(ctx, issued.Token.ID, newestAcceptedAt); err != nil {
		t.Fatalf("RecordCollectorTokenUse(newer): %v", err)
	}
	listed, err := store.ListCollectorTokens(ctx)
	if err != nil {
		t.Fatalf("ListCollectorTokens(): %v", err)
	}
	if len(listed) != 1 || !listed[0].LastUsedAt.Equal(newestAcceptedAt) {
		t.Fatalf("ListCollectorTokens() = %#v, want LastUsedAt %v", listed, newestAcceptedAt)
	}
	if listed[0].Version != originalVersion || !listed[0].UpdatedAt.Equal(originalUpdatedAt) {
		t.Fatalf("listed observation changed administrative CAS metadata: %#v", listed[0])
	}
}

func TestRecordCollectorTokenUseRequiresActiveUnexpiredToken(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 7, 28, 11, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }

	if err := store.RecordCollectorTokenUse(ctx, "missing-token", now); !errors.Is(err, ErrInactiveToken) {
		t.Fatalf("RecordCollectorTokenUse(missing) error = %v, want ErrInactiveToken", err)
	}
	if err := store.RecordCollectorTokenUse(ctx, "missing-token", time.Time{}); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("RecordCollectorTokenUse(zero time) error = %v, want ErrInvalidArgument", err)
	}

	revoked, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "revoked",
		AllowedIndexNames: []string{"main"},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(revoked): %v", err)
	}
	if _, err := store.RevokeCollectorToken(ctx, revoked.Token.ID, revoked.Token.Version); err != nil {
		t.Fatalf("RevokeCollectorToken(): %v", err)
	}
	if err := store.RecordCollectorTokenUse(ctx, revoked.Token.ID, now); !errors.Is(err, ErrInactiveToken) {
		t.Fatalf("RecordCollectorTokenUse(revoked) error = %v, want ErrInactiveToken", err)
	}

	expired, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "expired",
		AllowedIndexNames: []string{"main"},
		ExpiresAt:         now.Add(time.Minute),
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(expired): %v", err)
	}
	if err := store.RecordCollectorTokenUse(ctx, expired.Token.ID, now.Add(time.Minute)); !errors.Is(err, ErrInactiveToken) {
		t.Fatalf("RecordCollectorTokenUse(expired) error = %v, want ErrInactiveToken", err)
	}

	disabled, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "disabled",
		AllowedIndexNames: []string{"main"},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(disabled): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(
		ctx,
		`UPDATE ingestion_tokens SET state = 'disabled' WHERE ingestion_token_id = ?`,
		disabled.Token.ID,
	); err != nil {
		t.Fatalf("disable collector token: %v", err)
	}
	if err := store.RecordCollectorTokenUse(ctx, disabled.Token.ID, now); !errors.Is(err, ErrInactiveToken) {
		t.Fatalf("RecordCollectorTokenUse(disabled) error = %v, want ErrInactiveToken", err)
	}

	active, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "active",
		AllowedIndexNames: []string{"main"},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(active): %v", err)
	}
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if err := store.RecordCollectorTokenUse(canceled, active.Token.ID, now); !errors.Is(err, context.Canceled) {
		t.Fatalf("RecordCollectorTokenUse(canceled) error = %v, want context.Canceled", err)
	}
	current, err := store.GetCollectorToken(ctx, active.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(active): %v", err)
	}
	if !current.LastUsedAt.IsZero() {
		t.Fatalf("canceled observation persisted LastUsedAt %v", current.LastUsedAt)
	}
}

func TestConcurrentCollectorTokenUseKeepsNewestObservation(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, []byte("0123456789abcdef0123456789abcdef"))
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	now := time.Date(2026, 7, 28, 12, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "concurrent",
		AllowedIndexNames: []string{"main"},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}

	const observations = 32
	start := make(chan struct{})
	errs := make(chan error, observations)
	var workers sync.WaitGroup
	for offset := range observations {
		workers.Add(1)
		go func() {
			defer workers.Done()
			<-start
			errs <- store.RecordCollectorTokenUse(
				ctx,
				issued.Token.ID,
				now.Add(time.Duration(offset+1)*time.Second),
			)
		}()
	}
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("RecordCollectorTokenUse(concurrent): %v", err)
		}
	}

	got, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	want := now.Add(observations * time.Second)
	if !got.LastUsedAt.Equal(want) {
		t.Fatalf("LastUsedAt = %v, want newest concurrent observation %v", got.LastUsedAt, want)
	}
	if got.Version != issued.Token.Version || !got.UpdatedAt.Equal(issued.Token.UpdatedAt) {
		t.Fatalf("concurrent observations changed administrative CAS metadata: %#v", got)
	}
}

func TestCollectorTokenLastUseSurvivesDatabaseReopen(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "control.sqlite")
	key := []byte("0123456789abcdef0123456789abcdef")
	db, err := control.Open(ctx, path)
	if err != nil {
		t.Fatalf("control.Open(first): %v", err)
	}
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		_ = db.Close()
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(db, key)
	if err != nil {
		_ = db.Close()
		t.Fatalf("NewStore(first): %v", err)
	}
	now := time.Date(2026, 7, 28, 13, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return now }
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:              "persistent-last-use",
		AllowedIndexNames: []string{"main"},
	})
	if err != nil {
		_ = db.Close()
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	acceptedAt := now.Add(time.Minute)
	if err := store.RecordCollectorTokenUse(ctx, issued.Token.ID, acceptedAt); err != nil {
		_ = db.Close()
		t.Fatalf("RecordCollectorTokenUse(): %v", err)
	}
	if err := db.Close(); err != nil {
		t.Fatalf("Close(first): %v", err)
	}

	reopened, err := control.Open(ctx, path)
	if err != nil {
		t.Fatalf("control.Open(second): %v", err)
	}
	defer reopened.Close()
	reopenedStore, err := NewStore(reopened, key)
	if err != nil {
		t.Fatalf("NewStore(second): %v", err)
	}
	got, err := reopenedStore.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(reopened): %v", err)
	}
	if !got.LastUsedAt.Equal(acceptedAt) {
		t.Fatalf("reopened LastUsedAt = %v, want %v", got.LastUsedAt, acceptedAt)
	}
}
