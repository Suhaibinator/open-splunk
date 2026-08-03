package auth

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

func TestCollectorTokenConstraintsRoundTripAndReplaceAtomically(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}

	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:                 "constrained",
		AllowedIndexNames:    []string{"main"},
		BoundCollectorID:     testCollectorID,
		AllowedHostRegexes:   []string{"^z$", "^a$", "^z$"},
		AllowedSourceRegexes: []string{`^/var/log/[^/]+$`, `.*`, `.*`},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	wantHosts := []string{"^a$", "^z$"}
	wantSources := []string{`.*`, `^/var/log/[^/]+$`}
	assertCollectorTokenConstraints(t, issued.Token, wantHosts, wantSources)

	// The creation result is detached from persistence.
	issued.Token.AllowedHostRegexes[0] = "mutated"
	issued.Token.AllowedSourceRegexes[0] = "mutated"
	got, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(): %v", err)
	}
	assertCollectorTokenConstraints(t, got, wantHosts, wantSources)
	listed, err := store.ListCollectorTokens(ctx)
	if err != nil {
		t.Fatalf("ListCollectorTokens(): %v", err)
	}
	if len(listed) != 1 {
		t.Fatalf("ListCollectorTokens() returned %d tokens", len(listed))
	}
	assertCollectorTokenConstraints(t, listed[0], wantHosts, wantSources)

	authentication, err := store.Authenticate(ctx, issued.Secret.Plaintext())
	if err != nil {
		t.Fatalf("Authenticate(): %v", err)
	}
	if !slices.Equal(authentication.AllowedHostRegexes, wantHosts) ||
		!slices.Equal(authentication.AllowedSourceRegexes, wantSources) {
		t.Fatalf("authentication constraints = hosts %q sources %q", authentication.AllowedHostRegexes, authentication.AllowedSourceRegexes)
	}

	updated, err := store.UpdateCollectorToken(
		ctx,
		got.ID,
		got.Version,
		UpdateCollectorTokenRequest{
			Name:                 "replaced",
			AllowedIndexNames:    []string{"main"},
			AllowedHostRegexes:   []string{"^new-host$"},
			AllowedSourceRegexes: []string{"^new-source$"},
		},
	)
	if err != nil {
		t.Fatalf("UpdateCollectorToken(): %v", err)
	}
	assertCollectorTokenConstraints(
		t,
		updated,
		[]string{"^new-host$"},
		[]string{"^new-source$"},
	)
	if _, err := store.UpdateCollectorToken(
		ctx,
		updated.ID,
		got.Version,
		UpdateCollectorTokenRequest{
			Name:                 "stale loser",
			AllowedIndexNames:    []string{"main"},
			AllowedHostRegexes:   []string{"^loser-host$"},
			AllowedSourceRegexes: []string{"^loser-source$"},
		},
	); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale UpdateCollectorToken() error = %v, want ErrVersionConflict", err)
	}
	afterConflict, err := store.GetCollectorToken(ctx, updated.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(after version conflict): %v", err)
	}
	assertCollectorTokenConstraints(
		t,
		afterConflict,
		[]string{"^new-host$"},
		[]string{"^new-source$"},
	)

	var stored []collectorTokenConstraintRecord
	if err := database.GORMDB().
		Where("ingestion_token_id = ?", got.ID).
		Order("constraint_kind").
		Order("ordinal").
		Find(&stored).Error; err != nil {
		t.Fatalf("read stored token constraints: %v", err)
	}
	if len(stored) != 2 ||
		stored[0].Ordinal != 0 || stored[1].Ordinal != 0 ||
		stored[0].Pattern != "^new-host$" ||
		stored[1].Pattern != "^new-source$" {
		t.Fatalf("stored replacement constraints = %#v", stored)
	}
}

func TestCollectorTokenConstraintValidationIsBoundedAndDeterministic(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}

	tooMany := make([]string, maximumTokenConstraintPatterns+1)
	for index := range tooMany {
		tooMany[index] = fmt.Sprintf("^host-%02d$", index)
	}
	overTotalBytes := make([]string, 9)
	for index := range overTotalBytes {
		overTotalBytes[index] = strings.Repeat("a", maximumTokenConstraintPatternBytes-1) + string(rune('a'+index))
	}
	tests := []struct {
		name    string
		hosts   []string
		sources []string
	}{
		{name: "empty", hosts: []string{""}},
		{name: "invalid UTF-8", hosts: []string{string([]byte{0xff})}},
		{name: "embedded NUL", hosts: []string{"^host\x00suffix$"}},
		{name: "invalid RE2", hosts: []string{"["}},
		{name: "pattern too large", hosts: []string{strings.Repeat("a", maximumTokenConstraintPatternBytes+1)}},
		{name: "too many patterns", hosts: tooMany},
		{name: "dimension too large", sources: overTotalBytes},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, createErr := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
				Name:                 "invalid constraints",
				AllowedIndexNames:    []string{"main"},
				BoundCollectorID:     testCollectorID,
				AllowedHostRegexes:   test.hosts,
				AllowedSourceRegexes: test.sources,
			})
			if !errors.Is(createErr, control.ErrInvalidArgument) {
				t.Fatalf("CreateCollectorToken() error = %v, want ErrInvalidArgument", createErr)
			}
		})
	}
	duplicates := make([]string, maximumTokenConstraintPatterns+1)
	for index := range duplicates {
		duplicates[index] = "^duplicate$"
	}
	if _, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:               "duplicate constraints",
		AllowedIndexNames:  []string{"main"},
		BoundCollectorID:   testCollectorID,
		AllowedHostRegexes: duplicates,
	}); err != nil {
		t.Fatalf("CreateCollectorToken(duplicate constraints): %v", err)
	}

	boundary := make([]string, 8)
	for index := range boundary {
		boundary[index] = strings.Repeat("b", maximumTokenConstraintPatternBytes-1) + string(rune('a'+index))
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:               "boundary constraints",
		AllowedIndexNames:  []string{"main"},
		BoundCollectorID:   testCollectorID,
		AllowedHostRegexes: boundary,
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(boundary): %v", err)
	}
	want := slices.Clone(boundary)
	slices.Sort(want)
	if !slices.Equal(issued.Token.AllowedHostRegexes, want) {
		t.Fatalf("boundary host constraints = %q, want %q", issued.Token.AllowedHostRegexes, want)
	}
}

func TestCollectorTokenConstraintHydrationFailsClosedOnCorruption(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:               "corruption target",
		AllowedIndexNames:  []string{"main"},
		BoundCollectorID:   testCollectorID,
		AllowedHostRegexes: []string{"^valid$"},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}

	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_token_constraints
		SET pattern = '['
		WHERE ingestion_token_id = ? AND constraint_kind = 'host' AND ordinal = 0`,
		issued.Token.ID,
	); err != nil {
		t.Fatalf("corrupt stored constraint: %v", err)
	}
	if _, err := store.GetCollectorToken(ctx, issued.Token.ID); err == nil {
		t.Fatal("GetCollectorToken() unexpectedly accepted an invalid stored regex")
	}
	if _, err := store.Authenticate(ctx, issued.Secret.Plaintext()); err == nil {
		t.Fatal("Authenticate() unexpectedly accepted an invalid stored regex")
	}

	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_token_constraints
		SET pattern = '^valid$', ordinal = 1
		WHERE ingestion_token_id = ? AND constraint_kind = 'host' AND ordinal = 0`,
		issued.Token.ID,
	); err != nil {
		t.Fatalf("create stored ordinal gap: %v", err)
	}
	if _, err := store.GetCollectorToken(ctx, issued.Token.ID); err == nil {
		t.Fatal("GetCollectorToken() unexpectedly accepted a stored ordinal gap")
	}

	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_token_constraints
		SET pattern = 'z', ordinal = 0
		WHERE ingestion_token_id = ? AND constraint_kind = 'host'`,
		issued.Token.ID,
	); err != nil {
		t.Fatalf("repair stored ordinal: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		INSERT INTO ingestion_token_constraints (
			ingestion_token_id,
			constraint_kind,
			ordinal,
			pattern
		) VALUES (?, 'host', 1, 'a')`,
		issued.Token.ID,
	); err != nil {
		t.Fatalf("store non-lexical constraint: %v", err)
	}
	if _, err := store.GetCollectorToken(ctx, issued.Token.ID); err == nil {
		t.Fatal("GetCollectorToken() unexpectedly accepted non-lexical constraints")
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_token_constraints
		SET pattern = 'z'
		WHERE ingestion_token_id = ? AND constraint_kind = 'host' AND ordinal = 1`,
		issued.Token.ID,
	); err != nil {
		t.Fatalf("store duplicate constraint: %v", err)
	}
	if _, err := store.GetCollectorToken(ctx, issued.Token.ID); err == nil {
		t.Fatal("GetCollectorToken() unexpectedly accepted duplicate constraints")
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		DELETE FROM ingestion_token_constraints
		WHERE ingestion_token_id = ? AND constraint_kind = 'host' AND ordinal = 1`,
		issued.Token.ID,
	); err != nil {
		t.Fatalf("remove duplicate constraint: %v", err)
	}
	withIgnoredSQLiteChecks(t, ctx, database, func(connection *sql.Conn) {
		if _, err := connection.ExecContext(ctx, `
			UPDATE ingestion_token_constraints
			SET constraint_kind = 'unknown'
			WHERE ingestion_token_id = ? AND constraint_kind = 'host'`,
			issued.Token.ID,
		); err != nil {
			t.Fatalf("store unknown constraint kind: %v", err)
		}
	})
	if _, err := store.GetCollectorToken(ctx, issued.Token.ID); err == nil {
		t.Fatal("GetCollectorToken() unexpectedly accepted an unknown constraint kind")
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		UPDATE ingestion_token_constraints
		SET constraint_kind = 'host', pattern = 'a'
		WHERE ingestion_token_id = ? AND constraint_kind = 'unknown'`,
		issued.Token.ID,
	); err != nil {
		t.Fatalf("repair stored constraint kind: %v", err)
	}
	withIgnoredSQLiteChecks(t, ctx, database, func(connection *sql.Conn) {
		if _, err := connection.ExecContext(ctx, `
			UPDATE ingestion_token_constraints
			SET pattern = ?
			WHERE ingestion_token_id = ? AND constraint_kind = 'host'`,
			strings.Repeat("a", maximumTokenConstraintPatternBytes+1),
			issued.Token.ID,
		); err != nil {
			t.Fatalf("store oversized constraint: %v", err)
		}
	})
	if _, err := store.GetCollectorToken(ctx, issued.Token.ID); err == nil {
		t.Fatal("GetCollectorToken() unexpectedly hydrated an oversized stored pattern")
	}

	withIgnoredSQLiteChecks(t, ctx, database, func(connection *sql.Conn) {
		if _, err := connection.ExecContext(ctx, `
			UPDATE ingestion_token_constraints
			SET pattern = 'a'
			WHERE ingestion_token_id = ? AND constraint_kind = 'host'`,
			issued.Token.ID,
		); err != nil {
			t.Fatalf("repair stored pattern width: %v", err)
		}
		for ordinal := 1; ordinal <= maximumTokenConstraintPatterns; ordinal++ {
			if _, err := connection.ExecContext(ctx, `
				INSERT INTO ingestion_token_constraints (
					ingestion_token_id,
					constraint_kind,
					ordinal,
					pattern
				) VALUES (?, 'host', ?, ?)`,
				issued.Token.ID,
				ordinal,
				fmt.Sprintf("b%02d", ordinal),
			); err != nil {
				t.Fatalf("store constraint fanout row %d: %v", ordinal, err)
			}
		}
	})
	if _, err := store.GetCollectorToken(ctx, issued.Token.ID); err == nil {
		t.Fatal("GetCollectorToken() unexpectedly hydrated corrupt constraint fanout")
	}
}

func TestCollectorTokenConstraintReplacementRollsBackOnPersistenceFailure(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	issued, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
		Name:                 "before",
		AllowedIndexNames:    []string{"main"},
		BoundCollectorID:     testCollectorID,
		AllowedHostRegexes:   []string{"^before-host$"},
		AllowedSourceRegexes: []string{"^before-source$"},
	})
	if err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	if _, err := database.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER reject_test_constraint_replacement
		BEFORE INSERT ON ingestion_token_constraints
		WHEN NEW.pattern = '^after-host$'
		BEGIN
			SELECT RAISE(ABORT, 'injected constraint persistence failure');
		END`); err != nil {
		t.Fatalf("create failure-injection trigger: %v", err)
	}

	if _, err := store.UpdateCollectorToken(
		ctx,
		issued.Token.ID,
		issued.Token.Version,
		UpdateCollectorTokenRequest{
			Name:                 "after",
			AllowedIndexNames:    []string{"main"},
			AllowedHostRegexes:   []string{"^after-host$"},
			AllowedSourceRegexes: []string{"^after-source$"},
		},
	); err == nil {
		t.Fatal("UpdateCollectorToken() unexpectedly survived injected persistence failure")
	}

	got, err := store.GetCollectorToken(ctx, issued.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(after rollback): %v", err)
	}
	if got.Version != issued.Token.Version || got.Name != "before" {
		t.Fatalf("metadata survived failed replacement as %#v", got)
	}
	assertCollectorTokenConstraints(
		t,
		got,
		[]string{"^before-host$"},
		[]string{"^before-source$"},
	)
}

func TestCollectorTokenConstraintRowsCascadeWhenRetentionPrunesParent(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	database := openControlDB(t)
	if _, err := database.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStoreWithOptions(
		database,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{RetainedRevokedTokenLimit: 1, TotalTokenRecordLimit: 3},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}
	create := func(name, pattern string) IssuedCollectorToken {
		t.Helper()
		issued, createErr := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:               name,
			AllowedIndexNames:  []string{"main"},
			BoundCollectorID:   testCollectorID,
			AllowedHostRegexes: []string{pattern},
		})
		if createErr != nil {
			t.Fatalf("CreateCollectorToken(%q): %v", name, createErr)
		}
		return issued
	}

	first := create("first", "^first$")
	if _, err := store.RevokeCollectorToken(ctx, first.Token.ID, first.Token.Version); err != nil {
		t.Fatalf("RevokeCollectorToken(first): %v", err)
	}
	second := create("second", "^second$")
	if _, err := store.RevokeCollectorToken(ctx, second.Token.ID, second.Token.Version); err != nil {
		t.Fatalf("RevokeCollectorToken(second): %v", err)
	}

	var firstRows, secondRows int64
	if err := database.GORMDB().Model(&collectorTokenConstraintRecord{}).
		Where("ingestion_token_id = ?", first.Token.ID).
		Count(&firstRows).Error; err != nil {
		t.Fatalf("count first token constraints: %v", err)
	}
	if err := database.GORMDB().Model(&collectorTokenConstraintRecord{}).
		Where("ingestion_token_id = ?", second.Token.ID).
		Count(&secondRows).Error; err != nil {
		t.Fatalf("count second token constraints: %v", err)
	}
	if firstRows != 0 || secondRows != 1 {
		t.Fatalf("constraint rows after prune = first %d second %d, want 0/1", firstRows, secondRows)
	}
}

func assertCollectorTokenConstraints(
	t *testing.T,
	token CollectorToken,
	wantHosts []string,
	wantSources []string,
) {
	t.Helper()
	if !slices.Equal(token.AllowedHostRegexes, wantHosts) ||
		!slices.Equal(token.AllowedSourceRegexes, wantSources) {
		t.Fatalf("token constraints = hosts %q sources %q, want hosts %q sources %q", token.AllowedHostRegexes, token.AllowedSourceRegexes, wantHosts, wantSources)
	}
}

func withIgnoredSQLiteChecks(
	t *testing.T,
	ctx context.Context,
	database *control.DB,
	mutate func(*sql.Conn),
) {
	t.Helper()
	connection, err := database.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	checksRestored := false
	defer func() {
		if !checksRestored {
			_, _ = connection.ExecContext(context.Background(), `PRAGMA ignore_check_constraints = OFF`)
		}
		if err := connection.Close(); err != nil {
			t.Errorf("close corruption connection: %v", err)
		}
	}()
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = ON`); err != nil {
		t.Fatalf("disable test-only check constraints: %v", err)
	}
	mutate(connection)
	if _, err := connection.ExecContext(ctx, `PRAGMA ignore_check_constraints = OFF`); err != nil {
		t.Fatalf("restore check constraints: %v", err)
	}
	checksRestored = true
}
