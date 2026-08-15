package auth

import (
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestCollectorTokenRevokedRetentionIndexMatchesMigration(t *testing.T) {
	t.Parallel()

	const indexName = "ingestion_tokens_revoked_retention_idx"
	db := openControlDB(t)
	statement := &gorm.Statement{DB: db.GORMDB()}
	if err := statement.Parse(&collectorTokenRecord{}); err != nil {
		t.Fatalf("parse collector token GORM model: %v", err)
	}

	var modelIndexColumns []string
	var modelIndexSorts []string
	var modelIndexWhere string
	for _, index := range statement.Schema.ParseIndexes() {
		if index.Name != indexName {
			continue
		}
		modelIndexWhere = index.Where
		for _, field := range index.Fields {
			modelIndexColumns = append(modelIndexColumns, field.DBName)
			modelIndexSorts = append(modelIndexSorts, strings.ToUpper(field.Sort))
		}
	}
	wantColumns := []string{"revoked_at_unix_micro", "ingestion_token_id"}
	wantSorts := []string{"DESC", "DESC"}
	const wantWhere = "state = 'revoked'"
	if !slices.Equal(modelIndexColumns, wantColumns) ||
		!slices.Equal(modelIndexSorts, wantSorts) ||
		modelIndexWhere != wantWhere {
		t.Fatalf(
			"GORM %s = columns %v sorts %v where %q, want columns %v sorts %v where %q",
			indexName,
			modelIndexColumns,
			modelIndexSorts,
			modelIndexWhere,
			wantColumns,
			wantSorts,
			wantWhere,
		)
	}

	type migratedIndexColumn struct {
		Name       string
		Descending int64 `gorm:"column:descending"`
	}
	var migratedColumns []migratedIndexColumn
	query := db.GORMDB().Raw(
		`SELECT name, "desc" AS descending
		 FROM pragma_index_xinfo(?)
		 WHERE "key" = 1
		 ORDER BY seqno`,
		indexName,
	).Scan(&migratedColumns)
	if query.Error != nil {
		t.Fatalf("read migrated %s columns: %v", indexName, query.Error)
	}
	gotColumns := make([]string, len(migratedColumns))
	gotSorts := make([]string, len(migratedColumns))
	for columnIndex, column := range migratedColumns {
		gotColumns[columnIndex] = column.Name
		if column.Descending == 1 {
			gotSorts[columnIndex] = "DESC"
		}
	}
	if !slices.Equal(gotColumns, wantColumns) ||
		!slices.Equal(gotSorts, wantSorts) {
		t.Fatalf(
			"migrated %s = columns %v sorts %v, want columns %v sorts %v",
			indexName,
			gotColumns,
			gotSorts,
			wantColumns,
			wantSorts,
		)
	}

	var migratedSQL string
	query = db.GORMDB().Raw(
		`SELECT sql
		 FROM sqlite_schema
		 WHERE type = 'index' AND name = ?`,
		indexName,
	).Scan(&migratedSQL)
	if query.Error != nil {
		t.Fatalf("read migrated %s definition: %v", indexName, query.Error)
	}
	const wantSQL = "CREATE INDEX ingestion_tokens_revoked_retention_idx " +
		"ON ingestion_tokens ( revoked_at_unix_micro DESC, ingestion_token_id DESC ) " +
		"WHERE state = 'revoked'"
	if got := strings.Join(strings.Fields(migratedSQL), " "); got != wantSQL {
		t.Fatalf("migrated %s SQL = %q, want %q", indexName, got, wantSQL)
	}
}

func TestCollectorTokenPruneVictimQueryUsesRevokedRetentionIndex(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{RetainedRevokedTokenLimit: 3},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}
	var tokenIDs []string
	generated := revokedCollectorTokenVictimQuery(
		db.GORMDB().Session(&gorm.Session{DryRun: true}),
		store.retainedRevokedTokenLimit-1,
		"current-token-id",
	).Find(&tokenIDs)
	if generated.Error != nil {
		t.Fatalf("build collector token prune victim query: %v", generated.Error)
	}
	if strings.Contains(generated.Statement.SQL.String(), "INDEXED BY") {
		t.Fatalf(
			"collector token prune victim SQL pins an index: %q",
			generated.Statement.SQL.String(),
		)
	}

	var planRows []struct {
		ID     int
		Parent int
		Unused int
		Detail string
	}
	query := db.GORMDB().Raw(
		"EXPLAIN QUERY PLAN "+generated.Statement.SQL.String(),
		generated.Statement.Vars...,
	).Scan(&planRows)
	if query.Error != nil {
		t.Fatalf("EXPLAIN QUERY PLAN collector token prune victims: %v", query.Error)
	}
	details := make([]string, len(planRows))
	for rowIndex, row := range planRows {
		details[rowIndex] = row.Detail
	}
	want := []string{
		"SCAN ingestion_tokens USING COVERING INDEX ingestion_tokens_revoked_retention_idx",
	}
	if !slices.Equal(details, want) {
		t.Fatalf(
			"collector token prune victim query plan = %q, want exactly %q",
			details,
			want,
		)
	}
}

func TestCollectorTokenRetentionOptions(t *testing.T) {
	t.Parallel()

	db := openControlDB(t)
	key := []byte("0123456789abcdef0123456789abcdef")

	defaultStore, err := NewStore(db, key)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	const (
		wantDefaultRetained = 256
		wantDefaultTotal    = 1024
	)
	if got := defaultStore.retainedRevokedTokenLimit; got != wantDefaultRetained {
		t.Fatalf(
			"default retained revoked token limit = %d, want %d",
			got,
			wantDefaultRetained,
		)
	}
	if got := defaultStore.totalTokenRecordLimit; got != wantDefaultTotal {
		t.Fatalf(
			"default total token record limit = %d, want %d",
			got,
			wantDefaultTotal,
		)
	}

	zeroStore, err := NewStoreWithOptions(db, key, StoreOptions{})
	if err != nil {
		t.Fatalf("NewStoreWithOptions(zero): %v", err)
	}
	if got := zeroStore.retainedRevokedTokenLimit; got != wantDefaultRetained {
		t.Fatalf(
			"zero-option retained revoked token limit = %d, want default %d",
			got,
			wantDefaultRetained,
		)
	}
	if got := zeroStore.totalTokenRecordLimit; got != wantDefaultTotal {
		t.Fatalf(
			"zero-option total token record limit = %d, want default %d",
			got,
			wantDefaultTotal,
		)
	}

	configuredStore, err := NewStoreWithOptions(db, key, StoreOptions{
		RetainedRevokedTokenLimit: 1,
		TotalTokenRecordLimit:     2,
	})
	if err != nil {
		t.Fatalf("NewStoreWithOptions(configured): %v", err)
	}
	if got := configuredStore.retainedRevokedTokenLimit; got != 1 {
		t.Fatalf("configured retained revoked token limit = %d, want 1", got)
	}
	if got := configuredStore.totalTokenRecordLimit; got != 2 {
		t.Fatalf("configured total token record limit = %d, want 2", got)
	}

	const (
		wantMaximumTotal    = 1024
		wantMaximumRetained = wantMaximumTotal - 1
	)
	maximumStore, err := NewStoreWithOptions(db, key, StoreOptions{
		RetainedRevokedTokenLimit: wantMaximumRetained,
		TotalTokenRecordLimit:     wantMaximumTotal,
	})
	if err != nil {
		t.Fatalf("NewStoreWithOptions(maximum): %v", err)
	}
	if got := maximumStore.retainedRevokedTokenLimit; got != wantMaximumRetained {
		t.Fatalf(
			"maximum usable retained revoked token limit = %d, want %d",
			got,
			wantMaximumRetained,
		)
	}
	if got := maximumStore.totalTokenRecordLimit; got != wantMaximumTotal {
		t.Fatalf(
			"maximum total token record limit = %d, want %d",
			got,
			wantMaximumTotal,
		)
	}

	for _, limit := range []int{-1, wantMaximumTotal + 1} {
		_, err := NewStoreWithOptions(db, key, StoreOptions{
			RetainedRevokedTokenLimit: limit,
		})
		if !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf(
				"NewStoreWithOptions(limit=%d) error = %v, want ErrInvalidArgument",
				limit,
				err,
			)
		}
	}
	for _, limit := range []int{-1, wantMaximumTotal + 1} {
		_, err := NewStoreWithOptions(db, key, StoreOptions{
			TotalTokenRecordLimit: limit,
		})
		if !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf(
				"NewStoreWithOptions(total limit=%d) error = %v, want ErrInvalidArgument",
				limit,
				err,
			)
		}
	}
	for _, options := range []StoreOptions{
		{
			RetainedRevokedTokenLimit: 1,
			TotalTokenRecordLimit:     1,
		},
		{
			RetainedRevokedTokenLimit: 3,
			TotalTokenRecordLimit:     2,
		},
		{
			RetainedRevokedTokenLimit: wantMaximumTotal,
			TotalTokenRecordLimit:     wantMaximumTotal,
		},
	} {
		_, err := NewStoreWithOptions(db, key, options)
		if !errors.Is(err, control.ErrInvalidArgument) {
			t.Fatalf(
				"NewStoreWithOptions(%+v) error = %v, want ErrInvalidArgument",
				options,
				err,
			)
		}
	}
}

func TestCreateCollectorTokenCapacityRequiresExplicitRevocationAndRecovers(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{
			RetainedRevokedTokenLimit: 2,
			TotalTokenRecordLimit:     3,
		},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}
	createTime := time.Date(2026, 7, 29, 15, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return createTime }
	create := func(name string, expiresAt time.Time) (IssuedCollectorToken, error) {
		return store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              name,
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
			ExpiresAt:         expiresAt,
		})
	}

	expiring, err := create("expiring", createTime.Add(time.Minute))
	if err != nil {
		t.Fatalf("CreateCollectorToken(expiring): %v", err)
	}
	for _, name := range []string{"active-1", "active-2"} {
		if _, err := create(name, time.Time{}); err != nil {
			t.Fatalf("CreateCollectorToken(%s): %v", name, err)
		}
	}

	store.now = func() time.Time { return createTime.Add(2 * time.Minute) }
	rejected, err := create("over-capacity", time.Time{})
	if !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf(
			"CreateCollectorToken(over capacity) error = %v, want ErrCapacityExceeded",
			err,
		)
	}
	if rejected.Token.ID != "" || rejected.Secret.Plaintext() != "" {
		t.Fatalf("capacity rejection returned token material: %#v", rejected)
	}
	expired, err := store.GetCollectorToken(ctx, expiring.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(expired at capacity): %v", err)
	}
	if expired.State != CollectorTokenStateExpired {
		t.Fatalf(
			"unrevoked expired token state = %q, want expired",
			expired.State,
		)
	}
	if got := countCollectorTokenRows(t, db, ""); got != 3 {
		t.Fatalf("token rows after capacity rejection = %d, want 3", got)
	}

	if _, err := store.RevokeCollectorToken(
		ctx,
		expiring.Token.ID,
		expiring.Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(expired): %v", err)
	}
	replacement, err := create("replacement", time.Time{})
	if err != nil {
		t.Fatalf("CreateCollectorToken(after revocation): %v", err)
	}
	if replacement.Token.ID == "" {
		t.Fatal("capacity recovery returned an empty replacement token")
	}
	if _, err := store.GetCollectorToken(
		ctx,
		expiring.Token.ID,
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"GetCollectorToken(compacted tombstone) error = %v, want ErrNotFound",
			err,
		)
	}
	if got := countCollectorTokenRows(t, db, ""); got != 3 {
		t.Fatalf("token rows after capacity recovery = %d, want 3", got)
	}
	if got := countCollectorTokenRows(
		t,
		db,
		"state = ?",
		CollectorTokenStateRevoked,
	); got != 0 {
		t.Fatalf("revoked token rows after capacity recovery = %d, want 0", got)
	}
	if got := countCollectorTokenScopeRows(t, db, ""); got != 3 {
		t.Fatalf("token scope rows after capacity recovery = %d, want 3", got)
	}
}

func TestConcurrentCollectorTokenCreatesRespectTotalRecordLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	const totalLimit = 4
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{
			RetainedRevokedTokenLimit: 1,
			TotalTokenRecordLimit:     totalLimit,
		},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}

	const attemptCount = 16
	start := make(chan struct{})
	results := make(chan error, attemptCount)
	var workers sync.WaitGroup
	for attempt := range attemptCount {
		workers.Go(func() {
			<-start
			_, createErr := store.CreateCollectorToken(
				ctx,
				CreateCollectorTokenRequest{
					Name:              fmt.Sprintf("concurrent-create-%02d", attempt),
					AllowedIndexNames: []string{"main"},
					BoundCollectorID:  fmt.Sprintf("collector-create-%02d", attempt),
				},
			)
			results <- createErr
		})
	}
	close(start)
	workers.Wait()
	close(results)

	successes := 0
	capacityErrors := 0
	for result := range results {
		switch {
		case result == nil:
			successes++
		case errors.Is(result, control.ErrCapacityExceeded):
			capacityErrors++
		default:
			t.Fatalf("concurrent CreateCollectorToken() error = %v", result)
		}
	}
	if successes != totalLimit || capacityErrors != attemptCount-totalLimit {
		t.Fatalf(
			"concurrent creates = %d successes/%d capacity errors, want %d/%d",
			successes,
			capacityErrors,
			totalLimit,
			attemptCount-totalLimit,
		)
	}
	if got := countCollectorTokenRows(t, db, ""); got != totalLimit {
		t.Fatalf("token rows after concurrent creates = %d, want %d", got, totalLimit)
	}
	if got := countCollectorTokenScopeRows(t, db, ""); got != totalLimit {
		t.Fatalf("scope rows after concurrent creates = %d, want %d", got, totalLimit)
	}
}

func TestCollectorTokenCatalogLimitsAreLocalAcrossStores(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	key := []byte("0123456789abcdef0123456789abcdef")
	largeStore, err := NewStoreWithOptions(db, key, StoreOptions{
		RetainedRevokedTokenLimit: 1,
		TotalTokenRecordLimit:     4,
	})
	if err != nil {
		t.Fatalf("NewStoreWithOptions(large): %v", err)
	}
	for tokenIndex := range 3 {
		if _, err := largeStore.CreateCollectorToken(
			ctx,
			CreateCollectorTokenRequest{
				Name:              fmt.Sprintf("reduction-%d", tokenIndex),
				AllowedIndexNames: []string{"main"},
				BoundCollectorID:  testCollectorID,
			},
		); err != nil {
			t.Fatalf("CreateCollectorToken(%d): %v", tokenIndex, err)
		}
	}

	reducedStore, err := NewStoreWithOptions(db, key, StoreOptions{
		RetainedRevokedTokenLimit: 1,
		TotalTokenRecordLimit:     2,
	})
	if err != nil {
		t.Fatalf("NewStoreWithOptions(reduced): %v", err)
	}
	listed, err := reducedStore.ListCollectorTokens(ctx)
	if err != nil {
		t.Fatalf("ListCollectorTokens(after limit reduction): %v", err)
	}
	if len(listed) != 3 {
		t.Fatalf(
			"ListCollectorTokens(after limit reduction) returned %d rows, want 3",
			len(listed),
		)
	}
	if _, err := reducedStore.CreateCollectorToken(
		ctx,
		CreateCollectorTokenRequest{
			Name:              "reduction-rejected",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		},
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf(
			"CreateCollectorToken(after limit reduction) error = %v, want ErrCapacityExceeded",
			err,
		)
	}
	if got := countCollectorTokenRows(t, db, ""); got != 3 {
		t.Fatalf("token rows after reduced-limit rejection = %d, want 3", got)
	}

	if _, err := largeStore.CreateCollectorToken(
		ctx,
		CreateCollectorTokenRequest{
			Name:              "large-store-fourth",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		},
	); err != nil {
		t.Fatalf("CreateCollectorToken(large store fourth): %v", err)
	}
	listed, err = reducedStore.ListCollectorTokens(ctx)
	if err != nil {
		t.Fatalf("ListCollectorTokens(lower-limit store after large create): %v", err)
	}
	if len(listed) != 4 {
		t.Fatalf(
			"lower-limit store listed %d mixed-store rows, want 4",
			len(listed),
		)
	}
}

func TestCreateCollectorTokenReconcilesReducedRetentionAndCapacityLimits(
	t *testing.T,
) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	key := []byte("0123456789abcdef0123456789abcdef")
	largeStore, err := NewStoreWithOptions(db, key, StoreOptions{
		RetainedRevokedTokenLimit: 3,
		TotalTokenRecordLimit:     4,
	})
	if err != nil {
		t.Fatalf("NewStoreWithOptions(large): %v", err)
	}
	baseTime := time.Date(2026, 7, 29, 15, 30, 0, 0, time.UTC)
	largeStore.now = func() time.Time { return baseTime }
	issued := make([]IssuedCollectorToken, 4)
	for tokenIndex := range issued {
		issued[tokenIndex], err = largeStore.CreateCollectorToken(
			ctx,
			CreateCollectorTokenRequest{
				Name:              fmt.Sprintf("reconcile-%d", tokenIndex),
				AllowedIndexNames: []string{"main"},
				BoundCollectorID:  testCollectorID,
			},
		)
		if err != nil {
			t.Fatalf("CreateCollectorToken(%d): %v", tokenIndex, err)
		}
	}
	for tokenIndex := range 3 {
		largeStore.now = func() time.Time {
			return baseTime.Add(time.Duration(tokenIndex+1) * time.Microsecond)
		}
		if _, err := largeStore.RevokeCollectorToken(
			ctx,
			issued[tokenIndex].Token.ID,
			issued[tokenIndex].Token.Version,
		); err != nil {
			t.Fatalf("RevokeCollectorToken(%d): %v", tokenIndex, err)
		}
	}

	reducedStore, err := NewStoreWithOptions(db, key, StoreOptions{
		RetainedRevokedTokenLimit: 1,
		TotalTokenRecordLimit:     3,
	})
	if err != nil {
		t.Fatalf("NewStoreWithOptions(reduced): %v", err)
	}
	replacement, err := reducedStore.CreateCollectorToken(
		ctx,
		CreateCollectorTokenRequest{
			Name:              "reconciled replacement",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		},
	)
	if err != nil {
		t.Fatalf("CreateCollectorToken(reduced limits): %v", err)
	}

	for tokenIndex := range 2 {
		if _, err := reducedStore.GetCollectorToken(
			ctx,
			issued[tokenIndex].Token.ID,
		); !errors.Is(err, control.ErrNotFound) {
			t.Fatalf(
				"GetCollectorToken(pruned %d) error = %v, want ErrNotFound",
				tokenIndex,
				err,
			)
		}
	}
	if retained, err := reducedStore.GetCollectorToken(
		ctx,
		issued[2].Token.ID,
	); err != nil {
		t.Fatalf("GetCollectorToken(newest retained): %v", err)
	} else if retained.State != CollectorTokenStateRevoked {
		t.Fatalf(
			"newest retained token state = %q, want revoked",
			retained.State,
		)
	}
	for _, tokenID := range []string{
		issued[3].Token.ID,
		replacement.Token.ID,
	} {
		if _, err := reducedStore.GetCollectorToken(ctx, tokenID); err != nil {
			t.Fatalf("GetCollectorToken(active %q): %v", tokenID, err)
		}
	}
	if got := countCollectorTokenRows(t, db, ""); got != 3 {
		t.Fatalf("token rows after reduced-limit reconciliation = %d, want 3", got)
	}
	if got := countCollectorTokenRows(
		t,
		db,
		"state = ?",
		CollectorTokenStateRevoked,
	); got != 1 {
		t.Fatalf("revoked rows after reduced-limit reconciliation = %d, want 1", got)
	}
	if got := countCollectorTokenScopeRows(t, db, ""); got != 3 {
		t.Fatalf("scope rows after reduced-limit reconciliation = %d, want 3", got)
	}
}

func TestListCollectorTokensDetectsPhysicalStructuralOverflow(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{
			RetainedRevokedTokenLimit: maximumTotalTokenRecordLimit - 1,
			TotalTokenRecordLimit:     maximumTotalTokenRecordLimit,
		},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}

	boundCollectorID := testCollectorID
	now := time.Date(2026, 7, 29, 16, 0, 0, 0, time.UTC).UnixMicro()
	corrupt := make(
		[]collectorTokenRecord,
		maximumTotalTokenRecordLimit+1,
	)
	for tokenIndex := range corrupt {
		tokenID := fmt.Sprintf("tok_physical_overflow_%04d", tokenIndex)
		digest := sha256.Sum256([]byte(tokenID))
		corrupt[tokenIndex] = collectorTokenRecord{
			IngestionTokenID:   tokenID,
			Version:            1,
			Name:               fmt.Sprintf("physical overflow %04d", tokenIndex),
			Description:        "deliberately missing its scope row",
			TokenPrefix:        "ost_v1_overflow",
			TokenDigest:        digest[:],
			State:              CollectorTokenStateActive,
			CreatedAtUnixMicro: now,
			UpdatedAtUnixMicro: now,
			BoundCollectorID:   &boundCollectorID,
		}
	}
	if err := db.GORMDB().
		WithContext(ctx).
		CreateInBatches(&corrupt, 128).
		Error; err != nil {
		t.Fatalf("seed physical structural overflow: %v", err)
	}
	if _, err := store.ListCollectorTokens(
		ctx,
	); !errors.Is(err, errCollectorTokenCatalogOverflow) {
		t.Fatalf(
			"ListCollectorTokens(structural overflow) error = %v, want catalog overflow",
			err,
		)
	}
}

func TestListCollectorTokensRejectsIncompletePhysicalHydration(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{
			RetainedRevokedTokenLimit: 1,
			TotalTokenRecordLimit:     2,
		},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}
	if _, err := store.CreateCollectorToken(
		ctx,
		CreateCollectorTokenRequest{
			Name:              "healthy scoped token",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		},
	); err != nil {
		t.Fatalf("CreateCollectorToken(healthy): %v", err)
	}

	const orphanTokenID = "tok_orphaned_scope"
	digest := sha256.Sum256([]byte(orphanTokenID))
	boundCollectorID := testCollectorID
	now := time.Date(2026, 7, 29, 16, 30, 0, 0, time.UTC).UnixMicro()
	orphan := collectorTokenRecord{
		IngestionTokenID:   orphanTokenID,
		Version:            1,
		Name:               "orphaned scope",
		Description:        "physical token without its required scope row",
		TokenPrefix:        "ost_v1_orphan",
		TokenDigest:        digest[:],
		State:              CollectorTokenStateActive,
		CreatedAtUnixMicro: now,
		UpdatedAtUnixMicro: now,
		BoundCollectorID:   &boundCollectorID,
	}
	if err := db.GORMDB().WithContext(ctx).Create(&orphan).Error; err != nil {
		t.Fatalf("seed token without scope: %v", err)
	}

	if _, err := store.ListCollectorTokens(
		ctx,
	); !errors.Is(err, errCollectorTokenCatalogInconsistent) {
		t.Fatalf(
			"ListCollectorTokens(incomplete hydration) error = %v, want catalog inconsistency",
			err,
		)
	}
}

func TestCollectorTokenMetadataScopeFanoutBounds(t *testing.T) {
	t.Run("maximum valid", func(t *testing.T) {
		ctx := context.Background()
		db := openControlDB(t)
		indexNames := seedCollectorTokenScopeIndexes(t, db, maximumTokenScopes)
		tokenIDs := seedCollectorTokenScopeCatalog(
			t,
			db,
			indexNames,
			[]int{maximumTokenScopes},
			nil,
		)
		store, err := NewStore(
			db,
			[]byte("0123456789abcdef0123456789abcdef"),
		)
		if err != nil {
			t.Fatalf("NewStore(): %v", err)
		}

		got, err := store.GetCollectorToken(ctx, tokenIDs[0])
		if err != nil {
			t.Fatalf("GetCollectorToken(maximum scopes): %v", err)
		}
		if !slices.Equal(got.AllowedIndexNames, indexNames) {
			t.Fatalf(
				"maximum valid scopes = %d, want %d",
				len(got.AllowedIndexNames),
				len(indexNames),
			)
		}
		listed, err := store.ListCollectorTokens(ctx)
		if err != nil {
			t.Fatalf("ListCollectorTokens(maximum scopes): %v", err)
		}
		if len(listed) != 1 ||
			!slices.Equal(listed[0].AllowedIndexNames, indexNames) {
			t.Fatalf("listed maximum-scope token = %#v", listed)
		}
	})

	t.Run("overscoped corruption", func(t *testing.T) {
		ctx := context.Background()
		db := openControlDB(t)
		indexNames := seedCollectorTokenScopeIndexes(
			t,
			db,
			maximumTokenScopes+1,
		)
		tokenIDs := seedCollectorTokenScopeCatalog(
			t,
			db,
			indexNames,
			[]int{maximumTokenScopes},
			nil,
		)
		if err := db.GORMDB().WithContext(ctx).Create(
			&collectorTokenIndexRecord{
				IngestionTokenID: tokenIDs[0],
				IndexID:          collectorTokenScopeTestIndexID(maximumTokenScopes),
			},
		).Error; err != nil {
			t.Fatalf("seed overscoped token membership: %v", err)
		}
		store, err := NewStore(
			db,
			[]byte("0123456789abcdef0123456789abcdef"),
		)
		if err != nil {
			t.Fatalf("NewStore(): %v", err)
		}

		for operation, run := range map[string]func() error{
			"get": func() error {
				_, operationErr := store.GetCollectorToken(ctx, tokenIDs[0])
				return operationErr
			},
			"list": func() error {
				_, operationErr := store.ListCollectorTokens(ctx)
				return operationErr
			},
		} {
			t.Run(operation, func(t *testing.T) {
				if err := run(); !errors.Is(
					err,
					errCollectorTokenCatalogInconsistent,
				) {
					t.Fatalf(
						"%s overscoped metadata error = %v, want catalog inconsistency",
						operation,
						err,
					)
				}
			})
		}
	})
}

func TestListCollectorTokensAtExactCatalogBounds(t *testing.T) {
	ctx := context.Background()
	db := openControlDB(t)
	indexNames := seedCollectorTokenScopeIndexes(t, db, maximumTokenScopes)
	scopeCounts := make([]int, 0, maximumTotalTokenRecordLimit)
	for range 60 {
		scopeCounts = append(scopeCounts, maximumTokenScopes)
	}
	scopeCounts = append(scopeCounts, 61)
	for len(scopeCounts) < maximumTotalTokenRecordLimit {
		scopeCounts = append(scopeCounts, 1)
	}
	tokenIDs := seedCollectorTokenScopeCatalog(
		t,
		db,
		indexNames,
		scopeCounts,
		nil,
	)
	if got := countCollectorTokenRows(t, db, ""); got !=
		maximumTotalTokenRecordLimit {
		t.Fatalf(
			"seeded token records = %d, want %d",
			got,
			maximumTotalTokenRecordLimit,
		)
	}
	if got := countCollectorTokenScopeRows(t, db, ""); got !=
		maximumTotalTokenScopeRecordLimit {
		t.Fatalf(
			"seeded scope records = %d, want %d",
			got,
			maximumTotalTokenScopeRecordLimit,
		)
	}
	store, err := NewStore(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}

	tokens, err := store.ListCollectorTokens(ctx)
	if err != nil {
		t.Fatalf("ListCollectorTokens(exact bounds): %v", err)
	}
	if len(tokens) != maximumTotalTokenRecordLimit {
		t.Fatalf(
			"listed token records = %d, want %d",
			len(tokens),
			maximumTotalTokenRecordLimit,
		)
	}
	for _, check := range []struct {
		index      int
		scopeCount int
	}{
		{index: 0, scopeCount: maximumTokenScopes},
		{index: 60, scopeCount: 61},
		{index: len(tokens) - 1, scopeCount: 1},
	} {
		if tokens[check.index].ID != tokenIDs[check.index] ||
			len(tokens[check.index].AllowedIndexNames) != check.scopeCount {
			t.Fatalf(
				"listed token %d = %#v, want ID %q with %d scopes",
				check.index,
				tokens[check.index],
				tokenIDs[check.index],
				check.scopeCount,
			)
		}
	}
}

func TestCollectorTokenMetadataRejectsOrphanScopeRows(t *testing.T) {
	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStore(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	if _, err := store.CreateCollectorToken(
		ctx,
		CreateCollectorTokenRequest{
			Name:              "healthy token",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		},
	); err != nil {
		t.Fatalf("CreateCollectorToken(): %v", err)
	}
	var indexID string
	if err := db.GORMDB().
		WithContext(ctx).
		Table("indexes").
		Select("index_id").
		Where("name = ?", "main").
		Scan(&indexID).
		Error; err != nil {
		t.Fatalf("read main index ID: %v", err)
	}

	connection, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire corruption connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA foreign_keys = OFF`,
	); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		`INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
		 VALUES ('tok_missing_parent', ?)`,
		indexID,
	); err != nil {
		t.Fatalf("insert orphan token scope: %v", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA foreign_keys = ON`,
	); err != nil {
		t.Fatalf("restore foreign keys: %v", err)
	}

	if _, err := store.ListCollectorTokens(ctx); !errors.Is(
		err,
		errCollectorTokenCatalogInconsistent,
	) {
		t.Fatalf(
			"ListCollectorTokens(orphan scope) error = %v, want catalog inconsistency",
			err,
		)
	}
}

func TestCollectorTokenMetadataWidthPreflight(t *testing.T) {
	t.Run("token ID boundary", func(t *testing.T) {
		ctx := context.Background()
		db := openControlDB(t)
		indexNames := seedCollectorTokenScopeIndexes(t, db, 1)
		tokenID := strings.Repeat("i", maximumTokenIDBytes)
		seedCollectorTokenRecordWithID(
			t,
			db,
			tokenID,
			collectorTokenScopeTestIndexID(0),
		)
		store, err := NewStore(
			db,
			[]byte("0123456789abcdef0123456789abcdef"),
		)
		if err != nil {
			t.Fatalf("NewStore(): %v", err)
		}
		got, err := store.GetCollectorToken(ctx, tokenID)
		if err != nil {
			t.Fatalf("GetCollectorToken(boundary ID): %v", err)
		}
		if got.ID != tokenID ||
			!slices.Equal(got.AllowedIndexNames, indexNames) {
			t.Fatalf("boundary token = %#v", got)
		}
	})

	for _, test := range []struct {
		name   string
		mutate func(*testing.T, *control.DB, string)
	}{
		{
			name: "oversized ID",
			mutate: func(
				t *testing.T,
				db *control.DB,
				_ string,
			) {
				t.Helper()
				seedCollectorTokenRecordWithID(
					t,
					db,
					strings.Repeat("x", maximumTokenIDBytes+1),
					collectorTokenScopeTestIndexID(0),
				)
			},
		},
		{
			name: "oversized description",
			mutate: func(
				t *testing.T,
				db *control.DB,
				tokenID string,
			) {
				t.Helper()
				hostile := strings.Repeat("d", maximumDescriptionBytes+1)
				if err := db.GORMDB().
					Model(&collectorTokenRecord{}).
					Where("ingestion_token_id = ?", tokenID).
					Update("description", hostile).
					Error; err != nil {
					t.Fatalf("install oversized description: %v", err)
				}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			ctx := context.Background()
			db := openControlDB(t)
			seedCollectorTokenScopeIndexes(t, db, 1)
			tokenIDs := seedCollectorTokenScopeCatalog(
				t,
				db,
				[]string{collectorTokenScopeTestIndexName(0)},
				[]int{1},
				nil,
			)
			test.mutate(t, db, tokenIDs[0])
			store, err := NewStore(
				db,
				[]byte("0123456789abcdef0123456789abcdef"),
			)
			if err != nil {
				t.Fatalf("NewStore(): %v", err)
			}
			if _, err := store.ListCollectorTokens(ctx); !errors.Is(
				err,
				errCollectorTokenCatalogInconsistent,
			) {
				t.Fatalf(
					"ListCollectorTokens(%s) error = %v, want catalog inconsistency",
					test.name,
					err,
				)
			}
		})
	}
}

func TestCreateCollectorTokenRecoversGlobalScopeCapacityFromRevokedTombstone(
	t *testing.T,
) {
	ctx := context.Background()
	db := openControlDB(t)
	indexNames := seedCollectorTokenScopeIndexes(t, db, maximumTokenScopes)
	scopeCounts := make([]int, maximumTotalTokenScopeRecordLimit/maximumTokenScopes)
	for index := range scopeCounts {
		scopeCounts[index] = maximumTokenScopes
	}
	tokenIDs := seedCollectorTokenScopeCatalog(
		t,
		db,
		indexNames,
		scopeCounts,
		map[int]bool{0: true},
	)
	if got := countCollectorTokenScopeRows(t, db, ""); got !=
		maximumTotalTokenScopeRecordLimit {
		t.Fatalf(
			"seeded scope records = %d, want %d",
			got,
			maximumTotalTokenScopeRecordLimit,
		)
	}
	store, err := NewStore(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}

	replacement, err := store.CreateCollectorToken(
		ctx,
		CreateCollectorTokenRequest{
			Name:              "scope capacity replacement",
			AllowedIndexNames: []string{indexNames[0]},
			BoundCollectorID:  testCollectorID,
		},
	)
	if err != nil {
		t.Fatalf("CreateCollectorToken(scope recovery): %v", err)
	}
	if replacement.Token.ID == "" {
		t.Fatal("scope-capacity recovery returned no token")
	}
	if _, err := store.GetCollectorToken(
		ctx,
		tokenIDs[0],
	); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf(
			"GetCollectorToken(reclaimed tombstone) error = %v, want ErrNotFound",
			err,
		)
	}
	wantScopes := maximumTotalTokenScopeRecordLimit -
		maximumTokenScopes +
		1
	if got := countCollectorTokenScopeRows(t, db, ""); got != int64(wantScopes) {
		t.Fatalf(
			"scope rows after capacity recovery = %d, want %d",
			got,
			wantScopes,
		)
	}
}

func TestUpdateCollectorTokenScopeCapacityRollsBack(t *testing.T) {
	ctx := context.Background()
	db := openControlDB(t)
	indexNames := seedCollectorTokenScopeIndexes(t, db, maximumTokenScopes)
	scopeCounts := make([]int, 0, 65)
	for range 63 {
		scopeCounts = append(scopeCounts, maximumTokenScopes)
	}
	scopeCounts = append(scopeCounts, maximumTokenScopes-1, 1)
	tokenIDs := seedCollectorTokenScopeCatalog(
		t,
		db,
		indexNames,
		scopeCounts,
		nil,
	)
	if got := countCollectorTokenScopeRows(t, db, ""); got !=
		maximumTotalTokenScopeRecordLimit {
		t.Fatalf(
			"seeded scope records = %d, want %d",
			got,
			maximumTotalTokenScopeRecordLimit,
		)
	}
	store, err := NewStore(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
	)
	if err != nil {
		t.Fatalf("NewStore(): %v", err)
	}
	currentID := tokenIDs[len(tokenIDs)-1]
	before, err := store.GetCollectorToken(ctx, currentID)
	if err != nil {
		t.Fatalf("GetCollectorToken(before update): %v", err)
	}

	_, err = store.UpdateCollectorToken(
		ctx,
		currentID,
		before.Version,
		UpdateCollectorTokenRequest{
			Name:              "must roll back",
			AllowedIndexNames: indexNames[:2],
		},
	)
	if !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf(
			"UpdateCollectorToken(scope capacity) error = %v, want ErrCapacityExceeded",
			err,
		)
	}
	after, err := store.GetCollectorToken(ctx, currentID)
	if err != nil {
		t.Fatalf("GetCollectorToken(after rejected update): %v", err)
	}
	if after.Version != before.Version ||
		after.Name != before.Name ||
		!slices.Equal(after.AllowedIndexNames, before.AllowedIndexNames) {
		t.Fatalf("scope-capacity rejection mutated token: %#v", after)
	}
	if got := countCollectorTokenScopeRows(t, db, ""); got !=
		maximumTotalTokenScopeRecordLimit {
		t.Fatalf(
			"scope rows after rejected update = %d, want %d",
			got,
			maximumTotalTokenScopeRecordLimit,
		)
	}
}

func TestCreateCollectorTokenInsertFailureRollsBackCapacityPrune(t *testing.T) {
	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{
			RetainedRevokedTokenLimit: 1,
			TotalTokenRecordLimit:     2,
		},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}
	create := func(name string) IssuedCollectorToken {
		t.Helper()
		issued, createErr := store.CreateCollectorToken(
			ctx,
			CreateCollectorTokenRequest{
				Name:              name,
				AllowedIndexNames: []string{"main"},
				BoundCollectorID:  testCollectorID,
			},
		)
		if createErr != nil {
			t.Fatalf("CreateCollectorToken(%s): %v", name, createErr)
		}
		return issued
	}
	revoked := create("rollback revoked")
	active := create("rollback active")
	if _, err := store.RevokeCollectorToken(
		ctx,
		revoked.Token.ID,
		revoked.Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER reject_token_insert_after_capacity_prune
		BEFORE INSERT ON ingestion_tokens
		BEGIN
			SELECT RAISE(ABORT, 'forced collector token insert failure');
		END`); err != nil {
		t.Fatalf("create insert-failure trigger: %v", err)
	}

	if _, err := store.CreateCollectorToken(
		ctx,
		CreateCollectorTokenRequest{
			Name:              "rejected replacement",
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		},
	); err == nil {
		t.Fatal("CreateCollectorToken(insert failure) unexpectedly succeeded")
	}
	revokedAfter, err := store.GetCollectorToken(ctx, revoked.Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(restored tombstone): %v", err)
	}
	if revokedAfter.State != CollectorTokenStateRevoked {
		t.Fatalf("restored tombstone state = %q", revokedAfter.State)
	}
	if _, err := store.GetCollectorToken(ctx, active.Token.ID); err != nil {
		t.Fatalf("GetCollectorToken(active): %v", err)
	}
	if got := countCollectorTokenRows(t, db, ""); got != 2 {
		t.Fatalf("token rows after insert rollback = %d, want 2", got)
	}
	if got := countCollectorTokenScopeRows(t, db, ""); got != 2 {
		t.Fatalf("scope rows after insert rollback = %d, want 2", got)
	}
}

func TestRevokeCollectorTokenStructuralOverflowDoesNotMutate(t *testing.T) {
	t.Run("parent rows", func(t *testing.T) {
		ctx := context.Background()
		db := openControlDB(t)
		if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
			t.Fatalf("CreateIndex(main): %v", err)
		}
		store, err := NewStore(
			db,
			[]byte("0123456789abcdef0123456789abcdef"),
		)
		if err != nil {
			t.Fatalf("NewStore(): %v", err)
		}
		target, err := store.CreateCollectorToken(
			ctx,
			CreateCollectorTokenRequest{
				Name:              "overflow revoke target",
				AllowedIndexNames: []string{"main"},
				BoundCollectorID:  testCollectorID,
			},
		)
		if err != nil {
			t.Fatalf("CreateCollectorToken(): %v", err)
		}
		seedCollectorTokenParentsWithoutScopes(
			t,
			db,
			maximumTotalTokenRecordLimit,
		)

		_, err = store.RevokeCollectorToken(
			ctx,
			target.Token.ID,
			target.Token.Version,
		)
		if !errors.Is(err, errCollectorTokenCatalogOverflow) {
			t.Fatalf(
				"RevokeCollectorToken(parent overflow) error = %v, want catalog overflow",
				err,
			)
		}
		assertCollectorTokenRemainsActive(t, db, target.Token)
	})

	t.Run("scope rows", func(t *testing.T) {
		ctx := context.Background()
		db := openControlDB(t)
		if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
			t.Fatalf("CreateIndex(main): %v", err)
		}
		store, err := NewStore(
			db,
			[]byte("0123456789abcdef0123456789abcdef"),
		)
		if err != nil {
			t.Fatalf("NewStore(): %v", err)
		}
		target, err := store.CreateCollectorToken(
			ctx,
			CreateCollectorTokenRequest{
				Name:              "scope overflow revoke target",
				AllowedIndexNames: []string{"main"},
				BoundCollectorID:  testCollectorID,
			},
		)
		if err != nil {
			t.Fatalf("CreateCollectorToken(): %v", err)
		}
		seedOrphanCollectorTokenScopes(
			t,
			db,
			maximumTotalTokenScopeRecordLimit,
		)

		_, err = store.RevokeCollectorToken(
			ctx,
			target.Token.ID,
			target.Token.Version,
		)
		if !errors.Is(err, errCollectorTokenCatalogOverflow) {
			t.Fatalf(
				"RevokeCollectorToken(scope overflow) error = %v, want catalog overflow",
				err,
			)
		}
		assertCollectorTokenRemainsActive(t, db, target.Token)
	})
}

func TestRevokeCollectorTokenPrunesOldestTombstonesAndScopes(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{RetainedRevokedTokenLimit: 2},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}
	createTime := time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC)
	store.now = func() time.Time { return createTime }

	issued := make([]IssuedCollectorToken, 0, 5)
	for _, name := range []string{"oldest", "middle", "newest", "active", "disabled"} {
		token, err := store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              name,
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		})
		if err != nil {
			t.Fatalf("CreateCollectorToken(%q): %v", name, err)
		}
		issued = append(issued, token)
	}
	if _, err := db.SQLDB().ExecContext(
		ctx,
		`UPDATE ingestion_tokens SET state = ? WHERE ingestion_token_id = ?`,
		CollectorTokenStateDisabled,
		issued[4].Token.ID,
	); err != nil {
		t.Fatalf("disable collector token: %v", err)
	}

	revokeBase := createTime.Add(time.Hour)
	for tokenIndex := range 3 {
		store.now = func() time.Time {
			return revokeBase.Add(time.Duration(tokenIndex) * time.Microsecond)
		}
		if _, err := store.RevokeCollectorToken(
			ctx,
			issued[tokenIndex].Token.ID,
			issued[tokenIndex].Token.Version,
		); err != nil {
			t.Fatalf("RevokeCollectorToken(%q): %v", issued[tokenIndex].Token.Name, err)
		}
	}

	if _, err := store.GetCollectorToken(ctx, issued[0].Token.ID); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("GetCollectorToken(oldest) error = %v, want ErrNotFound", err)
	}
	for _, tokenIndex := range []int{1, 2} {
		got, err := store.GetCollectorToken(ctx, issued[tokenIndex].Token.ID)
		if err != nil {
			t.Fatalf("GetCollectorToken(%q): %v", issued[tokenIndex].Token.Name, err)
		}
		if got.State != CollectorTokenStateRevoked {
			t.Fatalf("token %q state = %q, want revoked", got.Name, got.State)
		}
	}
	for tokenIndex, wantState := range map[int]CollectorTokenState{
		3: CollectorTokenStateActive,
		4: CollectorTokenStateDisabled,
	} {
		got, err := store.GetCollectorToken(ctx, issued[tokenIndex].Token.ID)
		if err != nil {
			t.Fatalf("GetCollectorToken(%q): %v", issued[tokenIndex].Token.Name, err)
		}
		if got.State != wantState {
			t.Fatalf("token %q state = %q, want %q", got.Name, got.State, wantState)
		}
	}

	if got := countCollectorTokenRows(t, db, "state = ?", CollectorTokenStateRevoked); got != 2 {
		t.Fatalf("retained revoked token rows = %d, want 2", got)
	}
	if got := countCollectorTokenScopeRows(t, db, issued[0].Token.ID); got != 0 {
		t.Fatalf("pruned token scope rows = %d, want 0", got)
	}
	for _, tokenIndex := range []int{1, 2, 3, 4} {
		if got := countCollectorTokenScopeRows(t, db, issued[tokenIndex].Token.ID); got != 1 {
			t.Fatalf("token %q scope rows = %d, want 1", issued[tokenIndex].Token.Name, got)
		}
	}
}

func TestRevokeCollectorTokenRetainsCurrentTombstoneOnOrderingTie(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{RetainedRevokedTokenLimit: 2},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}
	store.now = func() time.Time {
		return time.Date(2026, 7, 29, 13, 0, 0, 0, time.UTC)
	}

	issued := make([]IssuedCollectorToken, 3)
	for tokenIndex := range issued {
		issued[tokenIndex], err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              fmt.Sprintf("tie-%d", tokenIndex),
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		})
		if err != nil {
			t.Fatalf("CreateCollectorToken(%d): %v", tokenIndex, err)
		}
	}
	slices.SortFunc(issued, func(left, right IssuedCollectorToken) int {
		return cmp.Compare(left.Token.ID, right.Token.ID)
	})
	priorToPrune := issued[1]
	priorToRetain := issued[2]
	current := issued[0]
	if _, err := store.RevokeCollectorToken(
		ctx,
		priorToPrune.Token.ID,
		priorToPrune.Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(prior to prune): %v", err)
	}
	if _, err := store.RevokeCollectorToken(
		ctx,
		priorToRetain.Token.ID,
		priorToRetain.Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(prior to retain): %v", err)
	}
	revoked, err := store.RevokeCollectorToken(
		ctx,
		current.Token.ID,
		current.Token.Version,
	)
	if err != nil {
		t.Fatalf("RevokeCollectorToken(current): %v", err)
	}
	if revoked.ID != current.Token.ID || revoked.State != CollectorTokenStateRevoked {
		t.Fatalf("current revoke result = %#v", revoked)
	}
	if _, err := store.GetCollectorToken(ctx, current.Token.ID); err != nil {
		t.Fatalf("GetCollectorToken(current): %v", err)
	}
	if _, err := store.GetCollectorToken(ctx, priorToRetain.Token.ID); err != nil {
		t.Fatalf("GetCollectorToken(prior to retain): %v", err)
	}
	if _, err := store.GetCollectorToken(ctx, priorToPrune.Token.ID); !errors.Is(err, control.ErrNotFound) {
		t.Fatalf("GetCollectorToken(prior to prune) error = %v, want ErrNotFound", err)
	}
}

func TestRevokeCollectorTokenPruneFailureRollsBack(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{RetainedRevokedTokenLimit: 1},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}

	issued := make([]IssuedCollectorToken, 2)
	for tokenIndex := range issued {
		issued[tokenIndex], err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              fmt.Sprintf("rollback-%d", tokenIndex),
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		})
		if err != nil {
			t.Fatalf("CreateCollectorToken(%d): %v", tokenIndex, err)
		}
	}
	if _, err := store.RevokeCollectorToken(
		ctx,
		issued[0].Token.ID,
		issued[0].Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(first): %v", err)
	}
	if _, err := db.SQLDB().ExecContext(ctx, `
		CREATE TRIGGER reject_revoked_token_prune
		BEFORE DELETE ON ingestion_tokens
		WHEN OLD.state = 'revoked'
		BEGIN
			SELECT RAISE(ABORT, 'forced token-prune failure');
		END`); err != nil {
		t.Fatalf("create prune failure trigger: %v", err)
	}

	if _, err := store.RevokeCollectorToken(
		ctx,
		issued[1].Token.ID,
		issued[1].Token.Version,
	); err == nil {
		t.Fatal("RevokeCollectorToken(second) unexpectedly succeeded")
	}
	current, err := store.GetCollectorToken(ctx, issued[1].Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(second): %v", err)
	}
	if current.State != CollectorTokenStateActive ||
		current.Version != issued[1].Token.Version ||
		!current.RevokedAt.IsZero() {
		t.Fatalf("failed prune did not roll back revocation: %#v", current)
	}
	first, err := store.GetCollectorToken(ctx, issued[0].Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(first): %v", err)
	}
	if first.State != CollectorTokenStateRevoked {
		t.Fatalf("first token state = %q, want revoked", first.State)
	}
}

func TestConcurrentCollectorTokenRevocationsPreserveRetentionLimit(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	const retainedLimit = 3
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{RetainedRevokedTokenLimit: retainedLimit},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}
	store.now = func() time.Time {
		return time.Date(2026, 7, 29, 14, 0, 0, 0, time.UTC)
	}

	const tokenCount = 12
	issued := make([]IssuedCollectorToken, tokenCount)
	for tokenIndex := range issued {
		issued[tokenIndex], err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              fmt.Sprintf("concurrent-%02d", tokenIndex),
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		})
		if err != nil {
			t.Fatalf("CreateCollectorToken(%d): %v", tokenIndex, err)
		}
	}

	start := make(chan struct{})
	errs := make(chan error, tokenCount)
	var workers sync.WaitGroup
	for tokenIndex := range issued {
		workers.Go(func() {
			<-start
			_, revokeErr := store.RevokeCollectorToken(
				ctx,
				issued[tokenIndex].Token.ID,
				issued[tokenIndex].Token.Version,
			)
			errs <- revokeErr
		})
	}
	close(start)
	workers.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("RevokeCollectorToken() error = %v", err)
		}
	}

	if got := countCollectorTokenRows(t, db, "state = ?", CollectorTokenStateRevoked); got != retainedLimit {
		t.Fatalf("retained revoked token rows = %d, want %d", got, retainedLimit)
	}
	if got := countCollectorTokenRows(t, db, "state != ?", CollectorTokenStateRevoked); got != 0 {
		t.Fatalf("non-revoked token rows = %d, want 0", got)
	}
	if got := countCollectorTokenScopeRows(t, db, ""); got != retainedLimit {
		t.Fatalf("retained token scope rows = %d, want %d", got, retainedLimit)
	}
}

func TestRevokeCollectorTokenRetentionHonorsCanceledContext(t *testing.T) {
	t.Parallel()

	ctx := context.Background()
	db := openControlDB(t)
	if _, err := db.CreateIndex(ctx, activeIndex("main")); err != nil {
		t.Fatalf("CreateIndex(main): %v", err)
	}
	store, err := NewStoreWithOptions(
		db,
		[]byte("0123456789abcdef0123456789abcdef"),
		StoreOptions{RetainedRevokedTokenLimit: 1},
	)
	if err != nil {
		t.Fatalf("NewStoreWithOptions(): %v", err)
	}

	issued := make([]IssuedCollectorToken, 2)
	for tokenIndex := range issued {
		issued[tokenIndex], err = store.CreateCollectorToken(ctx, CreateCollectorTokenRequest{
			Name:              fmt.Sprintf("canceled-%d", tokenIndex),
			AllowedIndexNames: []string{"main"},
			BoundCollectorID:  testCollectorID,
		})
		if err != nil {
			t.Fatalf("CreateCollectorToken(%d): %v", tokenIndex, err)
		}
	}
	if _, err := store.RevokeCollectorToken(
		ctx,
		issued[0].Token.ID,
		issued[0].Token.Version,
	); err != nil {
		t.Fatalf("RevokeCollectorToken(first): %v", err)
	}

	canceled, cancel := context.WithCancel(ctx)
	cancel()
	if _, err := store.RevokeCollectorToken(
		canceled,
		issued[1].Token.ID,
		issued[1].Token.Version,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("RevokeCollectorToken(canceled) error = %v, want context.Canceled", err)
	}
	current, err := store.GetCollectorToken(ctx, issued[1].Token.ID)
	if err != nil {
		t.Fatalf("GetCollectorToken(second): %v", err)
	}
	if current.State != CollectorTokenStateActive ||
		current.Version != issued[1].Token.Version ||
		!current.RevokedAt.IsZero() {
		t.Fatalf("canceled revoke mutated token: %#v", current)
	}
	if got := countCollectorTokenRows(t, db, "state = ?", CollectorTokenStateRevoked); got != 1 {
		t.Fatalf("retained revoked token rows = %d, want 1", got)
	}
}

func seedCollectorTokenScopeIndexes(
	t *testing.T,
	db *control.DB,
	count int,
) []string {
	t.Helper()
	if count < 1 {
		t.Fatal("collector token scope index count must be positive")
	}
	now := time.Date(2026, 7, 29, 10, 0, 0, 0, time.UTC).UnixMicro()
	if _, err := db.SQLDB().ExecContext(context.Background(), `
		WITH RECURSIVE sequence(value) AS (
			SELECT 0
			UNION ALL
			SELECT value + 1
			FROM sequence
			WHERE value + 1 < ?
		)
		INSERT INTO indexes (
			index_id, version, name, display_name,
			ingestion_enabled, search_enabled, state,
			created_at_unix_micro, updated_at_unix_micro
		)
		SELECT
			printf('idx_token_scope_%03d', value),
			1,
			printf('token-scope-%03d', value),
			printf('Token Scope %03d', value),
			1,
			1,
			'active',
			?,
			?
		FROM sequence`,
		count,
		now,
		now,
	); err != nil {
		t.Fatalf("seed collector token scope indexes: %v", err)
	}
	names := make([]string, count)
	for index := range names {
		names[index] = collectorTokenScopeTestIndexName(index)
	}
	return names
}

func collectorTokenScopeTestIndexID(index int) string {
	return fmt.Sprintf("idx_token_scope_%03d", index)
}

func collectorTokenScopeTestIndexName(index int) string {
	return fmt.Sprintf("token-scope-%03d", index)
}

func seedCollectorTokenScopeCatalog(
	t *testing.T,
	db *control.DB,
	indexNames []string,
	scopeCounts []int,
	revoked map[int]bool,
) []string {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 10, 30, 0, 0, time.UTC)
	boundCollectorID := testCollectorID
	records := make([]collectorTokenRecord, len(scopeCounts))
	tokenIDs := make([]string, len(scopeCounts))
	memberships := make(
		[]collectorTokenIndexRecord,
		0,
		len(scopeCounts)*maximumTokenScopes,
	)
	for tokenIndex, scopeCount := range scopeCounts {
		if scopeCount < 1 ||
			scopeCount > maximumTokenScopes ||
			scopeCount > len(indexNames) {
			t.Fatalf(
				"invalid seeded scope count %d for %d indexes",
				scopeCount,
				len(indexNames),
			)
		}
		tokenID := fmt.Sprintf("tok_scope_catalog_%04d", tokenIndex)
		tokenIDs[tokenIndex] = tokenID
		digest := sha256.Sum256([]byte(tokenID))
		state := CollectorTokenStateActive
		updatedAt := now.UnixMicro()
		var revokedAt *int64
		if revoked[tokenIndex] {
			state = CollectorTokenStateRevoked
			value := now.Add(time.Duration(tokenIndex+1) * time.Microsecond).
				UnixMicro()
			updatedAt = value
			revokedAt = &value
		}
		records[tokenIndex] = collectorTokenRecord{
			IngestionTokenID:   tokenID,
			Version:            1,
			Name:               fmt.Sprintf("scope catalog %04d", tokenIndex),
			Description:        "bounded scope-catalog fixture",
			TokenPrefix:        fmt.Sprintf("ost_v1_seed_%04d", tokenIndex),
			TokenDigest:        append([]byte(nil), digest[:]...),
			State:              state,
			CreatedAtUnixMicro: now.UnixMicro(),
			UpdatedAtUnixMicro: updatedAt,
			RevokedAtUnixMicro: revokedAt,
			BoundCollectorID:   &boundCollectorID,
		}
		for scopeIndex := range scopeCount {
			memberships = append(
				memberships,
				collectorTokenIndexRecord{
					IngestionTokenID: tokenID,
					IndexID: collectorTokenScopeTestIndexID(
						scopeIndex,
					),
				},
			)
		}
	}
	if err := db.GORMDB().
		WithContext(ctx).
		CreateInBatches(&records, 128).
		Error; err != nil {
		t.Fatalf("seed collector token parents: %v", err)
	}
	if err := db.GORMDB().
		WithContext(ctx).
		CreateInBatches(&memberships, 256).
		Error; err != nil {
		t.Fatalf("seed collector token scopes: %v", err)
	}
	return tokenIDs
}

func seedCollectorTokenRecordWithID(
	t *testing.T,
	db *control.DB,
	tokenID string,
	indexID string,
) {
	t.Helper()
	ctx := context.Background()
	now := time.Date(2026, 7, 29, 11, 0, 0, 0, time.UTC).UnixMicro()
	digest := sha256.Sum256([]byte(tokenID))
	boundCollectorID := testCollectorID
	record := collectorTokenRecord{
		IngestionTokenID:   tokenID,
		Version:            1,
		Name:               "projection width fixture",
		Description:        "projection width fixture",
		TokenPrefix:        "ost_v1_width",
		TokenDigest:        append([]byte(nil), digest[:]...),
		State:              CollectorTokenStateActive,
		CreatedAtUnixMicro: now,
		UpdatedAtUnixMicro: now,
		BoundCollectorID:   &boundCollectorID,
	}
	if err := db.GORMDB().WithContext(ctx).Create(&record).Error; err != nil {
		t.Fatalf("seed width-fixture token: %v", err)
	}
	if err := db.GORMDB().WithContext(ctx).Create(
		&collectorTokenIndexRecord{
			IngestionTokenID: tokenID,
			IndexID:          indexID,
		},
	).Error; err != nil {
		t.Fatalf("seed width-fixture scope: %v", err)
	}
}

func seedCollectorTokenParentsWithoutScopes(
	t *testing.T,
	db *control.DB,
	count int,
) {
	t.Helper()
	now := time.Date(2026, 7, 29, 11, 30, 0, 0, time.UTC).UnixMicro()
	boundCollectorID := testCollectorID
	records := make([]collectorTokenRecord, count)
	for index := range records {
		tokenID := fmt.Sprintf("tok_parent_overflow_%04d", index)
		digest := sha256.Sum256([]byte(tokenID))
		records[index] = collectorTokenRecord{
			IngestionTokenID:   tokenID,
			Version:            1,
			Name:               fmt.Sprintf("parent overflow %04d", index),
			Description:        "structural overflow fixture",
			TokenPrefix:        "ost_v1_overflow",
			TokenDigest:        append([]byte(nil), digest[:]...),
			State:              CollectorTokenStateActive,
			CreatedAtUnixMicro: now,
			UpdatedAtUnixMicro: now,
			BoundCollectorID:   &boundCollectorID,
		}
	}
	if err := db.GORMDB().
		CreateInBatches(&records, 128).
		Error; err != nil {
		t.Fatalf("seed collector token parent overflow: %v", err)
	}
}

func seedOrphanCollectorTokenScopes(
	t *testing.T,
	db *control.DB,
	count int,
) {
	t.Helper()
	ctx := context.Background()
	connection, err := db.SQLDB().Conn(ctx)
	if err != nil {
		t.Fatalf("acquire orphan-scope connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA foreign_keys = OFF`,
	); err != nil {
		t.Fatalf("disable foreign keys: %v", err)
	}
	foreignKeysDisabled := true
	defer func() {
		if !foreignKeysDisabled {
			return
		}
		if _, err := connection.ExecContext(
			context.Background(),
			`PRAGMA foreign_keys = ON`,
		); err != nil {
			t.Errorf("restore foreign keys after orphan scopes: %v", err)
		}
	}()

	if _, err := connection.ExecContext(ctx, `
		WITH digits(value) AS (
			VALUES (0), (1), (2), (3), (4), (5), (6), (7), (8), (9)
		),
		numbers(value) AS (
			SELECT
				ones.value +
				10 * tens.value +
				100 * hundreds.value +
				1000 * thousands.value +
				10000 * ten_thousands.value
			FROM digits AS ones
			CROSS JOIN digits AS tens
			CROSS JOIN digits AS hundreds
			CROSS JOIN digits AS thousands
			CROSS JOIN digits AS ten_thousands
		)
		INSERT INTO ingestion_token_indexes (ingestion_token_id, index_id)
		SELECT
			printf('tok_orphan_%05d', value),
			printf('idx_orphan_%05d', value)
		FROM numbers
		WHERE value < ?`,
		count,
	); err != nil {
		t.Fatalf("seed orphan token scope overflow: %v", err)
	}
	if _, err := connection.ExecContext(
		ctx,
		`PRAGMA foreign_keys = ON`,
	); err != nil {
		t.Fatalf("restore foreign keys: %v", err)
	}
	foreignKeysDisabled = false
}

func assertCollectorTokenRemainsActive(
	t *testing.T,
	db *control.DB,
	want CollectorToken,
) {
	t.Helper()
	var record collectorTokenRecord
	if err := db.GORMDB().
		Select("ingestion_token_id", "version", "state", "revoked_at_unix_micro").
		Where("ingestion_token_id = ?", want.ID).
		Take(&record).
		Error; err != nil {
		t.Fatalf("read token after rejected revocation: %v", err)
	}
	if record.Version != int64(want.Version) ||
		record.State != CollectorTokenStateActive ||
		record.RevokedAtUnixMicro != nil {
		t.Fatalf("rejected revocation mutated token: %#v", record)
	}
}

func countCollectorTokenRows(t *testing.T, db *control.DB, condition string, args ...any) int64 {
	t.Helper()
	var count int64
	query := db.GORMDB().Model(&collectorTokenRecord{})
	if condition != "" {
		query = query.Where(condition, args...)
	}
	if err := query.Count(&count).Error; err != nil {
		t.Fatalf("count collector token rows: %v", err)
	}
	return count
}

func countCollectorTokenScopeRows(t *testing.T, db *control.DB, tokenID string) int64 {
	t.Helper()
	var count int64
	query := db.GORMDB().Model(&collectorTokenIndexRecord{})
	if tokenID != "" {
		query = query.Where("ingestion_token_id = ?", tokenID)
	}
	if err := query.Count(&count).Error; err != nil {
		t.Fatalf("count collector token scope rows: %v", err)
	}
	return count
}
