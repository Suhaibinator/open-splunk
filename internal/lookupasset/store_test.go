package lookupasset

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
)

const (
	testLookupTenant = "tenant-lookup"
	testLookupOwner  = "owner-lookup"
)

func TestStoreStagePublishReplaceAndReadExactVersions(t *testing.T) {
	database := openLookupTestDatabase(t)
	clock := newTestClock(time.Date(2026, 8, 13, 10, 0, 0, 123456000, time.UTC))
	stageSequence := atomic.Uint64{}
	assetSequence := atomic.Uint64{}
	store, err := NewStore(database, StoreOptions{
		Clock: clock.Now,
		StageIDGenerator: func() (string, error) {
			return fmt.Sprintf("stage-%d", stageSequence.Add(1)), nil
		},
		AssetIDGenerator: func() (string, error) {
			return fmt.Sprintf("asset-%d", assetSequence.Add(1)), nil
		},
		StageTTL: time.Hour,
	})
	if err != nil {
		t.Fatalf("construct store: %v", err)
	}

	firstAsset := mustParseAsset(t, "service,owner\napi,alice\n")
	stage, err := store.StageCSV(t.Context(), StageRequest{
		TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: firstAsset,
	})
	if err != nil {
		t.Fatalf("stage first asset: %v", err)
	}
	if stage.StageID != "stage-1" || stage.RowCount != 1 || stage.ColumnCount != 2 ||
		stage.ContentSHA256 != firstAsset.ContentSHA256() || stage.SourceSHA256 != firstAsset.SourceSHA256() {
		t.Fatalf("unexpected stage projection: %#v", stage)
	}
	first, err := store.Publish(t.Context(), PublishRequest{
		TenantID: testLookupTenant, OwnerID: testLookupOwner, StageID: stage.StageID,
	})
	if err != nil {
		t.Fatalf("publish first asset: %v", err)
	}
	if first.Ref.LookupAssetID != "asset-1" || first.Ref.Version != 1 || first.Ref.TenantID != testLookupTenant {
		t.Fatalf("unexpected first version: %#v", first.Ref)
	}
	if err := store.DiscardStage(t.Context(), testLookupTenant, testLookupOwner, stage.StageID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("consumed stage discard = %v", err)
	}
	loaded, err := store.GetVersion(t.Context(), first.Ref)
	if err != nil {
		t.Fatalf("read exact first version: %v", err)
	}
	if got := string(loaded.Asset.CanonicalCSV()); got != "service,owner\napi,alice\n" {
		t.Fatalf("loaded first CSV = %q", got)
	}

	clock.Advance(time.Microsecond)
	secondAsset := mustParseAsset(t, "service,owner\napi,bob\n")
	secondStage, err := store.StageCSV(t.Context(), StageRequest{
		TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: secondAsset,
	})
	if err != nil {
		t.Fatalf("stage replacement: %v", err)
	}
	second, err := store.Publish(t.Context(), PublishRequest{
		TenantID:               testLookupTenant,
		OwnerID:                testLookupOwner,
		StageID:                secondStage.StageID,
		LookupAssetID:          first.Ref.LookupAssetID,
		ExpectedCurrentVersion: 1,
	})
	if err != nil {
		t.Fatalf("publish replacement: %v", err)
	}
	if second.Ref.Version != 2 || second.Ref.ContentSHA256 == first.Ref.ContentSHA256 {
		t.Fatalf("unexpected replacement identity: %#v", second.Ref)
	}
	current, err := store.GetCurrent(t.Context(), testLookupTenant, first.Ref.LookupAssetID)
	if err != nil || current.Ref != second.Ref {
		t.Fatalf("current version = %#v, %v", current.Ref, err)
	}
	historical, err := store.GetVersion(t.Context(), first.Ref)
	if err != nil || string(historical.Asset.CanonicalCSV()) != "service,owner\napi,alice\n" {
		t.Fatalf("historical version was not retained: %#v, %v", historical.Ref, err)
	}

	var stages, identities, versions, storedBytes int64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT staged_asset_count, asset_identity_count,
		       published_version_count, stored_content_bytes
		FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = ?`, testLookupTenant,
	).Scan(&stages, &identities, &versions, &storedBytes); err != nil {
		t.Fatalf("read lookup ledger: %v", err)
	}
	wantBytes := int64(first.Ref.SizeBytes + second.Ref.SizeBytes)
	if stages != 0 || identities != 1 || versions != 2 || storedBytes != wantBytes {
		t.Fatalf("ledger = stages %d identities %d versions %d bytes %d, want 0/1/2/%d", stages, identities, versions, storedBytes, wantBytes)
	}
}

func TestStoreAtomicPublicationRollsBackPhysicalVersionWhenFinalizerFails(t *testing.T) {
	database := openLookupTestDatabase(t)
	store := mustLookupStore(t, database, StoreOptions{
		AssetIDGenerator: func() (string, error) { return "asset-atomic", nil },
	})
	asset := mustParseAsset(t, "key,value\na,one\n")
	stage, err := store.StageCSV(t.Context(), StageRequest{
		TenantID: testLookupTenant,
		OwnerID:  testLookupOwner,
		Asset:    asset,
	})
	if err != nil {
		t.Fatalf("StageCSV(): %v", err)
	}
	wantErr := errors.New("logical publication conflict")
	_, err = store.PublishAtomic(
		t.Context(),
		PublishRequest{
			TenantID: testLookupTenant,
			OwnerID:  testLookupOwner,
			StageID:  stage.StageID,
		},
		func(ctx context.Context, transaction PublicationTransaction, published Version) error {
			var count int
			if queryErr := transaction.QueryRowContext(ctx, `
				SELECT count(*) FROM knowledge_lookup_asset_versions
				WHERE tenant_id = ? AND lookup_asset_id = ? AND asset_version = ?`,
				published.Ref.TenantID,
				published.Ref.LookupAssetID,
				published.Ref.Version,
			).Scan(&count); queryErr != nil || count != 1 {
				t.Fatalf("uncommitted physical version = %d, %v", count, queryErr)
			}
			return wantErr
		},
	)
	if !errors.Is(err, wantErr) {
		t.Fatalf("PublishAtomic() error = %v", err)
	}
	var stages, identities, versions int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT staged_asset_count, asset_identity_count, published_version_count
		FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = ?`,
		testLookupTenant,
	).Scan(&stages, &identities, &versions); err != nil {
		t.Fatalf("read rollback ledger: %v", err)
	}
	if stages != 1 || identities != 0 || versions != 0 {
		t.Fatalf("rollback ledger = stages %d identities %d versions %d", stages, identities, versions)
	}
	if err := store.DiscardStage(t.Context(), testLookupTenant, testLookupOwner, stage.StageID); err != nil {
		t.Fatalf("discard restored stage: %v", err)
	}
}

func TestStorePublicationReclaimsStagedBytesBeforeVersionCapacityCheck(t *testing.T) {
	database := openLookupTestDatabase(t)
	store := mustLookupStore(t, database, StoreOptions{
		AssetIDGenerator: func() (string, error) { return "asset-exact-quota", nil },
	})
	asset := mustParseAsset(t, "key,value\na,one\n")
	stage, err := store.StageCSV(t.Context(), StageRequest{
		TenantID: testLookupTenant,
		OwnerID:  testLookupOwner,
		Asset:    asset,
	})
	if err != nil {
		t.Fatalf("StageCSV(): %v", err)
	}

	// Model a tenant whose byte quota has exactly enough room for the final
	// immutable version, but not for both that version and its staging copy.
	// Publication must consume the stage before version-capacity triggers run.
	if _, err := database.SQLDB().ExecContext(t.Context(), fmt.Sprintf(`
		CREATE TRIGGER test_lookup_asset_exact_byte_quota
		BEFORE INSERT ON knowledge_lookup_asset_versions
		WHEN EXISTS (
			SELECT 1 FROM knowledge_lookup_asset_tenant_ledgers
			WHERE tenant_id = NEW.tenant_id
			  AND stored_content_bytes > %d - NEW.canonical_bytes
		)
		BEGIN
			SELECT RAISE(ABORT, 'test lookup asset exact byte quota exhausted');
		END`, stage.SizeBytes)); err != nil {
		t.Fatalf("install exact-byte quota: %v", err)
	}

	published, err := store.Publish(t.Context(), PublishRequest{
		TenantID: testLookupTenant,
		OwnerID:  testLookupOwner,
		StageID:  stage.StageID,
	})
	if err != nil {
		t.Fatalf("Publish() at exact final quota: %v", err)
	}
	var stages, versions int
	var storedBytes uint64
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT staged_asset_count, published_version_count, stored_content_bytes
		FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = ?`,
		testLookupTenant,
	).Scan(&stages, &versions, &storedBytes); err != nil {
		t.Fatalf("read exact-quota ledger: %v", err)
	}
	if stages != 0 || versions != 1 || storedBytes != stage.SizeBytes ||
		published.Ref.SizeBytes != stage.SizeBytes {
		t.Fatalf(
			"exact-quota publication = stages %d versions %d ledger bytes %d ref bytes %d, want 0/1/%d/%d",
			stages,
			versions,
			storedBytes,
			published.Ref.SizeBytes,
			stage.SizeBytes,
			stage.SizeBytes,
		)
	}
}

func TestStorePublicationScopeVersionAndReferenceChecks(t *testing.T) {
	database := openLookupTestDatabase(t)
	store := mustLookupStore(t, database, StoreOptions{})
	asset := mustParseAsset(t, "key,value\na,one\n")
	stage, err := store.StageCSV(t.Context(), StageRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: asset})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	if _, err := store.Publish(t.Context(), PublishRequest{
		TenantID: testLookupTenant, OwnerID: "other-owner", StageID: stage.StageID,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("cross-owner publish = %v", err)
	}
	first, err := store.Publish(t.Context(), PublishRequest{
		TenantID: testLookupTenant, OwnerID: testLookupOwner, StageID: stage.StageID,
	})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}

	replacement, err := store.StageCSV(t.Context(), StageRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: asset})
	if err != nil {
		t.Fatalf("stage replacement: %v", err)
	}
	if _, err := store.Publish(t.Context(), PublishRequest{
		TenantID: testLookupTenant, OwnerID: testLookupOwner, StageID: replacement.StageID,
		LookupAssetID: first.Ref.LookupAssetID, ExpectedCurrentVersion: 2,
	}); !errors.Is(err, ErrConflict) {
		t.Fatalf("stale expected version = %v", err)
	}
	badRef := first.Ref
	badRef.ContentSHA256[0] ^= 0xff
	if _, err := store.GetVersion(t.Context(), badRef); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("digest disagreement = %v", err)
	}
	badRef = first.Ref
	badRef.SizeBytes++
	if _, err := store.GetVersion(t.Context(), badRef); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("size disagreement = %v", err)
	}
	if _, err := store.GetCurrent(t.Context(), testLookupTenant, "missing-asset"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing current asset = %v", err)
	}
}

func TestStoreExpiredStagesAreUnpublishableAndPrunable(t *testing.T) {
	database := openLookupTestDatabase(t)
	clock := newTestClock(time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC))
	store := mustLookupStore(t, database, StoreOptions{Clock: clock.Now, StageTTL: time.Minute})
	asset := mustParseAsset(t, "key\na\n")
	stage, err := store.StageCSV(t.Context(), StageRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: asset})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	clock.Advance(time.Minute)
	if _, err := store.Publish(t.Context(), PublishRequest{
		TenantID: testLookupTenant, OwnerID: testLookupOwner, StageID: stage.StageID,
	}); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expired publication = %v", err)
	}
	pruned, err := store.PruneExpiredStages(t.Context(), clock.Now(), 10)
	if err != nil || pruned != 1 {
		t.Fatalf("prune = %d, %v", pruned, err)
	}
	if err := store.DiscardStage(t.Context(), testLookupTenant, testLookupOwner, stage.StageID); !errors.Is(err, ErrNotFound) {
		t.Fatalf("pruned stage remains: %v", err)
	}
}

func TestStoreStagingStartsTTLAfterWaitingForWriter(t *testing.T) {
	database := openLookupTestDatabase(t)
	start := time.Date(2026, 8, 14, 10, 0, 0, 0, time.UTC)
	advanced := start.Add(10 * time.Minute)
	var unixMicro atomic.Int64
	unixMicro.Store(start.UnixMicro())
	clockCalls := make(chan struct{}, 4)
	stageIDGenerated := make(chan struct{})
	store := mustLookupStore(t, database, StoreOptions{
		Clock: func() time.Time {
			clockCalls <- struct{}{}
			return time.UnixMicro(unixMicro.Load()).UTC()
		},
		StageTTL: time.Minute,
		StageIDGenerator: func() (string, error) {
			close(stageIDGenerated)
			return "stage-after-writer-wait", nil
		},
	})
	asset := mustParseAsset(t, "key\na\n")

	writer, err := database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("acquire blocking writer: %v", err)
	}
	defer writer.Close()
	if _, err := writer.ExecContext(t.Context(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin blocking writer: %v", err)
	}
	writerOpen := true
	defer func() {
		if writerOpen {
			_, _ = writer.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()

	type stageResult struct {
		stage StagedAsset
		err   error
	}
	staged := make(chan stageResult, 1)
	go func() {
		stage, stageErr := store.StageCSV(context.Background(), StageRequest{
			TenantID: testLookupTenant,
			OwnerID:  testLookupOwner,
			Asset:    asset,
		})
		staged <- stageResult{stage: stage, err: stageErr}
	}()
	select {
	case <-clockCalls:
		t.Fatal("staging sampled its TTL before attempting to acquire the SQLite writer")
	case <-stageIDGenerated:
	case <-time.After(3 * time.Second):
		t.Fatal("StageCSV() did not reach the write boundary")
	}
	select {
	case <-clockCalls:
		t.Fatal("staging sampled its TTL while still blocked behind the SQLite writer")
	case <-time.After(100 * time.Millisecond):
	}

	unixMicro.Store(advanced.UnixMicro())
	if _, err := writer.ExecContext(t.Context(), `COMMIT`); err != nil {
		t.Fatalf("release blocking writer: %v", err)
	}
	writerOpen = false
	select {
	case result := <-staged:
		if result.err != nil {
			t.Fatalf("StageCSV(after writer wait): %v", result.err)
		}
		if !result.stage.CreatedAt.Equal(advanced) ||
			!result.stage.ExpiresAt.Equal(advanced.Add(time.Minute)) {
			t.Fatalf("staged TTL = %v..%v, want %v..%v", result.stage.CreatedAt, result.stage.ExpiresAt, advanced, advanced.Add(time.Minute))
		}
		if err := store.DiscardStage(t.Context(), testLookupTenant, testLookupOwner, result.stage.StageID); err != nil {
			t.Fatalf("discard staged fixture: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("StageCSV() did not resume after the writer was released")
	}
}

func TestStorePublicationRechecksExpiryAfterWaitingForWriter(t *testing.T) {
	database := openLookupTestDatabase(t)
	start := time.Date(2026, 8, 13, 10, 0, 0, 0, time.UTC)
	var unixMicro atomic.Int64
	unixMicro.Store(start.UnixMicro())
	clockCalls := make(chan struct{}, 4)
	publishStarted := make(chan struct{})
	store := mustLookupStore(t, database, StoreOptions{
		Clock: func() time.Time {
			clockCalls <- struct{}{}
			return time.UnixMicro(unixMicro.Load()).UTC()
		},
		StageTTL: time.Minute,
		AssetIDGenerator: func() (string, error) {
			close(publishStarted)
			return "asset-after-writer-wait", nil
		},
	})
	asset := mustParseAsset(t, "key\na\n")
	stage, err := store.StageCSV(t.Context(), StageRequest{
		TenantID: testLookupTenant,
		OwnerID:  testLookupOwner,
		Asset:    asset,
	})
	if err != nil {
		t.Fatalf("StageCSV(): %v", err)
	}
	<-clockCalls // StageCSV's creation/expiry sample.

	writer, err := database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("acquire blocking writer: %v", err)
	}
	defer writer.Close()
	if _, err := writer.ExecContext(t.Context(), `BEGIN IMMEDIATE`); err != nil {
		t.Fatalf("begin blocking writer: %v", err)
	}
	writerOpen := true
	defer func() {
		if writerOpen {
			_, _ = writer.ExecContext(context.Background(), `ROLLBACK`)
		}
	}()

	published := make(chan error, 1)
	go func() {
		_, publishErr := store.Publish(context.Background(), PublishRequest{
			TenantID: testLookupTenant,
			OwnerID:  testLookupOwner,
			StageID:  stage.StageID,
		})
		published <- publishErr
	}()
	<-publishStarted
	select {
	case <-clockCalls:
		t.Fatal("publication sampled its clock before acquiring the SQLite writer")
	case <-time.After(100 * time.Millisecond):
	}

	unixMicro.Store(start.Add(time.Minute).UnixMicro())
	if _, err := writer.ExecContext(t.Context(), `COMMIT`); err != nil {
		t.Fatalf("release blocking writer: %v", err)
	}
	writerOpen = false
	select {
	case err := <-published:
		if !errors.Is(err, ErrNotFound) {
			t.Fatalf("Publish(stage expired while waiting) error = %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("Publish() did not resume after the writer was released")
	}
	if err := store.DiscardStage(t.Context(), testLookupTenant, testLookupOwner, stage.StageID); err != nil {
		t.Fatalf("expired stage was consumed despite rejected publication: %v", err)
	}
}

func TestStorePublicationCancellationDuringPersistedVerificationRollsBack(t *testing.T) {
	database := openLookupTestDatabase(t)
	now := time.Date(2026, 8, 14, 12, 0, 0, 0, time.UTC)
	var clockCalls atomic.Int64
	var publishContext *checkpointCancellationContext
	store := mustLookupStore(t, database, StoreOptions{
		Clock: func() time.Time {
			if clockCalls.Add(1) == 2 {
				// Publish samples its clock only after BEGIN IMMEDIATE. Leave
				// enough checkpoints for the staged row to be read, then cancel
				// during its context-aware persisted CSV verification.
				publishContext.arm(4)
			}
			return now
		},
		AssetIDGenerator: func() (string, error) { return "asset-canceled-verification", nil },
	})
	asset := mustParseAsset(t, "key,value\n"+strings.Repeat("a,b\n", 2_000))
	stage, err := store.StageCSV(t.Context(), StageRequest{
		TenantID: testLookupTenant,
		OwnerID:  testLookupOwner,
		Asset:    asset,
	})
	if err != nil {
		t.Fatalf("StageCSV(): %v", err)
	}

	publishContext = newCheckpointCancellationContext()
	_, err = store.Publish(publishContext, PublishRequest{
		TenantID: testLookupTenant,
		OwnerID:  testLookupOwner,
		StageID:  stage.StageID,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("Publish(canceled verification) error = %v, want context cancellation", err)
	}
	if clockCalls.Load() != 2 {
		t.Fatalf("clock calls = %d, want cancellation after publication acquired its writer", clockCalls.Load())
	}

	// A cancellation error must synchronously roll back the transaction: both
	// the stage and generated asset identity remain available for a retry.
	published, err := store.Publish(context.Background(), PublishRequest{
		TenantID: testLookupTenant,
		OwnerID:  testLookupOwner,
		StageID:  stage.StageID,
	})
	if err != nil {
		t.Fatalf("Publish(after canceled verification): %v", err)
	}
	if published.Ref.LookupAssetID != "asset-canceled-verification" || published.Ref.Version != 1 {
		t.Fatalf("published retry = %#v", published.Ref)
	}
}

func TestStoreConcurrentReplacementPublishesOneSuccessor(t *testing.T) {
	database := openLookupTestDatabase(t)
	store := mustLookupStore(t, database, StoreOptions{})
	asset := mustParseAsset(t, "key,value\na,one\n")
	initialStage, err := store.StageCSV(t.Context(), StageRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: asset})
	if err != nil {
		t.Fatalf("stage initial: %v", err)
	}
	initial, err := store.Publish(t.Context(), PublishRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, StageID: initialStage.StageID})
	if err != nil {
		t.Fatalf("publish initial: %v", err)
	}
	stages := make([]StagedAsset, 2)
	for index := range stages {
		stages[index], err = store.StageCSV(t.Context(), StageRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: asset})
		if err != nil {
			t.Fatalf("stage replacement %d: %v", index, err)
		}
	}

	start := make(chan struct{})
	errorsOut := make(chan error, 2)
	var wait sync.WaitGroup
	for _, stage := range stages {
		wait.Add(1)
		go func(stage StagedAsset) {
			defer wait.Done()
			<-start
			_, err := store.Publish(context.Background(), PublishRequest{
				TenantID: testLookupTenant, OwnerID: testLookupOwner, StageID: stage.StageID,
				LookupAssetID: initial.Ref.LookupAssetID, ExpectedCurrentVersion: 1,
			})
			errorsOut <- err
		}(stage)
	}
	close(start)
	wait.Wait()
	close(errorsOut)
	var successes, conflicts int
	for err := range errorsOut {
		switch {
		case err == nil:
			successes++
		case errors.Is(err, ErrConflict):
			conflicts++
		default:
			t.Fatalf("unexpected concurrent publication error: %v", err)
		}
	}
	if successes != 1 || conflicts != 1 {
		t.Fatalf("concurrent outcomes = %d successes, %d conflicts", successes, conflicts)
	}
	current, err := store.GetCurrent(t.Context(), testLookupTenant, initial.Ref.LookupAssetID)
	if err != nil || current.Ref.Version != 2 {
		t.Fatalf("current after concurrent publication = %#v, %v", current.Ref, err)
	}
}

func TestMigrationForbidsPublishedVersionMutationAndUnpairedPointerAdvance(t *testing.T) {
	database := openLookupTestDatabase(t)
	store := mustLookupStore(t, database, StoreOptions{})
	asset := mustParseAsset(t, "key\na\n")
	stage, err := store.StageCSV(t.Context(), StageRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: asset})
	if err != nil {
		t.Fatalf("stage: %v", err)
	}
	published, err := store.Publish(t.Context(), PublishRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, StageID: stage.StageID})
	if err != nil {
		t.Fatalf("publish: %v", err)
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), `
		UPDATE knowledge_lookup_asset_versions SET row_count = 0
		WHERE tenant_id = ? AND lookup_asset_id = ? AND asset_version = 1`,
		testLookupTenant, published.Ref.LookupAssetID,
	); err == nil {
		t.Fatal("published version update unexpectedly succeeded")
	}
	if _, err := database.SQLDB().ExecContext(t.Context(), `
		DELETE FROM knowledge_lookup_asset_versions
		WHERE tenant_id = ? AND lookup_asset_id = ? AND asset_version = 1`,
		testLookupTenant, published.Ref.LookupAssetID,
	); err == nil {
		t.Fatal("published version delete unexpectedly succeeded")
	}
	tx, err := database.SQLDB().BeginTx(t.Context(), nil)
	if err != nil {
		t.Fatalf("begin unpaired advance: %v", err)
	}
	if _, err := tx.ExecContext(t.Context(), `
		UPDATE knowledge_lookup_assets
		SET current_version = 2, updated_at_unix_micro = updated_at_unix_micro + 1
		WHERE tenant_id = ? AND lookup_asset_id = ?`, testLookupTenant, published.Ref.LookupAssetID); err != nil {
		_ = tx.Rollback()
		t.Fatalf("stage deferred unpaired advance: %v", err)
	}
	if err := tx.Commit(); err == nil {
		t.Fatal("unpaired current-version advance unexpectedly committed")
	}
}

func TestStoreStagingCapacityIsAtomic(t *testing.T) {
	database := openLookupTestDatabase(t)
	store := mustLookupStore(t, database, StoreOptions{})
	asset := mustParseAsset(t, "key\na\n")
	for index := range MaximumStagedAssetsPerTenant {
		if _, err := store.StageCSV(t.Context(), StageRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: asset}); err != nil {
			t.Fatalf("stage %d: %v", index, err)
		}
	}
	if _, err := store.StageCSV(t.Context(), StageRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: asset}); !errors.Is(err, ErrResourceLimit) {
		t.Fatalf("over-capacity stage = %v", err)
	}
	var count, ledger int
	if err := database.SQLDB().QueryRowContext(t.Context(), `SELECT count(*) FROM knowledge_lookup_asset_stages WHERE tenant_id = ?`, testLookupTenant).Scan(&count); err != nil {
		t.Fatalf("count stages: %v", err)
	}
	if err := database.SQLDB().QueryRowContext(t.Context(), `SELECT staged_asset_count FROM knowledge_lookup_asset_tenant_ledgers WHERE tenant_id = ?`, testLookupTenant).Scan(&ledger); err != nil {
		t.Fatalf("read stage ledger: %v", err)
	}
	if count != MaximumStagedAssetsPerTenant || ledger != count {
		t.Fatalf("staging count/ledger = %d/%d", count, ledger)
	}
}

func TestStageCSVReclaimsExpiredStagesWithoutExternalMaintenance(t *testing.T) {
	database := openLookupTestDatabase(t)
	clock := newTestClock(time.Unix(1_700_000_000, 0).UTC())
	var sequence atomic.Uint64
	store := mustLookupStore(t, database, StoreOptions{
		Clock:    clock.Now,
		StageTTL: time.Minute,
		StageIDGenerator: func() (string, error) {
			return fmt.Sprintf("stage-expiry-%d", sequence.Add(1)), nil
		},
	})
	asset := mustParseAsset(t, "key,value\none,first\n")
	for range MaximumStagedAssetsPerTenant {
		if _, err := store.StageCSV(t.Context(), StageRequest{
			TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: asset,
		}); err != nil {
			t.Fatalf("StageCSV(fill): %v", err)
		}
	}
	clock.Advance(time.Minute)
	stage, err := store.StageCSV(t.Context(), StageRequest{
		TenantID: testLookupTenant, OwnerID: testLookupOwner, Asset: asset,
	})
	if err != nil {
		t.Fatalf("StageCSV(after expiry): %v", err)
	}
	if stage.StageID != fmt.Sprintf("stage-expiry-%d", MaximumStagedAssetsPerTenant+1) {
		t.Fatalf("replacement stage = %q", stage.StageID)
	}
	var count int
	if err := database.SQLDB().QueryRowContext(t.Context(), `
		SELECT count(*) FROM knowledge_lookup_asset_stages
		WHERE tenant_id = ?`, testLookupTenant).Scan(&count); err != nil {
		t.Fatalf("count stages: %v", err)
	}
	if count != 1 {
		t.Fatalf("stage count after opportunistic reclaim = %d, want 1", count)
	}
}

func TestNewStoreAndRequestsRejectInvalidInput(t *testing.T) {
	if _, err := NewStore(nil, StoreOptions{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil database = %v", err)
	}
	database := openLookupTestDatabase(t)
	if _, err := NewStore(database, StoreOptions{StageTTL: 8 * 24 * time.Hour}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("oversized TTL = %v", err)
	}
	store := mustLookupStore(t, database, StoreOptions{})
	//nolint:staticcheck // The nil context is the invalid input under test.
	if _, err := store.StageCSV(nil, StageRequest{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("nil context = %v", err)
	}
	if _, err := store.StageCSV(t.Context(), StageRequest{}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("empty stage = %v", err)
	}
	if _, err := store.Publish(t.Context(), PublishRequest{TenantID: testLookupTenant, OwnerID: testLookupOwner, StageID: "stage", LookupAssetID: "asset"}); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("ambiguous create = %v", err)
	}
	if _, err := store.PruneExpiredStages(t.Context(), time.Now(), 0); !errors.Is(err, ErrInvalidArgument) {
		t.Fatalf("zero prune limit = %v", err)
	}
}

func openLookupTestDatabase(t *testing.T) *control.DB {
	t.Helper()
	database, err := control.Open(t.Context(), filepath.Join(t.TempDir(), "control.sqlite"))
	if err != nil {
		t.Fatalf("open control database: %v", err)
	}
	t.Cleanup(func() {
		if err := database.Close(); err != nil {
			t.Errorf("close control database: %v", err)
		}
	})
	if _, err := database.SQLDB().ExecContext(t.Context(), `
		INSERT INTO knowledge_catalog_tenants (tenant_id) VALUES (?)`, testLookupTenant); err != nil {
		t.Fatalf("provision lookup tenant: %v", err)
	}
	return database
}

func mustLookupStore(t *testing.T, database *control.DB, options StoreOptions) *Store {
	t.Helper()
	store, err := NewStore(database, options)
	if err != nil {
		t.Fatalf("construct lookup store: %v", err)
	}
	return store
}

type testClock struct {
	mu  sync.Mutex
	now time.Time
}

func newTestClock(now time.Time) *testClock { return &testClock{now: now} }

func (clock *testClock) Now() time.Time {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	return clock.now
}

func (clock *testClock) Advance(duration time.Duration) {
	clock.mu.Lock()
	defer clock.mu.Unlock()
	clock.now = clock.now.Add(duration)
}
