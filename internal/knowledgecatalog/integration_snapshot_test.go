package knowledgecatalog

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/base64"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"gorm.io/gorm"
)

type integrationObjectExpectation struct {
	version     uint64
	name        string
	description string
	hostPattern string
	digest      []byte
}

type integrationGetResult struct {
	object Object
	err    error
}

type integrationListResult struct {
	page ListPage
	err  error
}

func TestIntegrationConcurrentPublicationReturnsOnlyOldOrNewSnapshots(t *testing.T) {
	database, store := newCatalogTestStore(t)
	oldDescription := "old immutable definition"
	oldDefinition := aliasDefinition(
		testApp,
		"alpha",
		SharingScopePrivate,
		&oldDescription,
		"old-host-*",
	)
	oldNormalized, err := knowledgedefinition.Normalize(oldDefinition)
	if err != nil {
		t.Fatalf("normalize old definition: %v", err)
	}
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-atomic",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: oldDefinition,
			state:      StateActive,
			mutation:   "create",
			timestamp:  10,
		}},
	})
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-zulu",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "zulu", SharingScopePrivate, nil, "zulu-*"),
			state:      StateActive,
			mutation:   "create",
			timestamp:  11,
		}},
	})

	firstRequest := ListRequest{PageSize: 1, IncludeTotal: true}
	first, err := store.List(context.Background(), testReadScope(), firstRequest)
	if err != nil {
		t.Fatalf("List(baseline first page): %v", err)
	}
	if !slices.Equal(names(first.Objects), []string{"alpha"}) || first.NextPageToken == "" ||
		first.TotalSize == nil || *first.TotalSize != 2 || first.CatalogRevision == 0 {
		t.Fatalf("baseline first page = %#v", first)
	}
	baselineRevision := first.CatalogRevision

	newDescription := "new immutable definition"
	staged, newNormalized := stageIntegrationKnownPublication(
		t,
		database,
		"ko-atomic",
		aliasDefinition(testApp, "charlie", SharingScopePrivate, &newDescription, "new-host-*"),
		StateActive,
		"update",
		20,
	)
	oldWant := integrationObjectExpectation{
		version:     1,
		name:        "alpha",
		description: oldDescription,
		hostPattern: "old-host-*",
		digest:      bytes.Clone(oldNormalized.Digest[:]),
	}
	newWant := integrationObjectExpectation{
		version:     2,
		name:        "charlie",
		description: newDescription,
		hostPattern: "new-host-*",
		digest:      bytes.Clone(newNormalized.Digest[:]),
	}

	// An immediate writer with every v2 row staged must not block WAL readers,
	// and uncommitted authorities must remain completely invisible.
	continuation := firstRequest
	continuation.PageToken = first.NextPageToken
	oldSecond, err := store.List(context.Background(), testReadScope(), continuation)
	if err != nil || !slices.Equal(names(oldSecond.Objects), []string{"zulu"}) || oldSecond.CatalogRevision != baselineRevision {
		t.Fatalf("List(uncommitted continuation) = %#v, %v", oldSecond, err)
	}

	const readers = 12
	var oldReads sync.WaitGroup
	oldReads.Add(readers)
	raceStart := make(chan struct{})
	commitDone := make(chan struct{})
	results := make(chan error, readers)
	for worker := range readers {
		go func(worker int) {
			if err := integrationAssertCatalogSnapshot(
				store,
				oldWant,
				newWant,
				baselineRevision,
				"old",
			); err != nil {
				oldReads.Done()
				results <- fmt.Errorf("reader %d before commit: %w", worker, err)
				return
			}
			oldReads.Done()
			<-raceStart
			if err := integrationAssertCatalogSnapshot(
				store,
				oldWant,
				newWant,
				baselineRevision,
				"either",
			); err != nil {
				results <- fmt.Errorf("reader %d racing commit: %w", worker, err)
				return
			}
			<-commitDone
			if err := integrationAssertCatalogSnapshot(
				store,
				oldWant,
				newWant,
				baselineRevision,
				"new",
			); err != nil {
				results <- fmt.Errorf("reader %d after commit: %w", worker, err)
				return
			}
			results <- nil
		}(worker)
	}
	oldReads.Wait()
	close(raceStart)
	commitErr := staged.Commit()
	close(commitDone)
	if commitErr != nil {
		t.Fatalf("commit staged publication: %v", commitErr)
	}
	for range readers {
		if err := <-results; err != nil {
			t.Error(err)
		}
	}

	if _, err := store.List(context.Background(), testReadScope(), continuation); !errors.Is(err, control.ErrPageInvalidated) {
		t.Fatalf("List(pre-commit cursor after publication) error = %v, want ErrPageInvalidated", err)
	}
	historicalVersion := uint64(1)
	historical, err := store.Get(context.Background(), testReadScope(), "ko-atomic", &historicalVersion)
	if err != nil {
		t.Fatalf("Get(retained v1): %v", err)
	}
	if _, err := integrationClassifyObject(historical, oldWant, oldWant); err != nil {
		t.Fatalf("retained v1: %v", err)
	}
}

func TestIntegrationGetRetainsEstablishedWALSnapshotAcrossPublication(t *testing.T) {
	database, store, staged, oldState, oldWant, newWant :=
		newIntegrationOverlapPublication(t, false)
	barrier := installIntegrationCatalogStateBarrier(t, database)

	result := make(chan integrationGetResult, 1)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() {
		object, err := store.Get(ctx, testReadScope(), "ko-atomic", nil)
		result <- integrationGetResult{object: object, err: err}
	}()

	barrier.waitUntilEstablished(t)
	if err := staged.Commit(); err != nil {
		t.Fatalf("commit publication while Get is paused: %v", err)
	}
	barrier.release()
	paused := waitIntegrationGetResult(t, result)
	if err := barrier.remove(); err != nil {
		t.Fatalf("remove Get catalog-state barrier: %v", err)
	}
	if paused.err != nil {
		t.Fatalf("paused Get: %v", paused.err)
	}
	if classification, err := integrationClassifyObject(paused.object, oldWant, newWant); err != nil || classification != "old" {
		t.Fatalf("paused Get classification = %q, %v; object: %s", classification, err, describeIntegrationObject(paused.object))
	}
	barrier.assertOldStateAcrossCommit(t, oldState)

	newState := readIntegrationCatalogState(t, database)
	assertIntegrationCatalogStateAdvanced(t, oldState, newState)
	fresh, err := store.Get(context.Background(), testReadScope(), "ko-atomic", nil)
	if err != nil {
		t.Fatalf("fresh Get: %v", err)
	}
	if classification, err := integrationClassifyObject(fresh, oldWant, newWant); err != nil || classification != "new" {
		t.Fatalf("fresh Get classification = %q, %v; object: %s", classification, err, describeIntegrationObject(fresh))
	}
}

func TestIntegrationListRetainsEstablishedWALSnapshotAcrossPublication(t *testing.T) {
	database, store, staged, oldState, oldWant, newWant :=
		newIntegrationOverlapPublication(t, true)
	barrier := installIntegrationCatalogStateBarrier(t, database)

	result := make(chan integrationListResult, 1)
	request := ListRequest{PageSize: 1, IncludeTotal: true}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	go func() {
		page, err := store.List(ctx, testReadScope(), request)
		result <- integrationListResult{page: page, err: err}
	}()

	barrier.waitUntilEstablished(t)
	if err := staged.Commit(); err != nil {
		t.Fatalf("commit publication while List is paused: %v", err)
	}
	barrier.release()
	paused := waitIntegrationListResult(t, result)
	if err := barrier.remove(); err != nil {
		t.Fatalf("remove List catalog-state barrier: %v", err)
	}
	if paused.err != nil {
		t.Fatalf("paused List: %v", paused.err)
	}
	assertIntegrationOverlapPage(t, paused.page, oldWant, newWant, "old", oldState)
	assertIntegrationCursorCatalogState(t, store, request, paused.page.NextPageToken, oldState)
	barrier.assertOldStateAcrossCommit(t, oldState)

	newState := readIntegrationCatalogState(t, database)
	assertIntegrationCatalogStateAdvanced(t, oldState, newState)
	fresh, err := store.List(context.Background(), testReadScope(), request)
	if err != nil {
		t.Fatalf("fresh List: %v", err)
	}
	assertIntegrationOverlapPage(t, fresh, oldWant, newWant, "new", newState)
	assertIntegrationCursorCatalogState(t, store, request, fresh.NextPageToken, newState)

	continuation := request
	continuation.PageToken = paused.page.NextPageToken
	if _, err := store.List(context.Background(), testReadScope(), continuation); !errors.Is(err, control.ErrPageInvalidated) {
		t.Fatalf("paused List cursor after commit error = %v, want ErrPageInvalidated", err)
	}
}

var integrationCatalogStateBarrierID atomic.Uint64

type integrationCatalogStateBarrier struct {
	database     *control.DB
	callbackName string
	established  chan struct{}
	resume       chan struct{}
	releaseOnce  sync.Once
	removeOnce   sync.Once
	removeErr    error
	reads        atomic.Int64
	afterCommit  catalogState
	captureErr   error
}

func installIntegrationCatalogStateBarrier(
	t *testing.T,
	database *control.DB,
) *integrationCatalogStateBarrier {
	t.Helper()
	barrier := &integrationCatalogStateBarrier{
		database: database,
		callbackName: fmt.Sprintf(
			"test:catalog-state-overlap-%d",
			integrationCatalogStateBarrierID.Add(1),
		),
		established: make(chan struct{}),
		resume:      make(chan struct{}),
	}
	if err := database.GORMDB().Callback().Query().After("gorm:query").Register(
		barrier.callbackName,
		func(tx *gorm.DB) {
			sqlText := tx.Statement.SQL.String()
			if !strings.Contains(sqlText, "knowledge_catalog_tenants AS tenant") ||
				!strings.Contains(sqlText, "knowledge_catalog_revision_heads AS head") {
				return
			}
			if barrier.reads.Add(1) != 1 {
				return
			}
			close(barrier.established)
			<-barrier.resume
			var revision int64
			var token []byte
			barrier.captureErr = tx.Statement.ConnPool.QueryRowContext(
				tx.Statement.Context,
				`SELECT tenant.catalog_revision, head.state_token
				 FROM knowledge_catalog_tenants AS tenant
				 JOIN knowledge_catalog_revision_heads AS head
				   ON head.tenant_id = tenant.tenant_id
				 WHERE tenant.tenant_id = ?`,
				testTenant,
			).Scan(&revision, &token)
			if barrier.captureErr == nil {
				if len(token) != catalogStateTokenBytes {
					barrier.captureErr = fmt.Errorf("captured state token bytes = %d, want %d", len(token), catalogStateTokenBytes)
					return
				}
				barrier.afterCommit = catalogState{
					revision: revision,
					token:    base64.RawURLEncoding.EncodeToString(token),
					found:    true,
				}
			}
		},
	); err != nil {
		t.Fatalf("register catalog-state overlap barrier: %v", err)
	}
	t.Cleanup(func() {
		barrier.release()
		if err := barrier.remove(); err != nil {
			t.Errorf("remove catalog-state overlap barrier: %v", err)
		}
	})
	return barrier
}

func (barrier *integrationCatalogStateBarrier) waitUntilEstablished(t *testing.T) {
	t.Helper()
	select {
	case <-barrier.established:
	case <-time.After(10 * time.Second):
		t.Fatal("catalog-state read transaction did not reach its deterministic barrier")
	}
}

func (barrier *integrationCatalogStateBarrier) release() {
	barrier.releaseOnce.Do(func() { close(barrier.resume) })
}

func (barrier *integrationCatalogStateBarrier) remove() error {
	barrier.removeOnce.Do(func() {
		barrier.removeErr = barrier.database.GORMDB().Callback().Query().Remove(barrier.callbackName)
	})
	return barrier.removeErr
}

func (barrier *integrationCatalogStateBarrier) assertOldStateAcrossCommit(
	t *testing.T,
	want catalogState,
) {
	t.Helper()
	if barrier.captureErr != nil {
		t.Fatalf("capture catalog state from paused transaction after writer commit: %v", barrier.captureErr)
	}
	if got := barrier.reads.Load(); got != 2 {
		t.Fatalf("catalog-state reads in paused transaction = %d, want initial and final reads", got)
	}
	if !barrier.afterCommit.equal(want) {
		t.Fatalf("paused transaction state after writer commit = %#v, want %#v", barrier.afterCommit, want)
	}
}

func newIntegrationOverlapPublication(
	t *testing.T,
	includeSecondObject bool,
) (
	*control.DB,
	*Store,
	*sql.Tx,
	catalogState,
	integrationObjectExpectation,
	integrationObjectExpectation,
) {
	t.Helper()
	database, store := newCatalogTestStore(t)
	oldDescription := "old deterministic overlap definition"
	oldDefinition := aliasDefinition(
		testApp,
		"alpha",
		SharingScopePrivate,
		&oldDescription,
		"old-overlap-*",
	)
	oldNormalized, err := knowledgedefinition.Normalize(oldDefinition)
	if err != nil {
		t.Fatalf("normalize old overlap definition: %v", err)
	}
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-atomic",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: oldDefinition,
			state:      StateActive,
			mutation:   "create",
			timestamp:  10,
		}},
	})
	if includeSecondObject {
		insertFixtureObject(t, database, fixtureObject{
			id:    "ko-zulu",
			owner: testOwner,
			versions: []fixtureVersion{{
				definition: aliasDefinition(testApp, "zulu", SharingScopePrivate, nil, "zulu-*"),
				state:      StateActive,
				mutation:   "create",
				timestamp:  11,
			}},
		})
	}
	oldState := readIntegrationCatalogState(t, database)
	newDescription := "new deterministic overlap definition"
	staged, newNormalized := stageIntegrationKnownPublication(
		t,
		database,
		"ko-atomic",
		aliasDefinition(testApp, "charlie", SharingScopePrivate, &newDescription, "new-overlap-*"),
		StateActive,
		"update",
		20,
	)
	return database, store, staged, oldState,
		integrationObjectExpectation{
			version:     1,
			name:        "alpha",
			description: oldDescription,
			hostPattern: "old-overlap-*",
			digest:      bytes.Clone(oldNormalized.Digest[:]),
		},
		integrationObjectExpectation{
			version:     2,
			name:        "charlie",
			description: newDescription,
			hostPattern: "new-overlap-*",
			digest:      bytes.Clone(newNormalized.Digest[:]),
		}
}

func readIntegrationCatalogState(t *testing.T, database *control.DB) catalogState {
	t.Helper()
	state, err := readCatalogState(database.GORMDB(), testTenant)
	if err != nil {
		t.Fatalf("read integration catalog state: %v", err)
	}
	if !state.found || state.revision < 1 || state.token == "" {
		t.Fatalf("integration catalog state = %#v", state)
	}
	return state
}

func assertIntegrationCatalogStateAdvanced(t *testing.T, oldState, newState catalogState) {
	t.Helper()
	if !newState.found || newState.revision != oldState.revision+1 ||
		newState.token == "" || newState.token == oldState.token {
		t.Fatalf("catalog state did not advance exactly: old=%#v new=%#v", oldState, newState)
	}
}

func assertIntegrationOverlapPage(
	t *testing.T,
	page ListPage,
	oldWant, newWant integrationObjectExpectation,
	wantClassification string,
	wantState catalogState,
) {
	t.Helper()
	if len(page.Objects) != 1 || page.TotalSize == nil || *page.TotalSize != 2 ||
		!page.TotalSizeExact || page.NextPageToken == "" ||
		page.CatalogRevision != uint64(wantState.revision) {
		t.Fatalf("%s overlap page shape = %#v", wantClassification, page)
	}
	classification, err := integrationClassifyObject(page.Objects[0], oldWant, newWant)
	if err != nil || classification != wantClassification {
		t.Fatalf("%s overlap page classification = %q, %v; object: %s", wantClassification, classification, err, describeIntegrationObject(page.Objects[0]))
	}
}

func assertIntegrationCursorCatalogState(
	t *testing.T,
	store *Store,
	request ListRequest,
	token string,
	want catalogState,
) {
	t.Helper()
	normalized, err := normalizeListRequest(testReadScope(), request)
	if err != nil {
		t.Fatalf("normalize cursor request: %v", err)
	}
	fingerprint, err := requestFingerprint(normalized)
	if err != nil {
		t.Fatalf("fingerprint cursor request: %v", err)
	}
	cursor, err := decodeCursor(store.cursorKey, token, fingerprint, normalized.sortBy)
	if err != nil {
		t.Fatalf("decode overlap cursor: %v", err)
	}
	if cursor.CatalogRevision != want.revision || cursor.CatalogState != want.token {
		t.Fatalf("overlap cursor state = revision %d token %q, want revision %d token %q", cursor.CatalogRevision, cursor.CatalogState, want.revision, want.token)
	}
}

func waitIntegrationGetResult(
	t *testing.T,
	result <-chan integrationGetResult,
) integrationGetResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(30 * time.Second):
		t.Fatal("paused Get did not complete after publication")
		return integrationGetResult{}
	}
}

func waitIntegrationListResult(
	t *testing.T,
	result <-chan integrationListResult,
) integrationListResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-time.After(30 * time.Second):
		t.Fatal("paused List did not complete after publication")
		return integrationListResult{}
	}
}

// integrationAssertCatalogSnapshot performs separate public Get and List
// transactions. Each call must be internally old or new; the two calls may
// legitimately straddle the writer commit when allowEither is true.
func integrationAssertCatalogSnapshot(
	store *Store,
	oldWant, newWant integrationObjectExpectation,
	oldRevision uint64,
	phase string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	object, err := store.Get(ctx, testReadScope(), "ko-atomic", nil)
	if err != nil {
		return fmt.Errorf("Get: %w", err)
	}
	classification, err := integrationClassifyObject(object, oldWant, newWant)
	if err != nil {
		return err
	}
	if phase != "either" && classification != phase {
		return fmt.Errorf("%s phase returned %s object: %s", phase, classification, describeIntegrationObject(object))
	}

	page, err := store.List(ctx, testReadScope(), ListRequest{PageSize: 10, IncludeTotal: true})
	if err != nil {
		return fmt.Errorf("List: %w", err)
	}
	if page.TotalSize == nil || *page.TotalSize != 2 || len(page.Objects) != 2 || page.NextPageToken != "" {
		return fmt.Errorf("List shape = %#v", page)
	}
	wantNames := []string{"alpha", "zulu"}
	wantRevision := oldRevision
	if page.CatalogRevision == oldRevision+1 {
		wantNames = []string{"charlie", "zulu"}
		wantRevision = oldRevision + 1
	}
	if page.CatalogRevision != wantRevision || !slices.Equal(names(page.Objects), wantNames) {
		return fmt.Errorf("List revision/names = %d/%v, want %d/%v", page.CatalogRevision, names(page.Objects), wantRevision, wantNames)
	}
	if phase == "old" && page.CatalogRevision != oldRevision ||
		phase == "new" && page.CatalogRevision != oldRevision+1 {
		return fmt.Errorf("%s phase returned revision %d", phase, page.CatalogRevision)
	}
	var listed Object
	for _, candidate := range page.Objects {
		if candidate.KnowledgeObjectID == "ko-atomic" {
			listed = candidate
		}
	}
	if listed.KnowledgeObjectID == "" {
		return errors.New("List omitted ko-atomic")
	}
	listedClass, err := integrationClassifyObject(listed, oldWant, newWant)
	if err != nil {
		return fmt.Errorf("listed object: %w", err)
	}
	if page.CatalogRevision == oldRevision && listedClass != "old" ||
		page.CatalogRevision == oldRevision+1 && listedClass != "new" {
		return fmt.Errorf("List mixed %s object with revision %d", listedClass, page.CatalogRevision)
	}
	return nil
}

func integrationClassifyObject(
	object Object,
	oldWant, newWant integrationObjectExpectation,
) (string, error) {
	if integrationObjectMatches(object, oldWant) {
		return "old", nil
	}
	if integrationObjectMatches(object, newWant) {
		return "new", nil
	}
	return "", fmt.Errorf("hybrid or unexpected object: %s", describeIntegrationObject(object))
}

func integrationObjectMatches(object Object, want integrationObjectExpectation) bool {
	if object.KnowledgeObjectID != "ko-atomic" || object.Version != want.version ||
		object.Name != want.name || object.State != StateActive || object.Definition == nil ||
		object.Definition.GetName() != want.name || object.Definition.GetDescription() != want.description ||
		!bytes.Equal(object.DefinitionSHA256, want.digest) {
		return false
	}
	patterns := object.Definition.GetSelector().GetHostPatterns()
	return len(patterns) == 1 && patterns[0].GetValue() == want.hostPattern
}

func TestIntegrationCatalogCancellationWhileConnectionPoolIsExhausted(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-cancel-busy",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: aliasDefinition(testApp, "cancel-busy", SharingScopePrivate, nil, "busy-*"),
			state:      StateActive,
			mutation:   "create",
			timestamp:  10,
		}},
	})
	database.SQLDB().SetMaxOpenConns(1)
	database.SQLDB().SetMaxIdleConns(1)
	held, err := database.SQLDB().Conn(context.Background())
	if err != nil {
		t.Fatalf("hold sole SQLite connection: %v", err)
	}

	for _, operation := range []struct {
		name string
		run  func(context.Context) error
	}{
		{
			name: "get",
			run: func(ctx context.Context) error {
				_, err := store.Get(ctx, testReadScope(), "ko-cancel-busy", nil)
				return err
			},
		},
		{
			name: "list",
			run: func(ctx context.Context) error {
				_, err := store.List(ctx, testReadScope(), ListRequest{})
				return err
			},
		},
	} {
		ctx, cancel := context.WithTimeout(context.Background(), 150*time.Millisecond)
		err := operation.run(ctx)
		cancel()
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s while pool exhausted error = %v, want DeadlineExceeded", operation.name, err)
		}
	}
	if err := held.Close(); err != nil {
		t.Fatalf("release sole SQLite connection: %v", err)
	}
	got, err := store.Get(context.Background(), testReadScope(), "ko-cancel-busy", nil)
	if err != nil || got.Name != "cancel-busy" {
		t.Fatalf("Get(after cancellation) = %#v, %v", got, err)
	}
}
