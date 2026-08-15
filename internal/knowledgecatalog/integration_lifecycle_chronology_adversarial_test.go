package knowledgecatalog

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

const integrationMalformedLifecycleSentinel = "lifecycle-secret-marker"

type integrationLifecycleRow struct {
	version       int64
	state         string
	disabledAt    sql.NullInt64
	quarantinedAt sql.NullInt64
	deletedAt     sql.NullInt64
	reason        sql.NullString
}

type integrationLifecycleWant struct {
	version       uint64
	state         State
	updatedAt     int64
	disabledAt    int64
	quarantinedAt int64
	deletedAt     int64
	reason        string
	bodyless      bool
}

func TestIntegrationLifecycleDisableMarkerSurvivesDisabledChangesBackupAndConcurrentReads(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-disabled-chronology",
		owner: testOwner,
		versions: []fixtureVersion{
			{
				definition: aliasDefinition(testApp, "chronology-v1", SharingScopePrivate, nil, "chronology-v1-*"),
				state:      StateActive,
				mutation:   "create",
				timestamp:  10,
			},
			{
				definition: aliasDefinition(testApp, "chronology-v2", SharingScopePrivate, nil, "chronology-v2-*"),
				state:      StateDisabled,
				mutation:   "disable",
				timestamp:  20,
			},
			{
				definition: aliasDefinition(testApp, "chronology-v3", SharingScopePrivate, nil, "chronology-v3-*"),
				state:      StateDisabled,
				mutation:   "update",
				timestamp:  30,
			},
			{
				definition: aliasDefinition(testApp, "chronology-v4", SharingScopeApp, nil, "chronology-v4-*"),
				state:      StateDisabled,
				mutation:   "scope_change",
				timestamp:  40,
			},
		},
	})

	wants := []integrationLifecycleWant{
		{version: 1, state: StateActive, updatedAt: 10},
		{version: 2, state: StateDisabled, updatedAt: 20, disabledAt: 20},
		{version: 3, state: StateDisabled, updatedAt: 30, disabledAt: 20},
		{version: 4, state: StateDisabled, updatedAt: 40, disabledAt: 20},
	}
	assertIntegrationLifecyclePublicReads(t, store, "ko-disabled-chronology", wants)
	assertIntegrationLifecycleRows(t, database, "ko-disabled-chronology", wants)
	sourceRows := readIntegrationLifecycleRows(t, database, "ko-disabled-chronology")

	backupPath := filepath.Join(t.TempDir(), "lifecycle-chronology.sqlite")
	backupContext, cancelBackup := context.WithTimeout(context.Background(), 10*time.Second)
	err := database.BackupTo(backupContext, backupPath)
	cancelBackup()
	if err != nil {
		t.Fatalf("BackupTo(lifecycle chronology): %v", err)
	}
	restored, err := control.OpenReadOnly(context.Background(), backupPath)
	if err != nil {
		t.Fatalf("control.OpenReadOnly(lifecycle backup): %v", err)
	}
	t.Cleanup(func() {
		if err := restored.Close(); err != nil {
			t.Errorf("close lifecycle backup: %v", err)
		}
	})
	restoredStore, err := New(restored, Options{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("New(lifecycle backup): %v", err)
	}
	assertIntegrationLifecyclePublicReads(t, restoredStore, "ko-disabled-chronology", wants)
	restoredRows := readIntegrationLifecycleRows(t, restored, "ko-disabled-chronology")
	if !reflect.DeepEqual(restoredRows, sourceRows) {
		t.Fatalf("restored lifecycle rows = %#v, want exact %#v", restoredRows, sourceRows)
	}

	// Repeated independent Store transactions against the same WAL snapshot
	// must never synthesize the update or scope-change time as the disable time.
	const workers = 12
	const iterations = 12
	var wait sync.WaitGroup
	wait.Add(workers)
	errorsByWorker := make(chan error, workers)
	for worker := range workers {
		go func(worker int) {
			defer wait.Done()
			for iteration := range iterations {
				if err := checkIntegrationLifecyclePublicReads(
					restoredStore,
					"ko-disabled-chronology",
					wants,
				); err != nil {
					errorsByWorker <- fmt.Errorf("worker %d iteration %d: %w", worker, iteration, err)
					return
				}
			}
		}(worker)
	}
	wait.Wait()
	close(errorsByWorker)
	for err := range errorsByWorker {
		t.Error(err)
	}
}

func TestIntegrationLifecycleDisableEnableDisableUsesSecondMarker(t *testing.T) {
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-disable-enable-disable",
		owner: testOwner,
		versions: []fixtureVersion{
			{
				definition: aliasDefinition(testApp, "ded-v1", SharingScopePrivate, nil, "ded-v1-*"),
				state:      StateActive,
				mutation:   "create",
				timestamp:  10,
			},
			{
				definition: aliasDefinition(testApp, "ded-v2", SharingScopePrivate, nil, "ded-v2-*"),
				state:      StateDisabled,
				mutation:   "disable",
				timestamp:  20,
			},
			{
				definition: aliasDefinition(testApp, "ded-v3", SharingScopePrivate, nil, "ded-v3-*"),
				state:      StateActive,
				mutation:   "enable",
				timestamp:  30,
			},
			{
				definition: aliasDefinition(testApp, "ded-v4", SharingScopePrivate, nil, "ded-v4-*"),
				state:      StateDisabled,
				mutation:   "disable",
				timestamp:  40,
			},
		},
	})

	wants := []integrationLifecycleWant{
		{version: 1, state: StateActive, updatedAt: 10},
		{version: 2, state: StateDisabled, updatedAt: 20, disabledAt: 20},
		{version: 3, state: StateActive, updatedAt: 30},
		{version: 4, state: StateDisabled, updatedAt: 40, disabledAt: 40},
	}
	assertIntegrationLifecyclePublicReads(t, store, "ko-disable-enable-disable", wants)
	assertIntegrationLifecycleRows(t, database, "ko-disable-enable-disable", wants)
}

func TestIntegrationLifecycleTerminalMarkersAreExact(t *testing.T) {
	t.Run("quarantine", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		reason := "root_corruption"
		insertFixtureObject(t, database, fixtureObject{
			id:    "ko-lifecycle-quarantine",
			owner: testOwner,
			versions: []fixtureVersion{
				{
					definition: aliasDefinition(testApp, "lifecycle-quarantine", SharingScopePrivate, nil, "quarantine-v1-*"),
					state:      StateActive,
					mutation:   "create",
					timestamp:  10,
				},
				{
					state:     StateQuarantined,
					mutation:  "quarantine",
					reason:    &reason,
					timestamp: 20,
				},
			},
		})

		wants := []integrationLifecycleWant{
			{version: 1, state: StateActive, updatedAt: 10},
			{
				version:       2,
				state:         StateQuarantined,
				updatedAt:     20,
				quarantinedAt: 20,
				reason:        reason,
				bodyless:      true,
			},
		}
		assertIntegrationLifecycleRows(t, database, "ko-lifecycle-quarantine", wants)
		assertIntegrationLifecycleCurrentAndList(
			t,
			store,
			"ko-lifecycle-quarantine",
			wants[len(wants)-1],
		)
		// Current quarantine redaction wins over an explicit historical request.
		versionOne := uint64(1)
		object, err := store.Get(
			context.Background(),
			testReadScope(),
			"ko-lifecycle-quarantine",
			&versionOne,
		)
		if err != nil {
			t.Fatalf("Get(quarantined historical request): %v", err)
		}
		assertIntegrationLifecycleObject(t, object, wants[len(wants)-1])

		// Quarantine redaction also wins over numerically future requests. The
		// returned scalar snapshot must be the exact current object rather than a
		// synthetic object carrying the attacker-selected version.
		current, err := store.Get(
			context.Background(),
			testReadScope(),
			"ko-lifecycle-quarantine",
			nil,
		)
		if err != nil {
			t.Fatalf("Get(quarantined current baseline): %v", err)
		}
		for _, requested := range []uint64{3, math.MaxInt64} {
			t.Run("future v"+strconv.FormatUint(requested, 10), func(t *testing.T) {
				got, getErr := store.Get(
					context.Background(),
					testReadScope(),
					"ko-lifecycle-quarantine",
					&requested,
				)
				if getErr != nil {
					t.Fatalf("Get(quarantined future v%d): %v", requested, getErr)
				}
				if !reflect.DeepEqual(got, current) {
					t.Fatalf("Get(quarantined future v%d) = %#v, want exact current %#v", requested, got, current)
				}
				assertIntegrationLifecycleObject(t, got, wants[len(wants)-1])
			})
		}
	})

	t.Run("delete", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		insertFixtureObject(t, database, fixtureObject{
			id:    "ko-lifecycle-delete",
			owner: testOwner,
			versions: []fixtureVersion{
				{
					definition: aliasDefinition(testApp, "lifecycle-delete-v1", SharingScopePrivate, nil, "delete-v1-*"),
					state:      StateActive,
					mutation:   "create",
					timestamp:  10,
				},
				{
					definition: aliasDefinition(testApp, "lifecycle-delete-v2", SharingScopePrivate, nil, "delete-v2-*"),
					state:      StateDeleted,
					mutation:   "delete",
					timestamp:  20,
				},
			},
		})

		wants := []integrationLifecycleWant{
			{version: 1, state: StateActive, updatedAt: 10},
			{version: 2, state: StateDeleted, updatedAt: 20, deletedAt: 20},
		}
		assertIntegrationLifecyclePublicReads(t, store, "ko-lifecycle-delete", wants)
		assertIntegrationLifecycleRows(t, database, "ko-lifecycle-delete", wants)
	})
}

func TestIntegrationLifecycleHistoricalTimestampCorruptionFailsClosedGenerically(t *testing.T) {
	tests := []struct {
		name             string
		requestedVersion uint64
		corruptVersion   int64
		corruptTimestamp int64
	}{
		{
			name:             "v1 differs from registry creation",
			requestedVersion: 1,
			corruptVersion:   1,
			corruptTimestamp: 11,
		},
		{
			name:             "requested history is after current",
			requestedVersion: 3,
			corruptVersion:   3,
			corruptTimestamp: 9_876_543_210,
		},
		{
			name:             "adjacent timestamps are inverted",
			requestedVersion: 3,
			corruptVersion:   3,
			corruptTimestamp: 15,
		},
	}
	var genericError string
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			const objectID = "ko-corrupt-history-chronology"
			insertIntegrationActiveLifecycleFixture(t, database, objectID, "corrupt-time", 4)
			corruptIntegrationLifecycleVersionTimestamp(
				t,
				database,
				objectID,
				test.corruptVersion,
				test.corruptTimestamp,
			)

			object, err := store.Get(
				context.Background(),
				testReadScope(),
				objectID,
				&test.requestedVersion,
			)
			if !errors.Is(err, ErrCorrupt) || !reflect.DeepEqual(object, Object{}) {
				t.Fatalf("Get(corrupt historical chronology) = %#v, %v, want zero/ErrCorrupt", object, err)
			}
			if test.corruptVersion == 1 {
				currentGetErr, currentListErr := assertIntegrationLifecycleCurrentReadsCorrupt(
					t,
					store,
					objectID,
				)
				for _, currentErr := range []error{currentGetErr, currentListErr} {
					for _, payload := range []string{objectID, strconv.FormatInt(test.corruptTimestamp, 10)} {
						if strings.Contains(currentErr.Error(), payload) {
							t.Errorf("current chronology error disclosed payload %q: %v", payload, currentErr)
						}
					}
				}
			}
			for _, payload := range []string{
				objectID,
				"corrupt-time-v" + strconv.FormatInt(test.corruptVersion, 10),
				strconv.FormatInt(test.corruptTimestamp, 10),
			} {
				if strings.Contains(err.Error(), payload) {
					t.Errorf("historical chronology error disclosed payload %q: %v", payload, err)
				}
			}
			if index == 0 {
				genericError = err.Error()
				return
			}
			if err.Error() != genericError {
				t.Errorf("historical chronology error disclosed subtype: got %q, want generic %q", err, genericError)
			}
		})
	}
}

func TestIntegrationLifecycleTransitionCorruptionFailsClosed(t *testing.T) {
	t.Run("terminal prior state followed by active", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		const objectID = "ko-terminal-prior-state"
		insertIntegrationActiveLifecycleFixture(t, database, objectID, "terminal-prior", 3)
		corruptIntegrationPriorVersionToDeleted(t, database, objectID, 2, 20)

		getErr, listErr := assertIntegrationLifecycleCurrentReadsCorrupt(t, store, objectID)
		assertIntegrationLifecycleErrorIsPayloadFree(t, getErr, objectID)
		assertIntegrationLifecycleErrorIsPayloadFree(t, listErr, objectID)

		versionTwo := uint64(2)
		object, historicalErr := store.Get(
			context.Background(),
			testReadScope(),
			objectID,
			&versionTwo,
		)
		if !errors.Is(historicalErr, ErrCorrupt) || !reflect.DeepEqual(object, Object{}) {
			t.Fatalf("Get(active after terminal history) = %#v, %v, want zero/ErrCorrupt", object, historicalErr)
		}
		assertIntegrationLifecycleErrorIsPayloadFree(t, historicalErr, objectID)
	})

	t.Run("disabled state without preceding disable", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		const objectID = "ko-disabled-without-disable"
		insertIntegrationDisabledLifecycleFixture(t, database, objectID, false)
		corruptIntegrationVersionMutation(t, database, objectID, 2, "update")

		getErr, listErr := assertIntegrationLifecycleCurrentReadsCorrupt(t, store, objectID)
		assertIntegrationLifecycleErrorIsPayloadFree(t, getErr, objectID)
		assertIntegrationLifecycleErrorIsPayloadFree(t, listErr, objectID)
	})

	t.Run("matching lifecycle and registry marker disagrees with latest disable", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		const objectID = "ko-wrong-latest-disable"
		insertIntegrationDisabledLifecycleFixture(t, database, objectID, true)
		corruptIntegrationCurrentDisableMarkers(t, database, objectID, 3, 10)

		getErr, listErr := assertIntegrationLifecycleCurrentReadsCorrupt(t, store, objectID)
		assertIntegrationLifecycleErrorIsPayloadFree(t, getErr, objectID)
		assertIntegrationLifecycleErrorIsPayloadFree(t, listErr, objectID)
		for _, err := range []error{getErr, listErr} {
			if strings.Contains(err.Error(), "10") || strings.Contains(err.Error(), "20") {
				t.Errorf("disable chronology error disclosed marker: %v", err)
			}
		}
	})
}

func TestCurrentChronologyResourceBudgetRejectsBeforeHistoryQuery(t *testing.T) {
	tests := []struct {
		name       string
		objectIDs  []string
		registries map[string]registryRecord
	}{
		{
			name:      "one impossible object version",
			objectIDs: []string{"ko-history-over-cap"},
			registries: map[string]registryRecord{
				"ko-history-over-cap": {
					TenantID:          testTenant,
					KnowledgeObjectID: "ko-history-over-cap",
					CurrentVersion:    maximumVersionsPerTenant + 1,
				},
			},
		},
		{
			name:      "cumulative list history exceeds tenant ceiling",
			objectIDs: []string{"ko-history-budget-a", "ko-history-budget-b"},
			registries: map[string]registryRecord{
				"ko-history-budget-a": {
					TenantID:          testTenant,
					KnowledgeObjectID: "ko-history-budget-a",
					CurrentVersion:    maximumVersionsPerTenant/2 + 1,
				},
				"ko-history-budget-b": {
					TenantID:          testTenant,
					KnowledgeObjectID: "ko-history-budget-b",
					CurrentVersion:    maximumVersionsPerTenant/2 + 1,
				},
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, _ := newCatalogTestStore(t)
			var queries atomic.Int64
			callbackName := "test:lifecycle-history-budget"
			if err := database.GORMDB().Callback().Query().Before("gorm:query").Register(
				callbackName,
				func(*gorm.DB) { queries.Add(1) },
			); err != nil {
				t.Fatalf("register lifecycle history query counter: %v", err)
			}
			err := validateCurrentChronologiesBatch(
				database.GORMDB(),
				testTenant,
				test.objectIDs,
				test.registries,
				map[string]versionRecord{},
			)
			if removeErr := database.GORMDB().Callback().Query().Remove(callbackName); removeErr != nil {
				t.Fatalf("remove lifecycle history query counter: %v", removeErr)
			}
			if !errors.Is(err, ErrCorrupt) {
				t.Fatalf("over-budget chronology error = %v, want ErrCorrupt", err)
			}
			if got := queries.Load(); got != 0 {
				t.Fatalf("over-budget chronology executed %d database queries, want rejection before history work", got)
			}
			for _, objectID := range test.objectIDs {
				if strings.Contains(err.Error(), objectID) {
					t.Errorf("over-budget chronology disclosed object identity %q: %v", objectID, err)
				}
			}
		})
	}
}

func TestIntegrationLifecycleRowCorruptionFailsClosedGenerically(t *testing.T) {
	tests := []struct {
		name       string
		corruption integrationLifecycleCorruption
	}{
		{name: "missing", corruption: integrationLifecycleMissing},
		{name: "malformed", corruption: integrationLifecycleMalformed},
		{name: "version state mismatch", corruption: integrationLifecycleVersionMismatch},
		{name: "registry lifecycle mismatch", corruption: integrationLifecycleRegistryMismatch},
	}
	var currentGetError, currentListError, historicalGetError string
	for index, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Run("current Get and List", func(t *testing.T) {
				database, store := newCatalogTestStore(t)
				const objectID = "ko-current-lifecycle-corruption"
				insertIntegrationDisabledLifecycleFixture(t, database, objectID, false)
				corruptIntegrationLifecycleRow(t, database, objectID, 2, test.corruption)

				object, getErr := store.Get(context.Background(), testReadScope(), objectID, nil)
				if !errors.Is(getErr, ErrCorrupt) || !reflect.DeepEqual(object, Object{}) {
					t.Fatalf("Get(corrupt current lifecycle) = %#v, %v, want zero/ErrCorrupt", object, getErr)
				}
				page, listErr := store.List(context.Background(), testReadScope(), ListRequest{})
				if !errors.Is(listErr, ErrCorrupt) || !reflect.DeepEqual(page, ListPage{}) {
					t.Fatalf("List(corrupt current lifecycle) = %#v, %v, want zero/ErrCorrupt", page, listErr)
				}
				assertIntegrationLifecycleErrorIsPayloadFree(t, getErr, objectID)
				assertIntegrationLifecycleErrorIsPayloadFree(t, listErr, objectID)
				if index == 0 {
					currentGetError = getErr.Error()
					currentListError = listErr.Error()
				} else if getErr.Error() != currentGetError || listErr.Error() != currentListError {
					t.Errorf(
						"current lifecycle corruption disclosed subtype: Get %q/%q, List %q/%q",
						getErr,
						currentGetError,
						listErr,
						currentListError,
					)
				}
			})

			t.Run("historical Get", func(t *testing.T) {
				database, store := newCatalogTestStore(t)
				const objectID = "ko-historical-lifecycle-corruption"
				insertIntegrationDisabledLifecycleFixture(t, database, objectID, true)
				corruptIntegrationLifecycleRow(t, database, objectID, 2, test.corruption)

				versionTwo := uint64(2)
				object, getErr := store.Get(
					context.Background(),
					testReadScope(),
					objectID,
					&versionTwo,
				)
				if !errors.Is(getErr, ErrCorrupt) || !reflect.DeepEqual(object, Object{}) {
					t.Fatalf("Get(corrupt historical lifecycle) = %#v, %v, want zero/ErrCorrupt", object, getErr)
				}
				assertIntegrationLifecycleErrorIsPayloadFree(t, getErr, objectID)
				if index == 0 {
					historicalGetError = getErr.Error()
				} else if getErr.Error() != historicalGetError {
					t.Errorf(
						"historical lifecycle corruption disclosed subtype: got %q, want generic %q",
						getErr,
						historicalGetError,
					)
				}
			})
		})
	}
}

func insertIntegrationDisabledLifecycleFixture(
	t *testing.T,
	database *control.DB,
	objectID string,
	includeDisabledUpdate bool,
) {
	t.Helper()
	versions := []fixtureVersion{
		{
			definition: aliasDefinition(testApp, "lifecycle-corrupt-v1", SharingScopePrivate, nil, "lifecycle-corrupt-v1-*"),
			state:      StateActive,
			mutation:   "create",
			timestamp:  10,
		},
		{
			definition: aliasDefinition(testApp, "lifecycle-corrupt-v2", SharingScopePrivate, nil, "lifecycle-corrupt-v2-*"),
			state:      StateDisabled,
			mutation:   "disable",
			timestamp:  20,
		},
	}
	if includeDisabledUpdate {
		versions = append(versions, fixtureVersion{
			definition: aliasDefinition(testApp, "lifecycle-corrupt-v3", SharingScopePrivate, nil, "lifecycle-corrupt-v3-*"),
			state:      StateDisabled,
			mutation:   "update",
			timestamp:  30,
		})
	}
	insertFixtureObject(t, database, fixtureObject{
		id:       objectID,
		owner:    testOwner,
		versions: versions,
	})
}

func insertIntegrationActiveLifecycleFixture(
	t *testing.T,
	database *control.DB,
	objectID string,
	namePrefix string,
	versionCount int,
) {
	t.Helper()
	if versionCount < 1 {
		t.Fatalf("active lifecycle fixture version count = %d, want positive", versionCount)
	}
	versions := make([]fixtureVersion, 0, versionCount)
	for index := 1; index <= versionCount; index++ {
		name := fmt.Sprintf("%s-v%d", namePrefix, index)
		mutation := "update"
		if index == 1 {
			mutation = "create"
		}
		versions = append(versions, fixtureVersion{
			definition: aliasDefinition(testApp, name, SharingScopePrivate, nil, name+"-*"),
			state:      StateActive,
			mutation:   mutation,
			timestamp:  int64(index * 10),
		})
	}
	insertFixtureObject(t, database, fixtureObject{
		id:       objectID,
		owner:    testOwner,
		versions: versions,
	})
}

func assertIntegrationLifecycleCurrentReadsCorrupt(
	t *testing.T,
	store *Store,
	objectID string,
) (error, error) {
	t.Helper()
	object, getErr := store.Get(context.Background(), testReadScope(), objectID, nil)
	if !errors.Is(getErr, ErrCorrupt) || !reflect.DeepEqual(object, Object{}) {
		t.Fatalf("Get(corrupt current lifecycle) = %#v, %v, want zero/ErrCorrupt", object, getErr)
	}
	page, listErr := store.List(context.Background(), testReadScope(), ListRequest{})
	if !errors.Is(listErr, ErrCorrupt) || !reflect.DeepEqual(page, ListPage{}) {
		t.Fatalf("List(corrupt current lifecycle) = %#v, %v, want zero/ErrCorrupt", page, listErr)
	}
	return getErr, listErr
}

func assertIntegrationLifecyclePublicReads(
	t *testing.T,
	store *Store,
	objectID string,
	wants []integrationLifecycleWant,
) {
	t.Helper()
	if err := checkIntegrationLifecyclePublicReads(store, objectID, wants); err != nil {
		t.Fatal(err)
	}
}

func checkIntegrationLifecyclePublicReads(
	store *Store,
	objectID string,
	wants []integrationLifecycleWant,
) error {
	if len(wants) == 0 {
		return errors.New("empty lifecycle expectation")
	}
	current, err := store.Get(context.Background(), testReadScope(), objectID, nil)
	if err != nil {
		return fmt.Errorf("Get(current lifecycle): %w", err)
	}
	if err := checkIntegrationLifecycleObject(current, wants[len(wants)-1]); err != nil {
		return fmt.Errorf("current lifecycle: %w", err)
	}
	page, err := store.List(context.Background(), testReadScope(), ListRequest{})
	if err != nil {
		return fmt.Errorf("List(current lifecycle): %w", err)
	}
	if len(page.Objects) != 1 || page.Objects[0].KnowledgeObjectID != objectID {
		return fmt.Errorf("List(current lifecycle) = %#v", page)
	}
	if err := checkIntegrationLifecycleObject(page.Objects[0], wants[len(wants)-1]); err != nil {
		return fmt.Errorf("listed lifecycle: %w", err)
	}
	for _, want := range wants[:len(wants)-1] {
		version := want.version
		object, err := store.Get(context.Background(), testReadScope(), objectID, &version)
		if err != nil {
			return fmt.Errorf("Get(historical v%d): %w", want.version, err)
		}
		if err := checkIntegrationLifecycleObject(object, want); err != nil {
			return fmt.Errorf("historical v%d lifecycle: %w", want.version, err)
		}
	}
	return nil
}

func assertIntegrationLifecycleCurrentAndList(
	t *testing.T,
	store *Store,
	objectID string,
	want integrationLifecycleWant,
) {
	t.Helper()
	current, err := store.Get(context.Background(), testReadScope(), objectID, nil)
	if err != nil {
		t.Fatalf("Get(current lifecycle): %v", err)
	}
	assertIntegrationLifecycleObject(t, current, want)
	page, err := store.List(context.Background(), testReadScope(), ListRequest{})
	if err != nil {
		t.Fatalf("List(current lifecycle): %v", err)
	}
	if len(page.Objects) != 1 || page.Objects[0].KnowledgeObjectID != objectID {
		t.Fatalf("List(current lifecycle) = %#v", page)
	}
	assertIntegrationLifecycleObject(t, page.Objects[0], want)
}

func assertIntegrationLifecycleObject(
	t *testing.T,
	object Object,
	want integrationLifecycleWant,
) {
	t.Helper()
	if err := checkIntegrationLifecycleObject(object, want); err != nil {
		t.Fatal(err)
	}
}

func checkIntegrationLifecycleObject(object Object, want integrationLifecycleWant) error {
	if object.Version != want.version || object.State != want.state || object.UpdatedAt.UnixMicro() != want.updatedAt {
		return fmt.Errorf(
			"object version/state/update = %d/%q/%d, want %d/%q/%d",
			object.Version,
			object.State,
			object.UpdatedAt.UnixMicro(),
			want.version,
			want.state,
			want.updatedAt,
		)
	}
	if err := checkIntegrationLifecycleTime("DisabledAt", object.DisabledAt, want.disabledAt); err != nil {
		return err
	}
	if err := checkIntegrationLifecycleTime("QuarantinedAt", object.QuarantinedAt, want.quarantinedAt); err != nil {
		return err
	}
	if err := checkIntegrationLifecycleTime("DeletedAt", object.DeletedAt, want.deletedAt); err != nil {
		return err
	}
	if want.reason == "" {
		if object.QuarantineReason != nil {
			return fmt.Errorf("QuarantineReason = %q, want nil", *object.QuarantineReason)
		}
	} else if object.QuarantineReason == nil || *object.QuarantineReason != want.reason {
		return fmt.Errorf("QuarantineReason = %v, want %q", object.QuarantineReason, want.reason)
	}
	if want.bodyless {
		if object.Definition != nil || object.DefinitionSHA256 != nil {
			return errors.New("bodyless lifecycle object returned definition bytes")
		}
	} else if object.Definition == nil || len(object.DefinitionSHA256) != 32 {
		return errors.New("non-quarantined lifecycle object omitted definition bytes")
	}
	return nil
}

func checkIntegrationLifecycleTime(name string, got *time.Time, want int64) error {
	if want == 0 {
		if got != nil {
			return fmt.Errorf("%s = %d, want nil", name, got.UnixMicro())
		}
		return nil
	}
	if got == nil || got.UnixMicro() != want || got.Location() != time.UTC {
		return fmt.Errorf("%s = %v, want UTC microsecond %d", name, got, want)
	}
	return nil
}

func assertIntegrationLifecycleRows(
	t *testing.T,
	database *control.DB,
	objectID string,
	wants []integrationLifecycleWant,
) {
	t.Helper()
	rows := readIntegrationLifecycleRows(t, database, objectID)
	if len(rows) != len(wants) {
		t.Fatalf("lifecycle rows = %#v, want %d rows", rows, len(wants))
	}
	for index, want := range wants {
		row := rows[index]
		if row.version != int64(want.version) || row.state != string(want.state) ||
			!integrationNullInt64Equals(row.disabledAt, want.disabledAt) ||
			!integrationNullInt64Equals(row.quarantinedAt, want.quarantinedAt) ||
			!integrationNullInt64Equals(row.deletedAt, want.deletedAt) ||
			!integrationNullStringEquals(row.reason, want.reason) {
			t.Errorf("lifecycle row %d = %#v, want %#v", index, row, want)
		}
	}
}

func readIntegrationLifecycleRows(
	t *testing.T,
	database *control.DB,
	objectID string,
) []integrationLifecycleRow {
	t.Helper()
	rows, err := database.SQLDB().QueryContext(context.Background(), `
		SELECT object_version, state, disabled_at_unix_micro,
		       quarantined_at_unix_micro, deleted_at_unix_micro, quarantine_reason
		FROM knowledge_object_version_lifecycle
		WHERE tenant_id = ? AND knowledge_object_id = ?
		ORDER BY object_version
	`, testTenant, objectID)
	if err != nil {
		t.Fatalf("read lifecycle rows: %v", err)
	}
	defer rows.Close()
	var result []integrationLifecycleRow
	for rows.Next() {
		var row integrationLifecycleRow
		if err := rows.Scan(
			&row.version,
			&row.state,
			&row.disabledAt,
			&row.quarantinedAt,
			&row.deletedAt,
			&row.reason,
		); err != nil {
			t.Fatalf("scan lifecycle row: %v", err)
		}
		result = append(result, row)
	}
	if err := rows.Err(); err != nil {
		t.Fatalf("iterate lifecycle rows: %v", err)
	}
	return result
}

func integrationNullInt64Equals(got sql.NullInt64, want int64) bool {
	if want == 0 {
		return !got.Valid
	}
	return got.Valid && got.Int64 == want
}

func integrationNullStringEquals(got sql.NullString, want string) bool {
	if want == "" {
		return !got.Valid
	}
	return got.Valid && got.String == want
}

func corruptIntegrationLifecycleVersionTimestamp(
	t *testing.T,
	database *control.DB,
	objectID string,
	version int64,
	timestamp int64,
) {
	t.Helper()
	dropIntegrationTableTriggers(t, database, "knowledge_object_versions")
	connection := integrationCorruptionConnection(t, database)
	defer closeIntegrationCorruptionConnection(t, connection)
	result, err := connection.ExecContext(context.Background(), `
		UPDATE knowledge_object_versions
		SET created_at_unix_micro = ?
		WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?
	`, timestamp, testTenant, objectID, version)
	if err != nil {
		t.Fatalf("corrupt lifecycle version timestamp: %v", err)
	}
	assertIntegrationRowsAffected(t, result, "corrupt lifecycle version timestamp")
}

func corruptIntegrationPriorVersionToDeleted(
	t *testing.T,
	database *control.DB,
	objectID string,
	version int64,
	deletedAt int64,
) {
	t.Helper()
	dropIntegrationTableTriggers(t, database, "knowledge_object_versions")
	dropIntegrationTableTriggers(t, database, "knowledge_object_version_lifecycle")
	connection := integrationCorruptionConnection(t, database)
	defer closeIntegrationCorruptionConnection(t, connection)
	versionResult, err := connection.ExecContext(context.Background(), `
		UPDATE knowledge_object_versions
		SET state = 'deleted', mutation_kind = 'delete', quarantine_reason = NULL
		WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?
	`, testTenant, objectID, version)
	if err != nil {
		t.Fatalf("corrupt prior version to deleted: %v", err)
	}
	assertIntegrationRowsAffected(t, versionResult, "corrupt prior version to deleted")
	lifecycleResult, err := connection.ExecContext(context.Background(), `
		UPDATE knowledge_object_version_lifecycle
		SET state = 'deleted', disabled_at_unix_micro = NULL,
		    quarantined_at_unix_micro = NULL, deleted_at_unix_micro = ?,
		    quarantine_reason = NULL
		WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?
	`, deletedAt, testTenant, objectID, version)
	if err != nil {
		t.Fatalf("corrupt prior lifecycle to deleted: %v", err)
	}
	assertIntegrationRowsAffected(t, lifecycleResult, "corrupt prior lifecycle to deleted")
}

func corruptIntegrationVersionMutation(
	t *testing.T,
	database *control.DB,
	objectID string,
	version int64,
	mutation string,
) {
	t.Helper()
	dropIntegrationTableTriggers(t, database, "knowledge_object_versions")
	connection := integrationCorruptionConnection(t, database)
	defer closeIntegrationCorruptionConnection(t, connection)
	result, err := connection.ExecContext(context.Background(), `
		UPDATE knowledge_object_versions
		SET mutation_kind = ?
		WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?
	`, mutation, testTenant, objectID, version)
	if err != nil {
		t.Fatalf("corrupt version mutation: %v", err)
	}
	assertIntegrationRowsAffected(t, result, "corrupt version mutation")
}

func corruptIntegrationCurrentDisableMarkers(
	t *testing.T,
	database *control.DB,
	objectID string,
	version int64,
	marker int64,
) {
	t.Helper()
	dropIntegrationTableTriggers(t, database, "knowledge_objects")
	dropIntegrationTableTriggers(t, database, "knowledge_object_version_lifecycle")
	connection := integrationCorruptionConnection(t, database)
	defer closeIntegrationCorruptionConnection(t, connection)
	registryResult, err := connection.ExecContext(context.Background(), `
		UPDATE knowledge_objects
		SET disabled_at_unix_micro = ?
		WHERE tenant_id = ? AND knowledge_object_id = ? AND current_version = ?
	`, marker, testTenant, objectID, version)
	if err != nil {
		t.Fatalf("corrupt registry disable marker: %v", err)
	}
	assertIntegrationRowsAffected(t, registryResult, "corrupt registry disable marker")
	lifecycleResult, err := connection.ExecContext(context.Background(), `
		UPDATE knowledge_object_version_lifecycle
		SET disabled_at_unix_micro = ?
		WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?
	`, marker, testTenant, objectID, version)
	if err != nil {
		t.Fatalf("corrupt lifecycle disable marker: %v", err)
	}
	assertIntegrationRowsAffected(t, lifecycleResult, "corrupt lifecycle disable marker")
}

type integrationLifecycleCorruption int

const (
	integrationLifecycleMissing integrationLifecycleCorruption = iota + 1
	integrationLifecycleMalformed
	integrationLifecycleVersionMismatch
	integrationLifecycleRegistryMismatch
)

func corruptIntegrationLifecycleRow(
	t *testing.T,
	database *control.DB,
	objectID string,
	version int64,
	corruption integrationLifecycleCorruption,
) {
	t.Helper()
	if corruption == integrationLifecycleRegistryMismatch {
		dropIntegrationTableTriggers(t, database, "knowledge_objects")
	} else {
		dropIntegrationTableTriggers(t, database, "knowledge_object_version_lifecycle")
	}
	connection := integrationCorruptionConnection(t, database)
	defer closeIntegrationCorruptionConnection(t, connection)
	var (
		result sql.Result
		err    error
	)
	switch corruption {
	case integrationLifecycleMissing:
		result, err = connection.ExecContext(context.Background(), `
			DELETE FROM knowledge_object_version_lifecycle
			WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?
		`, testTenant, objectID, version)
	case integrationLifecycleMalformed:
		result, err = connection.ExecContext(context.Background(), `
			UPDATE knowledge_object_version_lifecycle
			SET deleted_at_unix_micro = 19, quarantine_reason = ?
			WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?
		`, integrationMalformedLifecycleSentinel, testTenant, objectID, version)
	case integrationLifecycleVersionMismatch:
		result, err = connection.ExecContext(context.Background(), `
			UPDATE knowledge_object_version_lifecycle
			SET state = 'active', disabled_at_unix_micro = NULL,
			    quarantined_at_unix_micro = NULL, deleted_at_unix_micro = NULL,
			    quarantine_reason = NULL
			WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?
		`, testTenant, objectID, version)
	case integrationLifecycleRegistryMismatch:
		result, err = connection.ExecContext(context.Background(), `
			UPDATE knowledge_objects
			SET disabled_at_unix_micro = created_at_unix_micro
			WHERE tenant_id = ? AND knowledge_object_id = ?
		`, testTenant, objectID)
	default:
		t.Fatalf("unknown lifecycle corruption %d", corruption)
	}
	if err != nil {
		t.Fatalf("apply lifecycle corruption %d: %v", corruption, err)
	}
	assertIntegrationRowsAffected(t, result, "apply lifecycle corruption")
}

func assertIntegrationLifecycleErrorIsPayloadFree(t *testing.T, err error, objectID string) {
	t.Helper()
	if err == nil {
		t.Fatal("nil lifecycle corruption error")
	}
	for _, payload := range []string{
		objectID,
		integrationMalformedLifecycleSentinel,
		"root_corruption",
		"dependency_recovery",
	} {
		if strings.Contains(err.Error(), payload) {
			t.Errorf("lifecycle corruption error disclosed payload %q: %v", payload, err)
		}
	}
}
