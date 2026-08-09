package knowledgecatalog

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"strings"
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestPublicationTransitionPersistenceAuthorityActiveProjectionIsPrivateAndDetached(
	t *testing.T,
) {
	harness := newPublicationTransitionPersistenceActiveHarness(t)
	if harness.authority.IsZero() {
		t.Fatal("minted ACTIVE persistence authority is zero")
	}

	projection, err := harness.authority.validateAndProject(
		t.Context(),
		harness.tx,
		harness.read.catalog,
		testTenant,
		harness.plan,
		harness.dependencies,
	)
	if err != nil || len(projection) != 1 ||
		projection[0].targetObjectID != "ko-authority-active-target" ||
		projection[0].targetVersion != 1 || harness.dependencies != nil {
		t.Fatalf("validate ACTIVE authority = (%#v, %v)", projection, err)
	}

	projection[0].targetObjectID = "ko-caller-mutated"
	again, err := harness.authority.validateAndProject(
		t.Context(),
		harness.tx,
		harness.read.catalog,
		testTenant,
		harness.plan,
		harness.dependencies,
	)
	if err != nil || len(again) != 1 || again[0].targetObjectID != "ko-authority-active-target" {
		t.Fatalf("revalidate after returned projection mutation = (%#v, %v)", again, err)
	}

	supplied := []publicationDependency{{
		targetObjectID: "ko-authority-active-target",
		targetVersion:  1,
	}}
	if _, err := harness.authority.validateAndProject(
		t.Context(), harness.tx, harness.read.catalog, testTenant, harness.plan, supplied,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("caller-supplied ACTIVE projection error = %v, want invalid argument", err)
	}

	mismatched := harness.plan
	mismatched.definition.name = "authority-active-other"
	if _, err := harness.authority.validateAndProject(
		t.Context(), harness.tx, harness.read.catalog, testTenant, mismatched, nil,
	); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("mismatched ACTIVE binding error = %v, want dependency conflict", err)
	}
	if _, err := harness.authority.validateAndProject(
		t.Context(), harness.tx, harness.read.catalog, "tenant-authority-swap", harness.plan, nil,
	); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("swapped ACTIVE tenant error = %v, want dependency conflict", err)
	}
}

func TestPublicationTransitionPersistenceAuthorityInactiveProjectionMatchesRetainedRows(
	t *testing.T,
) {
	harness := newPublicationTransitionPersistenceInactiveHarness(t)
	projection, err := harness.authority.validateAndProject(
		t.Context(),
		harness.tx,
		harness.read.catalog,
		testTenant,
		harness.plan,
		harness.dependencies,
	)
	if err != nil || len(projection) != 1 ||
		projection[0].targetObjectID != "ko-authority-inactive-target" ||
		projection[0].targetVersion != 1 {
		t.Fatalf("validate inactive authority = (%#v, %v)", projection, err)
	}

	projection[0].targetObjectID = "ko-caller-mutated"
	again, err := harness.authority.validateAndProject(
		t.Context(),
		harness.tx,
		harness.read.catalog,
		testTenant,
		harness.plan,
		harness.dependencies,
	)
	if err != nil || len(again) != 1 || again[0].targetObjectID != "ko-authority-inactive-target" {
		t.Fatalf("revalidate inactive authority after mutation = (%#v, %v)", again, err)
	}

	mismatched := []publicationDependency{{
		targetObjectID: "ko-authority-inactive-other",
		targetVersion:  1,
	}}
	if _, err := harness.authority.validateAndProject(
		t.Context(), harness.tx, harness.read.catalog, testTenant, harness.plan, mismatched,
	); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("mismatched inactive dependency error = %v, want dependency conflict", err)
	}

	malformed := harness.plan
	malformed.definition.digest = nil
	if _, err := harness.authority.validateAndProject(
		t.Context(), harness.tx, harness.read.catalog, testTenant, malformed, harness.dependencies,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("malformed plan endpoint error = %v, want invalid argument", err)
	}
	beforeSwap := harness.plan
	current := *beforeSwap.current
	current.Name = "authority-inactive-swapped"
	beforeSwap.current = &current
	if _, err := harness.authority.validateAndProject(
		t.Context(), harness.tx, harness.read.catalog, testTenant, beforeSwap, harness.dependencies,
	); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("swapped before endpoint error = %v, want dependency conflict", err)
	}
	if _, err := harness.authority.validateAndProject(
		t.Context(),
		harness.tx,
		harness.read.catalog,
		"tenant-inactive-swap",
		harness.plan,
		harness.dependencies,
	); !errors.Is(err, control.ErrDependencyConflict) {
		t.Fatalf("swapped inactive tenant error = %v, want dependency conflict", err)
	}
}

func TestPublicationTransitionPersistenceAuthorityPreservesPresentEmptyActiveProjection(
	t *testing.T,
) {
	harness := newPublicationTransitionPersistencePresentEmptyActiveHarness(t)
	if harness.authority.IsZero() {
		t.Fatal("present-empty ACTIVE persistence authority is zero")
	}
	projection, err := harness.authority.validateAndProject(
		t.Context(),
		harness.tx,
		harness.read.catalog,
		testTenant,
		harness.plan,
		harness.dependencies,
	)
	if err != nil || len(projection) != 0 {
		t.Fatalf("validate present-empty ACTIVE authority = (%#v, %v)", projection, err)
	}
	second, err := harness.authority.validateAndProject(
		t.Context(),
		harness.tx,
		harness.read.catalog,
		testTenant,
		harness.plan,
		harness.dependencies,
	)
	if err != nil || len(second) != 0 {
		t.Fatalf("revalidate present-empty ACTIVE authority = (%#v, %v)", second, err)
	}
}

func TestPublicationTransitionPersistenceAuthorityBindsExactTransaction(t *testing.T) {
	harness := newPublicationTransitionPersistenceInactiveHarness(t)
	transaction, ok := harness.tx.Statement.ConnPool.(*sql.Tx)
	if !ok || transaction == nil || harness.read.transaction != transaction {
		t.Fatalf("inventory transaction identity = read:%p tx:%p", harness.read.transaction, transaction)
	}

	if _, err := harness.authority.validateAndProject(
		t.Context(),
		harness.database.GORMDB(),
		harness.read.catalog,
		testTenant,
		harness.plan,
		harness.dependencies,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("base-handle validation error = %v, want invalid argument", err)
	}

	otherDatabase, _ := newCatalogTestStore(t)
	otherTx := beginPublicationTransitionPersistenceAuthorityTestTransaction(t, otherDatabase)
	if _, err := harness.authority.validateAndProject(
		t.Context(),
		otherTx,
		harness.read.catalog,
		testTenant,
		harness.plan,
		harness.dependencies,
	); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("different-transaction validation error = %v, want version conflict", err)
	}
	if _, err := mintPublicationTransitionPersistenceAuthority(
		t.Context(),
		otherTx,
		harness.read,
	); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("different-transaction mint error = %v, want version conflict", err)
	}
	malformedRead := harness.read
	malformedRead.transaction = nil
	if _, err := mintPublicationTransitionPersistenceAuthority(
		t.Context(), harness.tx, malformedRead,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("malformed inventory-read mint error = %v, want invalid argument", err)
	}

	malformedCatalog := harness.read.catalog
	malformedCatalog.token = "not-a-canonical-catalog-token"
	if _, err := harness.authority.validateAndProject(
		t.Context(), harness.tx, malformedCatalog, testTenant, harness.plan, harness.dependencies,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("malformed old catalog error = %v, want invalid argument", err)
	}
	staleCatalog := harness.read.catalog
	staleCatalog.revision++
	if _, err := harness.authority.validateAndProject(
		t.Context(), harness.tx, staleCatalog, testTenant, harness.plan, harness.dependencies,
	); !errors.Is(err, control.ErrVersionConflict) {
		t.Fatalf("stale old catalog error = %v, want version conflict", err)
	}

	canceled, cancel := context.WithCancel(t.Context())
	cancel()
	if _, err := mintPublicationTransitionPersistenceAuthority(
		canceled, harness.tx, harness.read,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled authority mint error = %v, want context canceled", err)
	}
	if _, err := harness.authority.validateAndProject(
		canceled, harness.tx, harness.read.catalog, testTenant, harness.plan, harness.dependencies,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled authority validation error = %v, want context canceled", err)
	}
	if !(publicationTransitionPersistenceAuthority{}).IsZero() {
		t.Fatal("zero persistence authority reports nonzero")
	}
}

func TestPublicationTransitionPersistenceAuthorityClassifiesFactChanges(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*testing.T, *publicationTransitionPersistenceAuthorityTestHarness)
		want   error
	}{
		{
			name: "catalog advanced",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE knowledge_catalog_tenants
					SET catalog_revision = catalog_revision + 1
					WHERE tenant_id = ?`, testTenant)
			},
			want: control.ErrVersionConflict,
		},
		{
			name: "catalog token changed without revision",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				replacement := bytes.Repeat([]byte{0xa5}, catalogStateTokenBytes)
				current, err := catalogStateTokenValue(harness.read.catalog)
				if err != nil {
					t.Fatalf("decode bound catalog token: %v", err)
				}
				if bytes.Equal(replacement, current) {
					replacement = bytes.Repeat([]byte{0x5a}, catalogStateTokenBytes)
				}
				execPublicationTransitionPersistenceAuthorityTest(
					t, harness.tx, "DROP TRIGGER knowledge_catalog_revision_head_transition_is_exact",
				)
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE knowledge_catalog_revision_heads
					SET state_token = ?
					WHERE tenant_id = ?`, replacement, testTenant)
			},
			want: ErrCorrupt,
		},
		{
			name: "catalog regressed",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				for _, trigger := range []string{
					"knowledge_catalog_revision_transition_is_valid",
					"knowledge_catalog_revision_rotates_state_token",
					"knowledge_catalog_revision_head_transition_is_exact",
				} {
					execPublicationTransitionPersistenceAuthorityTest(
						t, harness.tx, "DROP TRIGGER "+trigger,
					)
				}
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE knowledge_catalog_tenants
					SET catalog_revision = catalog_revision - 1
					WHERE tenant_id = ?`, testTenant)
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE knowledge_catalog_revision_heads
					SET catalog_revision = catalog_revision - 1
					WHERE tenant_id = ?`, testTenant)
			},
			want: ErrCorrupt,
		},
		{
			name: "catalog revision reused token",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				boundToken, err := catalogStateTokenValue(harness.read.catalog)
				if err != nil {
					t.Fatalf("decode bound catalog token: %v", err)
				}
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE knowledge_catalog_tenants
					SET catalog_revision = catalog_revision + 1
					WHERE tenant_id = ?`, testTenant)
				execPublicationTransitionPersistenceAuthorityTest(
					t, harness.tx, "DROP TRIGGER knowledge_catalog_revision_head_transition_is_exact",
				)
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE knowledge_catalog_revision_heads
					SET state_token = ?
					WHERE tenant_id = ?`, boundToken, testTenant)
			},
			want: ErrCorrupt,
		},
		{
			name: "app catalog advanced",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE app_catalog_revisions
					SET revision = revision + 1
					WHERE tenant_id = ?`, testTenant)
			},
			want: control.ErrVersionConflict,
		},
		{
			name: "index catalog advanced",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE index_catalog_state
					SET revision = revision + 1, physical_count = physical_count
					WHERE singleton_id = 1`)
			},
			want: control.ErrVersionConflict,
		},
		{
			name: "index count changed without revision",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				execPublicationTransitionPersistenceAuthorityTest(
					t, harness.tx, "DROP TRIGGER index_catalog_state_transition_is_valid",
				)
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE index_catalog_state
					SET physical_count = physical_count - 1
					WHERE singleton_id = 1`)
			},
			want: ErrCorrupt,
		},
		{
			name: "index count regressed with revision",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				execPublicationTransitionPersistenceAuthorityTest(
					t, harness.tx, "DROP TRIGGER index_catalog_state_transition_is_valid",
				)
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE index_catalog_state
					SET revision = revision + 1, physical_count = physical_count - 1
					WHERE singleton_id = 1`)
			},
			want: ErrCorrupt,
		},
		{
			name: "catalog head missing",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				execPublicationTransitionPersistenceAuthorityTest(
					t, harness.tx, "DROP TRIGGER knowledge_catalog_revision_head_delete_is_forbidden",
				)
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					DELETE FROM knowledge_catalog_revision_heads WHERE tenant_id = ?`, testTenant)
			},
			want: ErrCorrupt,
		},
		{
			name: "app catalog missing",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				execPublicationTransitionPersistenceAuthorityTest(
					t, harness.tx, "DROP TRIGGER app_catalog_revision_delete_is_forbidden",
				)
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					DELETE FROM app_catalog_revisions WHERE tenant_id = ?`, testTenant)
			},
			want: ErrCorrupt,
		},
		{
			name: "index catalog missing",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				execPublicationTransitionPersistenceAuthorityTest(
					t, harness.tx, "DROP TRIGGER index_catalog_state_delete_is_forbidden",
				)
				execPublicationTransitionPersistenceAuthorityTest(
					t, harness.tx, "DELETE FROM index_catalog_state WHERE singleton_id = 1",
				)
			},
			want: ErrCorrupt,
		},
		{
			name: "app catalog regressed",
			mutate: func(t *testing.T, harness *publicationTransitionPersistenceAuthorityTestHarness) {
				execPublicationTransitionPersistenceAuthorityTest(
					t, harness.tx, "DROP TRIGGER app_catalog_revision_transition_is_exact",
				)
				execPublicationTransitionPersistenceAuthorityTest(t, harness.tx, `
					UPDATE app_catalog_revisions
					SET revision = revision - 1
					WHERE tenant_id = ?`, testTenant)
			},
			want: ErrCorrupt,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			harness := newPublicationTransitionPersistenceInactiveHarness(t)
			test.mutate(t, &harness)
			if _, err := harness.authority.validateAndProject(
				t.Context(),
				harness.tx,
				harness.read.catalog,
				testTenant,
				harness.plan,
				harness.dependencies,
			); !errors.Is(err, test.want) {
				t.Fatalf("validation after %s error = %v, want %v", test.name, err, test.want)
			}
		})
	}
}

type publicationTransitionPersistenceAuthorityTestHarness struct {
	database     *control.DB
	tx           *gorm.DB
	read         publicationActiveTransitionInventoryRead
	authority    publicationTransitionPersistenceAuthority
	plan         publicationPlan
	dependencies []publicationDependency
}

func newPublicationTransitionPersistenceActiveHarness(
	t *testing.T,
) publicationTransitionPersistenceAuthorityTestHarness {
	t.Helper()
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-authority-active-target",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: publicationTransitionTestIndexDefinition(
				dependencyExtractionDefinition(
					testApp,
					"authority-active-target",
					SharingScopePrivate,
					nil,
					"",
					dependencyFixtureInputField,
				),
				"main",
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}},
	})
	createPublicationTransitionTestIndex(t, database, "main")
	tx := beginPublicationTransitionPersistenceAuthorityTestTransaction(t, database)
	initial := readPublicationTransitionPersistenceAuthorityTestInventory(
		t, store, tx, publicationTransitionEndpoint{}, publicationTransitionEndpoint{},
	)
	if publicationTransitionPersistenceAuthorityTestWinner(
		t, initial.inventory, "ko-authority-active-target",
	).object.KnowledgeObjectID == "" {
		t.Fatal("ACTIVE target is absent from inventory")
	}
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-authority-active-candidate",
		1,
		publicationTransitionTestIndexDefinition(
			dependencyAliasDefinition(
				testApp,
				"authority-active-candidate",
				SharingScopePrivate,
				nil,
				"",
				dependencyFixtureInputField,
				"authority_active_alias",
			),
			"main",
		),
	)}
	read := readPublicationTransitionPersistenceAuthorityTestInventory(
		t,
		store,
		tx,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
	)
	authority, err := mintPublicationTransitionPersistenceAuthority(t.Context(), tx, read)
	if err != nil {
		t.Fatalf("mint ACTIVE persistence authority: %v", err)
	}
	plan := publicationTransitionPersistenceAuthorityTestPlan(
		t,
		read,
		candidate,
		StateActive,
		nil,
		nil,
	)
	return publicationTransitionPersistenceAuthorityTestHarness{
		database:  database,
		tx:        tx,
		read:      read,
		authority: authority,
		plan:      plan,
	}
}

func newPublicationTransitionPersistencePresentEmptyActiveHarness(
	t *testing.T,
) publicationTransitionPersistenceAuthorityTestHarness {
	t.Helper()
	database, store := newCatalogTestStore(t)
	createPublicationTransitionTestIndex(t, database, "main")
	tx := beginPublicationTransitionPersistenceAuthorityTestTransaction(t, database)
	candidate := publicationWinner{object: publicationTestObject(
		t,
		"ko-authority-present-empty",
		1,
		publicationTransitionTestIndexDefinition(
			dependencyExtractionDefinition(
				testApp,
				"authority-present-empty",
				SharingScopePrivate,
				nil,
				"",
				"authority_present_empty_output",
			),
			"main",
		),
	)}
	read := readPublicationTransitionPersistenceAuthorityTestInventory(
		t,
		store,
		tx,
		publicationTransitionEndpoint{},
		publicationTransitionEndpoint{present: true, state: StateActive, winner: candidate},
	)
	authority, err := mintPublicationTransitionPersistenceAuthority(t.Context(), tx, read)
	if err != nil {
		t.Fatalf("mint present-empty ACTIVE persistence authority: %v", err)
	}
	return publicationTransitionPersistenceAuthorityTestHarness{
		database:  database,
		tx:        tx,
		read:      read,
		authority: authority,
		plan: publicationTransitionPersistenceAuthorityTestPlan(
			t,
			read,
			candidate,
			StateActive,
			nil,
			nil,
		),
	}
}

func newPublicationTransitionPersistenceInactiveHarness(
	t *testing.T,
) publicationTransitionPersistenceAuthorityTestHarness {
	t.Helper()
	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-authority-inactive-target",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: publicationTransitionTestIndexDefinition(
				dependencyExtractionDefinition(
					testApp,
					"authority-inactive-target",
					SharingScopePrivate,
					nil,
					"",
					dependencyFixtureInputField,
				),
				"main",
			),
			state: StateActive, mutation: "create", timestamp: 10,
		}},
	})
	insertFixtureObject(t, database, fixtureObject{
		id:    "ko-authority-inactive-source",
		owner: testOwner,
		versions: []fixtureVersion{{
			definition: publicationTransitionTestIndexDefinition(
				dependencyAliasDefinition(
					testApp,
					"authority-inactive-source",
					SharingScopePrivate,
					nil,
					"",
					dependencyFixtureInputField,
					"authority_inactive_alias",
				),
				"main",
			),
			state: StateActive, mutation: "create", timestamp: 20,
			dependencies: []fixtureDependency{{
				targetObjectID: "ko-authority-inactive-target",
				targetVersion:  1,
			}},
		}},
	})
	createPublicationTransitionTestIndex(t, database, "main")
	tx := beginPublicationTransitionPersistenceAuthorityTestTransaction(t, database)
	initial := readPublicationTransitionPersistenceAuthorityTestInventory(
		t, store, tx, publicationTransitionEndpoint{}, publicationTransitionEndpoint{},
	)
	beforeWinner := publicationTransitionPersistenceAuthorityTestWinner(
		t,
		initial.inventory,
		"ko-authority-inactive-source",
	)
	afterWinner := publicationCloneWinner(beforeWinner)
	afterWinner.object.Version++
	read := readPublicationTransitionPersistenceAuthorityTestInventory(
		t,
		store,
		tx,
		publicationTransitionEndpoint{present: true, state: StateActive, winner: beforeWinner},
		publicationTransitionEndpoint{present: true, state: StateDisabled, winner: afterWinner},
	)
	authority, err := mintPublicationTransitionPersistenceAuthority(t.Context(), tx, read)
	if err != nil {
		t.Fatalf("mint inactive persistence authority: %v", err)
	}
	dependencies := make([]publicationDependency, len(afterWinner.existingDependencies))
	for index, dependency := range afterWinner.existingDependencies {
		dependencies[index] = publicationDependency{
			targetObjectID: strings.Clone(dependency.targetObjectID),
			targetVersion:  dependency.targetVersion,
		}
	}
	current, found, err := readRegistryRecord(tx.WithContext(t.Context()).Model(&registryRecord{}).Where(
		"tenant_id = ? AND knowledge_object_id = ?",
		testTenant,
		afterWinner.object.KnowledgeObjectID,
	))
	if err != nil || !found {
		t.Fatalf("read inactive persistence plan current = (%#v, %t, %v)", current, found, err)
	}
	plan := publicationTransitionPersistenceAuthorityTestPlan(
		t,
		read,
		afterWinner,
		StateDisabled,
		&current,
		dependencies,
	)
	canonicalDependencies, err := canonicalPublicationDependencies(plan)
	if err != nil {
		t.Fatalf("canonicalize inactive persistence plan dependencies: %v", err)
	}
	return publicationTransitionPersistenceAuthorityTestHarness{
		database:     database,
		tx:           tx,
		read:         read,
		authority:    authority,
		plan:         plan,
		dependencies: canonicalDependencies,
	}
}

func publicationTransitionPersistenceAuthorityTestPlan(
	t *testing.T,
	read publicationActiveTransitionInventoryRead,
	winner publicationWinner,
	state State,
	current *registryRecord,
	dependencies []publicationDependency,
) publicationPlan {
	t.Helper()
	objectType, typeValid := objectTypeFromProto(winner.object.ObjectType)
	sharingScope, scopeValid := sharingScopeFromProto(winner.object.SharingScope)
	if !typeValid || !scopeValid {
		t.Fatalf(
			"persistence plan object type/scope = (%v, %v)",
			winner.object.ObjectType,
			winner.object.SharingScope,
		)
	}
	return publicationPlan{
		objectID: winner.object.KnowledgeObjectID,
		version:  int64(winner.object.Version),
		state:    state,
		definition: definitionAuthority{
			digest:       bytes.Clone(winner.object.DefinitionSHA256),
			objectType:   objectType,
			appID:        winner.object.AppID,
			name:         winner.object.Name,
			sharingScope: sharingScope,
		},
		dependencies:    clonePublicationTransitionProjection(dependencies),
		ownerID:         winner.object.OwnerID,
		current:         current,
		oldCatalogState: read.catalog,
	}
}

func beginPublicationTransitionPersistenceAuthorityTestTransaction(
	t *testing.T,
	database *control.DB,
) *gorm.DB {
	t.Helper()
	transaction := database.GORMDB().Begin()
	if transaction.Error != nil {
		t.Fatalf("begin persistence authority test transaction: %v", transaction.Error)
	}
	t.Cleanup(func() {
		if err := transaction.Rollback().Error; err != nil {
			t.Errorf("roll back persistence authority test transaction: %v", err)
		}
	})
	return transaction
}

func readPublicationTransitionPersistenceAuthorityTestInventory(
	t *testing.T,
	store *Store,
	tx *gorm.DB,
	before publicationTransitionEndpoint,
	after publicationTransitionEndpoint,
) publicationActiveTransitionInventoryRead {
	t.Helper()
	read, err := store.readPublicationActiveTransitionInventory(tx, testTenant, before, after)
	if err != nil {
		t.Fatalf("read persistence authority inventory: %v", err)
	}
	return read
}

func publicationTransitionPersistenceAuthorityTestWinner(
	t *testing.T,
	inventory publicationActiveTransitionInventory,
	objectID string,
) publicationWinner {
	t.Helper()
	for _, winner := range inventory.currentActive {
		if winner.object.KnowledgeObjectID == objectID {
			return publicationCloneWinner(winner)
		}
	}
	t.Fatalf("inventory has no winner %q", objectID)
	return publicationWinner{}
}

func execPublicationTransitionPersistenceAuthorityTest(
	t *testing.T,
	tx *gorm.DB,
	query string,
	arguments ...any,
) {
	t.Helper()
	if err := tx.WithContext(t.Context()).Exec(query, arguments...).Error; err != nil {
		t.Fatalf("execute persistence authority mutation: %v", err)
	}
}
