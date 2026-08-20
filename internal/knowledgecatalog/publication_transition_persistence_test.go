package knowledgecatalog

import (
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	opensplunk "github.com/Suhaibinator/open-splunk/gen/go/open_splunk"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"gorm.io/gorm"
)

func TestValidatePublicationTransitionPersistenceTargetsExactAndDetached(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	seedPublicationTransitionPersistenceTarget(t, database, "ko-transition-persist-target", false)
	expected, sourceStage, authority := publicationTransitionPersistenceTestAuthority(
		t,
		database,
		"ko-transition-persist-target",
	)
	tx := publicationTransitionPersistenceTestTransaction(t, database)

	projection, err := validatePublicationTransitionPersistenceTargets(
		t.Context(),
		tx,
		testTenant,
		expected,
		sourceStage,
		authority,
	)
	if err != nil || len(projection) != 1 ||
		projection[0].targetObjectID != "ko-transition-persist-target" ||
		projection[0].targetVersion != 1 {
		t.Fatalf("validate exact persistence targets = (%#v, %v)", projection, err)
	}
	projection[0].targetObjectID = "caller-mutated"
	retained := authority.databaseProjection()
	if len(retained) != 1 || retained[0].targetObjectID != "ko-transition-persist-target" {
		t.Fatalf("returned projection aliases authority = %#v", retained)
	}
}

func TestValidatePublicationTransitionPersistenceTargetsPresentEmpty(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	expected := publicationTransitionPersistenceTestCandidate()
	authority := candidateDependencyAuthority{state: &candidateDependencyAuthorityState{
		candidate:   expected,
		sourceStage: opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
	}}
	if _, err := validatePublicationTransitionPersistenceTargets(
		t.Context(),
		database.GORMDB(),
		testTenant,
		expected,
		opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
		authority,
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("base-handle persistence authority error = %v, want invalid argument", err)
	}
	tx := publicationTransitionPersistenceTestTransaction(t, database)
	projection, err := validatePublicationTransitionPersistenceTargets(
		t.Context(),
		tx,
		testTenant,
		expected,
		opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
		authority,
	)
	if err != nil || len(projection) != 0 {
		t.Fatalf("validate present-empty persistence targets = (%#v, %v)", projection, err)
	}
	if _, err := validatePublicationTransitionPersistenceTargets(
		t.Context(),
		tx,
		testTenant,
		expected,
		opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
		candidateDependencyAuthority{},
	); !errors.Is(err, control.ErrInvalidArgument) {
		t.Fatalf("zero persistence authority error = %v, want invalid argument", err)
	}
}

func TestValidatePublicationTransitionPersistenceTargetsRejectsAuthorityDrift(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	seedPublicationTransitionPersistenceTarget(t, database, "ko-transition-persist-drift", false)
	expected, sourceStage, base := publicationTransitionPersistenceTestAuthority(
		t,
		database,
		"ko-transition-persist-drift",
	)
	tx := publicationTransitionPersistenceTestTransaction(t, database)

	tests := []struct {
		name   string
		mutate func(*candidateDependencyAuthority)
		want   error
	}{
		{
			name: "candidate binding",
			mutate: func(authority *candidateDependencyAuthority) {
				authority.state.candidate.ownerID = "owner-other"
			},
			want: control.ErrDependencyConflict,
		},
		{
			name: "source stage",
			mutate: func(authority *candidateDependencyAuthority) {
				authority.state.sourceStage = opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS
			},
			want: control.ErrDependencyConflict,
		},
		{
			name: "digest",
			mutate: func(authority *candidateDependencyAuthority) {
				authority.state.targets[0].definitionDigest[0] ^= 0xff
			},
			want: ErrCorrupt,
		},
		{
			name: "owner",
			mutate: func(authority *candidateDependencyAuthority) {
				authority.state.targets[0].ownerID = "owner-other"
			},
			want: ErrCorrupt,
		},
		{
			name: "type stage",
			mutate: func(authority *candidateDependencyAuthority) {
				authority.state.targets[0].targetStage =
					opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS
			},
			want: ErrCorrupt,
		},
		{
			name: "role",
			mutate: func(authority *candidateDependencyAuthority) {
				authority.state.targets[0].role =
					opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_UNSPECIFIED
			},
			want: ErrCorrupt,
		},
		{
			name: "projection",
			mutate: func(authority *candidateDependencyAuthority) {
				authority.state.projection[0].targetVersion++
			},
			want: ErrCorrupt,
		},
		{
			name: "duplicate",
			mutate: func(authority *candidateDependencyAuthority) {
				authority.state.targets = append(
					authority.state.targets,
					authority.state.targets[0],
				)
				authority.state.projection = append(
					authority.state.projection,
					authority.state.projection[0],
				)
			},
			want: ErrCorrupt,
		},
		{
			name: "over-bound target count",
			mutate: func(authority *candidateDependencyAuthority) {
				authority.state.targets = make(
					[]publicationDerivedDependencyTarget,
					maximumDependenciesPerVersion+1,
				)
				authority.state.projection = make(
					[]publicationDependency,
					maximumDependenciesPerVersion+1,
				)
			},
			want: ErrCorrupt,
		},
		{
			name: "over-bound target object identity",
			mutate: func(authority *candidateDependencyAuthority) {
				objectID := strings.Repeat("o", maximumObjectIDBytes+1)
				authority.state.targets[0].objectID = objectID
				authority.state.projection[0].targetObjectID = objectID
			},
			want: ErrCorrupt,
		},
		{
			name: "over-bound target owner identity",
			mutate: func(authority *candidateDependencyAuthority) {
				authority.state.targets[0].ownerID = strings.Repeat("o", maximumOwnerIDBytes+1)
			},
			want: ErrCorrupt,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			authority := base.detached()
			test.mutate(&authority)
			if _, err := validatePublicationTransitionPersistenceTargets(
				t.Context(),
				tx,
				testTenant,
				expected,
				sourceStage,
				authority,
			); !errors.Is(err, test.want) {
				t.Fatalf("validate drift error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestValidatePublicationTransitionPersistenceTargetsRequiresCurrentActiveClosure(t *testing.T) {
	t.Run("noncurrent", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		seedPublicationTransitionPersistenceTarget(t, database, "ko-transition-persist-noncurrent", true)
		expected, sourceStage, authority := publicationTransitionPersistenceTestAuthority(
			t,
			database,
			"ko-transition-persist-noncurrent",
		)
		authority.state.targets[0].version = 1
		authority.state.projection[0].targetVersion = 1
		tx := publicationTransitionPersistenceTestTransaction(t, database)
		if _, err := validatePublicationTransitionPersistenceTargets(
			t.Context(), tx, testTenant, expected, sourceStage, authority,
		); !errors.Is(err, control.ErrDependencyConflict) {
			t.Fatalf("noncurrent target error = %v, want dependency conflict", err)
		}
	})

	t.Run("inactive", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		seedPublicationTransitionPersistenceInactiveTarget(
			t,
			database,
			"ko-transition-persist-inactive",
		)
		expected, sourceStage, authority := publicationTransitionPersistenceTestAuthority(
			t,
			database,
			"ko-transition-persist-inactive",
		)
		tx := publicationTransitionPersistenceTestTransaction(t, database)
		if _, err := validatePublicationTransitionPersistenceTargets(
			t.Context(), tx, testTenant, expected, sourceStage, authority,
		); !errors.Is(err, control.ErrDependencyConflict) {
			t.Fatalf("inactive target error = %v, want dependency conflict", err)
		}
	})

	t.Run("missing", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		seedPublicationTransitionPersistenceTarget(t, database, "ko-transition-persist-template", false)
		expected, sourceStage, authority := publicationTransitionPersistenceTestAuthority(
			t,
			database,
			"ko-transition-persist-template",
		)
		authority.state.targets[0].objectID = "ko-transition-persist-missing"
		authority.state.projection[0].targetObjectID = "ko-transition-persist-missing"
		tx := publicationTransitionPersistenceTestTransaction(t, database)
		if _, err := validatePublicationTransitionPersistenceTargets(
			t.Context(), tx, testTenant, expected, sourceStage, authority,
		); !errors.Is(err, control.ErrDependencyConflict) {
			t.Fatalf("missing target error = %v, want dependency conflict", err)
		}
	})

	t.Run("historical only", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		seedPublicationTransitionPersistenceTarget(t, database, "ko-transition-persist-source", false)
		expected, sourceStage, authority := publicationTransitionPersistenceTestAuthority(
			t,
			database,
			"ko-transition-persist-source",
		)
		const historicalID = "ko-transition-persist-historical"
		seedPublicationTransitionHistoricalOnlyTarget(t, database, historicalID, authority.state.targets[0])
		authority.state.targets[0].objectID = historicalID
		authority.state.projection[0].targetObjectID = historicalID
		tx := publicationTransitionPersistenceTestTransaction(t, database)
		if _, err := validatePublicationTransitionPersistenceTargets(
			t.Context(), tx, testTenant, expected, sourceStage, authority,
		); !errors.Is(err, control.ErrDependencyConflict) {
			t.Fatalf("historical-only target error = %v, want dependency conflict", err)
		}
	})

	t.Run("cross tenant", func(t *testing.T) {
		database, _ := newCatalogTestStore(t)
		seedPublicationTransitionPersistenceTarget(t, database, "ko-transition-persist-cross", false)
		expected, sourceStage, authority := publicationTransitionPersistenceTestAuthority(
			t,
			database,
			"ko-transition-persist-cross",
		)
		tx := publicationTransitionPersistenceTestTransaction(t, database)
		if _, err := validatePublicationTransitionPersistenceTargets(
			t.Context(), tx, "tenant-other", expected, sourceStage, authority,
		); !errors.Is(err, control.ErrDependencyConflict) {
			t.Fatalf("cross-tenant target error = %v, want dependency conflict", err)
		}
	})
}

func TestValidatePublicationTransitionPersistenceTargetsHonorsCancellation(t *testing.T) {
	database, _ := newCatalogTestStore(t)
	seedPublicationTransitionPersistenceTarget(t, database, "ko-transition-persist-cancel", false)
	expected, sourceStage, authority := publicationTransitionPersistenceTestAuthority(
		t,
		database,
		"ko-transition-persist-cancel",
	)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	tx := publicationTransitionPersistenceTestTransaction(t, database)
	if _, err := validatePublicationTransitionPersistenceTargets(
		ctx, tx, testTenant, expected, sourceStage, authority,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled target validation error = %v, want context canceled", err)
	}
}

func publicationTransitionPersistenceTestTransaction(
	t *testing.T,
	database *control.DB,
) *gorm.DB {
	t.Helper()
	tx := database.GORMDB().Begin()
	if tx.Error != nil {
		t.Fatalf("begin publication persistence test transaction: %v", tx.Error)
	}
	t.Cleanup(func() {
		if err := tx.Rollback().Error; err != nil {
			t.Errorf("roll back publication persistence test transaction: %v", err)
		}
	})
	return tx
}

func publicationTransitionPersistenceTestCandidate() publicationCandidateAuthority {
	digest := sha256.Sum256([]byte("publication-transition-persistence-candidate"))
	return publicationCandidateAuthority{
		objectID:         "ko-transition-persist-candidate",
		version:          2,
		definitionDigest: digest,
		ownerID:          testOwner,
	}
}

func publicationTransitionPersistenceTestAuthority(
	t *testing.T,
	database *control.DB,
	targetID string,
) (
	publicationCandidateAuthority,
	opensplunk.KnowledgeSearchStage,
	candidateDependencyAuthority,
) {
	t.Helper()
	registry, found, err := readRegistryRecord(database.GORMDB().Model(&registryRecord{}).Where(
		"tenant_id = ? AND knowledge_object_id = ?",
		testTenant,
		targetID,
	))
	if err != nil || !found {
		t.Fatalf("read persistence target registry = (%#v, %t, %v)", registry, found, err)
	}
	version, found, err := readVersionRecord(
		database.GORMDB(),
		testTenant,
		targetID,
		registry.CurrentVersion,
	)
	if err != nil || !found {
		t.Fatalf("read persistence target version = (%#v, %t, %v)", version, found, err)
	}
	objectType, typeValid := objectTypeToProto(version.ObjectType)
	targetStage, _, stageValid := publicationStageForObjectType(objectType)
	if !typeValid || !stageValid {
		t.Fatalf("persistence target type/stage = (%v, %v)", objectType, targetStage)
	}
	var targetDigest [sha256.Size]byte
	copy(targetDigest[:], version.DefinitionDigest)
	expected := publicationTransitionPersistenceTestCandidate()
	sourceStage := opensplunk.KnowledgeSearchStage_KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD
	return expected, sourceStage, candidateDependencyAuthority{state: &candidateDependencyAuthorityState{
		candidate:   expected,
		sourceStage: sourceStage,
		targets: []publicationDerivedDependencyTarget{{
			objectID:         targetID,
			version:          version.ObjectVersion,
			definitionDigest: targetDigest,
			ownerID:          version.OwnerID,
			role:             opensplunk.KnowledgeDependencyRole_KNOWLEDGE_DEPENDENCY_ROLE_FIELD_INPUT,
			targetStage:      targetStage,
		}},
		projection: []publicationDependency{{
			targetObjectID: targetID,
			targetVersion:  version.ObjectVersion,
		}},
	}}
}

func seedPublicationTransitionPersistenceTarget(
	t *testing.T,
	database *control.DB,
	objectID string,
	advance bool,
) {
	t.Helper()
	description := "publication persistence target"
	versions := []fixtureVersion{{
		definition: dependencyExtractionDefinition(
			testApp,
			"publication-persistence-target-"+objectID,
			SharingScopePrivate,
			&description,
			"main",
			dependencyFixtureInputField,
		),
		state:     StateActive,
		mutation:  "create",
		timestamp: 10,
	}}
	if advance {
		updatedDescription := "publication persistence target updated"
		versions = append(versions, fixtureVersion{
			definition: dependencyExtractionDefinition(
				testApp,
				"publication-persistence-target-"+objectID,
				SharingScopePrivate,
				&updatedDescription,
				"main",
				dependencyFixtureInputField,
			),
			state:     StateActive,
			mutation:  "update",
			timestamp: 20,
		})
	}
	insertFixtureObject(t, database, fixtureObject{
		id:       objectID,
		owner:    testOwner,
		versions: versions,
	})
}

func seedPublicationTransitionPersistenceInactiveTarget(
	t *testing.T,
	database *control.DB,
	objectID string,
) {
	t.Helper()
	description := "publication persistence inactive target"
	definition := dependencyExtractionDefinition(
		testApp,
		"publication-persistence-inactive-"+objectID,
		SharingScopePrivate,
		&description,
		"main",
		dependencyFixtureInputField,
	)
	insertFixtureObject(t, database, fixtureObject{
		id:    objectID,
		owner: testOwner,
		versions: []fixtureVersion{
			{definition: definition, state: StateActive, mutation: "create", timestamp: 10},
			{definition: definition, state: StateDisabled, mutation: "disable", timestamp: 20},
		},
	})
}

func seedPublicationTransitionHistoricalOnlyTarget(
	t *testing.T,
	database *control.DB,
	objectID string,
	target publicationDerivedDependencyTarget,
) {
	t.Helper()
	connection, err := database.SQLDB().Conn(t.Context())
	if err != nil {
		t.Fatalf("open historical-only target connection: %v", err)
	}
	defer connection.Close()
	if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = OFF`); err != nil {
		t.Fatalf("disable historical-only target foreign keys: %v", err)
	}
	defer func() {
		if _, err := connection.ExecContext(t.Context(), `PRAGMA foreign_keys = ON`); err != nil {
			t.Errorf("restore historical-only target foreign keys: %v", err)
		}
	}()
	result, err := connection.ExecContext(t.Context(), `INSERT INTO knowledge_object_versions (
		tenant_id, knowledge_object_id, object_version,
		app_id, owner_id, object_type, name, sharing_scope, state,
		definition_digest, dependency_count, mutation_kind,
		quarantine_reason, created_at_unix_micro
	) SELECT ?, ?, 1, app_id, owner_id, object_type, ?, sharing_scope, 'active',
		definition_digest, 0, 'create', NULL, 30
	FROM knowledge_object_versions
	WHERE tenant_id = ? AND knowledge_object_id = ? AND object_version = ?`,
		testTenant,
		objectID,
		"publication-persistence-historical",
		testTenant,
		target.objectID,
		target.version,
	)
	if err != nil {
		t.Fatalf("seed historical-only target: %v", err)
	}
	rows, err := result.RowsAffected()
	if err != nil || rows != 1 {
		t.Fatalf("seed historical-only target = rows:%d error:%v", rows, err)
	}
}
