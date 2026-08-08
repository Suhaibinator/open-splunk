package knowledgecatalog

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	opensplunkv1 "github.com/Suhaibinator/open-splunk/gen/go/open_splunk/v1"
	"github.com/Suhaibinator/open-splunk/internal/clickhouse"
	"github.com/Suhaibinator/open-splunk/internal/control"
	"github.com/Suhaibinator/open-splunk/internal/knowledgedefinition"
	"github.com/Suhaibinator/open-splunk/internal/knowledgeprogram"
	"github.com/Suhaibinator/open-splunk/internal/knowledgesnapshot"
	"github.com/Suhaibinator/open-splunk/internal/plan"
	"github.com/Suhaibinator/open-splunk/internal/spl"
	"github.com/Suhaibinator/open-splunk/internal/splpath"
	"github.com/Suhaibinator/open-splunk/internal/splregex"
	"gorm.io/gorm"
)

func TestResolverEmptyCatalogRetainsDurableRevisionZero(t *testing.T) {
	_, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)

	resolved, err := resolver.Resolve(t.Context(), testResolutionScope("main", "archive", "main"))
	if err != nil {
		t.Fatalf("Resolve(empty): %v", err)
	}
	if resolved.IsZero() || !(Resolution{}).IsZero() {
		t.Fatalf("resolution zero state = resolved:%t zero:%t", resolved.IsZero(), (Resolution{}).IsZero())
	}
	summary := resolved.Summary()
	if summary.TenantID != testTenant || summary.PrincipalID != testOwner || summary.AppID != testApp ||
		summary.TenantCatalogRevision != 0 || len(summary.TenantCatalogStateToken) != 32 ||
		summary.AppCatalogRevision != nil ||
		!slices.Equal(summary.EffectiveAuthorizedIndexes, []string{"archive", "main"}) ||
		summary.ExecutableObjects != 0 || summary.Dependencies != 0 || summary.Shadows != 0 {
		t.Fatalf("empty summary = %#v", summary)
	}
	if charges := resolved.StaticCharges(); charges != (ResolutionStaticCharges{}) {
		t.Fatalf("empty static charges = %#v", charges)
	}
	if prelude := resolved.Prelude(); prelude.IsZero() || !prelude.IsEmpty() || prelude.ObjectCount() != 0 {
		t.Fatalf("empty prelude = zero:%t empty:%t objects:%d", prelude.IsZero(), prelude.IsEmpty(), prelude.ObjectCount())
	}
	// Every pointer- or slice-backed summary field is detached.
	summary.TenantCatalogStateToken[0] ^= 0xff
	summary.EffectiveAuthorizedIndexes[0] = "mutated"
	again := resolved.Summary()
	if again.TenantCatalogStateToken[0] == summary.TenantCatalogStateToken[0] ||
		again.AppCatalogRevision != nil || again.EffectiveAuthorizedIndexes[0] != "archive" {
		t.Fatalf("summary accessor retained caller memory: %#v", again)
	}
}

func TestResolutionFinalizeKeepsPreparedAuthorityOpaque(t *testing.T) {
	t.Parallel()

	_, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)
	resolved, err := resolver.Resolve(t.Context(), testResolutionScope("main"))
	if err != nil {
		t.Fatalf("Resolve(empty): %v", err)
	}
	compiled := compileResolutionQuery(t, testTenant, []string{"main"}, `index=main`, resolved.Prelude())
	snapshot, err := resolved.Finalize(compiled)
	if err != nil || snapshot.IsZero() || snapshot.Proto().GetTenantId() != testTenant {
		t.Fatalf("Finalize = (%#v, %v)", snapshot.Proto(), err)
	}
}

func TestResolutionFinalizeRejectsNonemptyAuthorityUntilKO1Prelude(t *testing.T) {
	t.Parallel()

	database, store := newCatalogTestStore(t)
	insertFixtureObject(t, database, fixtureObject{id: "ko-finalize-nonempty", versions: []fixtureVersion{{
		definition: resolutionAliasDefinition(testApp, "finalize-nonempty", SharingScopePrivate, "main"),
		state:      StateActive,
		mutation:   "create",
		timestamp:  10,
	}}})
	resolved, err := mustTestResolver(t, store).Resolve(t.Context(), testResolutionScope("main"))
	if err != nil || resolved.Summary().ExecutableObjects != 1 {
		t.Fatalf("Resolve(nonempty) = (%#v, %v)", resolved.Summary(), err)
	}
	if prelude := resolved.Prelude(); prelude.IsZero() || prelude.IsEmpty() || prelude.ObjectCount() != 1 {
		t.Fatalf("Resolve(nonempty) prelude = zero:%t empty:%t objects:%d", prelude.IsZero(), prelude.IsEmpty(), prelude.ObjectCount())
	}
	compiled := compileResolutionQuery(t, testTenant, []string{"main"}, `index=main`)
	if snapshot, finalizeErr := resolved.Finalize(compiled); !snapshot.IsZero() ||
		!errors.Is(finalizeErr, knowledgesnapshot.ErrInvalidInput) {
		t.Fatalf("Finalize(nonempty resolution) = (%#v, %v), want zero/ErrInvalidInput", snapshot, finalizeErr)
	}
}

func TestResolverPrunesBeforeWholeObjectPrecedenceAndDetaches(t *testing.T) {
	database, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)

	insertFixtureObject(t, database, fixtureObject{id: "ko-shared-global", versions: []fixtureVersion{{
		definition: resolutionAliasDefinition(testAppTwo, "shared", SharingScopeGlobal, "main"),
		state:      StateActive, mutation: "create", timestamp: 10,
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-shared-app", versions: []fixtureVersion{{
		definition: resolutionAliasDefinition(testApp, "shared", SharingScopeApp, "main"),
		state:      StateActive, mutation: "create", timestamp: 11,
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-shared-private-pruned", versions: []fixtureVersion{{
		definition: resolutionAliasDefinition(testApp, "shared", SharingScopePrivate, "other"),
		state:      StateActive, mutation: "create", timestamp: 12,
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-winning-global", versions: []fixtureVersion{{
		definition: resolutionAliasDefinition(testAppTwo, "winning", SharingScopeGlobal, "main"),
		state:      StateActive, mutation: "create", timestamp: 13,
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-winning-app", versions: []fixtureVersion{{
		definition: resolutionAliasDefinition(testApp, "winning", SharingScopeApp, "main"),
		state:      StateActive, mutation: "create", timestamp: 14,
	}}})
	insertFixtureObject(t, database, fixtureObject{id: "ko-winning-private", versions: []fixtureVersion{{
		definition: resolutionAliasDefinition(testApp, "winning", SharingScopePrivate, "main"),
		state:      StateActive, mutation: "create", timestamp: 15,
	}}})

	resolved, err := resolver.Resolve(t.Context(), testResolutionScope("main"))
	if err != nil {
		t.Fatalf("Resolve(): %v", err)
	}
	objects := resolved.Objects()
	if len(objects) != 2 || objects[0].KnowledgeObjectID != "ko-shared-app" ||
		objects[1].KnowledgeObjectID != "ko-winning-private" {
		t.Fatalf("winners = %#v", resolved.ObjectSummaries())
	}
	shadows := resolved.Shadows()
	if len(shadows) != 3 || shadows[0].KnowledgeObjectID != "ko-shared-global" ||
		shadows[1].KnowledgeObjectID != "ko-winning-app" ||
		shadows[2].KnowledgeObjectID != "ko-winning-global" {
		t.Fatalf("shadows = %#v", shadows)
	}
	for _, shadow := range shadows {
		if shadow.Definition != nil {
			t.Fatal("prepared resolution retained a loser definition body")
		}
		if shadow.KnowledgeObjectID == "ko-shared-private-pruned" {
			t.Fatal("index-impossible private candidate became a shadow")
		}
	}
	if charges := resolved.StaticCharges(); charges.GeneratedFields != 2 ||
		charges.ExtractionRegexPrograms != 0 || charges.ExtractionOutputs != 0 ||
		charges.JSONEvaluationWorkUnits != 0 || charges.ScalarExpressions != 0 {
		t.Fatalf("precedence static charges = %#v", charges)
	}

	want := resolved.Objects()
	wantShadows := resolved.Shadows()

	// Mutate every body-bearing accessor, including concurrently. The retained
	// resolution and a later snapshot must remain byte-identical under -race.
	var workers sync.WaitGroup
	for worker := 0; worker < 8; worker++ {
		workers.Add(1)
		go func(worker int) {
			defer workers.Done()
			gotObjects := resolved.Objects()
			gotObjects[0].Definition.Name = fmt.Sprintf("mutated-%d", worker)
			gotObjects[0].DefinitionSHA256[0] ^= byte(worker + 1)
			gotShadows := resolved.Shadows()
			gotShadows[0].Name = fmt.Sprintf("shadow-mutated-%d", worker)
			gotShadows[0].DefinitionSHA256[0] ^= byte(worker + 1)
		}(worker)
	}
	workers.Wait()
	second := resolved.Objects()
	if len(second) != len(want) || !bytes.Equal(second[0].DefinitionSHA256, want[0].DefinitionSHA256) ||
		!bytes.Equal(second[1].DefinitionSHA256, want[1].DefinitionSHA256) ||
		second[0].Definition.GetName() != want[0].Definition.GetName() ||
		second[1].Definition.GetName() != want[1].Definition.GetName() {
		t.Fatalf("detached objects changed: %#v", resolved.ObjectSummaries())
	}
	secondShadows := resolved.Shadows()
	if len(secondShadows) != len(wantShadows) || secondShadows[0].Definition != nil ||
		secondShadows[0].Name != wantShadows[0].Name ||
		!bytes.Equal(secondShadows[0].DefinitionSHA256, wantShadows[0].DefinitionSHA256) {
		t.Fatalf("detached shadows changed: %#v", secondShadows)
	}
}

func TestResolverCompilesEveryVisibleDefinitionBeforeSelectorPruning(t *testing.T) {
	tests := []struct {
		name       string
		definition func() *opensplunkv1.KnowledgeObjectDefinition
	}{
		{
			name: "malformed regex",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				return resolutionRegexDefinition(testAppTwo, "collision", SharingScopeGlobal, "other", `(?<field>`, []string{"field"})
			},
		},
		{
			name: "regex output mismatch",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				return resolutionRegexDefinition(testAppTwo, "collision", SharingScopeGlobal, "other", `(?<actual>\w+)`, []string{"declared"})
			},
		},
		{
			name: "unnamed regex capture",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				return resolutionRegexDefinition(testAppTwo, "collision", SharingScopeGlobal, "other", `(\w+)(?<actual>\w+)`, []string{"actual"})
			},
		},
		{
			name: "malformed JSON path",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				return resolutionJSONDefinition(testAppTwo, "collision", SharingScopeGlobal, "other", "payload..value", "field")
			},
		},
		{
			name: "malformed calculated expression",
			definition: func() *opensplunkv1.KnowledgeObjectDefinition {
				return resolutionCalculatedDefinition(testAppTwo, "collision", SharingScopeGlobal, "other", "lower(", "field")
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			resolver := mustTestResolver(t, store)
			insertFixtureObject(t, database, fixtureObject{id: "ko-valid-private", versions: []fixtureVersion{{
				definition: resolutionAliasDefinition(testApp, "collision", SharingScopePrivate, "main"),
				state:      StateActive, mutation: "create", timestamp: 10,
			}}})
			insertFixtureObject(t, database, fixtureObject{id: "ko-invalid-pruned-global", versions: []fixtureVersion{{
				definition: test.definition(), state: StateActive, mutation: "create", timestamp: 11,
			}}})

			if _, err := resolver.Resolve(t.Context(), testResolutionScope("main")); !errors.Is(err, ErrCorrupt) {
				t.Fatalf("Resolve(invalid visible pruned candidate) error = %v, want ErrCorrupt", err)
			}
		})
	}
}

func TestResolverRetainsExactWinningSemanticCharges(t *testing.T) {
	database, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)
	pattern := `(?<method>[A-Z]+)\s+(?<path>\S+)`
	insertFixtureObject(t, database, fixtureObject{id: "ko-charge-regex", versions: []fixtureVersion{{
		definition: resolutionRegexDefinition(testApp, "charge-regex", SharingScopePrivate, "main", pattern, []string{"method", "path"}),
		state:      StateActive, mutation: "create", timestamp: 10,
	}}})
	jsonPath := "payload.items{0}.name"
	insertFixtureObject(t, database, fixtureObject{id: "ko-charge-json", versions: []fixtureVersion{{
		definition: resolutionJSONDefinition(testApp, "charge-json", SharingScopePrivate, "main", jsonPath, "item_name"),
		state:      StateActive, mutation: "create", timestamp: 11,
	}}})
	expression := `if(method="GET", lower(path), "fallback")`
	insertFixtureObject(t, database, fixtureObject{id: "ko-charge-calculated", versions: []fixtureVersion{{
		definition: resolutionCalculatedDefinition(testApp, "charge-calculated", SharingScopePrivate, "main", expression, "normalized_path"),
		state:      StateActive, mutation: "create", timestamp: 12,
		dependencies: []fixtureDependency{{
			targetObjectID: "ko-charge-regex", targetVersion: 1,
		}},
	}}})

	resolved, err := resolver.Resolve(t.Context(), testResolutionScope("main"))
	if err != nil {
		t.Fatalf("Resolve(charges): %v", err)
	}
	compiled, err := splregex.CompileExtractionPattern(pattern)
	if err != nil {
		t.Fatal(err)
	}
	steps, err := splpath.ParseJSON(jsonPath)
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := spl.ParseScalarExpression(expression)
	if err != nil {
		t.Fatal(err)
	}
	analysis, err := spl.AnalyzeScalarExpression(parsed)
	if err != nil {
		t.Fatal(err)
	}
	want := ResolutionStaticCharges{
		GeneratedFields:          4,
		ExtractionRegexPrograms:  1,
		ExtractionRegexWorkUnits: uint64(compiled.ProgramWorkUnits),
		ExtractionOutputs:        3,
		JSONEvaluationWorkUnits:  uint32(splpath.EvaluationWorkUnits(steps)),
		ScalarExpressions:        1,
		ScalarExpressionNodes:    analysis.Nodes,
		ScalarPredicates:         analysis.Predicates,
	}
	if got := resolved.StaticCharges(); got != want {
		t.Fatalf("StaticCharges() = %#v, want %#v", got, want)
	}
}

func TestResolverRejectsPossibleParallelDestinationCollisionsAndChains(t *testing.T) {
	tests := []struct {
		name          string
		leftSource    string
		leftTarget    string
		rightSource   string
		rightTarget   string
		leftSelector  string
		rightSelector string
		wantCorrupt   bool
	}{
		{
			name: "overlapping destination collision", leftSource: "left", leftTarget: "shared",
			rightSource: "right", rightTarget: "shared", wantCorrupt: true,
		},
		{
			name: "overlapping same-stage chain", leftSource: "raw", leftTarget: "intermediate",
			rightSource: "intermediate", rightTarget: "result", wantCorrupt: true,
		},
		{
			name: "literal-disjoint collision", leftSource: "left", leftTarget: "shared",
			rightSource: "right", rightTarget: "shared", leftSelector: "main", rightSelector: "audit",
		},
		{
			name: "ambiguous wildcard collision fails closed", leftSource: "left", leftTarget: "shared",
			rightSource: "right", rightTarget: "shared", leftSelector: "worker-*", rightSelector: "api-*", wantCorrupt: true,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			resolver := mustTestResolver(t, store)
			left := resolutionAliasDefinition(testApp, "parallel-left", SharingScopePrivate, test.leftSelector)
			left.GetFieldAlias().SourceField = test.leftSource
			left.GetFieldAlias().DestinationField = test.leftTarget
			right := resolutionAliasDefinition(testApp, "parallel-right", SharingScopePrivate, test.rightSelector)
			right.GetFieldAlias().SourceField = test.rightSource
			right.GetFieldAlias().DestinationField = test.rightTarget
			insertFixtureObject(t, database, fixtureObject{id: "ko-parallel-left", versions: []fixtureVersion{{
				definition: left, state: StateActive, mutation: "create", timestamp: 10,
			}}})
			insertFixtureObject(t, database, fixtureObject{id: "ko-parallel-right", versions: []fixtureVersion{{
				definition: right, state: StateActive, mutation: "create", timestamp: 11,
			}}})

			resolved, err := resolver.Resolve(t.Context(), testResolutionScope("main", "audit", "worker-1", "api-1"))
			if test.wantCorrupt {
				if !resolved.IsZero() || !errors.Is(err, ErrCorrupt) {
					t.Fatalf("Resolve() = (%+v, %v), want zero/ErrCorrupt", resolved, err)
				}
				return
			}
			if err != nil || len(resolved.ObjectSummaries()) != 2 {
				t.Fatalf("Resolve() = (%+v, %v), want two winners", resolved.ObjectSummaries(), err)
			}
		})
	}
}

func TestResolverRejectsAggregateSemanticWorkBeyondQueryLimits(t *testing.T) {
	t.Run("combined regex and JSON outputs", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		for index := range 4 {
			pattern, outputs := resolutionNamedCapturePattern(fmt.Sprintf("object_%02d", index), 16)
			insertFixtureObject(t, database, fixtureObject{id: fmt.Sprintf("ko-output-regex-%02d", index), versions: []fixtureVersion{{
				definition: resolutionRegexDefinition(testApp, fmt.Sprintf("output-regex-%02d", index), SharingScopePrivate, "main", pattern, outputs),
				state:      StateActive, mutation: "create", timestamp: int64(10 + index),
			}}})
		}
		insertFixtureObject(t, database, fixtureObject{id: "ko-output-json", versions: []fixtureVersion{{
			definition: resolutionJSONDefinition(testApp, "output-json", SharingScopePrivate, "main", "payload.value", "json_output"),
			state:      StateActive, mutation: "create", timestamp: 20,
		}}})
		if _, err := resolver.Resolve(t.Context(), testResolutionScope("main")); !errors.Is(err, control.ErrCapacityExceeded) {
			t.Fatalf("Resolve(65 extraction outputs) error = %v, want ErrCapacityExceeded", err)
		}
	})

	t.Run("JSON evaluation work", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		for index := range 11 {
			insertFixtureObject(t, database, fixtureObject{id: fmt.Sprintf("ko-json-work-%02d", index), versions: []fixtureVersion{{
				definition: resolutionJSONDefinition(testApp, fmt.Sprintf("json-work-%02d", index), SharingScopePrivate, "main", "payload.value", fmt.Sprintf("json_%02d", index)),
				state:      StateActive, mutation: "create", timestamp: int64(10 + index),
			}}})
		}
		if _, err := resolver.Resolve(t.Context(), testResolutionScope("main")); !errors.Is(err, control.ErrCapacityExceeded) {
			t.Fatalf("Resolve(JSON work 33) error = %v, want ErrCapacityExceeded", err)
		}
	})

	t.Run("calculated predicate leaves", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		for index := range 17 {
			insertFixtureObject(t, database, fixtureObject{id: fmt.Sprintf("ko-predicate-%02d", index), versions: []fixtureVersion{{
				definition: resolutionCalculatedDefinition(testApp, fmt.Sprintf("predicate-%02d", index), SharingScopePrivate, "main", `if(a=1 AND b=2, a, b)`, fmt.Sprintf("calculated_%02d", index)),
				state:      StateActive, mutation: "create", timestamp: int64(10 + index),
			}}})
		}
		if _, err := resolver.Resolve(t.Context(), testResolutionScope("main")); !errors.Is(err, control.ErrCapacityExceeded) {
			t.Fatalf("Resolve(34 predicates) error = %v, want ErrCapacityExceeded", err)
		}
	})
}

func TestResolverVisibleLoserCorruptionFailsClosedButHiddenBodyIsNotRead(t *testing.T) {
	t.Run("visible loser", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		insertFixtureObject(t, database, fixtureObject{id: "ko-corrupt-global", versions: []fixtureVersion{{
			definition: resolutionAliasDefinition(testAppTwo, "collision", SharingScopeGlobal, "main"),
			state:      StateActive, mutation: "create", timestamp: 10,
		}}})
		insertFixtureObject(t, database, fixtureObject{id: "ko-good-private", versions: []fixtureVersion{{
			definition: resolutionAliasDefinition(testApp, "collision", SharingScopePrivate, "main"),
			state:      StateActive, mutation: "create", timestamp: 11,
		}}})
		dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
		mustExec(t, database, `UPDATE knowledge_definition_blobs
			SET definition_proto = X'00', definition_bytes = 1
			WHERE tenant_id = ? AND definition_digest = (
				SELECT definition_digest FROM knowledge_objects
				WHERE tenant_id = ? AND knowledge_object_id = 'ko-corrupt-global'
			)`, testTenant, testTenant)

		if _, err := resolver.Resolve(t.Context(), testResolutionScope("main")); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Resolve(corrupt visible loser) error = %v, want ErrCorrupt", err)
		}
	})

	t.Run("visible loser dependency selector", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		target := dependencyExtractionDefinition(
			testAppTwo,
			"loser-dependency-target",
			SharingScopeGlobal,
			nil,
			"",
			dependencyFixtureInputField,
		)
		target.Selector = &opensplunkv1.KnowledgeSelector{
			IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "other"}},
		}
		insertFixtureObject(t, database, fixtureObject{id: "ko-loser-dependency-target", versions: []fixtureVersion{{
			definition: target, state: StateActive, mutation: "create", timestamp: 10,
		}}})
		source := dependencyAliasDefinition(
			testAppTwo,
			"collision",
			SharingScopeGlobal,
			nil,
			"",
			dependencyFixtureInputField,
			"loser-output",
		)
		source.Selector = &opensplunkv1.KnowledgeSelector{
			IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: "main"}},
		}
		insertFixtureObject(t, database, fixtureObject{id: "ko-invalid-global-loser", versions: []fixtureVersion{{
			definition: source, state: StateActive, mutation: "create", timestamp: 11,
			dependencies: []fixtureDependency{{targetObjectID: "ko-loser-dependency-target", targetVersion: 1}},
		}}})
		insertFixtureObject(t, database, fixtureObject{id: "ko-private-winner", versions: []fixtureVersion{{
			definition: resolutionAliasDefinition(testApp, "collision", SharingScopePrivate, "main"),
			state:      StateActive, mutation: "create", timestamp: 12,
		}}})

		if _, err := resolver.Resolve(t.Context(), testResolutionScope("main", "other")); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Resolve(corrupt visible loser dependency) error = %v, want ErrCorrupt", err)
		}
	})

	t.Run("hidden body", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		insertFixtureObject(t, database, fixtureObject{id: "ko-visible", versions: []fixtureVersion{{
			definition: resolutionAliasDefinition(testApp, "visible", SharingScopePrivate, "main"),
			state:      StateActive, mutation: "create", timestamp: 10,
		}}})
		insertFixtureObject(t, database, fixtureObject{id: "ko-hidden", owner: "owner-b", versions: []fixtureVersion{{
			definition: resolutionAliasDefinition(testApp, "hidden", SharingScopePrivate, "main"),
			state:      StateActive, mutation: "create", timestamp: 11,
		}}})
		dropTrigger(t, database, "knowledge_definition_blob_update_is_forbidden")
		mustExec(t, database, `UPDATE knowledge_definition_blobs
			SET definition_proto = X'00', definition_bytes = 1
			WHERE tenant_id = ? AND definition_digest = (
				SELECT definition_digest FROM knowledge_objects
				WHERE tenant_id = ? AND knowledge_object_id = 'ko-hidden'
			)`, testTenant, testTenant)

		resolved, err := resolver.Resolve(t.Context(), testResolutionScope("main"))
		if err != nil || len(resolved.ObjectSummaries()) != 1 ||
			resolved.ObjectSummaries()[0].KnowledgeObjectID != "ko-visible" {
			t.Fatalf("Resolve(with corrupt hidden body) = %#v, %v", resolved.ObjectSummaries(), err)
		}
	})
}

func TestResolverRejectsActiveGlobalObjectFromArchivedDefiningApp(t *testing.T) {
	database, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)
	insertFixtureObject(t, database, fixtureObject{id: "ko-archived-app-global", versions: []fixtureVersion{{
		definition: resolutionAliasDefinition(testAppTwo, "archived-app-global", SharingScopeGlobal, "main"),
		state:      StateActive, mutation: "create", timestamp: 10,
	}}})

	// Simulate stored-authority corruption that bypassed the normal invariant:
	// an app defining an ACTIVE global object may not be archived. The bounded
	// bodyless candidate projection must detect the missing active app before
	// any definition hydration can treat the global object as executable.
	dropTrigger(t, database, "knowledge_active_app_workspace_cannot_be_archived")
	mustExec(t, database, `UPDATE app_workspaces
		SET version = version + 1,
			state = 'archived',
			updated_at_unix_micro = updated_at_unix_micro + 1,
			archived_at_unix_micro = updated_at_unix_micro + 1
		WHERE tenant_id = ? AND app_id = ?`, testTenant, testAppTwo)

	var hydratedDefinition bool
	const callbackName = "test:resolver-archived-global-before-hydration"
	if err := database.GORMDB().Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		if strings.Contains(tx.Statement.SQL.String(), "knowledge_definition_blobs") {
			hydratedDefinition = true
		}
	}); err != nil {
		t.Fatalf("register query observer: %v", err)
	}
	if _, err := resolver.Resolve(t.Context(), testResolutionScope("main")); !errors.Is(err, ErrCorrupt) {
		t.Fatalf("Resolve(active global from archived app) error = %v, want ErrCorrupt", err)
	}
	if err := database.GORMDB().Callback().Query().Remove(callbackName); err != nil {
		t.Fatalf("remove query observer: %v", err)
	}
	if hydratedDefinition {
		t.Fatal("resolver hydrated a definition after defining-app authority failed")
	}
}

func TestResolverRequiresExactWinningDependencyClosure(t *testing.T) {
	t.Run("complete", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		insertResolutionDependencyPair(t, database, SharingScopePrivate, "", "main")

		resolved, err := resolver.Resolve(t.Context(), testResolutionScope("main"))
		if err != nil {
			t.Fatalf("Resolve(complete closure): %v", err)
		}
		dependencies := resolved.Dependencies()
		if len(dependencies) != 1 || dependencies[0].SourceObjectID != "ko-dependency-source" ||
			dependencies[0].TargetObjectID != "ko-dependency-target" {
			t.Fatalf("dependencies = %#v", dependencies)
		}
		if charges := resolved.StaticCharges(); charges.GeneratedFields != 2 ||
			charges.ExtractionOutputs != 1 || charges.JSONEvaluationWorkUnits != 3 {
			t.Fatalf("dependency static charges = %#v", charges)
		}
	})

	t.Run("pruned target", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		insertResolutionDependencyPair(t, database, SharingScopeGlobal, "", "main")
		insertFixtureObject(t, database, fixtureObject{id: "ko-dependency-target-winner", versions: []fixtureVersion{{
			definition: dependencyExtractionDefinition(
				testApp, "dependency-target", SharingScopePrivate, nil, "", dependencyFixtureInputField,
			),
			state: StateActive, mutation: "create", timestamp: 12,
		}}})

		if _, err := resolver.Resolve(t.Context(), testResolutionScope("main")); !errors.Is(err, control.ErrDependencyConflict) {
			t.Fatalf("Resolve(pruned target) error = %v, want ErrDependencyConflict", err)
		}
	})

	t.Run("source selector does not imply target", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		insertResolutionDependencyPair(t, database, SharingScopePrivate, "other", "main")

		if _, err := resolver.Resolve(t.Context(), testResolutionScope("main", "other")); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Resolve(disjoint dependency selectors) error = %v, want ErrCorrupt", err)
		}
	})

	t.Run("conservative wildcard containment rejection", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		insertResolutionDependencyPair(t, database, SharingScopePrivate, "m*", "main*")

		if _, err := resolver.Resolve(t.Context(), testResolutionScope("main-prod")); !errors.Is(err, ErrCorrupt) {
			t.Fatalf("Resolve(nontrivial wildcard containment) error = %v, want conservative ErrCorrupt", err)
		}
	})

	t.Run("source literal is covered by target wildcard", func(t *testing.T) {
		database, store := newCatalogTestStore(t)
		resolver := mustTestResolver(t, store)
		insertResolutionDependencyPair(t, database, SharingScopePrivate, "m*", "main-prod")

		resolved, err := resolver.Resolve(t.Context(), testResolutionScope("main-prod"))
		if err != nil || len(resolved.Dependencies()) != 1 {
			t.Fatalf("Resolve(literal covered by wildcard) dependencies = %#v, %v", resolved.Dependencies(), err)
		}
	})
}

func TestResolverHydratesEachCandidateAuthorityOnce(t *testing.T) {
	database, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)
	// The source ID sorts before its target, so a root-by-root semantic walk
	// would re-open the later target unless the complete hydration batch seeded
	// every decoded current authority first.
	insertResolutionDependencyPair(t, database, SharingScopePrivate, "", "main")

	var versionQueries, dependencyQueries, definitionBlobQueries atomic.Int64
	const callbackName = "test:resolver-single-hydration-authority-sweep"
	if err := database.GORMDB().Callback().Query().After("gorm:query").Register(callbackName, func(tx *gorm.DB) {
		sqlText := tx.Statement.SQL.String()
		if strings.Contains(sqlText, "FROM knowledge_object_versions AS version") &&
			strings.Contains(sqlText, "object.current_version = version.object_version") {
			versionQueries.Add(1)
		}
		if strings.Contains(sqlText, "knowledge_object_dependencies AS dependency") {
			dependencyQueries.Add(1)
		}
		if strings.Contains(sqlText, "knowledge_definition_blobs") {
			definitionBlobQueries.Add(1)
		}
	}); err != nil {
		t.Fatalf("register resolver query observer: %v", err)
	}
	resolved, err := resolver.Resolve(t.Context(), testResolutionScope("main"))
	if removeErr := database.GORMDB().Callback().Query().Remove(callbackName); removeErr != nil {
		t.Fatalf("remove resolver query observer: %v", removeErr)
	}
	if err != nil {
		t.Fatalf("Resolve(single hydration sweep): %v", err)
	}
	if len(resolved.ObjectSummaries()) != 2 || len(resolved.Dependencies()) != 1 {
		t.Fatalf("resolved authority = objects:%#v dependencies:%#v", resolved.ObjectSummaries(), resolved.Dependencies())
	}
	if got := versionQueries.Load(); got != 2 {
		t.Fatalf("current-version batch queries = %d, want one width and one payload query", got)
	}
	if got := dependencyQueries.Load(); got != 3 {
		t.Fatalf("dependency batch queries = %d, want physical, aggregate, and payload queries", got)
	}
	if got := definitionBlobQueries.Load(); got != 2 {
		t.Fatalf("definition-blob batch queries = %d, want one width and one payload query", got)
	}
}

func TestResolverCancellationAndFailFastGate(t *testing.T) {
	database, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)
	secondResolver := mustTestResolver(t, store)
	if resolver.gate != secondResolver.gate {
		t.Fatal("resolvers over one Store do not share one admission gate")
	}
	secondStore, err := New(database, Options{CursorKey: testCursorKey})
	if err != nil {
		t.Fatalf("New(second Store): %v", err)
	}
	thirdResolver := mustTestResolver(t, secondStore)
	if resolver.gate != thirdResolver.gate {
		t.Fatal("resolvers over one control database do not share one admission gate")
	}
	scope := testResolutionScope("main")

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolver.Resolve(canceled, scope); !errors.Is(err, context.Canceled) {
		t.Fatalf("Resolve(canceled) error = %v, want context.Canceled", err)
	}

	for range MaximumConcurrentResolutions {
		if !resolver.gate.TryAcquire() {
			t.Fatal("shared resolver gate saturated before its documented bound")
		}
	}
	started := time.Now()
	if _, err := thirdResolver.Resolve(t.Context(), scope); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("Resolve(second Store while shared gate saturated) error = %v, want ErrCapacityExceeded", err)
	}
	if elapsed := time.Since(started); elapsed > 50*time.Millisecond {
		t.Fatalf("saturated resolver waited %v", elapsed)
	}
	for range MaximumConcurrentResolutions {
		resolver.gate.Release()
	}
}

func TestResolveCandidatePrecedenceBoundsWinnersBeforePairwiseWork(t *testing.T) {
	objects := make([]Object, knowledgesnapshot.MaximumExecutableObjects+1)
	for index := range objects {
		name := fmt.Sprintf("capacity-%03d", index)
		definition := resolutionAliasDefinition(testApp, name, SharingScopePrivate, "main")
		normalized, err := knowledgedefinition.Normalize(definition)
		if err != nil {
			t.Fatal(err)
		}
		objects[index] = Object{
			KnowledgeObjectID: fmt.Sprintf("ko-capacity-%03d", index),
			TenantID:          testTenant,
			AppID:             testApp,
			OwnerID:           testOwner,
			ObjectType:        ObjectTypeFieldAlias,
			Name:              normalized.Name,
			Version:           1,
			SharingScope:      SharingScopePrivate,
			State:             StateActive,
			Definition:        normalized.Definition,
			DefinitionSHA256:  bytes.Clone(normalized.Digest[:]),
		}
	}
	scope := normalizedResolutionScope{
		tenantID: testTenant, principalID: testOwner, appID: testApp, indexes: []string{"main"},
	}
	var charges ResolutionStaticCharges
	if _, err := resolveCandidatePrecedence(
		t.Context(), scope, catalogState{}, make([]byte, 32), objects, nil, nil, &charges,
	); !errors.Is(err, control.ErrCapacityExceeded) {
		t.Fatalf("resolveCandidatePrecedence(257 winners) error = %v, want ErrCapacityExceeded", err)
	}

	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := resolveCandidatePrecedence(
		canceled, scope, catalogState{}, make([]byte, 32), objects, nil, nil, &charges,
	); !errors.Is(err, context.Canceled) {
		t.Fatalf("resolveCandidatePrecedence(canceled) error = %v, want context.Canceled", err)
	}
}

func TestResolverConcurrentPublicationReturnsOnlyOldOrNewAuthority(t *testing.T) {
	database, store := newCatalogTestStore(t)
	resolver := mustTestResolver(t, store)
	insertFixtureObject(t, database, fixtureObject{id: "ko-resolution-atomic", versions: []fixtureVersion{{
		definition: resolutionAliasDefinition(testApp, "old-name", SharingScopePrivate, "main"),
		state:      StateActive, mutation: "create", timestamp: 10,
	}}})
	oldResolution, err := resolver.Resolve(t.Context(), testResolutionScope("main"))
	if err != nil {
		t.Fatalf("Resolve(old): %v", err)
	}
	oldState := readIntegrationCatalogState(t, database)
	if oldResolution.Summary().TenantCatalogRevision != uint64(oldState.revision) {
		t.Fatalf(
			"old resolution revision = %d, want persisted %d",
			oldResolution.Summary().TenantCatalogRevision,
			oldState.revision,
		)
	}

	staged, _ := stageIntegrationKnownPublication(
		t,
		database,
		"ko-resolution-atomic",
		resolutionAliasDefinition(testApp, "new-name", SharingScopePrivate, "main"),
		StateActive,
		"update",
		20,
	)
	barrier := installIntegrationCatalogStateBarrier(t, database)
	normalizedScope, err := normalizeResolutionScope(testResolutionScope("main"))
	if err != nil {
		t.Fatalf("normalize paused resolution scope: %v", err)
	}
	resolveContext, cancelResolve := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancelResolve()
	result := make(chan struct {
		resolved Resolution
		err      error
	}, 1)
	go func() {
		// The barrier intentionally suspends the read transaction across an
		// external commit. Exercise the same single-attempt core without spending
		// the public 250 ms admission budget on test synchronization.
		resolved, resolveErr := resolver.resolveOnce(resolveContext, normalizedScope)
		result <- struct {
			resolved Resolution
			err      error
		}{resolved: resolved, err: resolveErr}
	}()
	barrier.waitUntilEstablished(t)
	if err := staged.Commit(); err != nil {
		t.Fatalf("commit staged publication: %v", err)
	}
	barrier.release()
	paused := <-result
	if paused.err != nil {
		t.Fatalf("Resolve(paused across commit): %v", paused.err)
	}
	barrier.assertOldStateAcrossCommit(t, oldState)
	if err := barrier.remove(); err != nil {
		t.Fatalf("remove catalog-state barrier: %v", err)
	}
	objects := paused.resolved.ObjectSummaries()
	if len(objects) != 1 || objects[0].Name != "old-name" ||
		paused.resolved.Summary().TenantCatalogRevision != uint64(oldState.revision) {
		t.Fatalf("paused resolution = summary:%#v objects:%#v, want old authority", paused.resolved.Summary(), objects)
	}

	fresh, err := resolver.Resolve(t.Context(), testResolutionScope("main"))
	if err != nil {
		t.Fatalf("Resolve(after commit): %v", err)
	}
	freshObjects := fresh.ObjectSummaries()
	if len(freshObjects) != 1 || freshObjects[0].Name != "new-name" ||
		fresh.Summary().TenantCatalogRevision != uint64(oldState.revision+1) {
		t.Fatalf("fresh resolution = summary:%#v objects:%#v, want new authority", fresh.Summary(), freshObjects)
	}
}

func mustTestResolver(t *testing.T, store *Store) *Resolver {
	t.Helper()
	resolver, err := NewResolver(store, ResolverOptions{})
	if err != nil {
		t.Fatalf("NewResolver(): %v", err)
	}
	return resolver
}

func testResolutionScope(indexes ...string) ResolutionScope {
	return ResolutionScope{
		TenantID:                   testTenant,
		PrincipalID:                testOwner,
		AppID:                      testApp,
		EffectiveAuthorizedIndexes: indexes,
	}
}

func compileResolutionQuery(
	t *testing.T,
	tenantID string,
	indexes []string,
	source string,
	prelude ...knowledgeprogram.Program,
) clickhouse.CompiledQuery {
	t.Helper()
	parsed, err := spl.Parse(source)
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	visibility := uint64(1)
	logical, err := plan.Build(parsed, plan.Scope{
		TenantID:          tenantID,
		AuthorizedIndexes: slices.Clone(indexes),
		Earliest:          time.Date(2026, 8, 1, 0, 0, 0, 0, time.UTC),
		Latest:            time.Date(2026, 8, 2, 0, 0, 0, 0, time.UTC),
		SearchStart:       time.Date(2026, 8, 2, 0, 0, 1, 0, time.UTC),
		SearchTimezone:    "UTC",
		IndexTimeCutoff:   time.Date(2026, 8, 2, 0, 0, 2, 0, time.UTC),
		VisibilityCutoff:  &visibility,
	})
	if err != nil {
		t.Fatalf("Build: %v", err)
	}
	if len(prelude) > 1 {
		t.Fatalf("compile resolution query: %d preludes", len(prelude))
	}
	if len(prelude) == 1 {
		logical, err = plan.InjectKnowledgePrelude(logical, prelude[0])
		if err != nil {
			t.Fatalf("InjectKnowledgePrelude: %v", err)
		}
	}
	compiled, err := (clickhouse.Compiler{}).Compile(logical)
	if err != nil {
		t.Fatalf("Compile: %v", err)
	}
	return compiled
}

func resolutionAliasDefinition(
	appID, name string,
	scope SharingScope,
	indexPattern string,
) *opensplunkv1.KnowledgeObjectDefinition {
	definition := aliasDefinition(appID, name, scope, nil, "")
	definition.GetFieldAlias().DestinationField = "resolved_" + strings.ReplaceAll(name, "-", "_")
	if indexPattern != "" {
		definition.Selector = &opensplunkv1.KnowledgeSelector{
			IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: indexPattern}},
		}
	}
	return definition
}

func resolutionRegexDefinition(
	appID, name string,
	scope SharingScope,
	indexPattern, pattern string,
	outputs []string,
) *opensplunkv1.KnowledgeObjectDefinition {
	definition := resolutionAliasDefinition(appID, name, scope, indexPattern)
	definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
		FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw",
			Extraction: &opensplunkv1.FieldExtractionDefinition_Regex{
				Regex: &opensplunkv1.RegexFieldExtractionDefinition{
					Pattern: pattern, OutputFields: slices.Clone(outputs),
				},
			},
		},
	}
	return definition
}

func resolutionJSONDefinition(
	appID, name string,
	scope SharingScope,
	indexPattern, path, output string,
) *opensplunkv1.KnowledgeObjectDefinition {
	definition := resolutionAliasDefinition(appID, name, scope, indexPattern)
	definition.Body = &opensplunkv1.KnowledgeObjectDefinition_FieldExtraction{
		FieldExtraction: &opensplunkv1.FieldExtractionDefinition{
			InputField: "_raw",
			Extraction: &opensplunkv1.FieldExtractionDefinition_Json{
				Json: &opensplunkv1.JsonFieldExtractionDefinition{Path: path, OutputField: output},
			},
		},
	}
	return definition
}

func resolutionCalculatedDefinition(
	appID, name string,
	scope SharingScope,
	indexPattern, expression, output string,
) *opensplunkv1.KnowledgeObjectDefinition {
	definition := resolutionAliasDefinition(appID, name, scope, indexPattern)
	definition.Body = &opensplunkv1.KnowledgeObjectDefinition_CalculatedField{
		CalculatedField: &opensplunkv1.CalculatedFieldDefinition{
			Expression: expression, DestinationField: output,
		},
	}
	return definition
}

func resolutionNamedCapturePattern(prefix string, count int) (string, []string) {
	var pattern strings.Builder
	outputs := make([]string, count)
	for index := range count {
		outputs[index] = fmt.Sprintf("%s_output_%02d", prefix, index)
		fmt.Fprintf(&pattern, "(?<%s>x)", outputs[index])
	}
	return pattern.String(), outputs
}

func insertResolutionDependencyPair(
	t *testing.T,
	database *control.DB,
	targetScope SharingScope,
	targetIndex, sourceIndex string,
) {
	t.Helper()
	target := dependencyExtractionDefinition(
		testApp,
		"dependency-target",
		targetScope,
		nil,
		"",
		dependencyFixtureInputField,
	)
	if targetIndex != "" {
		target.Selector = &opensplunkv1.KnowledgeSelector{
			IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: targetIndex}},
		}
	}
	insertFixtureObject(t, database, fixtureObject{id: "ko-dependency-target", versions: []fixtureVersion{{
		definition: target, state: StateActive, mutation: "create", timestamp: 10,
	}}})
	source := dependencyAliasDefinition(
		testApp,
		"dependency-source",
		SharingScopePrivate,
		nil,
		"",
		dependencyFixtureInputField,
		"dependency-output",
	)
	if sourceIndex != "" {
		source.Selector = &opensplunkv1.KnowledgeSelector{
			IndexPatterns: []*opensplunkv1.KnowledgeSelectorPattern{{Value: sourceIndex}},
		}
	}
	insertFixtureObject(t, database, fixtureObject{id: "ko-dependency-source", versions: []fixtureVersion{{
		definition: source, state: StateActive, mutation: "create", timestamp: 11,
		dependencies: []fixtureDependency{{targetObjectID: "ko-dependency-target", targetVersion: 1}},
	}}})
}
