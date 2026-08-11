package knowledgecatalog

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

func TestCompatibilityV0_1CatalogAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerKnowledgeCatalog, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"resolver-precedence": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "precedence.private-app-global", Stage: "resolution", Expect: "private"},
			{ID: "precedence.app-global", Stage: "resolution", Expect: "app"},
		}, func(t *testing.T) {
			t.Run("resolution-winners", TestResolverPrunesBeforeWholeObjectPrecedenceAndDetaches)
		}),
		"authorization-nondisclosure": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "visibility.private-nondisclosure", Stage: "resolution", Expect: "omitted"},
			{ID: "visibility.corrupt-hidden", Stage: "resolution", Expect: "omitted-without-disclosure"},
			{ID: "forward-compat.hidden-corrupt", Stage: "resolution", Expect: "omitted-without-disclosure"},
		}, func(t *testing.T) {
			t.Run("catalog-authorization", TestCatalogAuthorizationTruthTableAcrossGetAndList)
			t.Run("resolver-hidden-corruption", TestResolverVisibleLoserCorruptionFailsClosedButHiddenBodyIsNotRead)
			t.Run("read-hidden-corruption", TestIntegrationHiddenCorruptionIsNotAReadOracle)
		}),
		"forward-compatibility": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "forward-compat.unknown-active-body", Stage: "resolution", Expect: "corruption-error"},
		}, func(t *testing.T) {
			t.Run("inactive-roundtrip-active-fail-closed", TestIntegrationInactiveFutureBodiesRoundTripAndActiveFailsClosed)
		}),
		"resolver-authorized-indexes": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "authorization.global-never-widens-indexes", Stage: "admission", Expect: "authorized-indexes-only"},
		}, func(t *testing.T) {
			database, store := newCatalogTestStore(t)
			insertFixtureObject(t, database, fixtureObject{id: "ko-global-forbidden-index", versions: []fixtureVersion{{
				definition: resolutionAliasDefinition(testAppTwo, "global-forbidden-index", SharingScopeGlobal, "forbidden"),
				state:      StateActive,
				mutation:   "create",
				timestamp:  10,
			}}})
			resolved, err := mustTestResolver(t, store).Resolve(t.Context(), testResolutionScope("main"))
			if err != nil {
				t.Fatalf("Resolve(main-only authority): %v", err)
			}
			if got := resolved.Summary().ExecutableObjects; got != 0 {
				t.Fatalf("main-only resolution executable objects = %d, want 0", got)
			}
		}),
		"coherent-snapshot": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "snapshot.catalog-mutation-during-admission", Stage: "admission", Expect: "coherent-old-or-new"},
		}, func(t *testing.T) {
			t.Run("concurrent-publication", TestResolverConcurrentPublicationReturnsOnlyOldOrNewAuthority)
		}),
		"resolver-bounds": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "resource.resolver-cache", Stage: "admission", Expect: "bounded-or-unavailable"},
		}, func(t *testing.T) {
			t.Run("fail-fast-gate", TestResolverCancellationAndFailFastGate)
			t.Run("connection-deadline", TestResolverSoleConnectionExhaustionHonorsBoundAndReleasesGate)
		}),
		"lost-response-replay": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "idempotency.lost-response", Stage: "mutation", Expect: "same-object-version-and-revision"},
		}, func(t *testing.T) {
			t.Run("after-commit-reopen", TestWriterAfterCommitLostResponseReplaysAfterReopen)
		}),
	})
}
