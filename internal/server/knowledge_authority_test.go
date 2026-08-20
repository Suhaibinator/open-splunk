package server

import (
	"testing"

	"github.com/Suhaibinator/open-splunk/internal/testsupport/knowledgecompat"
)

func TestKnowledgeServerAuthorities(t *testing.T) {
	knowledgecompat.Run(t, knowledgecompat.OwnerServer, map[knowledgecompat.Vector]knowledgecompat.Assertion{
		"inspection-reauthorization": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "snapshot.response-reauthorization", Stage: "inspection", Expect: "redacted-ordinal-only"},
		}, func(t *testing.T) {
			t.Run("inspection-redaction", TestSearchJobProjectionRedactsDetachedKnowledgeObjectDisclosures)
			t.Run("history-redaction", TestSearchHistoryGetAndListRedactKnowledgeObjectDisclosures)
		}),
		"admin-route-boundary": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "authorization.admin-crud", Stage: "api", Expect: "not-found-or-forbidden-without-disclosure"},
		}, func(t *testing.T) {
			t.Run("user-before-body", TestKnowledgeHTTPUserIsRejectedBeforeMalformedBodyDecode)
			t.Run("hidden-redaction", TestKnowledgeHTTPMissingHiddenAndRedactedAreIndistinguishable)
		}),
		"privileged-attempt-ordering": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "audit.rejected-privileged", Stage: "audit", Expect: "recorded-before-response"},
		}, func(t *testing.T) {
			t.Run("append-before-response", TestKnowledgeAttemptBoundaryUserAppendPrecedesFixedForbiddenAndLeavesBodyUnread)
			t.Run("journal-failure-fail-closed", TestKnowledgeHTTPRejectedAttemptJournalFailureReturnsFixedUnavailable)
		}),
		"saved-search-rerun-current": knowledgecompat.Exact([]knowledgecompat.WantCase{
			{ID: "lifecycle.rerun-resolves-current", Stage: "admission", Expect: "current-version-new-digest"},
		}, func(t *testing.T) {
			t.Run(
				"saved-search-and-history-rerun",
				TestSavedSearchAndHistoryRerunResolveCurrentKnowledgeWhileExportRetainsOriginal,
			)
		}),
	})
}
