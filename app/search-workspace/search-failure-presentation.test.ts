import assert from "node:assert/strict";
import test from "node:test";

import { SearchFailureCode } from "@/gen/ts/open_splunk/search";

import {
  activeTransportSearchFailure,
  invalidSplSearchFailure,
  presentSearchFailure,
  transportSearchFailure,
} from "./search-failure-presentation";

test("query complexity diagnostics expose bounded-search guidance and settings", () => {
  const failure = invalidSplSearchFailure("The source crossed the expression budget.");
  const presentation = presentSearchFailure(failure, [{
    diagnostic: {
      code: "SPL_QUERY_TOO_COMPLEX",
      message: "The source crossed the expression budget.",
      range: null,
      severity: "error",
      suggestions: [],
    },
    fix: null,
    stale: false,
  }]);

  assert.equal(presentation.title, "Search is too complex");
  assert.deepEqual(presentation.actions, ["server-settings"]);
  assert.equal(presentation.guidance.length, 3);
});

test("failure codes keep specific recovery titles", () => {
  const presentation = presentSearchFailure({
    code: SearchFailureCode.SEARCH_FAILURE_CODE_TIMEOUT,
    diagnostics: [],
    message: "The search exceeded its deadline.",
    retryable: true,
  }, []);

  assert.equal(presentation.title, "Search timed out");
  assert.deepEqual(presentation.actions, ["retry"]);
});

test("synthetic failures distinguish invalid SPL from retryable transport errors", () => {
  assert.equal(invalidSplSearchFailure("invalid").retryable, false);
  assert.equal(
    invalidSplSearchFailure("invalid").code,
    SearchFailureCode.SEARCH_FAILURE_CODE_INVALID_SPL,
  );
  assert.equal(transportSearchFailure("offline").retryable, true);
  assert.equal(
    transportSearchFailure("offline").code,
    SearchFailureCode.SEARCH_FAILURE_CODE_INTERNAL,
  );
});

test("active transport failures retain the exact retry query and time range", () => {
  const timeRange = {
    label: "DST boundary",
    earliest: "2025-03-09T00:00:00",
    latest: "2025-03-10T00:00:00",
    timezone: "America/New_York",
  };
  const failure = activeTransportSearchFailure(
    "Unable to open the saved search.",
    "index=main | head 10",
    timeRange,
  );

  assert.equal(failure.source, "index=main | head 10");
  assert.deepEqual(failure.timeRange, timeRange);
  assert.notEqual(failure.timeRange, timeRange);
  assert.equal(failure.failure.retryable, true);
  assert.deepEqual(failure.problems, []);
});

test("launch transport failures retain their exact retry target", () => {
  const retryLaunch = { source: "searchJobId", value: "job-49", run: false } as const;
  const failure = activeTransportSearchFailure(
    "Unable to load the retained job.",
    "",
    { label: "Last 24 hours", earliest: "-24h", latest: "now" },
    retryLaunch,
  );

  assert.deepEqual(failure.retryLaunch, retryLaunch);
});
