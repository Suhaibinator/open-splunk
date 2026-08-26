import assert from "node:assert/strict";
import test from "node:test";

import { SearchJobState } from "@/gen/ts/open_splunk/search";

import { homeSearchFinishedAt, homeSearchStatus } from "./home-dashboard-data";

test("home search history presents terminal backend states", () => {
  assert.deepEqual(homeSearchStatus(SearchJobState.SEARCH_JOB_STATE_COMPLETED), {
    label: "Completed",
    tone: "complete",
  });
  assert.deepEqual(homeSearchStatus(SearchJobState.SEARCH_JOB_STATE_CANCELED), {
    label: "Canceled",
    tone: "neutral",
  });
  assert.deepEqual(homeSearchStatus(SearchJobState.SEARCH_JOB_STATE_EXPIRED), {
    label: "Expired",
    tone: "failed",
  });
  assert.deepEqual(homeSearchStatus(SearchJobState.SEARCH_JOB_STATE_FAILED), {
    label: "Failed",
    tone: "failed",
  });
  assert.deepEqual(homeSearchStatus(SearchJobState.UNRECOGNIZED), {
    label: "Unknown",
    tone: "neutral",
  });
});

test("home search history prefers the terminal timestamp", () => {
  const createdAt = new Date("2026-08-24T08:00:00Z");
  const finishedAt = new Date("2026-08-24T08:00:03Z");
  assert.equal(homeSearchFinishedAt({ createdAt, finishedAt }), finishedAt);
  assert.equal(homeSearchFinishedAt({ createdAt, finishedAt: null }), createdAt);
  assert.equal(homeSearchFinishedAt({ createdAt: null, finishedAt: null }), null);
});
