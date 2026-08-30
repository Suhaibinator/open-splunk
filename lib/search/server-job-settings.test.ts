import assert from "node:assert/strict";
import test from "node:test";

import { RetainedResultStatus, SearchJobOrigin, SearchJobRetentionClass, SearchJobVisibility } from "@/gen/ts/open_splunk/search";
import { adaptRetainedJobReference, adaptServerJobSettings } from "./server-job-settings";

test("adapts complete shared-job settings", () => {
  const settings = adaptServerJobSettings({
    searchJobId: "job-1",
    stateVersion: 9n,
    visibility: SearchJobVisibility.SEARCH_JOB_VISIBILITY_EVERYONE,
    retentionClass: SearchJobRetentionClass.SEARCH_JOB_RETENTION_CLASS_SHARED,
    retentionLifetime: { seconds: 604_800n, nanos: 0 },
    expiresAt: new Date("2026-09-05T12:00:00Z"),
    lastAccessedAt: new Date("2026-08-29T12:00:00Z"),
    retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_AVAILABLE,
    source: { origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_SAVED_SEARCH } as never,
  } as never);
  assert.equal(settings.visibility, "everyone");
  assert.equal(settings.retentionClass, "shared");
  assert.equal(settings.lifetimeMs, 7 * 24 * 60 * 60 * 1_000);
  assert.equal(settings.retainedResultState, "available");
  assert.equal(settings.provenance, "Saved search");
});

test("rejects unspecified durable metadata", () => {
  assert.throws(() => adaptServerJobSettings({
    searchJobId: "job-1",
    stateVersion: 1n,
    visibility: SearchJobVisibility.SEARCH_JOB_VISIBILITY_UNSPECIFIED,
    retentionClass: SearchJobRetentionClass.SEARCH_JOB_RETENTION_CLASS_MANUAL,
    retentionLifetime: { seconds: 600n, nanos: 0 },
    retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_AVAILABLE,
  } as never), /visibility/);
});

test("accepts pending retained jobs before terminal expiry is assigned", () => {
  assert.deepEqual(adaptRetainedJobReference({
    retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_PENDING,
    searchJobId: "job-pending",
  }), {
    retainedResultState: "pending",
    searchJobExpiresAt: null,
    searchJobId: "job-pending",
  });
});

test("accepts unavailable retained jobs when their expiry is unknown", () => {
  for (const [status, state] of [
    [RetainedResultStatus.RETAINED_RESULT_STATUS_MISSING, "missing"],
    [RetainedResultStatus.RETAINED_RESULT_STATUS_CORRUPT, "corrupt"],
  ] as const) {
    assert.deepEqual(adaptRetainedJobReference({
      retainedResultStatus: status,
      searchJobId: `job-${state}`,
    }), {
      retainedResultState: state,
      searchJobExpiresAt: null,
      searchJobId: `job-${state}`,
    });
  }
});

test("requires expiry for available and expired retained jobs", () => {
  for (const status of [
    RetainedResultStatus.RETAINED_RESULT_STATUS_AVAILABLE,
    RetainedResultStatus.RETAINED_RESULT_STATUS_EXPIRED,
  ]) {
    assert.throws(() => adaptRetainedJobReference({
      retainedResultStatus: status,
      searchJobId: "job-terminal",
    }), /incoherent retained-result metadata/);
  }
});

test("uses the shared origin presentation for durable settings", () => {
  const base = {
    searchJobId: "job-1",
    stateVersion: 1n,
    visibility: SearchJobVisibility.SEARCH_JOB_VISIBILITY_PRIVATE,
    retentionClass: SearchJobRetentionClass.SEARCH_JOB_RETENTION_CLASS_MANUAL,
    retentionLifetime: { seconds: 600n, nanos: 0 },
    retainedResultStatus: RetainedResultStatus.RETAINED_RESULT_STATUS_PENDING,
  } as const;
  assert.equal(adaptServerJobSettings({
    ...base,
    source: { origin: SearchJobOrigin.SEARCH_JOB_ORIGIN_AD_HOC },
  } as never).provenance, "Ad hoc search");
  assert.equal(adaptServerJobSettings(base as never).provenance, "Unknown");
});
