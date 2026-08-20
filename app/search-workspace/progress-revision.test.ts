import assert from "node:assert/strict";
import test from "node:test";

import {
  SearchExecutionPhase,
  type SearchProgress,
} from "../../gen/ts/open_splunk/search";
import {
  reconcileSearchProgress,
  type ProgressRevisionState,
  type SearchProgressSource,
} from "./progress-revision";

const authoritative = (envelopeRevision: bigint): SearchProgressSource => ({
  kind: "authoritative",
  envelopeRevision,
});
const live: SearchProgressSource = { kind: "live" };
const terminal = (envelopeRevision: bigint): SearchProgressSource => ({
  kind: "terminal",
  envelopeRevision,
});

function progress(
  stateVersion: bigint,
  overrides: Partial<SearchProgress> = {},
): SearchProgress {
  return {
    phase: SearchExecutionPhase.SEARCH_EXECUTION_PHASE_EXECUTING,
    scannedRows: 10n,
    scannedBytes: 100n,
    matchedEvents: 7n,
    producedRows: 5n,
    resultBytes: 64n,
    percentComplete: 40,
    elapsed: { seconds: 2n, nanos: 3 },
    queueWait: { seconds: 1n, nanos: 2 },
    updatedAt: new Date("2026-07-26T00:00:02.003Z"),
    countersAreEstimates: false,
    stateVersion,
    ...overrides,
  };
}

function applied(
  current: ProgressRevisionState,
  candidate: SearchProgress,
  source: SearchProgressSource,
): NonNullable<ProgressRevisionState> {
  const decision = reconcileSearchProgress(current, candidate, source);
  assert.equal(decision.kind, "apply");
  if (decision.kind !== "apply") throw new Error("expected progress to apply");
  return decision.state;
}

test("authoritative progress establishes a revision and lower live replay cannot regress it", () => {
  const current = applied(null, progress(10n), authoritative(10n));
  const decision = reconcileSearchProgress(
    current,
    progress(9n, { scannedRows: 1n }),
    live,
  );

  assert.deepEqual(decision, {
    kind: "ignore",
    reason: "lower",
    state: current,
  });
  assert.strictEqual(decision.state, current);
});

test("equal live timing drift is a duplicate but stable conflicts recover without mutation", () => {
  const current = applied(null, progress(10n), authoritative(10n));
  const timingDrift = reconcileSearchProgress(
    current,
    progress(10n, {
      elapsed: { seconds: 20n, nanos: 30 },
      updatedAt: new Date("2026-07-26T00:00:20.030Z"),
    }),
    live,
  );
  assert.deepEqual(timingDrift, {
    kind: "ignore",
    reason: "duplicate",
    state: current,
  });

  const stableConflicts: SearchProgress[] = [
    progress(10n, { phase: SearchExecutionPhase.SEARCH_EXECUTION_PHASE_MATERIALIZING_RESULTS }),
    progress(10n, { scannedRows: 11n }),
    progress(10n, { scannedBytes: 101n }),
    progress(10n, { matchedEvents: 8n }),
    progress(10n, { producedRows: 6n }),
    progress(10n, { resultBytes: 65n }),
    progress(10n, { percentComplete: undefined }),
    progress(10n, { percentComplete: 41 }),
    progress(10n, { queueWait: undefined }),
    progress(10n, { queueWait: { seconds: 1n, nanos: 3 } }),
    progress(10n, { countersAreEstimates: true }),
  ];
  for (const candidate of stableConflicts) {
    const decision = reconcileSearchProgress(current, candidate, live);
    assert.deepEqual(decision, {
      kind: "recover",
      reason: "equal-conflict",
      state: current,
    });
    assert.strictEqual(decision.state, current);
  }
});

test("stable percent comparison preserves protobuf presence and Object.is semantics", () => {
  const missing = applied(
    null,
    progress(10n, { percentComplete: undefined }),
    authoritative(10n),
  );
  assert.equal(
    reconcileSearchProgress(missing, progress(10n, { percentComplete: 0 }), live).kind,
    "recover",
  );

  const nan = applied(
    null,
    progress(11n, { percentComplete: Number.NaN }),
    authoritative(11n),
  );
  assert.equal(
    reconcileSearchProgress(
      nan,
      progress(11n, {
        percentComplete: Number.NaN,
        elapsed: { seconds: 99n, nanos: 0 },
      }),
      live,
    ).kind,
    "ignore",
  );
  assert.equal(
    reconcileSearchProgress(
      applied(null, progress(12n, { percentComplete: 0 }), authoritative(12n)),
      progress(12n, { percentComplete: -0 }),
      live,
    ).kind,
    "recover",
  );
});

test("higher revisions apply even when counters decrease", () => {
  const current = applied(null, progress(10n), authoritative(10n));
  const next = applied(
    current,
    progress(11n, {
      scannedRows: 1n,
      scannedBytes: 2n,
      matchedEvents: 0n,
      producedRows: 0n,
      resultBytes: 0n,
    }),
    live,
  );

  assert.equal(next.revision, 11n);
  assert.equal(next.progress.scannedRows, 1n);
});

test("standalone unversioned progress is ignored and requests authoritative recovery", () => {
  const decision = reconcileSearchProgress(null, progress(0n), live);
  assert.deepEqual(decision, {
    kind: "ignore",
    reason: "unversioned",
    state: null,
  });
});

test("authoritative and terminal envelopes inherit legacy nested zero but reject mismatch", () => {
  const fromLegacyRest = applied(
    null,
    progress(0n),
    authoritative(12n),
  );
  assert.equal(fromLegacyRest.revision, 12n);

  const legacyTerminal = applied(
    null,
    progress(0n),
    terminal(12n),
  );
  assert.equal(legacyTerminal.revision, 12n);

  const versionedDuplicate = reconcileSearchProgress(
    fromLegacyRest,
    progress(12n, {
      elapsed: { seconds: 99n, nanos: 0 },
      updatedAt: new Date("2026-07-26T00:01:39.000Z"),
    }),
    live,
  );
  assert.deepEqual(versionedDuplicate, {
    kind: "ignore",
    reason: "duplicate",
    state: fromLegacyRest,
  });

  const mismatch = reconcileSearchProgress(
    fromLegacyRest,
    progress(11n),
    terminal(12n),
  );
  assert.deepEqual(mismatch, {
    kind: "recover",
    reason: "revision-mismatch",
    state: fromLegacyRest,
  });
});

test("equal authoritative REST replaces timing and stable fields while lower REST is stale", () => {
  const current = applied(null, progress(10n), authoritative(10n));
  const replacement = progress(10n, {
    scannedRows: 20n,
    elapsed: { seconds: 30n, nanos: 40 },
    updatedAt: new Date("2026-07-26T00:00:30.040Z"),
  });
  const replacementState = applied(current, replacement, authoritative(10n));
  assert.equal(replacementState.progress.scannedRows, 20n);
  assert.deepEqual(replacementState.progress.elapsed, { seconds: 30n, nanos: 40 });

  const lowerDecision = reconcileSearchProgress(
    replacementState,
    progress(9n),
    authoritative(9n),
  );
  assert.deepEqual(lowerDecision, {
    kind: "ignore",
    reason: "lower",
    state: replacementState,
  });
});

test("invalid uint64 revisions recover without mutating current state", () => {
  const current = applied(null, progress(10n), authoritative(10n));
  const maximumUint64 = (1n << 64n) - 1n;
  const cases: Array<[SearchProgress, SearchProgressSource]> = [
    [progress(-1n), live],
    [progress(maximumUint64 + 1n), live],
    [progress(10n), authoritative(0n)],
    [progress(10n), authoritative(-1n)],
    [progress(10n), terminal(maximumUint64 + 1n)],
  ];
  for (const [candidate, source] of cases) {
    const decision = reconcileSearchProgress(current, candidate, source);
    assert.deepEqual(decision, {
      kind: "recover",
      reason: "invalid-revision",
      state: current,
    });
    assert.strictEqual(decision.state, current);
  }
});

test("resetting revision state permits a new job to begin at version one", () => {
  const oldJob = applied(null, progress(100n), authoritative(100n));
  assert.equal(oldJob.revision, 100n);

  const newJob = applied(null, progress(1n), authoritative(1n));
  assert.equal(newJob.revision, 1n);
});

test("applied state is isolated from later mutation of the decoded input", () => {
  const candidate = progress(10n);
  const state = applied(null, candidate, authoritative(10n));
  candidate.scannedRows = 999n;
  candidate.queueWait!.nanos = 999;
  candidate.updatedAt!.setUTCFullYear(2000);

  assert.equal(state.progress.scannedRows, 10n);
  assert.deepEqual(state.progress.queueWait, { seconds: 1n, nanos: 2 });
  assert.equal(state.progress.updatedAt?.toISOString(), "2026-07-26T00:00:02.003Z");
});
