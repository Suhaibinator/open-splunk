import type { SearchProgress } from "../../gen/ts/open_splunk/search";

const MAXIMUM_UINT64 = (1n << 64n) - 1n;

export type ProgressRevisionState = {
  revision: bigint;
  progress: SearchProgress;
} | null;

export type SearchProgressSource =
  | { kind: "authoritative"; envelopeRevision: bigint }
  | { kind: "live" }
  | { kind: "terminal"; envelopeRevision: bigint };

export type ProgressDecision =
  | {
    kind: "apply";
    state: NonNullable<ProgressRevisionState>;
  }
  | {
    kind: "ignore";
    reason: "lower" | "duplicate" | "unversioned";
    state: ProgressRevisionState;
  }
  | {
    kind: "recover";
    reason: "equal-conflict" | "revision-mismatch" | "invalid-revision";
    state: ProgressRevisionState;
  };

function isUint64Revision(revision: bigint): boolean {
  return typeof revision === "bigint"
    && revision >= 0n
    && revision <= MAXIMUM_UINT64;
}

export function isVersionedSearchRevision(revision: bigint): boolean {
  return isUint64Revision(revision) && revision !== 0n;
}

function cloneProgress(progress: SearchProgress): SearchProgress {
  return {
    ...progress,
    elapsed: progress.elapsed === undefined ? undefined : { ...progress.elapsed },
    queueWait: progress.queueWait === undefined ? undefined : { ...progress.queueWait },
    updatedAt: progress.updatedAt === undefined
      ? undefined
      : new Date(progress.updatedAt.getTime()),
  };
}

function equalDuration(
  left: SearchProgress["queueWait"],
  right: SearchProgress["queueWait"],
): boolean {
  if (left === undefined || right === undefined) return left === right;
  return left.seconds === right.seconds && left.nanos === right.nanos;
}

function stableProgressEqual(left: SearchProgress, right: SearchProgress): boolean {
  return left.phase === right.phase
    && left.scannedRows === right.scannedRows
    && left.scannedBytes === right.scannedBytes
    && left.matchedEvents === right.matchedEvents
    && left.producedRows === right.producedRows
    && left.resultBytes === right.resultBytes
    && Object.is(left.percentComplete, right.percentComplete)
    && equalDuration(left.queueWait, right.queueWait)
    && left.countersAreEstimates === right.countersAreEstimates;
}

function recover(
  state: ProgressRevisionState,
  reason: Extract<ProgressDecision, { kind: "recover" }>["reason"],
): ProgressDecision {
  return { kind: "recover", reason, state };
}

function applyDecision(
  progress: SearchProgress,
  revision: bigint,
): Extract<ProgressDecision, { kind: "apply" }> {
  return {
    kind: "apply",
    state: {
      revision,
      progress: cloneProgress(progress),
    },
  };
}

function effectiveRevision(
  progress: SearchProgress,
  source: SearchProgressSource,
):
  | { kind: "versioned"; revision: bigint }
  | { kind: "unversioned" }
  | { kind: "invalid" }
  | { kind: "mismatch" } {
  const nestedRevision = progress.stateVersion;
  if (!isUint64Revision(nestedRevision)) return { kind: "invalid" };

  if (source.kind === "live") {
    return nestedRevision === 0n
      ? { kind: "unversioned" }
      : { kind: "versioned", revision: nestedRevision };
  }

  if (
    !isVersionedSearchRevision(source.envelopeRevision)
  ) {
    return { kind: "invalid" };
  }
  if (nestedRevision === 0n) {
    return { kind: "versioned", revision: source.envelopeRevision };
  }
  return nestedRevision === source.envelopeRevision
    ? { kind: "versioned", revision: nestedRevision }
    : { kind: "mismatch" };
}

/**
 * Orders progress in the SearchJob state-version domain. Projection-time
 * elapsed/updatedAt fields intentionally do not make equal-version live
 * messages conflict; an equal-version REST snapshot may refresh them.
 */
export function reconcileSearchProgress(
  current: ProgressRevisionState,
  progress: SearchProgress,
  source: SearchProgressSource,
): ProgressDecision {
  const effective = effectiveRevision(progress, source);
  switch (effective.kind) {
    case "invalid":
      return recover(current, "invalid-revision");
    case "mismatch":
      return recover(current, "revision-mismatch");
    case "unversioned":
      return { kind: "ignore", reason: "unversioned", state: current };
    case "versioned":
      break;
  }

  if (current !== null && effective.revision < current.revision) {
    return { kind: "ignore", reason: "lower", state: current };
  }
  if (
    current === null
    || effective.revision > current.revision
  ) {
    return applyDecision(progress, effective.revision);
  }
  if (source.kind === "authoritative") {
    // REST is the convergence authority. At the same revision it may refresh
    // projection timing and must also replace a conflicting live snapshot;
    // treating that conflict as recoverable would retry the same authority
    // forever.
    return applyDecision(progress, effective.revision);
  }
  if (stableProgressEqual(current.progress, progress)) {
    return { kind: "ignore", reason: "duplicate", state: current };
  }
  return recover(current, "equal-conflict");
}
