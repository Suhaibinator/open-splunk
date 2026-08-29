export const TOKEN_CREATE_CLOCK_EPSILON_MS = 250;
export const TOKEN_CREATE_ZERO_CONFIRMATION_INTERVAL_MS = 2_000;

const TOKEN_CREATE_POLL_BACKOFF_MS = [1_000, 2_000, 4_000, 8_000, 10_000] as const;

export type TokenRecoveryOwnership =
  | "idle"
  | "acquiring"
  | "owned"
  | "contended"
  | "lost"
  | "failed";

export interface TokenRecoveryTiming {
  dispatchedServerTimeMs: number | null;
  requestTimeoutMs: number;
  clockUncertaintyMs: number;
}

export interface ZeroCandidateObservation {
  attemptId: string;
  observedServerTimeMs: number;
}

export type ZeroCandidateDecision =
  | { kind: "pending"; firstObservation: null }
  | { kind: "confirm"; firstObservation: ZeroCandidateObservation }
  | { kind: "clear"; firstObservation: ZeroCandidateObservation };

export type TokenRecoverySnapshotDecision = ZeroCandidateDecision
  | { kind: "candidates"; firstObservation: null };

export function tokenRecoveryQuiescenceDeadline(
  timing: TokenRecoveryTiming,
): number | null {
  if (
    timing.dispatchedServerTimeMs === null
    || !Number.isFinite(timing.dispatchedServerTimeMs)
    || !Number.isFinite(timing.requestTimeoutMs)
    || timing.requestTimeoutMs <= 0
    || !Number.isFinite(timing.clockUncertaintyMs)
    || timing.clockUncertaintyMs < 0
  ) {
    return null;
  }
  return timing.dispatchedServerTimeMs
    + 2 * timing.requestTimeoutMs
    + Math.max(timing.clockUncertaintyMs, TOKEN_CREATE_CLOCK_EPSILON_MS);
}

export function tokenRecoveryPollDelayMs(attempt: number): number {
  if (!Number.isFinite(attempt) || attempt <= 0) return TOKEN_CREATE_POLL_BACKOFF_MS[0];
  const index = Math.min(Math.floor(attempt), TOKEN_CREATE_POLL_BACKOFF_MS.length - 1);
  return TOKEN_CREATE_POLL_BACKOFF_MS[index];
}

export function zeroCandidateDecision(options: {
  attemptId: string;
  serverNowMs: number;
  quiescenceDeadlineMs: number | null;
  previousObservation: ZeroCandidateObservation | null;
}): ZeroCandidateDecision {
  const {
    attemptId,
    serverNowMs,
    quiescenceDeadlineMs,
    previousObservation,
  } = options;
  if (
    quiescenceDeadlineMs === null
    || !Number.isFinite(serverNowMs)
    || serverNowMs < quiescenceDeadlineMs
  ) {
    return { kind: "pending", firstObservation: null };
  }
  if (
    previousObservation === null
    || previousObservation.attemptId !== attemptId
    || previousObservation.observedServerTimeMs < quiescenceDeadlineMs
    || previousObservation.observedServerTimeMs > serverNowMs
  ) {
    return {
      kind: "confirm",
      firstObservation: { attemptId, observedServerTimeMs: serverNowMs },
    };
  }
  if (
    serverNowMs - previousObservation.observedServerTimeMs
    < TOKEN_CREATE_ZERO_CONFIRMATION_INTERVAL_MS
  ) {
    return { kind: "confirm", firstObservation: previousObservation };
  }
  return { kind: "clear", firstObservation: previousObservation };
}

export function tokenRecoverySnapshotDecision(options: {
  candidateCount: number;
  attemptId: string;
  serverNowMs: number;
  quiescenceDeadlineMs: number | null;
  previousObservation: ZeroCandidateObservation | null;
}): TokenRecoverySnapshotDecision {
  if (!Number.isSafeInteger(options.candidateCount) || options.candidateCount > 0) {
    return { kind: "candidates", firstObservation: null };
  }
  return zeroCandidateDecision(options);
}

export function tokenRecoveryEnvironmentCanPoll(
  visibilityState: DocumentVisibilityState,
  online: boolean,
): boolean {
  return visibilityState === "visible" && online;
}

export function tokenCreateDialogRequiresExclusiveAttention(options: {
  createRequestInProgress: boolean;
  plaintextToken: string | null;
}): boolean {
  return options.createRequestInProgress
    || tokenSecretRequiresNavigationProtection(options.plaintextToken);
}

export function isAuthoritativeTokenCreateRejection(error: {
  status?: unknown;
  message?: unknown;
  responseBody?: unknown;
}): boolean {
  const expectedMessage = error.status === 408
    ? "administrative request was canceled"
    : error.status === 429
      ? "ingestion token capacity is exhausted"
      : null;
  if (
    expectedMessage === null
    || error.message !== expectedMessage
    || typeof error.responseBody !== "string"
  ) return false;
  try {
    const body: unknown = JSON.parse(error.responseBody);
    if (typeof body !== "object" || body === null) return false;
    const envelope = body as { error?: unknown };
    if (typeof envelope.error !== "object" || envelope.error === null) return false;
    return (envelope.error as { message?: unknown }).message === expectedMessage;
  } catch {
    return false;
  }
}

export function tokenSecretRequiresNavigationProtection(
  plaintextToken: string | null,
): boolean {
  return plaintextToken !== null && plaintextToken.length > 0;
}
