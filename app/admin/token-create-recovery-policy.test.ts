import assert from "node:assert/strict";
import test from "node:test";

import {
  TOKEN_CREATE_ZERO_CONFIRMATION_INTERVAL_MS,
  isAuthoritativeTokenCreateRejection,
  tokenCreateDialogRequiresExclusiveAttention,
  tokenRecoveryEnvironmentCanPoll,
  tokenRecoveryPollDelayMs,
  tokenRecoveryQuiescenceDeadline,
  tokenRecoverySnapshotDecision,
  tokenSecretRequiresNavigationProtection,
  zeroCandidateDecision,
} from "./token-create-recovery-policy";

test("quiescence uses two request timeouts plus bounded clock uncertainty", () => {
  assert.equal(tokenRecoveryQuiescenceDeadline({
    dispatchedServerTimeMs: 1_000,
    requestTimeoutMs: 30_000,
    clockUncertaintyMs: 10,
  }), 61_250);
  assert.equal(tokenRecoveryQuiescenceDeadline({
    dispatchedServerTimeMs: 1_000,
    requestTimeoutMs: 30_000,
    clockUncertaintyMs: 750,
  }), 61_750);
  assert.equal(tokenRecoveryQuiescenceDeadline({
    dispatchedServerTimeMs: null,
    requestTimeoutMs: 30_000,
    clockUncertaintyMs: 10,
  }), null);
});

test("polling backs off to a ten second cap", () => {
  assert.deepEqual(
    [0, 1, 2, 3, 4, 5, 99].map(tokenRecoveryPollDelayMs),
    [1_000, 2_000, 4_000, 8_000, 10_000, 10_000, 10_000],
  );
});

test("zero candidates require two post-quiescence observations", () => {
  const before = zeroCandidateDecision({
    attemptId: "attempt-1",
    serverNowMs: 60_999,
    quiescenceDeadlineMs: 61_000,
    previousObservation: null,
  });
  assert.deepEqual(before, { kind: "pending", firstObservation: null });

  const first = zeroCandidateDecision({
    attemptId: "attempt-1",
    serverNowMs: 61_000,
    quiescenceDeadlineMs: 61_000,
    previousObservation: null,
  });
  assert.equal(first.kind, "confirm");
  assert.notEqual(first.firstObservation, null);

  const tooSoon = zeroCandidateDecision({
    attemptId: "attempt-1",
    serverNowMs: 61_000 + TOKEN_CREATE_ZERO_CONFIRMATION_INTERVAL_MS - 1,
    quiescenceDeadlineMs: 61_000,
    previousObservation: first.firstObservation,
  });
  assert.equal(tooSoon.kind, "confirm");

  const clear = zeroCandidateDecision({
    attemptId: "attempt-1",
    serverNowMs: 61_000 + TOKEN_CREATE_ZERO_CONFIRMATION_INTERVAL_MS,
    quiescenceDeadlineMs: 61_000,
    previousObservation: first.firstObservation,
  });
  assert.equal(clear.kind, "clear");

  const differentAttempt = zeroCandidateDecision({
    attemptId: "attempt-2",
    serverNowMs: 70_000,
    quiescenceDeadlineMs: 61_000,
    previousObservation: first.firstObservation,
  });
  assert.equal(differentAttempt.kind, "confirm");
});

test("a candidate interrupts zero-result completion", () => {
  const first = tokenRecoverySnapshotDecision({
    candidateCount: 0,
    attemptId: "attempt-1",
    serverNowMs: 61_000,
    quiescenceDeadlineMs: 61_000,
    previousObservation: null,
  });
  assert.equal(first.kind, "confirm");
  const interrupted = tokenRecoverySnapshotDecision({
    candidateCount: 1,
    attemptId: "attempt-1",
    serverNowMs: 62_000,
    quiescenceDeadlineMs: 61_000,
    previousObservation: first.firstObservation,
  });
  assert.deepEqual(interrupted, { kind: "candidates", firstObservation: null });
  const restarted = tokenRecoverySnapshotDecision({
    candidateCount: 0,
    attemptId: "attempt-1",
    serverNowMs: 64_000,
    quiescenceDeadlineMs: 61_000,
    previousObservation: interrupted.firstObservation,
  });
  assert.equal(restarted.kind, "confirm");
});

test("only exact Open Splunk 408 and 429 errors are definite create rejections", () => {
  assert.equal(isAuthoritativeTokenCreateRejection({
    status: 408,
    message: "administrative request was canceled",
    responseBody: JSON.stringify({
      error: { message: "administrative request was canceled" },
    }),
  }), true);
  assert.equal(isAuthoritativeTokenCreateRejection({
    status: 429,
    message: "ingestion token capacity is exhausted",
    responseBody: JSON.stringify({
      error: { message: "ingestion token capacity is exhausted" },
    }),
  }), true);
  assert.equal(isAuthoritativeTokenCreateRejection({
    status: 408,
    message: "Request Timeout",
  }), false);
  assert.equal(isAuthoritativeTokenCreateRejection({
    status: 429,
    message: "Too Many Requests",
  }), false);
  assert.equal(isAuthoritativeTokenCreateRejection({
    status: 504,
    message: "Request timed out",
  }), false);
  assert.equal(isAuthoritativeTokenCreateRejection({
    status: 408,
    message: "administrative request was canceled",
    responseBody: "administrative request was canceled",
  }), false);
  assert.equal(isAuthoritativeTokenCreateRejection({
    status: 429,
    message: "ingestion token capacity is exhausted",
    responseBody: "<html>ingestion token capacity is exhausted</html>",
  }), false);
  assert.equal(isAuthoritativeTokenCreateRejection({
    status: 429,
    message: "ingestion token capacity is exhausted",
    responseBody: JSON.stringify({
      message: "ingestion token capacity is exhausted",
    }),
  }), false);
});

test("only an in-memory plaintext secret protects navigation", () => {
  assert.equal(tokenSecretRequiresNavigationProtection(null), false);
  assert.equal(tokenSecretRequiresNavigationProtection(""), false);
  assert.equal(tokenSecretRequiresNavigationProtection("ost_secret"), true);
  assert.equal(tokenCreateDialogRequiresExclusiveAttention({
    createRequestInProgress: true,
    plaintextToken: null,
  }), true);
  assert.equal(tokenCreateDialogRequiresExclusiveAttention({
    createRequestInProgress: false,
    plaintextToken: "ost_secret",
  }), true);
  assert.equal(tokenCreateDialogRequiresExclusiveAttention({
    createRequestInProgress: false,
    plaintextToken: null,
  }), false);
});

test("automatic recovery pauses while hidden or offline", () => {
  assert.equal(tokenRecoveryEnvironmentCanPoll("visible", true), true);
  assert.equal(tokenRecoveryEnvironmentCanPoll("hidden", true), false);
  assert.equal(tokenRecoveryEnvironmentCanPoll("visible", false), false);
});
