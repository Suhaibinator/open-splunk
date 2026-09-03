import assert from "node:assert/strict";
import test from "node:test";

import {
  TokenRecoveryStartupController,
  type TokenRecoveryStartupDependencies,
  type TokenRecoveryStartupSnapshot,
} from "./token-recovery-startup";

function dependencies(
  overrides: Partial<TokenRecoveryStartupDependencies> = {},
): TokenRecoveryStartupDependencies {
  return {
    lockAvailable: () => true,
    parse: () => null,
    randomId: () => "unreadable-attempt",
    read: () => null,
    requestLock: async (_apiBaseUrl, _signal, callback) => callback(null),
    ...overrides,
  };
}

function handlers(cleanup: () => void) {
  return {
    onCleanup: cleanup,
    onOwned: async () => undefined,
  };
}

function observe(controller: TokenRecoveryStartupController) {
  const snapshots: TokenRecoveryStartupSnapshot[] = [];
  const unsubscribe = controller.subscribe(() => snapshots.push(controller.getSnapshot()));
  return { snapshots, unsubscribe };
}

test("startup fails closed when durable storage cannot be read", () => {
  let cleanupCount = 0;
  let lockRequested = false;
  const controller = new TokenRecoveryStartupController(dependencies({
    read: () => {
      throw new Error("storage denied");
    },
    requestLock: async () => {
      lockRequested = true;
    },
  }));
  const { snapshots } = observe(controller);

  const stop = controller.start(
    "https://splunk.example",
    1_000,
    handlers(() => { cleanupCount += 1; }),
  );

  assert.deepEqual(snapshots.map((snapshot) => snapshot.kind), [
    "preflight",
    "storage-unavailable",
  ]);
  assert.equal(controller.getSnapshot(), snapshots.at(-1));
  assert.equal(Object.isFrozen(controller.getSnapshot()), true);
  assert.equal(controller.getSnapshot().lockAvailable, true);
  assert.equal(lockRequested, false);
  stop();
  stop();
  assert.equal(controller.getSnapshot(), controller.getServerSnapshot());
  assert.deepEqual(snapshots.map((snapshot) => snapshot.kind), [
    "preflight",
    "storage-unavailable",
    "idle",
  ]);
  assert.equal(cleanupCount, 1);
});

test("startup with no durable guard remains idle without requesting a lock", () => {
  let lockRequested = false;
  const controller = new TokenRecoveryStartupController(dependencies({
    requestLock: async () => {
      lockRequested = true;
    },
  }));
  const { snapshots, unsubscribe } = observe(controller);

  controller.start(
    "https://splunk.example",
    null,
    handlers(() => undefined),
  );

  assert.deepEqual(snapshots.map((snapshot) => snapshot.kind), ["preflight", "empty"]);
  assert.equal(lockRequested, false);
  unsubscribe();
  controller.stop();
  assert.deepEqual(snapshots.map((snapshot) => snapshot.kind), ["preflight", "empty"]);
});

test("startup preserves unreadable records and refuses ownership without Web Locks", () => {
  const controller = new TokenRecoveryStartupController(dependencies({
    lockAvailable: () => false,
    read: () => "damaged-record",
  }));
  const { snapshots } = observe(controller);

  controller.start(
    "https://splunk.example",
    1_234,
    handlers(() => undefined),
  );

  assert.deepEqual(snapshots.map((snapshot) => snapshot.kind), [
    "preflight",
    "stored",
    "lock-unavailable",
  ]);
  const snapshot = controller.getSnapshot();
  assert.equal(snapshot.kind, "lock-unavailable");
  if (snapshot.kind !== "lock-unavailable") return;
  assert.equal(snapshot.record.unreadableRecovery?.attemptId, "unreadable-attempt");
  assert.equal(snapshot.record.unreadableRecovery?.raw, "damaged-record");
  assert.equal(snapshot.record.unreadableRecovery?.observedServerTimeMs, 1_234);
});

test("startup reports cross-tab lock contention without claiming ownership", async () => {
  let owned = false;
  const controller = new TokenRecoveryStartupController(dependencies({
    read: () => "damaged-record",
  }));
  const { snapshots } = observe(controller);

  controller.start("https://splunk.example", null, {
    ...handlers(() => undefined),
    onOwned: async () => { owned = true; },
  });
  await Promise.resolve();

  assert.deepEqual(snapshots.map((snapshot) => snapshot.kind), [
    "preflight",
    "stored",
    "acquiring",
    "contended",
  ]);
  assert.equal(owned, false);
});

test("retry cancels the prior acquisition and ignores its stale lock callback", async () => {
  const attempts: Array<{
    callback: (lock: Lock | null) => Promise<void>;
    signal: AbortSignal;
  }> = [];
  const owned: string[] = [];
  let cleanupCount = 0;
  const controller = new TokenRecoveryStartupController(dependencies({
    read: () => "damaged-record",
    requestLock: async (_apiBaseUrl, signal, callback) => {
      attempts.push({ callback, signal });
    },
  }));
  const { snapshots } = observe(controller);
  const startupHandlers = {
    ...handlers(() => { cleanupCount += 1; }),
    onOwned: async (_record: unknown, context: { isCurrent: () => boolean }) => {
      if (context.isCurrent()) owned.push("owned");
    },
  };

  controller.start("https://splunk.example", null, startupHandlers);
  const stop = controller.start("https://splunk.example", null, startupHandlers);

  assert.equal(attempts.length, 2);
  assert.equal(attempts[0]?.signal.aborted, true);
  assert.equal(attempts[1]?.signal.aborted, false);
  assert.equal(cleanupCount, 1);
  const currentSnapshot = controller.getSnapshot();
  await attempts[0]?.callback({ name: "stale" } as Lock);
  assert.equal(controller.getSnapshot(), currentSnapshot);
  await attempts[1]?.callback({ name: "current" } as Lock);
  assert.deepEqual(owned, ["owned"]);
  stop();
  assert.equal(attempts[1]?.signal.aborted, true);
  assert.equal(cleanupCount, 2);
  assert.equal(snapshots.at(-1)?.kind, "idle");
});

test("lock acquisition failure publishes a fail-closed snapshot", async () => {
  const failure = new Error("lock denied");
  const controller = new TokenRecoveryStartupController(dependencies({
    read: () => "damaged-record",
    requestLock: async () => {
      throw failure;
    },
  }));
  const { snapshots } = observe(controller);
  let owned = false;

  controller.start("https://splunk.example", null, {
    ...handlers(() => undefined),
    onOwned: async () => { owned = true; },
  });
  await Promise.resolve();

  assert.deepEqual(snapshots.map((snapshot) => snapshot.kind), [
    "preflight",
    "stored",
    "acquiring",
    "lock-failed",
  ]);
  const snapshot = controller.getSnapshot();
  assert.equal(snapshot.kind, "lock-failed");
  if (snapshot.kind !== "lock-failed") return;
  assert.equal(snapshot.error, failure);
  assert.equal(owned, false);
});
