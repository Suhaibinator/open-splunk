import assert from "node:assert/strict";
import test from "node:test";

import type { SearchJob, SearchProgress } from "@/gen/ts/open_splunk/search";
import type { SearchWebSocketClient } from "@/lib/api";

import type { LivePreviewSnapshot } from "./live-preview";
import { RunningSearchController } from "./running-search-controller";

test("a backend launch supersedes the prior job and resets reconciliation state", () => {
  const running = new RunningSearchController();
  const priorGeneration = running.beginGeneration();
  running.adoptAuthoritativeJob(
    priorGeneration,
    {
      searchJobId: "prior-job",
      stateVersion: 9n,
      progress: { stateVersion: 0n } as SearchProgress,
    } as SearchJob,
  );
  running.advanceLiveUpdateEpoch();
  assert.equal(running.beginCancel(), priorGeneration);
  let relatedRequestsAborted = false;

  const generation = running.beginGeneration();
  const launch = running.resetBackendRun(() => {
    relatedRequestsAborted = true;
  });

  assert.equal(generation, 2);
  assert.equal(launch.supersededJobId, "prior-job");
  assert.equal(running.isCurrent(generation), true);
  assert.deepEqual(running.jobSnapshot(), {
    id: null,
    job: null,
    version: 0n,
  });
  assert.equal(running.captureLiveUpdateEpoch(), 0n);
  assert.equal(running.cancelIsPending(), false);
  assert.equal(running.cancelWasRequested(), false);
  assert.equal(relatedRequestsAborted, true);

  const nextJob = {
    searchJobId: "next-job",
    stateVersion: 1n,
    progress: { stateVersion: 1n } as SearchProgress,
  } as SearchJob;
  running.adoptAuthoritativeJob(generation, nextJob);
  assert.deepEqual(running.jobSnapshot(), {
    id: "next-job",
    job: nextJob,
    version: 1n,
  });
});

test("cancel completion cannot clear a newer search generation", () => {
  const running = new RunningSearchController();
  const canceledGeneration = running.beginGeneration();
  assert.equal(running.beginCancel(), canceledGeneration);
  running.beginGeneration();

  running.finishCancel(canceledGeneration);

  assert.equal(running.cancelIsPending(), true);
  assert.equal(running.cancelWasRequested(), true);
});

test("live update state is fenced by socket identity and replacement disposes ownership", () => {
  const running = new RunningSearchController();
  let firstDisposed = 0;
  let secondDisposed = 0;
  const first = { dispose: () => { firstDisposed += 1; } } as unknown as SearchWebSocketClient;
  const second = { dispose: () => { secondDisposed += 1; } } as unknown as SearchWebSocketClient;

  running.replaceSocket(first);
  running.advanceLiveUpdateEpoch();
  const epoch = running.captureLiveUpdateEpoch();
  assert.equal(running.liveUpdateEpochIs(epoch), true);

  running.replaceSocket(second);
  assert.equal(firstDisposed, 1);
  running.disposeSocket(first);
  running.advanceLiveUpdateEpoch();
  assert.equal(running.liveUpdateEpochIs(epoch), false);

  running.stopLiveUpdates();
  assert.equal(firstDisposed, 1);
  assert.equal(secondDisposed, 1);
  running.stopLiveUpdates();
  assert.equal(secondDisposed, 1);
  running.clearJob();
  assert.equal(running.captureLiveUpdateEpoch(), 0n);
});

test("job adoption and live reconciliation keep identity, version, and progress together", () => {
  const running = new RunningSearchController();
  const generation = running.beginGeneration();
  const progress = { stateVersion: 3n } as SearchProgress;
  const job = { searchJobId: "job-1", stateVersion: 3n, progress } as SearchJob;

  running.adoptAuthoritativeJob(generation, job);
  assert.deepEqual(running.jobSnapshot(), {
    id: "job-1",
    job,
    version: 3n,
  });
  assert.throws(
    () => running.adoptAuthoritativeJob(generation, { ...job, searchJobId: "job-2" }),
    { name: "AbortError" },
  );
  assert.equal(running.reconcileLiveJobVersion(2n), "stale");
  assert.equal(running.reconcileLiveJobVersion(3n), "current");
  assert.equal(running.reconcileLiveJobVersion(4n), "advanced");
  assert.equal(running.jobSnapshot().version, 4n);
  const nextProgress = { stateVersion: 4n } as SearchProgress;
  const applied = running.reconcileProgress(nextProgress, { kind: "live" });
  assert.equal(applied.kind, "apply");
  if (applied.kind !== "apply") return;
  assert.equal(applied.state.revision, 4n);
  const ignored = running.reconcileProgress(progress, { kind: "live" });
  assert.equal(ignored.kind, "ignore");
  if (ignored.kind !== "ignore") return;
  assert.equal(ignored.reason, "lower");
  assert.equal(ignored.state?.revision, 4n);
});

test("request replacement aborts prior ownership and release is identity-safe", () => {
  const running = new RunningSearchController();
  const first = new AbortController();
  const second = new AbortController();

  running.replaceRequest(first);
  running.replaceRequest(second);
  assert.equal(first.signal.aborted, true);
  assert.equal(second.signal.aborted, false);
  running.releaseRequest(first);
  running.abortRequest();
  assert.equal(second.signal.aborted, true);
});

test("preview transitions expose one consistent rendering snapshot", () => {
  const running = new RunningSearchController();
  const preview = {
    revision: 2n,
    rows: [],
    schemaId: "schema-1",
    truncated: false,
  } as LivePreviewSnapshot;

  running.configurePreview(50);
  running.applyPreview(preview, "waiting");
  assert.deepEqual(running.previewSnapshot(), {
    rowLimit: 50,
    snapshot: preview,
    status: "waiting",
  });
  running.clearPreview("resyncing");
  assert.deepEqual(running.previewSnapshot(), {
    rowLimit: 50,
    snapshot: null,
    status: "resyncing",
  });
});

test("scheduled work can be cleared and the launch lock releases on its own timer", (t) => {
  const descriptor = Object.getOwnPropertyDescriptor(globalThis, "window");
  const callbacks = new Map<number, () => void>();
  const cleared: number[] = [];
  let nextTimer = 1;
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: {
      clearTimeout(timer: number) {
        cleared.push(timer);
        callbacks.delete(timer);
      },
      setTimeout(callback: () => void) {
        const timer = nextTimer;
        nextTimer += 1;
        callbacks.set(timer, callback);
        return timer;
      },
    },
  });
  t.after(() => {
    if (descriptor === undefined) delete (globalThis as { window?: Window }).window;
    else Object.defineProperty(globalThis, "window", descriptor);
  });

  const running = new RunningSearchController();
  running.lockLaunch();
  assert.equal(running.launchIsLocked(), true);
  const releaseLaunch = callbacks.get(1);
  callbacks.delete(1);
  releaseLaunch?.();
  assert.equal(running.launchIsLocked(), false);

  let invoked = false;
  running.schedule(() => { invoked = true; }, 50);
  running.schedule(() => { invoked = true; }, 100);
  running.clearTimers();
  assert.deepEqual(cleared, [2, 3]);
  assert.equal(callbacks.size, 0);
  assert.equal(invoked, false);
  running.clearTimers();
  assert.deepEqual(cleared, [2, 3]);
});
