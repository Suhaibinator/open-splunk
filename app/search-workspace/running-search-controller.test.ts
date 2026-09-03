import assert from "node:assert/strict";
import test from "node:test";

import type { SearchJob } from "@/gen/ts/open_splunk/search";
import type { SearchWebSocketClient } from "@/lib/api";

import { RunningSearchController } from "./running-search-controller";

test("a backend launch supersedes the prior job and resets reconciliation state", () => {
  const running = new RunningSearchController();
  running.jobIdRef.current = "prior-job";
  running.jobRef.current = { searchJobId: "prior-job" } as SearchJob;
  running.jobVersionRef.current = 9n;
  running.liveUpdateEpochRef.current = 4n;
  running.cancelPendingRef.current = true;
  running.cancelRequestedRef.current = true;
  let relatedRequestsAborted = false;

  const generation = running.beginGeneration();
  const launch = running.resetBackendRun(() => {
    relatedRequestsAborted = true;
  });

  assert.equal(generation, 1);
  assert.equal(launch.supersededJobId, "prior-job");
  assert.equal(running.isCurrent(generation), true);
  assert.equal(running.jobIdRef.current, null);
  assert.equal(running.jobRef.current, null);
  assert.equal(running.jobVersionRef.current, 0n);
  assert.equal(running.liveUpdateEpochRef.current, 0n);
  assert.equal(running.cancelPendingRef.current, false);
  assert.equal(running.cancelRequestedRef.current, false);
  assert.equal(relatedRequestsAborted, true);
});

test("cancel completion cannot clear a newer search generation", () => {
  const running = new RunningSearchController();
  const canceledGeneration = running.beginGeneration();
  assert.equal(running.beginCancel(), canceledGeneration);
  running.beginGeneration();

  running.finishCancel(canceledGeneration);

  assert.equal(running.cancelPendingRef.current, true);
  assert.equal(running.cancelRequestedRef.current, true);
});

test("live update state is fenced by socket identity and job lifetime", () => {
  const running = new RunningSearchController();
  let firstDisposed = 0;
  let secondDisposed = 0;
  const first = { dispose: () => { firstDisposed += 1; } } as unknown as SearchWebSocketClient;
  const second = { dispose: () => { secondDisposed += 1; } } as unknown as SearchWebSocketClient;

  running.attachSocket(first);
  running.recordUnversionedLiveUpdate();
  const epoch = running.captureLiveUpdateEpoch();
  assert.equal(running.liveUpdateEpochIs(epoch), true);

  running.attachSocket(second);
  running.releaseSocket(first);
  assert.equal(running.socketRef.current, second);
  running.recordUnversionedLiveUpdate();
  assert.equal(running.liveUpdateEpochIs(epoch), false);

  running.stopLiveUpdates();
  assert.equal(firstDisposed, 0);
  assert.equal(secondDisposed, 1);
  assert.equal(running.socketRef.current, null);
  running.clearJob();
  assert.equal(running.captureLiveUpdateEpoch(), 0n);
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
