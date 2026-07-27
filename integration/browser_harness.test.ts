import assert from "node:assert/strict";
import test from "node:test";

import {
  BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX,
  BoundedObservationRegistry,
  MAXIMUM_BROWSER_DIAGNOSTIC_BYTES,
  boundedDiagnostic,
  boundedRecorder,
} from "./browser_harness";

const utf8 = new TextEncoder();

test("bounded diagnostics cap both entry count and UTF-8 bytes", () => {
  const recorder = boundedRecorder(2);
  const oversized = `prefix-${"🙂".repeat(MAXIMUM_BROWSER_DIAGNOSTIC_BYTES)}-secret-tail`;

  recorder.add(oversized);
  recorder.add("safe");
  recorder.add("discarded");

  const snapshot = recorder.snapshot();
  assert.equal(snapshot.length, 3);
  assert.ok(
    utf8.encode(snapshot[0]).byteLength <= MAXIMUM_BROWSER_DIAGNOSTIC_BYTES,
    "the retained diagnostic exceeded its UTF-8 byte bound",
  );
  assert.match(snapshot[0], /\[truncated\]$/);
  assert.doesNotMatch(snapshot[0], /secret-tail/);
  assert.equal(snapshot[1], "safe");
  assert.equal(snapshot[2], "... 1 additional entry");
});

test("diagnostic byte boundaries preserve complete UTF-8 code points and the suffix budget", () => {
  const exact = "x".repeat(MAXIMUM_BROWSER_DIAGNOSTIC_BYTES);
  assert.equal(boundedDiagnostic(exact), exact);

  for (const oversized of [
    `${exact}x`,
    "🙂".repeat(MAXIMUM_BROWSER_DIAGNOSTIC_BYTES),
    `${"x".repeat(MAXIMUM_BROWSER_DIAGNOSTIC_BYTES - 1)}\ud800`,
  ]) {
    const bounded = boundedDiagnostic(oversized);
    assert.ok(utf8.encode(bounded).byteLength <= MAXIMUM_BROWSER_DIAGNOSTIC_BYTES);
    assert.ok(bounded.endsWith(BROWSER_DIAGNOSTIC_TRUNCATION_SUFFIX));
    assert.equal(bounded.includes("\ufffd"), false);
  }
});

test("bounded recorder snapshots are isolated from caller mutation", () => {
  const recorder = boundedRecorder(1);
  recorder.add("original");
  const first = recorder.snapshot();
  first[0] = "mutated";
  assert.deepEqual(recorder.snapshot(), ["original"]);
});

test("bounded observation registry never attaches beyond its live-key limit", () => {
  const registry = new BoundedObservationRegistry<object>(2);
  const first = {};
  const second = {};
  const excess = {};
  const attached: object[] = [];
  const detached: object[] = [];
  const observe = (socket: object): (() => void) => {
    attached.push(socket);
    return () => detached.push(socket);
  };

  assert.equal(registry.tryObserve(first, () => observe(first)), true);
  assert.equal(registry.tryObserve(second, () => observe(second)), true);
  assert.equal(registry.tryObserve(first, () => observe(first)), true);
  assert.equal(registry.tryObserve(excess, () => observe(excess)), false);
  assert.equal(registry.size, 2);
  assert.deepEqual(attached, [first, second]);

  registry.clear();
  assert.equal(registry.size, 0);
  assert.deepEqual(detached, [first, second]);
  assert.equal(registry.tryObserve(excess, () => observe(excess)), true);
});
