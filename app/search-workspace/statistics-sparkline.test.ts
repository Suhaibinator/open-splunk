import assert from "node:assert/strict";
import test from "node:test";

import {
  STATS_SPARKLINE_MARKER,
  statsSparklineSegments,
  statsSparklineValues,
  statsSparklineValuesForPresentation,
} from "./statistics-sparkline";

test("stats sparkline transport is recognized without coercing missing values", () => {
  assert.equal(statsSparklineValues(["ordinary", "1", "2"]), null);
  assert.deepEqual(
    statsSparklineValues([STATS_SPARKLINE_MARKER, "1", "", 2, "invalid", null]),
    [1, null, 2, null, null],
  );
});

test("stats sparkline presentation requires authenticated column metadata", () => {
  const spoofed = [STATS_SPARKLINE_MARKER, "1", "2"];
  assert.equal(statsSparklineValuesForPresentation(spoofed, false), null);
  assert.deepEqual(statsSparklineValuesForPresentation(spoofed, true), [1, 2]);
});

test("stats sparkline segments scale values and break across missing buckets", () => {
  assert.deepEqual(
    statsSparklineSegments([0, 5, 10], 100, 20, 0),
    [["0.00,20.00", "50.00,10.00", "100.00,0.00"]],
  );
  assert.deepEqual(
    statsSparklineSegments([2, null, 2], 100, 20, 0),
    [["0.00,10.00"], ["100.00,10.00"]],
  );
  assert.deepEqual(statsSparklineSegments([null, null]), []);
});
