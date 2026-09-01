import assert from "node:assert/strict";
import test from "node:test";

import {
  collapsePageEvents,
  eventPageSizeOptions,
  expandPageEvents,
  maximumReachableResultPage,
  resultPageCount,
  serializeRawPageForClipboard,
} from "./event-page-controls";

test("page expansion and collapse preserve event state from other pages", () => {
  const expanded = new Set(["hidden-page-event", "visible-a"]);
  assert.deepEqual(
    [...expandPageEvents(expanded, ["visible-a", "visible-b"])],
    ["hidden-page-event", "visible-a", "visible-b"],
  );
  assert.deepEqual(
    [...collapsePageEvents(expanded, ["visible-a", "visible-b"])],
    ["hidden-page-event"],
  );
});

test("reachable result pages include only valid cursors for the active page size", () => {
  assert.equal(
    maximumReachableResultPage(["20:1", "20:2", "20:5", "50:9", "20:not-a-page"], 20),
    5,
  );
  assert.equal(maximumReachableResultPage([], 20), 1);
});

test("raw page serialization preserves each event and adds only row separators", () => {
  assert.equal(
    serializeRawPageForClipboard([{ raw: "first\ncontinuation" }, { raw: "second" }]),
    "first\ncontinuation\nsecond",
  );
  assert.equal(serializeRawPageForClipboard([]), "");
});

test("the displayed page count follows the reported total once one is known", () => {
  assert.equal(resultPageCount(95, 10, 2), 10);
  assert.equal(resultPageCount(100, 10, 2), 10);
  assert.equal(resultPageCount(9, 10, 1), 1);
  assert.equal(resultPageCount(0, 10, 1), 1);
});

test("the displayed page count never falls below the pages already reached", () => {
  assert.equal(resultPageCount(null, 10, 4), 4);
  assert.equal(resultPageCount(null, 10, 1), 1);
  // A truncated total can understate the retained rows; pages already cursored still exist.
  assert.equal(resultPageCount(20, 10, 5), 5);
  assert.equal(resultPageCount(-1, 10, 3), 3);
  assert.equal(resultPageCount(95, 0, 3), 3);
});

test("events-per-page options add the server maximum without displacing the base ladder", () => {
  assert.deepEqual(eventPageSizeOptions(1_000, 20), [10, 20, 50, 100, 500, 1_000]);
  assert.deepEqual(eventPageSizeOptions(100, 20), [10, 20, 50, 100, 500]);
  assert.deepEqual(eventPageSizeOptions(null, 20), [10, 20, 50, 100, 500]);
  assert.deepEqual(eventPageSizeOptions(1_000, 250), [10, 20, 50, 100, 250, 500, 1_000]);
  assert.deepEqual(eventPageSizeOptions(0, 20), [10, 20, 50, 100, 500]);
});
