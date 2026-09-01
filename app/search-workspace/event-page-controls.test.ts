import assert from "node:assert/strict";
import test from "node:test";

import {
  collapsePageEvents,
  eventPageSizeOptions,
  expandPageEvents,
  maximumReachableResultPage,
  planResultPageWalk,
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

test("a reached page outranks a total that implies fewer pages", () => {
  // The in-memory result manager also bounds a page by bytes, so a page can hold fewer rows than
  // the page size and the cursor chain yields more pages than the reported total implies. The
  // floor is the furthest page actually reached, which the caller cannot be argued out of.
  assert.equal(resultPageCount(95, 10, 11), 11);
  assert.equal(resultPageCount(5_000, 500, 11), 11);
  // Without a next cursor on the last page the arithmetic still stands.
  assert.equal(resultPageCount(5_000, 500, 10), 10);
});

test("a page jump starts at the furthest cursored page and walks forward to the target", () => {
  // Page 1 loaded, cursor known for page 2: reaching page 7 follows cursors 2 through 7.
  assert.deepEqual(planResultPageWalk(7, 2, 25), { startPage: 2, targetPage: 7 });
  // Paging back to an already-cursored page is a single hop, served from cache.
  assert.deepEqual(planResultPageWalk(3, 9, 25), { startPage: 3, targetPage: 3 });
  // Next from the furthest loaded page is one hop.
  assert.deepEqual(planResultPageWalk(10, 9, 25), { startPage: 9, targetPage: 10 });
  assert.deepEqual(planResultPageWalk(1, 1, 25), { startPage: 1, targetPage: 1 });
});

test("a page jump follows at most the permitted number of cursors", () => {
  assert.deepEqual(planResultPageWalk(500, 2, 25), { startPage: 2, targetPage: 27 });
  // Exactly at the cap still reaches the requested page; one page beyond it stops short.
  assert.deepEqual(planResultPageWalk(27, 2, 25), { startPage: 2, targetPage: 27 });
  assert.deepEqual(planResultPageWalk(28, 2, 25), { startPage: 2, targetPage: 27 });
});

test("a page jump never starts below page one or above the requested page", () => {
  assert.deepEqual(planResultPageWalk(0, 5, 25), { startPage: 1, targetPage: 1 });
  assert.deepEqual(planResultPageWalk(-3, 5, 25), { startPage: 1, targetPage: 1 });
  assert.deepEqual(planResultPageWalk(4, 0, 25), { startPage: 1, targetPage: 4 });
});
