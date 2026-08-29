import assert from "node:assert/strict";
import test from "node:test";

import {
  collapsePageEvents,
  expandPageEvents,
  maximumReachableResultPage,
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
