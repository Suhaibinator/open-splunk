import assert from "node:assert/strict";
import test from "node:test";

import {
  pruneCursorChainFrom,
  recordNextPageToken,
} from "./pagination";

test("cursor divergence removes every downstream token, start, cache entry, and seen token", () => {
  const pages = new Map([
    ["20:1", "page 1"],
    ["20:2", "page 2"],
    ["20:3", "page 3"],
    ["50:2", "other page-size cache"],
  ]);
  const pageTokens = new Map<string, string | undefined>([
    ["20:1", undefined],
    ["20:2", "cursor-1"],
    ["20:3", "cursor-2"],
    ["50:1", undefined],
    ["50:2", "cursor-other-size"],
  ]);
  const pageStarts = new Map([
    ["20:1", 1],
    ["20:2", 21],
    ["20:3", 41],
    ["50:1", 1],
    ["50:2", 51],
  ]);
  const seenTokens = new Set(["cursor-1", "cursor-2", "cursor-other-size"]);

  pruneCursorChainFrom(
    pages,
    pageTokens,
    pageStarts,
    seenTokens,
    20,
    2,
  );

  assert.deepEqual([...pages], [
    ["20:1", "page 1"],
    ["50:2", "other page-size cache"],
  ]);
  assert.deepEqual([...pageTokens], [
    ["20:1", undefined],
    ["50:1", undefined],
    ["50:2", "cursor-other-size"],
  ]);
  assert.deepEqual([...pageStarts], [
    ["20:1", 1],
    ["50:1", 1],
    ["50:2", 51],
  ]);
  assert.deepEqual([...seenTokens], ["cursor-other-size"]);
  assert.equal(
    recordNextPageToken(seenTokens, "cursor-1", "Search results"),
    "cursor-1",
    "a token from the discarded suffix can be admitted only as part of a fresh chain",
  );
});

test("cursor-chain pruning rejects unsafe page coordinates", () => {
  assert.throws(
    () => pruneCursorChainFrom(new Map(), new Map(), new Map(), new Set(), 0, 1),
    /page size/,
  );
  assert.throws(
    () => pruneCursorChainFrom(new Map(), new Map(), new Map(), new Set(), 20, 0),
    /first cursor page/,
  );
});
