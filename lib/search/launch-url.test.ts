import assert from "node:assert/strict";
import test from "node:test";

import {
  boundedIndexSearchQuery,
  searchLaunchHref,
} from "./launch-url";

test("bounded index searches put head before any expensive default pipeline", () => {
  assert.equal(boundedIndexSearchQuery("main"), "index=main\n| head 10");
  assert.equal(boundedIndexSearchQuery("operations east"), 'index="operations east"\n| head 10');
  assert.throws(() => boundedIndexSearchQuery(" \t "), /index name is required/);
});

test("search launch URLs preserve an explicitly bounded query", () => {
  const query = boundedIndexSearchQuery("main");
  const url = new URL(searchLaunchHref(query), "https://example.test");
  assert.equal(url.searchParams.get("q"), query);
  assert.equal(url.searchParams.get("run"), "1");
});
