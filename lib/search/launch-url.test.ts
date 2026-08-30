import assert from "node:assert/strict";
import test from "node:test";

import {
  boundedIndexSearchQuery,
  parseSearchLaunch,
  replaceSearchLaunchSource,
  savedSearchLaunchHref,
  searchJobLaunchHref,
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

test("query launch parsing and replacement preserve exact SPL whitespace", () => {
  const query = "  index=main\n| stats count  \n";
  const parsed = parseSearchLaunch(new URLSearchParams({ q: query, run: "1" }));
  assert.equal(parsed.value, query);

  const replaced = replaceSearchLaunchSource(
    new URLSearchParams("savedSearchId=saved-1"),
    "q",
    query,
  );
  assert.equal(replaced.get("q"), query);
  assert.equal(replaced.get("savedSearchId"), null);
  assert.throws(
    () => replaceSearchLaunchSource(new URLSearchParams(), "q", " \n\t "),
    /launch value is required/,
  );

  const objectLaunch = replaceSearchLaunchSource(new URLSearchParams(), "searchJobId", "  job-1  ");
  assert.equal(objectLaunch.get("searchJobId"), "job-1");
  assert.deepEqual(parseSearchLaunch(new URLSearchParams({ savedSearchId: "  saved-1  " })), {
    source: "savedSearchId",
    value: "saved-1",
    run: true,
  });
});

test("saved-search and exact-job links contain one exclusive source", () => {
  const saved = new URL(savedSearchLaunchHref("saved-1"), "https://example.test");
  assert.deepEqual(parseSearchLaunch(saved.searchParams), {
    source: "savedSearchId",
    value: "saved-1",
    run: true,
  });
  const job = new URL(searchJobLaunchHref("job-1"), "https://example.test");
  assert.deepEqual(parseSearchLaunch(job.searchParams), {
    source: "searchJobId",
    value: "job-1",
    run: false,
  });
});

test("launch parsing rejects mixed sources and replacement removes stale values", () => {
  assert.throws(
    () => parseSearchLaunch(new URLSearchParams("q=index%3Dmain&savedSearchId=saved-1")),
    /exactly one source/,
  );
  const replaced = replaceSearchLaunchSource(
    new URLSearchParams("q=index%3Dmain&earliest=-24h&historySearchId=stale"),
    "searchJobId",
    "job-1",
  );
  assert.equal(replaced.get("q"), null);
  assert.equal(replaced.get("historySearchId"), null);
  assert.equal(replaced.get("searchJobId"), "job-1");
  assert.equal(replaced.get("earliest"), null);
  assert.equal(replaced.get("latest"), null);
  assert.equal(replaced.get("label"), null);
  assert.equal(replaced.get("timezone"), null);
  assert.equal(replaced.get("run"), "0");
});

test("launch parsing rejects duplicate and empty source keys", () => {
  assert.throws(
    () => parseSearchLaunch(new URLSearchParams("q=first&q=second")),
    /exactly one source/,
  );
  assert.throws(
    () => parseSearchLaunch(new URLSearchParams("savedSearchId=a&savedSearchId=a")),
    /exactly one source/,
  );
  assert.throws(
    () => parseSearchLaunch(new URLSearchParams("q=&historySearchId=stale")),
    /exactly one source/,
  );
  assert.throws(
    () => parseSearchLaunch(new URLSearchParams("searchJobId=%20%20")),
    /cannot be empty/,
  );
});
