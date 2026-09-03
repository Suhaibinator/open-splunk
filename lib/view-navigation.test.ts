import assert from "node:assert/strict";
import test from "node:test";

import {
  ACTIVITY_VIEWS,
  activityViewFromPathname,
  activityViewPath,
} from "../app/activity/activity-navigation";
import {
  REPORT_VIEWS,
  reportsViewFromPathname,
  reportsViewPath,
} from "../app/reports/reports-view-state";
import {
  SEARCH_RESULT_VIEWS,
  searchResultViewForQuery,
  searchResultViewFromPathname,
  searchResultViewPath,
} from "./search/result-view-navigation";
import { commitRoutedView, routedViewHref } from "./view-navigation";

test("view routes resolve every canonical tab and reject parent and unknown paths", () => {
  for (const view of ACTIVITY_VIEWS) {
    assert.equal(activityViewFromPathname(activityViewPath(view)), view);
  }
  for (const view of REPORT_VIEWS) {
    assert.equal(reportsViewFromPathname(reportsViewPath(view)), view);
  }
  for (const view of SEARCH_RESULT_VIEWS) {
    assert.equal(searchResultViewFromPathname(searchResultViewPath(view)), view);
  }
  assert.equal(activityViewFromPathname("/activity/unknown/"), null);
  assert.equal(reportsViewFromPathname("/reports/unknown/"), null);
  assert.equal(searchResultViewFromPathname("/search/unknown/"), null);
  assert.equal(activityViewFromPathname("/activity/"), null);
  assert.equal(reportsViewFromPathname("/reports/"), null);
  assert.equal(searchResultViewFromPathname("/search/"), null);
});

test("view href changes only the path and preserves query and hash state", () => {
  assert.equal(
    routedViewHref("https://example.test/activity/?appId=ops#latest", "/activity/", "history"),
    "/activity/history/?appId=ops#latest",
  );
});

test("view commits use the requested browser-history operation", () => {
  const calls: Array<{ href: string; mode: "push" | "replace"; state: unknown }> = [];
  const navigationWindow = {
    history: {
      state: { framework: true },
      pushState(state: unknown, _unused: string, href: string) {
        calls.push({ href, mode: "push", state });
      },
      replaceState(state: unknown, _unused: string, href: string) {
        calls.push({ href, mode: "replace", state });
      },
    },
    location: { href: "https://example.test/search/events/?appId=ops#results" },
  } as unknown as Window;

  commitRoutedView(navigationWindow, "/search/", "patterns", "push", { tab: "patterns" });
  commitRoutedView(navigationWindow, "/search/", "statistics", "replace");

  assert.deepEqual(calls, [
    { href: "/search/patterns/?appId=ops#results", mode: "push", state: { tab: "patterns" } },
    { href: "/search/statistics/?appId=ops#results", mode: "replace", state: { framework: true } },
  ]);
});

test("query-shaped search links choose their canonical result view", () => {
  assert.equal(searchResultViewForQuery("index=main"), "events");
  assert.equal(searchResultViewForQuery("index=main | stats count"), "statistics");
  assert.equal(searchResultViewForQuery("index=main | timechart count"), "visualization");
});
