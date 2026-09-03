import assert from "node:assert/strict";
import test from "node:test";

import {
  commitSearchLaunch,
  historyNavigationDecision,
  launchTransition,
  readSearchLaunchState,
  sameSearchLaunchState,
  searchLaunchLocation,
  searchLaunchUrl,
  stampSearchLaunchState,
  type SearchLaunchLocation,
} from "./search-launch-state";

const LAST_DAY = { earliest: "-24h", label: "Last 24 hours", latest: "now" };
const LAST_WEEK = { earliest: "-7d", label: "Last 7 days", latest: "now" };

function location(parameters: Record<string, string>): SearchLaunchLocation {
  return searchLaunchLocation(new URLSearchParams(parameters));
}

function installFakeWindow(initialHref: string, initialState: unknown) {
  const originalWindow = Object.getOwnPropertyDescriptor(globalThis, "window");
  const current = new URL(initialHref);
  const entries: Array<{ operation: "push" | "replace"; state: unknown; href: string }> = [];
  const history = {
    state: initialState,
    pushState(state: unknown, _unused: string, href: string | URL) {
      entries.push({ operation: "push", state, href: String(href) });
      history.state = state;
      current.href = String(href);
    },
    replaceState(state: unknown, _unused: string, href: string | URL) {
      entries.push({ operation: "replace", state, href: String(href) });
      history.state = state;
      current.href = String(href);
    },
  };
  Object.defineProperty(globalThis, "window", {
    configurable: true,
    value: { history, location: current } as unknown as Window,
  });
  return {
    entries,
    history,
    restore() {
      if (originalWindow === undefined) delete (globalThis as { window?: Window }).window;
      else Object.defineProperty(globalThis, "window", originalWindow);
    },
  };
}

test("a launch location reads the source, the run flag and the range parameters", () => {
  const launch = location({ q: "index=main", run: "1", earliest: "-24h", latest: "now", label: "Last 24 hours" });
  assert.deepEqual(launch, {
    earliest: "-24h",
    label: "Last 24 hours",
    latest: "now",
    run: true,
    source: "q",
    timezone: null,
    value: "index=main",
  });
  assert.deepEqual(location({}), {
    earliest: null, label: null, latest: null, run: true, source: null, timezone: null, value: null,
  });
});

test("an ambiguous URL counts as a bare page rather than throwing", () => {
  const launch = location({ q: "index=main", savedSearchId: "saved-1" });
  assert.equal(launch.source, null);
  assert.equal(launch.value, null);
  assert.equal(launch.run, false);
});

test("the transition table: same launch is none, a bare page adopts, refinements replace, changes push", () => {
  const draft = location({ q: "index=main", run: "0" });
  const ran = location({ q: "index=main", run: "1", ...LAST_DAY });
  const ranLastWeek = location({ q: "index=main", run: "1", ...LAST_WEEK });
  const other = location({ q: "index=main | stats count", run: "1", ...LAST_DAY });
  const saved = location({ savedSearchId: "saved-1", run: "0" });
  const bare = location({});

  assert.equal(launchTransition(ran, ran, "navigate"), "none");
  assert.equal(launchTransition(bare, ran, "navigate"), "replace");
  assert.equal(launchTransition(draft, ran, "navigate"), "replace", "a draft gaining a range refines its entry");
  assert.equal(launchTransition(ran, ranLastWeek, "navigate"), "push", "a different range is a new launch");
  assert.equal(launchTransition(ran, other, "navigate"), "push");
  assert.equal(launchTransition(ran, saved, "navigate"), "push");
  assert.equal(launchTransition(saved, location({ savedSearchId: "saved-1", run: "1" }), "navigate"), "replace");
  assert.equal(launchTransition(ran, saved, "rewrite"), "replace", "relabelling never adds history");
  assert.equal(launchTransition(saved, ran, "rewrite"), "replace");
});

test("launch URLs keep unrelated parameters and drop the previous range when none is given", () => {
  const current = new URL("https://example.test/search/?appId=ops&q=old&run=1&earliest=-7d&latest=now&label=Last+7+days&timezone=UTC");
  const url = searchLaunchUrl(current, "savedSearchId", "saved-1", false, null);
  assert.equal(url.searchParams.get("appId"), "ops");
  assert.equal(url.searchParams.get("savedSearchId"), "saved-1");
  assert.equal(url.searchParams.get("run"), "0");
  assert.equal(url.searchParams.has("q"), false);
  assert.equal(url.searchParams.has("earliest"), false);
  assert.equal(url.searchParams.has("timezone"), false);

  const ranged = searchLaunchUrl(current, "q", "index=main", true, { ...LAST_DAY, timezone: "Europe/London" });
  assert.equal(ranged.searchParams.get("q"), "index=main");
  assert.equal(ranged.searchParams.get("earliest"), "-24h");
  assert.equal(ranged.searchParams.get("label"), "Last 24 hours");
  assert.equal(ranged.searchParams.get("timezone"), "Europe/London");
});

test("history state is only trusted when the whole draft is present", () => {
  assert.equal(readSearchLaunchState(null), null);
  assert.equal(readSearchLaunchState({ __NA: true }), null);
  assert.equal(readSearchLaunchState({ q: "index=main", earliest: "-24h", latest: "now" }), null);
  assert.deepEqual(readSearchLaunchState({ q: "index=main", ...LAST_DAY, __NA: true, searchJobId: "", timezone: "UTC" }), {
    earliest: "-24h",
    label: "Last 24 hours",
    latest: "now",
    q: "index=main",
    searchJobId: undefined,
    timezone: "UTC",
  });
  assert.equal(readSearchLaunchState({ q: "index=main", ...LAST_DAY, searchJobId: "job-1" })?.searchJobId, "job-1");
  assert.equal(readSearchLaunchState({ q: "index=main", ...LAST_DAY, resultView: "statistics" })?.resultView, "statistics");
  assert.equal(readSearchLaunchState({ q: "index=main", ...LAST_DAY, resultView: "unknown" })?.resultView, undefined);
});

test("tab-only history entries retain the same launch identity", () => {
  const events = { q: "index=main", ...LAST_DAY, resultView: "events" as const };
  const statistics = { ...events, resultView: "statistics" as const };
  assert.equal(sameSearchLaunchState(events, statistics), true);
  assert.equal(sameSearchLaunchState(events, { ...statistics, q: "index=other" }), false);
});

test("the popstate decision table", () => {
  const remembered = { q: "index=main", ...LAST_DAY, searchJobId: "job-9" };
  const draftOnly = { q: "index=main | head 5", ...LAST_WEEK };
  const ran = new URLSearchParams({ q: "index=main", run: "1", ...LAST_DAY });
  const draft = new URLSearchParams({ q: "index=main", run: "0" });

  assert.deepEqual(historyNavigationDecision(ran, remembered, true), { kind: "open-job", searchJobId: "job-9" });
  assert.deepEqual(historyNavigationDecision(ran, remembered, false), {
    kind: "restore", query: "index=main", range: { ...LAST_DAY, timezone: undefined }, run: true,
  }, "the demo has no retained job to reopen, so it runs again");
  assert.deepEqual(historyNavigationDecision(ran, null, true), {
    kind: "restore", query: "index=main", range: { ...LAST_DAY, timezone: undefined }, run: true,
  }, "a run entry without a remembered job runs again");
  assert.deepEqual(historyNavigationDecision(draft, draftOnly, true), {
    kind: "restore", query: "index=main", range: { ...LAST_WEEK, timezone: undefined }, run: false,
  }, "the URL query wins, the remembered range fills in");
  assert.deepEqual(historyNavigationDecision(draft, null, true), {
    kind: "restore", query: "index=main", range: null, run: false,
  });
  assert.deepEqual(historyNavigationDecision(new URLSearchParams(), draftOnly, false), {
    kind: "restore", query: "index=main | head 5", range: { ...LAST_WEEK, timezone: undefined }, run: false,
  }, "a bare entry restores its remembered draft without running");
  assert.deepEqual(historyNavigationDecision(new URLSearchParams(), null, false), { kind: "launch" });
  assert.deepEqual(historyNavigationDecision(new URLSearchParams({ savedSearchId: "saved-1", run: "1" }), draftOnly, false), { kind: "launch" });
  assert.deepEqual(historyNavigationDecision(new URLSearchParams({ searchJobId: "job-1" }), null, true), { kind: "launch" });
  assert.equal(historyNavigationDecision(new URLSearchParams({ q: "a", savedSearchId: "b" }), null, true).kind, "invalid");
});

test("committing a launch pushes or replaces per the transition and remembers the draft beside framework state", () => {
  const fake = installFakeWindow("https://example.test/search/?q=index%3Dmain&run=0", { __NA: true, tree: ["/"] });
  try {
    const first = commitSearchLaunch("q", "index=main", LAST_DAY, { mode: "navigate", state: { q: "index=main", ...LAST_DAY } });
    assert.equal(first, "replace");
    assert.equal(fake.entries.at(-1)?.operation, "replace");
    assert.equal(new URL(fake.entries.at(-1)?.href ?? "").searchParams.get("earliest"), "-24h");
    assert.deepEqual(fake.history.state, {
      __NA: true, tree: ["/"], q: "index=main", ...LAST_DAY, searchJobId: undefined, timezone: undefined,
    });

    stampSearchLaunchState({ searchJobId: "job-1" });
    assert.equal(fake.entries.at(-1)?.operation, "replace");
    assert.equal((fake.history.state as { searchJobId?: string }).searchJobId, "job-1");

    const second = commitSearchLaunch("q", "index=main | stats count", LAST_DAY, {
      mode: "navigate",
      state: { q: "index=main | stats count", ...LAST_DAY },
    });
    assert.equal(second, "push");
    assert.equal(fake.entries.at(-1)?.operation, "push");
    assert.equal(
      (fake.history.state as { searchJobId?: string }).searchJobId,
      undefined,
      "a new entry must not inherit the previous entry's job",
    );
    assert.equal(new URL(fake.entries.at(-1)?.href ?? "").searchParams.get("q"), "index=main | stats count");

    const saved = commitSearchLaunch("savedSearchId", "saved-1", null, {
      mode: "rewrite",
      state: { q: "index=main | stats count", ...LAST_DAY },
    });
    assert.equal(saved, "replace");
    assert.equal(new URL(fake.entries.at(-1)?.href ?? "").searchParams.get("savedSearchId"), "saved-1");
    assert.equal(fake.entries.filter((entry) => entry.operation === "push").length, 1);
  } finally {
    fake.restore();
  }
});
