import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import { SearchFailurePanel } from "./search-failure-panel";

test("failure panel is a persistent region with source and recovery actions", () => {
  const markup = renderToStaticMarkup(
    <SearchFailurePanel
      activeTab="events"
      canNavigateSource
      onFocusProblem={() => undefined}
      onRetry={() => undefined}
      problems={[{
        diagnostic: {
          code: "SPL_QUERY_TOO_COMPLEX",
          message: "The expression is too deep.",
          range: { column: 8, end: 12, line: 2, start: 7 },
          severity: "error",
          suggestions: [],
        },
        fix: null,
        stale: false,
      }]}
      serverSettingsHref="/admin/?section=server"
      presentation={{
        actions: ["retry", "server-settings"],
        detail: "Reduce the query before running it again.",
        guidance: ["Narrow the time range."],
        title: "Search is too complex",
      }}
    />,
  );

  assert.match(markup, /role="tabpanel"/u);
  assert.match(markup, /role="region"/u);
  assert.match(markup, /Line 2, column 8/u);
  assert.match(markup, /Retry/u);
  assert.match(markup, /href="\/admin\?section=server"/u);
});

test("stale source locations are disabled", () => {
  const markup = renderToStaticMarkup(
    <SearchFailurePanel
      activeTab="statistics"
      canNavigateSource={false}
      onFocusProblem={() => undefined}
      onRetry={() => undefined}
      problems={[{
        diagnostic: {
          code: "SPL_EXPECTED_FIELD",
          message: "Expected a field.",
          range: { column: 1, end: 1, line: 1, start: 0 },
          severity: "error",
          suggestions: [],
        },
        fix: null,
        stale: true,
      }]}
      serverSettingsHref="/admin/?section=server"
      presentation={{ actions: [], detail: "Fix the query.", guidance: [], title: "Search failed" }}
    />,
  );

  assert.match(markup, /disabled=""/u);
  assert.doesNotMatch(markup, /Retry/u);
});
