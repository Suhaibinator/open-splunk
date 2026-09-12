import assert from "node:assert/strict";
import test from "node:test";
import type { ComponentProps } from "react";
import { renderToStaticMarkup } from "react-dom/server";
import { PatternsPanel } from "./patterns-panel";

function renderPanel(overrides: Partial<ComponentProps<typeof PatternsPanel>> = {}) {
  return renderToStaticMarkup(<PatternsPanel
    menu={null}
    patternRows={[]}
    patternSensitivity="Balanced"
    onMenuChange={() => undefined}
    onPatternSensitivityChange={() => undefined}
    onShowToast={() => undefined}
    onViewEvents={() => undefined}
    onTabChange={() => undefined}
    {...overrides}
  />);
}

test("Patterns explains the eligible denominator and retained scope", () => {
  assert.match(renderPanel(), /Coverage is the share of eligible events/u);
});

test("retained counts, truncation and eligible denominator are visible with pageable groups", () => {
  const markup = renderPanel({
    coverage: { retainedRows: 10_000, eligibleRows: 9_900, excludedRows: 100, totalGroups: 45, retainedTruncated: true, snapshotComplete: true, algorithmVersion: "1" },
    patternRows: [{ patternId: "opaque-pattern", signature: "request <int>", count: 99, percent: 1 }],
    pageNumber: 2, pageSize: 20, hasNextPage: true, onPageChange: () => undefined,
    onViewPattern: () => undefined,
  });
  assert.match(markup, /9,900 eligible events of 10,000 retained rows/u);
  assert.match(markup, /100 excluded/u);
  assert.match(markup, /retained snapshot is truncated/u);
  assert.match(markup, /1\.0%/u);
  assert.match(markup, /class="pattern-rank">21</u);
  assert.match(markup, /aria-label="Pattern pages"/u);
});

test("server patterns cannot fall back to wildcard callbacks", () => {
  const markup = renderPanel({ patternRows: [{ patternId: "opaque-pattern", signature: "* literal", count: 1, percent: 100 }] });
  assert.match(markup, /class="button button--link pattern-action"[^>]*disabled=""/u);
});

test("loading, retry and empty states describe the operation", () => {
  assert.match(renderPanel({ loading: true }), /role="status">Loading patterns/u);
  assert.match(renderPanel({ error: "Snapshot expired", onRetry: () => undefined }), /role="alert">Snapshot expired/u);
  assert.match(renderPanel(), /No eligible event patterns/u);
});
