import assert from "node:assert/strict";
import test from "node:test";
import { Children, isValidElement, type ComponentProps, type ReactNode } from "react";
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

function findPatternAction(node: ReactNode): (() => void) | undefined {
  for (const child of Children.toArray(node)) {
    if (!isValidElement<{ className?: string; onClick?: () => void; children?: ReactNode }>(child)) continue;
    if (child.props.className?.includes("pattern-action")) return child.props.onClick;
    const nested = findPatternAction(child.props.children);
    if (nested) return nested;
  }
  return undefined;
}

test("View events passes the opaque group to the exact callback without a wildcard query", () => {
  const pattern = { patternId: "opaque-pattern", signature: "literal * <int>", count: 3, percent: 100 };
  const calls: unknown[] = [];
  const element = PatternsPanel({
    menu: null, patternRows: [pattern], patternSensitivity: "Balanced",
    onMenuChange: () => undefined, onPatternSensitivityChange: () => undefined, onShowToast: () => undefined,
    onTabChange: (tab) => calls.push({ legacyTab: tab }),
    onViewEvents: (signature) => calls.push({ legacySignature: signature }),
    onViewPattern: (selected) => calls.push(selected),
  });
  const action = findPatternAction(element);
  assert.ok(action);
  action();
  assert.deepEqual(calls, [pattern]);
});
