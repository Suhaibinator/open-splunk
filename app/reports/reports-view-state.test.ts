import assert from "node:assert/strict";
import test from "node:test";

import {
  reportsViewForKey,
  scheduledReportConfigurationHref,
  scheduledReportConfigurationTarget,
} from "./reports-view-state";

test("report tabs support wrapping arrow navigation and boundary keys", () => {
  assert.equal(reportsViewForKey("saved-searches", "ArrowRight"), "alerts");
  assert.equal(reportsViewForKey("alerts", "ArrowRight"), "saved-searches");
  assert.equal(reportsViewForKey("saved-searches", "ArrowLeft"), "alerts");
  assert.equal(reportsViewForKey("alerts", "Home"), "saved-searches");
  assert.equal(reportsViewForKey("saved-searches", "End"), "alerts");
  assert.equal(reportsViewForKey("saved-searches", "Enter"), null);
});

test("report schedule links round-trip exactly one saved-search target", () => {
  const href = scheduledReportConfigurationHref("saved/a b");
  assert.equal(href, "/reports/saved-searches/?scheduleSavedSearchId=saved%2Fa+b");
  assert.equal(
    scheduledReportConfigurationTarget(new URL(href, "https://example.test").searchParams),
    "saved/a b",
  );
  assert.equal(scheduledReportConfigurationTarget(new URLSearchParams()), null);
  assert.throws(() => scheduledReportConfigurationTarget(new URLSearchParams("scheduleSavedSearchId=")), /invalid/);
  assert.throws(() => scheduledReportConfigurationTarget(new URLSearchParams("scheduleSavedSearchId=a&scheduleSavedSearchId=b")), /invalid/);
});
