import assert from "node:assert/strict";
import test from "node:test";

import { SharingScope } from "@/gen/ts/open_splunk/common";
import type { DashboardDefinition } from "@/gen/ts/open_splunk/dashboard";

import {
  cloneDashboardDefinition,
  dashboardDefinitionsEqual,
  reconcileSavedDashboardDraft,
} from "./dashboard-editor-state";

function definition(name = "Operations", spl = "index=main"): DashboardDefinition {
  return {
    name,
    description: undefined,
    appId: "app_default",
    sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
    ownerId: "owner",
    panels: [{
      panelId: "panel_1",
      title: "Errors",
      description: undefined,
      search: {
        spl,
        timeRange: { earliest: "-24h", latest: "now", timezone: "UTC" },
        appId: "app_default",
        indexScope: ["main"],
        preferredResultTab: 0,
        selectedFields: [],
        visualization: undefined,
      },
      column: 0,
      row: 0,
      width: 12,
      height: 4,
    }],
  };
}

test("dashboard definition equality covers nested panel edits", () => {
  const baseline = definition();
  assert.equal(dashboardDefinitionsEqual(baseline, cloneDashboardDefinition(baseline)), true);
  assert.equal(dashboardDefinitionsEqual(baseline, definition("Operations", "index=other")), false);
  assert.equal(dashboardDefinitionsEqual(baseline, null), false);
});

test("a save response replaces only the draft snapshot that was submitted", () => {
  const submitted = definition();
  const persisted = definition("Operations (normalized)");
  const reconciled = reconcileSavedDashboardDraft(
    cloneDashboardDefinition(submitted),
    submitted,
    persisted,
  );
  assert.notEqual(reconciled, persisted);
  assert.equal(dashboardDefinitionsEqual(reconciled, persisted), true);
});

test("a save response preserves edits made after submission", () => {
  const submitted = definition();
  const newerDraft = definition("Unsaved newer name");
  const reconciled = reconcileSavedDashboardDraft(newerDraft, submitted, definition("Persisted name"));
  assert.equal(reconciled, newerDraft);
});
