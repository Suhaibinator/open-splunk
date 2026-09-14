import assert from "node:assert/strict";
import test from "node:test";

import {
  dashboardActionError,
  dashboardLoadError,
  dashboardPanelRunCanPublish,
  dashboardViewState,
} from "./dashboard-manager-state";

test("dashboard load errors retain only their own exact retry target", () => {
  assert.deepEqual(dashboardLoadError("switch failed", "switch", "app_requested"), {
    message: "switch failed",
    retry: { appId: "app_requested", mode: "switch" },
  });
  assert.deepEqual(dashboardLoadError("initial load failed", "initial", "app_initial"), {
    message: "initial load failed",
    retry: { appId: "app_initial", mode: "reload" },
  });
  assert.deepEqual(dashboardActionError("save failed"), {
    message: "save failed",
    retry: null,
  });
});

test("dashboard onboarding appears only after a successful empty catalog load", () => {
  const base = { appCount: 1, available: true, dashboardCount: 0, error: null, loadedCatalog: true, loading: false };
  assert.equal(dashboardViewState(base), "empty");
  assert.equal(dashboardViewState({ ...base, loading: true }), "loading");
  assert.equal(dashboardViewState({ ...base, available: false }), "unavailable");
  assert.equal(dashboardViewState({ ...base, loadedCatalog: false, error: dashboardLoadError("failed", "initial", undefined) }), "error");
  assert.equal(dashboardViewState({ ...base, loadedCatalog: false, available: false }), "unavailable");
  assert.equal(dashboardViewState({ ...base, appCount: 0, error: dashboardLoadError("failed", "reload", undefined) }), "no-apps");
  assert.equal(dashboardViewState({ ...base, error: dashboardActionError("create failed") }), "empty");
  assert.equal(dashboardViewState({ ...base, error: dashboardLoadError("switch failed", "switch", "app-2") }), "empty");
  assert.equal(dashboardViewState({ ...base, appCount: 0, loadedCatalog: false }), "loading");
  assert.equal(dashboardViewState({ ...base, appCount: 0 }), "no-apps");
  assert.equal(dashboardViewState({ ...base, dashboardCount: 1 }), "ready");
});

test("panel output belongs only to the current non-aborted run token", () => {
  const expected = {};
  assert.equal(dashboardPanelRunCanPublish(expected, expected, false), true);
  assert.equal(dashboardPanelRunCanPublish(expected, {}, false), false);
  assert.equal(dashboardPanelRunCanPublish(expected, undefined, false), false);
  assert.equal(dashboardPanelRunCanPublish(expected, expected, true), false);
});
