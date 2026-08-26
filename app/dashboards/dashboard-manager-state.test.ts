import assert from "node:assert/strict";
import test from "node:test";

import {
  dashboardActionError,
  dashboardLoadError,
  dashboardPanelRunCanPublish,
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

test("panel output belongs only to the current non-aborted run token", () => {
  const expected = {};
  assert.equal(dashboardPanelRunCanPublish(expected, expected, false), true);
  assert.equal(dashboardPanelRunCanPublish(expected, {}, false), false);
  assert.equal(dashboardPanelRunCanPublish(expected, undefined, false), false);
  assert.equal(dashboardPanelRunCanPublish(expected, expected, true), false);
});
