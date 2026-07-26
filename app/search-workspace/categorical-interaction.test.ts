import assert from "node:assert/strict";
import test from "node:test";

import { categoricalActivation } from "./categorical-interaction";

test("touch-like categorical activation inspects before drilldown", () => {
  assert.equal(categoricalActivation("touch"), "inspect");
  assert.equal(categoricalActivation("pen"), "inspect");
});

test("mouse and keyboard categorical activation retain direct drilldown", () => {
  assert.equal(categoricalActivation("mouse"), "drilldown");
  assert.equal(categoricalActivation(null), "drilldown");
});
