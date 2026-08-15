import assert from "node:assert/strict";
import test from "node:test";

import { applyFieldPivot, isSplFieldRepresentable } from "./query-pivots";

test("pivot field validation rejects every unpaired surrogate position", () => {
  assert.equal(isSplFieldRepresentable("a\ud800"), false);
  assert.equal(isSplFieldRepresentable("\ud800"), false);
  assert.equal(isSplFieldRepresentable("a\ud800b"), false);
  assert.equal(isSplFieldRepresentable("a\udc00"), false);
  assert.equal(isSplFieldRepresentable("\u{1f600}field"), true);
});

test("pivot leaves the draft unchanged for a trailing unpaired surrogate", () => {
  assert.equal(applyFieldPivot("index=web", "a\ud800", "value", "include"), "index=web");
  assert.equal(applyFieldPivot("index=web", "status", "value", "include"), 'index=web status="value"');
});
