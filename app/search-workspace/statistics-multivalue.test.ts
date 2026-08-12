import assert from "node:assert/strict";
import test from "node:test";

import { statsFlatMultivalueDisplay } from "./statistics-multivalue";

test("flat stats multivalue display distinguishes absent and empty delimiters", () => {
  const members = ["alpha", "beta", "gamma"];
  assert.equal(statsFlatMultivalueDisplay(members, undefined), undefined);
  assert.equal(statsFlatMultivalueDisplay(members, " / "), "alpha / beta / gamma");
  assert.equal(statsFlatMultivalueDisplay(members, ""), "alphabetagamma");
  assert.equal(statsFlatMultivalueDisplay([], " / "), "");
});

test("flat stats multivalue display fails closed for non-string cells", () => {
  assert.equal(statsFlatMultivalueDisplay("alpha", ","), undefined);
  assert.equal(statsFlatMultivalueDisplay(["alpha", 2], ","), undefined);
  assert.equal(statsFlatMultivalueDisplay(["alpha", null], ","), undefined);
});
