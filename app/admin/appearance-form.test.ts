import assert from "node:assert/strict";
import test from "node:test";

import { DEFAULT_PALETTE, PALETTES } from "@/lib/palettes";

import { PALETTE_OPTIONS, paletteOptionId, paletteOptions } from "./appearance-form";

test("every shipped palette has a label and a one-line description, and nothing else does", () => {
  assert.deepEqual(Object.keys(PALETTE_OPTIONS).toSorted(), [...PALETTES].toSorted());
  for (const palette of PALETTES) {
    const option = PALETTE_OPTIONS[palette];
    assert.match(option.label, /^[A-Z][a-z]+$/u, `${palette}: the label is one capitalised word`);
    assert.equal(option.label.toLowerCase(), palette, `${palette}: the label is the palette's own name`);
    assert.doesNotMatch(option.description, /\n/u, `${palette}: the description is one line`);
    assert.match(option.description, /^[A-Z].*\.$/u, `${palette}: the description is a sentence`);
    assert.ok(option.description.length <= 80, `${palette}: the description fits under the label`);
  }
});

test("the default palette is described as the current look", () => {
  assert.equal(DEFAULT_PALETTE, "classic");
  assert.equal(PALETTE_OPTIONS[DEFAULT_PALETTE].description, "The current Splunk-style look.");
});

test("the options list follows PALETTES order and the ids are distinct", () => {
  assert.deepEqual(paletteOptions().map(([palette]) => palette), PALETTES);
  assert.deepEqual(paletteOptions().map(([, option]) => option), PALETTES.map((palette) => PALETTE_OPTIONS[palette]));
  const ids = PALETTES.map(paletteOptionId);
  assert.equal(new Set(ids).size, PALETTES.length);
  for (const id of ids) assert.match(id, /^[a-z][a-z0-9-]*$/u);
});
