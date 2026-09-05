import assert from "node:assert/strict";
import test from "node:test";

import { UiPalette } from "@/gen/ts/open_splunk/server_settings_api";
import { DEFAULT_PALETTE, PALETTES } from "@/lib/palettes";

import { paletteFromProto, paletteToProto } from "./ui-palette";

test("every shipped palette round-trips through the wire enum", () => {
  const seen = new Set<UiPalette>();
  for (const palette of PALETTES) {
    const wire = paletteToProto(palette);
    assert.notEqual(wire, UiPalette.UI_PALETTE_UNSPECIFIED, palette);
    assert.notEqual(wire, UiPalette.UNRECOGNIZED, palette);
    assert.equal(paletteFromProto(wire), palette);
    seen.add(wire);
  }
  assert.equal(seen.size, PALETTES.length, "two palettes share an enum value");
});

test("the enum and the palette list spell the same names in the same order", () => {
  // The Go enum, the SQL CHECK and the proto enum list the six names in
  // PALETTES order starting at 1; the TypeScript mapping has to agree.
  assert.deepEqual(
    PALETTES.map((palette) => paletteToProto(palette)),
    PALETTES.map((_, index) => index + 1),
  );
  assert.deepEqual(
    PALETTES.map((palette) => UiPalette[paletteToProto(palette)]),
    PALETTES.map((palette) => `UI_PALETTE_${palette.toUpperCase()}`),
  );
});

test("anything the enum cannot name paints classic", () => {
  assert.equal(DEFAULT_PALETTE, "classic");
  assert.equal(paletteFromProto(UiPalette.UI_PALETTE_UNSPECIFIED), "classic");
  assert.equal(paletteFromProto(UiPalette.UNRECOGNIZED), "classic");
  assert.equal(paletteFromProto(undefined), "classic");
  // A number a newer server might send that ts-proto has not decoded into
  // UNRECOGNIZED because it arrived through fromPartial.
  assert.equal(paletteFromProto(99 as UiPalette), "classic");
  assert.equal(paletteFromProto(-2 as UiPalette), "classic");
});

test("every enum number from -1 to 7 maps to exactly the palette at that position, or classic", () => {
  // 0 is UNSPECIFIED, 1..6 are the six palettes in PALETTES order, 7 is the
  // first number a future server could add, and -1 is ts-proto's UNRECOGNIZED.
  const expected: Record<number, string> = { [-1]: "classic", 0: "classic", 7: "classic" };
  PALETTES.forEach((palette, index) => {
    expected[index + 1] = palette;
  });
  for (let number = -1; number <= 7; number += 1) {
    assert.equal(paletteFromProto(number as UiPalette), expected[number], `enum ${number}`);
  }
  assert.equal(UiPalette.UNRECOGNIZED, -1);
  assert.equal(UiPalette.UI_PALETTE_UNSPECIFIED, 0);
  assert.equal(Object.values(UiPalette).filter((value) => typeof value === "number").length, PALETTES.length + 2);
  // Number-like values that are not integers, and non-numbers, never match a
  // Map keyed by integers; a loose comparison would have let "1" through.
  for (const odd of [1.5, Number.NaN, "1", "ocean", null, true, 1n]) {
    assert.equal(paletteFromProto(odd as unknown as UiPalette), "classic", String(odd));
  }
});
