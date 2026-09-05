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
