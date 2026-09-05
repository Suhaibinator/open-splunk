import { UiPalette } from "@/gen/ts/open_splunk/server_settings_api";
import { DEFAULT_PALETTE, type Palette } from "@/lib/palettes";

/**
 * The wire spelling of the instance palette.
 *
 * `lib/palettes.ts` owns the names the stylesheet knows; this module owns only
 * the mapping to and from the generated `UiPalette` enum. Anything the enum
 * cannot name -- `UNSPECIFIED` from a server without a settings service,
 * `UNRECOGNIZED` from a newer server, an absent field -- paints classic, so a
 * version skew never leaves the page without a palette.
 */
const PALETTE_BY_PROTO: ReadonlyMap<UiPalette, Palette> = new Map<UiPalette, Palette>([
  [UiPalette.UI_PALETTE_CLASSIC, "classic"],
  [UiPalette.UI_PALETTE_OCEAN, "ocean"],
  [UiPalette.UI_PALETTE_EMBER, "ember"],
  [UiPalette.UI_PALETTE_GRAPHITE, "graphite"],
  [UiPalette.UI_PALETTE_GLASS, "glass"],
  [UiPalette.UI_PALETTE_TERMINAL, "terminal"],
]);

const PROTO_BY_PALETTE: Readonly<Record<Palette, UiPalette>> = {
  classic: UiPalette.UI_PALETTE_CLASSIC,
  ocean: UiPalette.UI_PALETTE_OCEAN,
  ember: UiPalette.UI_PALETTE_EMBER,
  graphite: UiPalette.UI_PALETTE_GRAPHITE,
  glass: UiPalette.UI_PALETTE_GLASS,
  terminal: UiPalette.UI_PALETTE_TERMINAL,
};

/** The palette a response names, or classic for anything this build cannot paint. */
export function paletteFromProto(value: UiPalette | undefined): Palette {
  if (value === undefined) return DEFAULT_PALETTE;
  return PALETTE_BY_PROTO.get(value) ?? DEFAULT_PALETTE;
}

/** The enum value a request carries for a shipped palette. */
export function paletteToProto(palette: Palette): UiPalette {
  return PROTO_BY_PALETTE[palette];
}
