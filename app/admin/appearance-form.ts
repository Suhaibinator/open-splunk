import { PALETTES, type Palette } from "@/lib/palettes";

/**
 * The Appearance card's data: its heading, what each palette is called and
 * the one line that tells an administrator what choosing it does. Data only
 * -- the names come from `lib/palettes.ts`, the wire mapping from
 * `lib/api/ui-palette.ts`, and the card itself from appearance-settings.tsx
 * -- so the copy can be held to the palette list by a unit test without
 * rendering anything, and `scripts/palette-gallery.mjs` can rebuild the card
 * from the same copy on the demo export, which has no Appearance card.
 */

/** The card's heading. */
export const APPEARANCE_TITLE = "Appearance";

/** The line under the heading. */
export const APPEARANCE_DESCRIPTION =
  "Instance-wide palette shown to every user, including the sign-in page. Light and dark stay each user's own choice.";

export interface PaletteOption {
  /** One line, a full sentence, shown under the label. */
  description: string;
  /** The name as the radio shows it and the toast repeats it. */
  label: string;
}

export const PALETTE_OPTIONS: Readonly<Record<Palette, PaletteOption>> = {
  classic: { label: "Classic", description: "The current Splunk-style look." },
  ocean: { label: "Ocean", description: "Cool blue surfaces and slate-blue bars." },
  ember: { label: "Ember", description: "Warm neutrals with a rust accent." },
  graphite: { label: "Graphite", description: "Near-monochrome, high contrast; colour is reserved for state and code." },
  glass: { label: "Glass", description: "Translucent surfaces, soft radii." },
  terminal: { label: "Terminal", description: "Monospace UI, square corners, high contrast." },
};

/** The `id` the radio for a palette carries, shared by its label. */
export function paletteOptionId(palette: Palette): string {
  return `appearance-palette-${palette}`;
}

/** The options in the order the card lists them: `PALETTES` order. */
export function paletteOptions(): ReadonlyArray<[Palette, PaletteOption]> {
  return PALETTES.map((palette) => [palette, PALETTE_OPTIONS[palette]]);
}
