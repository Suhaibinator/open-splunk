/**
 * The instance palettes: the second axis of the theme, beside light and dark.
 *
 * An administrator picks one palette for the whole instance; each user keeps
 * their own System / Light / Dark choice on top of it. `classic` is the base
 * pair in `app/styles/tokens-color.css` and has no file of its own; every
 * other name owns `app/styles/tokens-palette-<name>.css`, which restates the
 * base pair for that look (docs/theming.md, "Adding a theme").
 *
 * This module deliberately holds nothing but the list and the contrast each
 * palette promises: `scripts/style-invariants.test.mjs` reads the `PALETTES`
 * and `PALETTE_CONTRAST_FLOOR` literals below by regex to learn which palette
 * files must exist and what floor each is held to, and the Go enum, the SQL
 * CHECK and the proto enum spell the same six names in the same order.
 */

/** Every palette the stylesheet can render. */
export type Palette = "classic" | "ocean" | "ember" | "graphite" | "glass" | "terminal";

/** The palettes in the order the admin form lists them. */
export const PALETTES: readonly Palette[] = ["classic", "ocean", "ember", "graphite", "glass", "terminal"];

/**
 * The contrast a palette promises where it promises more than WCAG AA (4.5:1).
 *
 * `graphite` is the high-contrast, near-monochrome palette and doubles as the
 * accessibility option, so it is held to AAA (7:1). A palette not listed here
 * is held to AA. The token invariants and the computed-style contracts both
 * read this one table, so the two guardrails cannot disagree about a palette.
 */
export const PALETTE_CONTRAST_FLOOR: Readonly<Partial<Record<Palette, number>>> = { graphite: 7 };

/** What renders when the server has chosen nothing, or a name this build does not know. */
export const DEFAULT_PALETTE: Palette = "classic";

/** Narrows an arbitrary string to a shipped palette, falling back to the default. */
export function resolvePalette(stored: string | null | undefined): Palette {
  return (PALETTES as readonly string[]).includes(stored ?? "") ? (stored as Palette) : DEFAULT_PALETTE;
}
