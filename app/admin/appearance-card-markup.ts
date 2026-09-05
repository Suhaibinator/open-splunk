import { DEFAULT_PALETTE, type Palette } from "@/lib/palettes";

import { APPEARANCE_DESCRIPTION, APPEARANCE_TITLE, paletteOptionId, paletteOptions } from "./appearance-form";

/**
 * The Appearance card as a string of HTML, byte for byte what
 * `renderToStaticMarkup(<AppearanceCard …/>)` in appearance-settings.tsx
 * produces for a card whose saved palette is `selected` and which is not busy.
 *
 * `scripts/palette-gallery.mjs` inserts this into the demo export's Server
 * section, which has no Appearance card of its own, so the gallery shows the
 * card every palette ships with. The script runs under node's own type
 * stripping and cannot render JSX, so the card is rebuilt here as a string;
 * the unit test beside the card holds the two renderings equal, so a change
 * to the card's structure in the component is caught the moment it lands
 * rather than turning up as a mis-styled capture.
 */
export function appearanceCardMarkup(selected: Palette, defaultPalette: Palette = DEFAULT_PALETTE): string {
  const radios = paletteOptions().map(([palette, option]) => {
    const id = paletteOptionId(palette);
    const checked = palette === selected;
    // react-dom writes an input's type, name, checked and value after every
    // other attribute, in that order, and self-closes the tag.
    return `<label${checked ? ' class="is-selected"' : ""} for="${id}">`
      + `<input aria-describedby="${id}-description" aria-label="${escapeHtml(option.label)}" id="${id}"`
      + ` type="radio" name="appearance-palette"${checked ? ' checked=""' : ""} value="${palette}"/>`
      + `<span><strong>${escapeHtml(option.label)}</strong>`
      + `<small id="${id}-description">${escapeHtml(option.description)}</small></span></label>`;
  }).join("");
  return `<section class="suite-card settings-group"><header><h3>${escapeHtml(APPEARANCE_TITLE)}</h3>`
    + `<p>${escapeHtml(APPEARANCE_DESCRIPTION)}</p></header>`
    + `<div class="appearance-palette-options" role="radiogroup" aria-label="Palette">${radios}</div></section>`
    + '<div class="settings-actions"><button class="button button--secondary"'
    + `${selected === defaultPalette ? ' disabled=""' : ""} type="button">Reset to default</button>`
    + '<button class="button button--primary" disabled="" type="submit">Apply</button></div>';
}

/** Escapes text the way react-dom's static renderer does, so the output can be held equal to it. */
function escapeHtml(text: string): string {
  return text
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#x27;");
}
