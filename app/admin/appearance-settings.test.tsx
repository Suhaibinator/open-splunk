import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import {
  GetServerAppearanceResponse,
  UiPalette,
  UpdateServerAppearanceResponse,
} from "@/gen/ts/open_splunk/server_settings_api";
import type { OpenSplunkApiClient } from "@/lib/api";
import { PALETTES, type Palette } from "@/lib/palettes";

import { PALETTE_OPTIONS, paletteOptionId } from "./appearance-form";
import {
  APPEARANCE_DESCRIPTION,
  APPEARANCE_TITLE,
  AppearanceCard,
  AppearanceSettings,
  adaptAppearance,
} from "./appearance-settings";

function renderCard(saved: Palette, selected: Palette, busy = false, defaultPalette: Palette = "classic"): string {
  return renderToStaticMarkup(
    <AppearanceCard
      busy={busy}
      defaultPalette={defaultPalette}
      onChoose={() => {}}
      saved={saved}
      selected={selected}
    />,
  );
}

/** The `<input …>` tag for one palette's radio, or `null` when the markup has none. */
function radioTag(markup: string, palette: Palette): string | null {
  const match = new RegExp(`<input[^>]*\\bid="${paletteOptionId(palette)}"[^>]*>`, "u").exec(markup);
  return match?.[0] ?? null;
}

/** The `<button …>…</button>` whose text is exactly `label`. */
function buttonTag(markup: string, label: string): string {
  const match = new RegExp(`<button[^>]*>${label}</button>`, "u").exec(markup);
  assert.ok(match, `no button labelled ${label}`);
  return match[0];
}

test("the card lists every palette once as a radio in its label, with the plan's copy", () => {
  const markup = renderCard("classic", "classic");
  assert.equal((markup.match(/type="radio"/gu) ?? []).length, PALETTES.length);
  assert.match(markup, /<div class="appearance-palette-options" role="radiogroup" aria-label="Palette">/u);
  assert.match(markup, /<section class="suite-card settings-group"><header><h3>Appearance<\/h3>/u);
  assert.equal(APPEARANCE_TITLE, "Appearance");
  assert.ok(markup.includes(`<p>${APPEARANCE_DESCRIPTION.replace(/'/gu, "&#x27;")}</p>`));
  assert.equal(
    APPEARANCE_DESCRIPTION,
    "Instance-wide palette shown to every user, including the sign-in page. Light and dark stay each user's own choice.",
  );
  for (const palette of PALETTES) {
    const id = paletteOptionId(palette);
    const tag = radioTag(markup, palette);
    assert.ok(tag, `${palette} has no radio`);
    assert.match(tag, /name="appearance-palette"/u);
    assert.match(tag, new RegExp(`value="${palette}"`, "u"));
    // The radio sits inside the label that names it, and the label points back.
    assert.match(markup, new RegExp(`<label[^>]*for="${id}"[^>]*>${tag.replace(/[$()*+.?[\\\]^{|}]/gu, "\\$&")}`, "u"));
    assert.ok(markup.includes(`<strong>${PALETTE_OPTIONS[palette].label}</strong>`), palette);
    assert.ok(markup.includes(`<small id="${id}-description">${PALETTE_OPTIONS[palette].description}</small>`), palette);
    assert.match(tag, new RegExp(`aria-describedby="${id}-description"`, "u"));
  }
});

test("exactly the selected palette is checked and its label carries the state class", () => {
  for (const selected of PALETTES) {
    const markup = renderCard("classic", selected);
    assert.equal((markup.match(/\bchecked=""/gu) ?? []).length, 1, selected);
    assert.equal((markup.match(/class="is-selected"/gu) ?? []).length, 1, selected);
    for (const palette of PALETTES) {
      const tag = radioTag(markup, palette);
      assert.ok(tag);
      assert.equal(/\bchecked=""/u.test(tag), palette === selected, `${selected}: ${palette}`);
    }
    assert.match(markup, new RegExp(`<label class="is-selected" for="${paletteOptionId(selected)}"`, "u"));
  }
});

test("Apply is disabled when the selection matches the saved palette and enabled when it differs", () => {
  assert.match(buttonTag(renderCard("ocean", "ocean"), "Apply"), /\bdisabled=""/u);
  assert.doesNotMatch(buttonTag(renderCard("ocean", "ember"), "Apply"), /\bdisabled=""/u);
  assert.doesNotMatch(buttonTag(renderCard("classic", "ocean"), "Apply"), /\bdisabled=""/u);
  assert.match(buttonTag(renderCard("classic", "classic"), "Apply"), /\bdisabled=""/u);
});

test("Reset to default is disabled only while the selection is the default", () => {
  assert.match(buttonTag(renderCard("classic", "classic"), "Reset to default"), /\bdisabled=""/u);
  assert.doesNotMatch(buttonTag(renderCard("classic", "glass"), "Reset to default"), /\bdisabled=""/u);
  // The default comes from the server, not from the module: a server that
  // defaults to ocean disables the reset on ocean.
  assert.match(buttonTag(renderCard("ocean", "ocean", false, "ocean"), "Reset to default"), /\bdisabled=""/u);
  assert.doesNotMatch(buttonTag(renderCard("classic", "classic", false, "ocean"), "Reset to default"), /\bdisabled=""/u);
});

test("saving disables every control and relabels Apply", () => {
  const markup = renderCard("classic", "terminal", true);
  assert.equal((markup.match(/<input[^>]*\bdisabled=""/gu) ?? []).length, PALETTES.length);
  assert.match(buttonTag(markup, "Applying…"), /\bdisabled=""/u);
  assert.match(buttonTag(markup, "Reset to default"), /\bdisabled=""/u);
});

test("the settings form renders its loading notice before any request resolves", () => {
  let calls = 0;
  const client = {
    serverSettings: {
      getAppearance: () => {
        calls += 1;
        return new Promise<GetServerAppearanceResponse>(() => {});
      },
    },
  } as unknown as OpenSplunkApiClient;
  const markup = renderToStaticMarkup(
    <AppearanceSettings client={client} onDirtyChange={() => {}} onStatus={() => {}} />,
  );
  assert.match(markup, /<output class="access-mode-notice">/u);
  assert.match(markup, /<strong>Loading appearance<\/strong>/u);
  assert.doesNotMatch(markup, /type="radio"/u);
  // A static render runs no effects, so the request is not issued here.
  assert.equal(calls, 0);
});

test("adaptAppearance narrows the envelope and paints classic for a palette this build lacks", () => {
  assert.equal(adaptAppearance(GetServerAppearanceResponse.fromPartial({})), null);
  assert.deepEqual(adaptAppearance(GetServerAppearanceResponse.fromPartial({
    current: { version: 3n, palette: UiPalette.UI_PALETTE_GRAPHITE },
    defaultPalette: UiPalette.UI_PALETTE_CLASSIC,
  })), { defaultPalette: "classic", palette: "graphite", version: 3n });
  assert.deepEqual(adaptAppearance(UpdateServerAppearanceResponse.fromPartial({
    current: { version: 4n, palette: 99 as UiPalette },
    defaultPalette: UiPalette.UI_PALETTE_UNSPECIFIED,
  })), { defaultPalette: "classic", palette: "classic", version: 4n });
});
