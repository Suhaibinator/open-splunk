import { readFileSync } from "node:fs";
import path from "node:path";

import { type Page } from "@playwright/test";

/**
 * The shipped stylesheets, in the order `app/layout.tsx` loads them.
 *
 * The contract suite mounts fixture markup in a bare page, so nothing loads
 * the application's CSS for it. The fixtures used
 * to inject `app/globals.css` alone, which was complete only while that one
 * file held every rule and every custom property. It now holds neither: the
 * token layer declares the properties and `app/styles/index.css` imports a
 * file per primitive and per feature, so injecting any single one of them
 * would leave the fixture painting browser defaults and the assertions
 * pinning the fallback rather than the shipped rule.
 *
 * `app/styles/index.css` cannot stand in for its imports here either: an
 * `@import` inside an injected `<style>` resolves against the page URL, which
 * for `page.setContent` is `about:blank`. So the list is read out of that file
 * instead of restated -- adding a stylesheet stays the one-line edit to
 * `index.css` that the layer promises, and this list can never drift from the
 * cascade order the browser actually gets.
 */
const stylesRoot = path.join(__dirname, "..", "..", "app", "styles");

function importedStylesheets(): readonly string[] {
  const index = readFileSync(path.join(stylesRoot, "index.css"), "utf8");
  const specifiers = [...index.matchAll(/@import\s+url\("([^"]+)"\)/gu)].map((match) => match[1]);
  if (specifiers.length === 0) throw new Error("app/styles/index.css imports nothing; the fixture suites would paint browser defaults");
  return specifiers.map((specifier) => path.join(stylesRoot, specifier));
}

export const APPLICATION_STYLESHEETS: readonly string[] = importedStylesheets();

/**
 * Injects the shipped stylesheets into a page built with `setContent`.
 *
 * One at a time, in list order: `addStyleTag` appends in resolution order, so
 * loading them in parallel would let the token layer land after the rules that
 * read it and decide the cascade by a race.
 */
export async function addApplicationStyles(page: Page): Promise<void> {
  const remaining = [...APPLICATION_STYLESHEETS];
  async function addNext(): Promise<void> {
    const stylesheet = remaining.shift();
    if (stylesheet === undefined) return;
    await page.addStyleTag({ path: stylesheet });
    await addNext();
  }
  await addNext();
}
