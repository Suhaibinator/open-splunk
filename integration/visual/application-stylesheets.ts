import path from "node:path";

import { type Page } from "@playwright/test";

/**
 * The shipped stylesheets, in the order `app/layout.tsx` loads them.
 *
 * Two suites mount fixture markup in a bare page instead of navigating to an
 * exported route, so nothing loads the application's CSS for them. They used
 * to inject `app/globals.css` alone, which was complete only while every
 * custom property was declared inside that file. Now that the token layer
 * declares them, injecting globals.css by itself would leave every `var()`
 * unresolved -- the fixture would silently fall back to browser defaults and
 * the assertions would pin the fallback rather than the shipped rule.
 *
 * `app/styles/index.css` cannot stand in for its two imports here: an
 * `@import` inside an injected `<style>` resolves against the page URL, which
 * for `page.setContent` is `about:blank`. The files are therefore listed
 * individually, and a new token file has to be added in both places.
 */
const applicationRoot = path.join(__dirname, "..", "..", "app");

export const APPLICATION_STYLESHEETS: readonly string[] = [
  path.join(applicationRoot, "styles", "tokens-color.css"),
  path.join(applicationRoot, "styles", "tokens-scale.css"),
  path.join(applicationRoot, "globals.css"),
];

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
