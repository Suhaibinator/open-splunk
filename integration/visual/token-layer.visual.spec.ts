import { expect, test, type Page } from "@playwright/test";

import { gotoVisualRoute } from "./visual-harness";

/**
 * The token layer as the shipped application actually loads it.
 *
 * `css-contracts.spec.ts` injects the stylesheets by path into a blank page,
 * which proves what the files say and nothing about whether the application
 * reads them. Deleting `import "./styles/index.css"` from `app/layout.tsx`, or
 * adding a token file and forgetting to `@import` it from
 * `app/styles/index.css`, leaves every one of those contracts green while the
 * real product renders with no tokens at all. These tests navigate to the
 * exported build instead, so the whole path -- layout, entry point, imports,
 * bundler, minifier -- is under assertion.
 *
 * Every value is checked by rendering it rather than by reading the token's
 * text. The export's minifier rewrites custom-property values (`100ms` becomes
 * `.1s`, `rgb(21 35 43 / 24%)` becomes `#15232b3d`), so comparing strings would
 * pin the minifier's spelling; rendering the token beside the literal it
 * replaced compares the two after the browser has resolved both, which is the
 * only comparison that says anything about pixels.
 *
 * They take no screenshot. They live in the visual suite because that is the
 * only harness that builds and serves the real export.
 */

/**
 * The literals `app/globals.css` declared in its `:root` at commit 7459a0cc,
 * against the name that carries each one today.
 *
 * Phase 1 moved those declarations into the token layer as chains of `var()`
 * references; Phase 2 rewrote the call sites and deleted the pre-refactor
 * aliases, so each row now names the role rather than the retired alias and the
 * contract -- "this renders exactly what that literal renders" -- is unchanged.
 * `--orange` and `--yellow` named no role, so the primitive each resolved to is
 * checked directly. The property each is checked through is the one it is used
 * with: a colour through `color`, the two stacks through `font-family`, the
 * elevation through `box-shadow`.
 */
const PRE_REFACTOR_RENDERING: readonly (readonly [string, string, string])[] = [
  ["color", "--accent", "#477f2b"],
  ["color", "--accent-hover", "#376a20"],
  ["color", "--accent-soft", "#e8f2e1"],
  ["color", "--amber-500", "#d2a600"],
  ["color", "--bg-canvas", "#f6f6f4"],
  ["color", "--bg-inverse", "#161b1f"],
  ["color", "--bg-raised", "#fbfbfa"],
  ["color", "--bg-subtle", "#f2f3f3"],
  ["color", "--bg-surface", "#ffffff"],
  ["color", "--border", "#cfd4d7"],
  ["color", "--border-strong", "#aeb6bb"],
  ["color", "--chrome-appbar", "#3f464c"],
  ["color", "--chrome-bar", "#1e252b"],
  ["color", "--chrome-hover", "#4b535a"],
  ["color", "--fg-faint", "#89949b"],
  ["color", "--fg-muted", "#64717a"],
  ["color", "--fg-strong", "#19252d"],
  ["color", "--fg-text", "#28343d"],
  ["color", "--orange-400", "#d97a23"],
  ["color", "--status-error", "#c93c37"],
  ["color", "--status-error-soft", "#fff0ee"],
  ["color", "--status-info", "#2878a8"],
  ["color", "--status-info-soft", "#e8f3f9"],
  ["font-family", "--font-sans", "Arial, Helvetica, sans-serif"],
  ["font-family", "--font-mono", '"SFMono-Regular", Consolas, "Liberation Mono", Menlo, monospace'],
  ["box-shadow", "--shadow-lg", "0 10px 30px rgb(18 29 36 / 18%), 0 2px 7px rgb(18 29 36 / 12%)"],
];

/**
 * One token from each family in `app/styles/tokens-scale.css`.
 *
 * The scale tier has no consumers yet, so nothing else on the page would notice
 * if it stopped loading. A sentinel per family catches a token file that fell
 * out of `app/styles/index.css` as well as a step someone edited by hand.
 */
const SCALE_SENTINELS: readonly (readonly [string, string, string])[] = [
  ["border-radius", "--radius-pill", "999px"],
  ["box-shadow", "--shadow-md", "0 3px 9px rgb(21 35 43 / 24%)"],
  ["font-size", "--type-xs", "10px"],
  ["transition-duration", "--dur-fast", "100ms"],
  ["transition-timing-function", "--ease", "ease-out"],
  ["width", "--space-4", "16px"],
  ["z-index", "--z-toast", "800"],
];

/**
 * Custom properties the CSS toolchain injects, which no token file declares.
 *
 * lightningcss compiles `color-scheme` into a pair of marker properties, one of
 * which is deliberately empty in each scheme. They are not part of the layer
 * and their emptiness is not a broken reference.
 */
const TOOLCHAIN_PROPERTY = /^--lightningcss-/u;

/**
 * Computes a list of `property: value` pairs against one throwaway element.
 *
 * Returned strings are the browser's own serialisation, so a token and the
 * literal it replaced are directly comparable however either was spelled in the
 * shipped bytes.
 */
async function renderedValues(
  page: Page,
  requests: readonly (readonly [string, string])[],
): Promise<string[]> {
  return page.evaluate((entries) => {
    const probe = document.createElement("div");
    document.body.append(probe);
    const computed = entries.map(([property, value]) => {
      probe.style.cssText = "";
      probe.style.setProperty(property, value);
      return globalThis.getComputedStyle(probe).getPropertyValue(property);
    });
    probe.remove();
    return computed;
  }, requests);
}

/** Renders each token beside the literal it is meant to reproduce. */
async function expectRenderedAsLiteral(
  page: Page,
  table: readonly (readonly [string, string, string])[],
): Promise<void> {
  const fromToken = await renderedValues(page, table.map(([property, name]) => [property, `var(${name})`]));
  const fromLiteral = await renderedValues(page, table.map(([property, , literal]) => [property, literal]));
  const drift = table
    .map(([property, name, literal], index) => ({
      literal: `${literal} renders ${fromLiteral[index]}`,
      property,
      token: `${name} renders ${fromToken[index]}`,
    }))
    .filter((row, index) => fromToken[index] !== fromLiteral[index] || fromToken[index] === "");
  expect(drift, "tokens whose rendered value differs from the literal they stand in for").toEqual([]);
}

/**
 * Every custom property the loaded stylesheets declare on `:root`.
 *
 * Read through the CSSOM rather than from the files: what is asserted is what
 * the browser parsed, so a token lost to a bundler or to an unresolved
 * `@import` simply is not in the list. Imported sheets are walked too, because
 * `app/styles/index.css` is nothing but `@import` rules.
 */
async function declaredRootTokens(page: Page): Promise<string[]> {
  return page.evaluate(() => {
    const names = new Set<string>();
    function visit(sheet: CSSStyleSheet): void {
      let rules: CSSRuleList;
      try {
        rules = sheet.cssRules;
      } catch {
        // A cross-origin sheet cannot be read; the export serves its own CSS,
        // so this only ever skips something the application did not ship.
        return;
      }
      for (const rule of rules) {
        if (rule instanceof CSSImportRule) {
          if (rule.styleSheet !== null) visit(rule.styleSheet);
        } else if (rule instanceof CSSStyleRule && rule.selectorText.includes(":root")) {
          for (const property of rule.style) {
            if (property.startsWith("--")) names.add(property);
          }
        }
      }
    }
    for (const sheet of document.styleSheets) visit(sheet);
    return [...names].toSorted();
  });
}

test.describe("token layer wiring", () => {
  test("the exported application resolves every token its stylesheets declare", async ({ page }) => {
    await gotoVisualRoute(page, "/");
    const declared = (await declaredRootTokens(page)).filter((name) => !TOOLCHAIN_PROPERTY.test(name));
    // A guard, not a target: the layer ships well over a hundred names, so a
    // handful means the walk found a stray sheet instead of the token files and
    // every assertion below it would be vacuous.
    expect(declared.length).toBeGreaterThan(120);
    expect(declared).toContain("--bg-canvas");
    expect(declared).toContain("--chart-series-12");
    expect(declared).toContain("--focus-ring");
    expect(declared).toContain("--space-8");

    const unresolved = await page.evaluate((names) => {
      const style = globalThis.getComputedStyle(document.documentElement);
      return names.filter((name) => style.getPropertyValue(name).trim().length === 0);
    }, declared);
    expect(unresolved, "tokens the shipped cascade leaves with no value").toEqual([]);
  });

  test("the roles that replaced the pre-refactor properties still render their values", async ({ page }) => {
    await gotoVisualRoute(page, "/");
    await expectRenderedAsLiteral(page, PRE_REFACTOR_RENDERING);
  });

  test("the scale tier reaches the browser through app/layout.tsx", async ({ page }) => {
    await gotoVisualRoute(page, "/");
    await expectRenderedAsLiteral(page, SCALE_SENTINELS);
    // The outgoing `--shadow` is gone from every stylesheet, so the name has to
    // resolve to nothing: a stylesheet that still declared it would keep the
    // duplicate elevation alive where nothing checks the two agree.
    const [retired] = await renderedValues(page, [["box-shadow", "var(--shadow, none)"]]);
    expect(retired, "--shadow is declared again; --shadow-lg is the only elevation name")
      .toEqual("none");
  });

  test("the dark theme is inert until data-theme is set", async ({ page }) => {
    await gotoVisualRoute(page, "/");
    const themed = ["--bg-canvas", "--bg-surface", "--fg-text", "--chrome-bar", "--status-error"];
    const asColours = themed.map((name) => ["color", `var(${name})`] as const);
    const shipped = await renderedValues(page, asColours);
    // Nothing in the product sets the attribute, so the shipped render is the
    // light one and the dark block may not reach it by any other route.
    expect(await page.evaluate(() => document.documentElement.dataset.theme)).toBeUndefined();

    await page.evaluate(() => { document.documentElement.dataset.theme = "dark"; });
    const dark = await renderedValues(page, asColours);
    for (const [index, name] of themed.entries()) {
      expect(dark[index], `${name} did not change under data-theme="dark"`).not.toEqual(shipped[index]);
    }

    await page.evaluate(() => { delete document.documentElement.dataset.theme; });
    expect(await renderedValues(page, asColours)).toEqual(shipped);
  });
});
