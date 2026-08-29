import { expect, test, type Page } from "@playwright/test";

import { gotoVisualRoute } from "./visual-harness";

/**
 * What the token sweep looks like once a browser has resolved it.
 *
 * `scripts/css-token-sweep.test.mjs` reads the stylesheets and can prove that a
 * literal is gone from the source. It cannot prove that the token which
 * replaced it resolves to the right thing in the shipped cascade, and it cannot
 * see a literal at all once the export's minifier has rewritten it. These tests
 * navigate the real export and read computed style, so what is under assertion
 * is the pixel the user gets.
 *
 * They take no screenshot, for the reason docs/theming.md records under Known
 * debt: `playwright.visual.config.ts` sets a pixel budget but no `threshold`,
 * so the per-pixel comparison is generous enough in YIQ space that a token
 * substitution can move a hue by tens of units on a channel and still pass. A
 * screenshot proves that no layout moved. Comparing resolved colours is the
 * only thing that proves a colour did not.
 *
 * They live in the visual suite because it is the only harness that builds and
 * serves the export, which is the same reason `token-layer.visual.spec.ts`
 * does.
 */

/** Every exported route, so no page is audited by having been forgotten. */
const EXPORTED_ROUTES = [
  "/",
  "/signin/",
  "/admin/",
  "/activity/",
  "/datasets/",
  "/dashboards/",
  "/reports/",
  "/analytics/",
  "/search/",
] as const;

/**
 * How light a ground has to be before the dark theme owes it a change.
 *
 * Relative luminance, not lightness: 0.6 sits well above every ground the dark
 * palette produces (`--bg-inverse` is the lightest at `--gray-50`) and well
 * below every surface the light palette produces, so nothing lands near the
 * line by accident.
 */
const LIGHT_GROUND_LUMINANCE = 0.6;

/** The mirror of the above for ink: a dark letter on a dark ground. */
const DARK_INK_LUMINANCE = 0.35;

/**
 * Status roles, and the tone modifier that is supposed to paint each one.
 *
 * Phase 3 collapsed six parallel status families into one, so these are now
 * `.status--*` modifiers rather than `.status-label--*`, and the vocabulary is
 * the roles themselves: `complete` and `failed` were two spellings of
 * `--status-success` and `--status-error`. `running` stays a name of its own
 * because it is `info` plus a pulse, and the assertion is exactly that it still
 * paints the informational role rather than a colour of its own.
 */
const STATUS_LABELS = [
  ["success", "--status-success"],
  ["info", "--status-info"],
  ["running", "--status-info"],
  ["error", "--status-error"],
  ["warning", "--status-warning"],
  ["neutral", "--status-neutral"],
] as const;

/** Severity swatch classes, and the level token each is supposed to paint. */
const SEVERITY_SWATCHES = [
  ["legend-info", "--level-info"],
  ["severity-info", "--level-info"],
  ["severity-warn", "--level-warn"],
  ["severity-error", "--level-error"],
  ["severity-debug", "--level-debug"],
] as const;

interface PaintedElement {
  background: string;
  description: string;
  ink: string;
  stamp: string;
}

/**
 * Resolves a list of custom properties against the document root.
 *
 * Returned as the browser's own serialisation of a painted colour rather than
 * the token's declared text, so it compares directly against a computed
 * `background-color`: the export's minifier rewrites custom-property values,
 * and comparing strings would pin the minifier's spelling instead of a colour.
 */
async function resolveTokens(page: Page, names: readonly string[]): Promise<string[]> {
  return page.evaluate((tokens) => {
    const probe = document.createElement("span");
    document.body.append(probe);
    const painted = tokens.map((name) => {
      probe.style.backgroundColor = `var(${name})`;
      return globalThis.getComputedStyle(probe).backgroundColor;
    });
    probe.remove();
    return painted;
  }, names);
}

/**
 * Mounts one throwaway fixture per class list and reports what each is painted.
 *
 * Fixtures rather than found markup: a status is a state the demo data may or
 * may not contain on any given page, and an assertion that silently matches
 * nothing is worse than no assertion. The element is appended to the live
 * document, so the rules under test are the shipped ones in the shipped
 * cascade, not an injected copy.
 *
 * `target` says which element in the fixture carries the class list, which
 * differs by component: a status label paints the swatch nested inside the row,
 * while a severity swatch *is* the element the class names. Mounting the list
 * on the wrong element returns a transparent background, which is why the
 * caller also rejects `rgba(0, 0, 0, 0)` rather than trusting a match.
 */
async function paintOf(
  page: Page,
  classLists: readonly string[],
  target: "self" | "swatch",
): Promise<string[]> {
  return page.evaluate(([lists, wanted]) => {
    const host = document.createElement("div");
    document.body.append(host);
    const painted = lists.map((classList) => {
      host.innerHTML = wanted === "self"
        ? `<span class="${classList}"></span>`
        : `<span class="status status--label"><i class="${classList}"></i></span>`;
      const element = wanted === "self" ? host.firstElementChild : host.querySelector("i");
      if (element === null) throw new Error(`fixture for ${classList} did not mount`);
      return globalThis.getComputedStyle(element).backgroundColor;
    });
    host.remove();
    return painted;
  }, [classLists, target] as const);
}

/**
 * Records the ground and ink of every element on the page, in a stable order.
 *
 * Each element is stamped with its index before it is read, and the second pass
 * finds it by that stamp. Comparing two passes by position would go wrong the
 * moment a client component re-rendered between them, and the failure would
 * look like a theming bug rather than like the race it is.
 */
async function paintedElements(page: Page, stamp: boolean): Promise<PaintedElement[]> {
  return page.evaluate((shouldStamp) => {
    const elements = shouldStamp
      ? [...document.querySelectorAll("body *")]
      : [...document.querySelectorAll("[data-token-sweep]")];
    return elements.map((element, index) => {
      if (shouldStamp) (element as HTMLElement).dataset.tokenSweep = String(index);
      const style = globalThis.getComputedStyle(element);
      // `className` is an SVGAnimatedString on an SVG node, so the guard is not
      // decoration: reading `.trim()` off one would throw and take the page
      // down with it.
      const classes = typeof element.className === "string" ? element.className.trim() : "";
      const own = element.tagName.toLowerCase()
        + (classes.length > 0 ? `.${classes.split(/\s+/u).join(".")}` : "");
      const parent = element.parentElement;
      return {
        background: style.backgroundColor,
        description: parent === null ? own : `${parent.tagName.toLowerCase()} > ${own}`,
        ink: style.color,
        stamp: (element as HTMLElement).dataset.tokenSweep ?? String(index),
      };
    });
  }, stamp);
}

/** Relative luminance of an opaque paint, or `null` when it is see-through. */
function luminance(paint: string): number | null {
  const parsed = /^rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)$/u.exec(paint);
  if (parsed === null) return null;
  if (parsed[4] !== undefined && Number(parsed[4]) < 0.9) return null;
  const [red, green, blue] = [Number(parsed[1]), Number(parsed[2]), Number(parsed[3])];
  return (0.2126 * red + 0.7152 * green + 0.0722 * blue) / 255;
}

test.describe("token sweep rendering", () => {
  test("every status label paints its own status token", async ({ page }) => {
    await gotoVisualRoute(page, "/");
    const painted = await paintOf(
      page,
      STATUS_LABELS.map(([modifier]) => `status status--dot status--${modifier}`),
      "swatch",
    );
    const expected = await resolveTokens(page, STATUS_LABELS.map(([, token]) => token));
    const wrong = STATUS_LABELS
      .map(([modifier, token], index) => (
        `.status--${modifier} paints ${painted[index]}, and var(${token}) is ${expected[index]}`
      ))
      .filter((_, index) => painted[index] !== expected[index] || painted[index] === "rgba(0, 0, 0, 0)");
    expect(wrong, "status dots painted from something other than their own role token").toEqual([]);
  });

  test("every severity swatch paints its own level token", async ({ page }) => {
    // The four levels are spelled twice -- as these classes, and as the inline
    // styles categoryColor() builds in visualization-panel.tsx. A legend that
    // disagrees with its own chart is a wrong colour, not a missing one, and
    // the page renders perfectly while it is wrong.
    await gotoVisualRoute(page, "/");
    const painted = await paintOf(
      page,
      SEVERITY_SWATCHES.map(([className]) => `severity-dot ${className}`),
      "self",
    );
    const expected = await resolveTokens(page, SEVERITY_SWATCHES.map(([, token]) => token));
    const wrong = SEVERITY_SWATCHES
      .map(([className, token], index) => (
        `.${className} paints ${painted[index]}, and var(${token}) is ${expected[index]}`
      ))
      .filter((_, index) => painted[index] !== expected[index] || painted[index] === "rgba(0, 0, 0, 0)");
    expect(wrong, "severity swatches painted from something other than their own level token").toEqual([]);
  });

  for (const route of EXPORTED_ROUTES) {
    test(`nothing on ${route} keeps a light-theme paint under data-theme="dark"`, async ({ page }) => {
      await gotoVisualRoute(page, route);
      // The categorical ramp is deliberately theme-invariant: its twelve hues
      // are chosen to separate from each other rather than from the ground, and
      // docs/theming.md exempts them from the dark block by name. Anything else
      // that fails to move is painted from a literal the sweep did not reach.
      const ramp = new Set(await resolveTokens(
        page,
        Array.from({ length: 12 }, (_, index) => `--chart-series-${index + 1}`),
      ));
      const light = await paintedElements(page, true);
      expect(light.length, `${route} rendered almost nothing, so this assertion proves nothing`)
        .toBeGreaterThan(50);
      await page.evaluate(() => { document.documentElement.dataset.theme = "dark"; });
      const dark = new Map((await paintedElements(page, false)).map((element) => [element.stamp, element]));
      expect(dark.size, "the dark pass saw a different set of elements from the light one")
        .toEqual(light.length);

      const grounds = new Set<string>();
      const inks = new Set<string>();
      let opaqueGrounds = 0;
      let readableInks = 0;
      for (const element of light) {
        const themed = dark.get(element.stamp);
        if (themed === undefined) continue;
        const ground = luminance(element.background);
        if (ground !== null) opaqueGrounds += 1;
        if (ground !== null && ground >= LIGHT_GROUND_LUMINANCE && !ramp.has(element.background)
          && themed.background === element.background) {
          grounds.add(`${element.description} keeps the light ground ${element.background}`);
        }
        const ink = luminance(element.ink);
        if (ink !== null) readableInks += 1;
        if (ink !== null && ink <= DARK_INK_LUMINANCE && !ramp.has(element.ink)
          && themed.ink === element.ink) {
          inks.add(`${element.description} keeps the dark ink ${element.ink}`);
        }
      }
      // Without these the whole assertion would pass by parsing nothing: an
      // unrecognised colour serialisation makes every element unjudgeable, and
      // an empty offender list reads exactly like a clean one. The two halves
      // carry their own floor rather than sharing one, because the ratio
      // between them is a property of the page: /signin/ is a form on one
      // canvas and paints six opaque grounds behind sixty-five inks, while
      // /search/ paints a hundred and seventy-one behind nine hundred. A single
      // count tuned to the dense pages calls the lean one unjudged when it is
      // only small, and would hide every ink on it.
      expect(opaqueGrounds, "no computed background parsed, so no ground here was judged")
        .toBeGreaterThan(4);
      expect(readableInks, "no computed ink parsed, so no text here was judged")
        .toBeGreaterThan(40);
      expect(
        [...grounds].toSorted(),
        "Surfaces that stay light when the dark theme is applied. Every ground the token layer\n"
          + "names moves with the theme, so one that does not is painted from a literal the sweep\n"
          + "left behind -- and in the dark theme it is a white patch on a black page.",
      ).toEqual([]);
      expect(
        [...inks].toSorted(),
        "Text that stays dark when the dark theme is applied, which in the dark theme is dark\n"
          + "letters on a dark ground. Each is painted from a colour literal rather than from an\n"
          + "--fg-* or --status-* role; docs/theming.md groups them under the tier-2 roles that\n"
          + "would absorb them, the largest being the secondary ink between --fg-text and\n"
          + "--fg-muted.",
      ).toEqual([]);
    });
  }
});
