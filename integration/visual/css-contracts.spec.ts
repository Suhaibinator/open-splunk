// Computed-style contracts for app/globals.css.
//
// These replace unit assertions that matched the raw text of the stylesheet.
// Text matching pinned formatting (newline placement, single-line media-query
// bodies, declaration order) rather than behaviour, so any tokenising or
// reformatting pass broke them without changing a single rendered pixel.
// Here the stylesheet is loaded into a real browser against fixture markup that
// mirrors the production DOM, and the assertions read resolved values from
// getComputedStyle, which is what the rules actually promise.
import { expect, test, type Page } from "@playwright/test";

import { addApplicationStyles } from "./application-stylesheets";

const COMPACT_WIDTH = 980;
const MOBILE_WIDTH = 760;
const NARROW_WIDTH = 480;
const DESKTOP_WIDTH = 1280;

async function mount(page: Page, markup: string, width: number): Promise<void> {
  await page.setViewportSize({ height: 900, width });
  await page.setContent(`<main>${markup}</main>`);
  await addApplicationStyles(page);
}

async function gridTracks(page: Page, selector: string): Promise<number[]> {
  return page.evaluate((target) => {
    const element = document.querySelector(target);
    if (element === null) throw new Error(`fixture is missing ${target}`);
    const columns = globalThis.getComputedStyle(element).gridTemplateColumns;
    if (columns === "none") throw new Error(`${target} is not a grid container`);
    return columns.split(" ").map((track) => Number.parseFloat(track));
  }, selector);
}

async function contentWidth(page: Page, selector: string): Promise<number> {
  return page.evaluate((target) => {
    const element = document.querySelector(target);
    if (element === null) throw new Error(`fixture is missing ${target}`);
    return element.getBoundingClientRect().width;
  }, selector);
}

// Focus the control the way a keyboard user reaches it: Chromium only matches
// :focus-visible once the last interaction was a key press.
async function expectKeyboardFocusRing(page: Page, selector: string): Promise<void> {
  const control = page.locator(selector).first();
  await control.evaluate((element: HTMLElement) => element.focus());
  await page.keyboard.press("Shift+Tab");
  await page.keyboard.press("Tab");
  await expect(control).toHaveCSS("outline-width", "3px");
  await expect(control).toHaveCSS("outline-style", "solid");
  await expect(control).toHaveCSS("outline-color", "rgba(42, 120, 158, 0.28)");
}

function expectEqualTracks(tracks: number[], count: number): void {
  expect(tracks).toHaveLength(count);
  for (const track of tracks) {
    expect(track).toBeCloseTo(tracks[0] ?? 0, 0);
    expect(track).toBeGreaterThan(0);
  }
}

const KNOWLEDGE_FILTER_OPTIONS = ["App scope", "Object type", "Sharing", "State"];

const knowledgeManagerMarkup = `
<div class="knowledge-manager">
  <div class="knowledge-manager__toolbar">
    <div class="knowledge-manager__filters">
      ${KNOWLEDGE_FILTER_OPTIONS.map((label) => `
      <label><span>${label}</span><select><option>All</option></select></label>`).join("")}
    </div>
  </div>
  <div class="knowledge-manager__advanced-filter-grid">
    <label><span>Name</span><input value="" /></label>
    <label><span>Owner</span><input value="" /></label>
    <label><span>Updated after</span><input value="" /></label>
    <label><span>Updated before</span><select><option>Any</option></select></label>
  </div>
  <div class="knowledge-manager__workspace knowledge-manager__workspace--detail">
    <div class="knowledge-manager__list-panel">
      <ul class="knowledge-manager__list">
        <li>
          <button class="knowledge-manager__row" type="button">
            <span data-label="Object"><strong>saved_search</strong><small>search · v3</small></span>
            <span data-label="Type">Saved search</span>
            <span data-label="State"><i class="knowledge-state knowledge-state--active"></i>Active</span>
            <span data-label="Scope">App</span>
          </button>
        </li>
      </ul>
    </div>
    <section class="knowledge-manager__detail" tabindex="-1">
      <div class="knowledge-manager__mutation-grid knowledge-manager__mutation-grid--selectors">
        <label><span>App</span><select><option>search</option></select></label>
        <label><span>Owner</span><input value="" /></label>
        <label><span>Scope</span><select><option>App</option></select></label>
        <label><span>State</span><select><option>Active</option></select></label>
      </div>
      <div class="knowledge-manager__mutation-grid">
        <label><span>Name</span><input value="" /></label>
        <label><span>Description</span><textarea></textarea></label>
      </div>
      <section class="knowledge-manager__delete-confirmation">
        <h4>Delete</h4>
        <input value="" />
      </section>
      <div class="knowledge-manager__relationships">
        <div class="knowledge-manager__relationship-section">
          <ul class="knowledge-manager__relationship-list">
            <li>
              <code>lookup:threat_intel</code>
              <span>Uses</span>
              <button class="knowledge-manager__relationship-inspect" type="button">Inspect</button>
              <span class="knowledge-manager__relationship-role">Dependency</span>
            </li>
          </ul>
          <p class="knowledge-manager__relationship-pagination">
            <span>Page 1</span>
            <button type="button">Next</button>
          </p>
        </div>
        <div class="knowledge-manager__related-inspector">
          <p class="knowledge-manager__related-status">
            <span>Loaded</span>
            <button type="button">Reload</button>
          </p>
          <div class="knowledge-manager__related-object">
            <strong>threat_intel</strong>
            <dl>
              <div><dt>App</dt><dd>search</dd></div>
              <div><dt>Owner</dt><dd>admin</dd></div>
            </dl>
          </div>
        </div>
      </div>
    </section>
  </div>
</div>`;

test.describe("knowledge manager layout contracts", () => {
  test("filter grids fan out on wide viewports", async ({ page }) => {
    await mount(page, knowledgeManagerMarkup, DESKTOP_WIDTH);

    const filters = await gridTracks(page, ".knowledge-manager__filters");
    expect(filters).toHaveLength(4);
    // The leading App-scope column is intentionally wider than the three peers.
    expect(filters[0]).toBeGreaterThan(filters[1] ?? 0);
    expectEqualTracks(filters.slice(1), 3);

    expectEqualTracks(await gridTracks(page, ".knowledge-manager__advanced-filter-grid"), 4);
    expectEqualTracks(await gridTracks(page, ".knowledge-manager__mutation-grid--selectors"), 4);

    // Both filter grids share one grouped rule for their labels, captions, and
    // controls, so both halves of every group are asserted: dropping either
    // selector from the group leaves the other passing on its own.
    await Promise.all([
      ".knowledge-manager__filters",
      ".knowledge-manager__advanced-filter-grid",
    ].map(async (grid) => {
      const caption = page.locator(`${grid} label > span`).first();
      await Promise.all([
        expect(page.locator(`${grid} label`).first()).toHaveCSS("display", "grid"),
        expect(caption).toHaveCSS("text-transform", "uppercase"),
        expect(caption).toHaveCSS("font-weight", "700"),
      ]);
    }));
    await Promise.all([
      ".knowledge-manager__filters select",
      ".knowledge-manager__advanced-filter-grid input",
      ".knowledge-manager__advanced-filter-grid select",
    ].map(async (control) => {
      const field = page.locator(control).first();
      await Promise.all([
        expect(field).toHaveCSS("height", "34px"),
        expect(field).toHaveCSS("border-top-width", "1px"),
      ]);
    }));

    // Detail mode keeps the list and the inspector side by side.
    expect(await gridTracks(page, ".knowledge-manager__workspace--detail")).toHaveLength(2);
    expect(await gridTracks(page, ".knowledge-manager__row")).toHaveLength(4);
  });

  test("filter and mutation grids halve at the compact breakpoint", async ({ page }) => {
    await mount(page, knowledgeManagerMarkup, COMPACT_WIDTH);

    expectEqualTracks(await gridTracks(page, ".knowledge-manager__filters"), 2);
    expectEqualTracks(await gridTracks(page, ".knowledge-manager__advanced-filter-grid"), 2);
    expectEqualTracks(await gridTracks(page, ".knowledge-manager__mutation-grid--selectors"), 2);
    expect(await gridTracks(page, ".knowledge-manager__workspace--detail")).toHaveLength(1);
  });

  test("mobile collapses rows, enlarges controls, and stacks relationship bars", async ({ page }) => {
    await mount(page, knowledgeManagerMarkup, MOBILE_WIDTH);

    // The row keeps a `minmax(0, 1fr)` content column plus a trailing `auto`
    // column sized to the badge. Two equal halves would also have two tracks,
    // so the trailing track is measured against the badge it must hug.
    const rowTracks = await gridTracks(page, ".knowledge-manager__row");
    expect(rowTracks).toHaveLength(2);
    const badge = await contentWidth(page, ".knowledge-manager__row > span:nth-child(3)");
    expect(rowTracks[1]).toBeCloseTo(badge, 0);
    expect(rowTracks[1]).toBeLessThan((rowTracks[0] ?? 0) / 2);
    expect(await gridTracks(page, ".knowledge-manager__mutation-grid")).toHaveLength(1);
    expect(await gridTracks(page, ".knowledge-manager__related-object dl")).toHaveLength(1);

    // Touch targets grow and font sizes stop triggering iOS zoom-on-focus.
    await Promise.all([
      ".knowledge-manager__toolbar select",
      ".knowledge-manager__advanced-filter-grid input",
      ".knowledge-manager__advanced-filter-grid select",
      ".knowledge-manager__mutation-grid input",
      ".knowledge-manager__mutation-grid select",
      ".knowledge-manager__delete-confirmation input",
    ].map(async (selector) => {
      const control = page.locator(selector).first();
      await expect(control).toHaveCSS("font-size", "16px");
      await expect(control).toHaveCSS("height", "44px");
    }));

    await Promise.all([
      ".knowledge-manager__relationship-pagination",
      ".knowledge-manager__related-status",
    ].map(async (selector) => {
      await expect(page.locator(selector)).toHaveCSS("flex-direction", "column");
      await expect(page.locator(selector)).toHaveCSS("align-items", "stretch");
    }));
    await Promise.all([
      ".knowledge-manager__relationship-pagination button",
      ".knowledge-manager__relationship-inspect",
      ".knowledge-manager__related-status button",
    ].map((selector) => expect(page.locator(selector)).toHaveCSS("min-height", "42px")));

    // Buttons in the bars that stack become full-width touch targets. The
    // inspect button sits in an auto grid track, so its width is content-sized
    // there and only the touch height above is observable.
    await Promise.all([
      ".knowledge-manager__relationship-pagination",
      ".knowledge-manager__related-status",
    ].map(async (bar) => {
      const [button, container] = await Promise.all([
        contentWidth(page, `${bar} button`),
        contentWidth(page, bar),
      ]);
      // Both bars carry `padding: 7px 8px`.
      expect(button).toBeCloseTo(container - 16, 0);
    }));
  });

  test("narrow viewports drop every filter grid to a single column", async ({ page }) => {
    await mount(page, knowledgeManagerMarkup, NARROW_WIDTH);

    expect(await gridTracks(page, ".knowledge-manager__filters")).toHaveLength(1);
    expect(await gridTracks(page, ".knowledge-manager__advanced-filter-grid")).toHaveLength(1);
    expect(await gridTracks(page, ".knowledge-manager__mutation-grid--selectors")).toHaveLength(1);
  });

  test("relationship and related-object structures stay gridded", async ({ page }) => {
    await mount(page, knowledgeManagerMarkup, DESKTOP_WIDTH);

    // Label, role, and inspect button share one row; the role wraps beneath.
    expect(await gridTracks(page, ".knowledge-manager__relationship-list li")).toHaveLength(3);
    await expect(page.locator(".knowledge-manager__related-inspector")).toHaveCSS("display", "grid");
    expectEqualTracks(await gridTracks(page, ".knowledge-manager__related-object dl"), 2);
  });

  test("keyboard focus is always visible inside the knowledge manager", async ({ page }) => {
    await mount(page, knowledgeManagerMarkup, DESKTOP_WIDTH);

    // Sequential by nature: only one element holds keyboard focus at a time.
    await expectKeyboardFocusRing(page, ".knowledge-manager__filters select");
    await expectKeyboardFocusRing(page, ".knowledge-manager__advanced-filter-grid input");
    await expectKeyboardFocusRing(page, ".knowledge-manager__row");
    await expectKeyboardFocusRing(page, ".knowledge-manager__mutation-grid textarea");

    const detail = page.locator(".knowledge-manager__detail");
    await page.keyboard.press("Tab");
    await detail.evaluate((element: HTMLElement) => element.focus());
    await expect(detail).toHaveCSS("box-shadow", "rgba(49, 126, 165, 0.23) 0px 0px 0px 3px");
  });
});

const statisticsMarkup = `
<table class="statistics-table">
  <tbody>
    <tr>
      <td><span class="statistics-multivalue-lines">alpha\nbeta\ngamma\ndelta</span></td>
    </tr>
  </tbody>
</table>`;

test.describe("statistics multivalue contracts", () => {
  test("multiline cells stay inside the fixed virtual row height", async ({ page }) => {
    await mount(page, statisticsMarkup, DESKTOP_WIDTH);

    const lines = page.locator(".statistics-multivalue-lines");
    await expect(lines).toHaveCSS("display", "block");
    await expect(lines).toHaveCSS("white-space", "pre-wrap");
    await expect(lines).toHaveCSS("overflow-x", "hidden");
    await expect(lines).toHaveCSS("overflow-y", "hidden");
    // calc(var(--statistics-row-height, 42px) - 8px) with the default row height.
    await expect(lines).toHaveCSS("max-height", "34px");

    // Four rendered lines cannot fit, so the cell clips instead of growing.
    const overflow = await lines.evaluate((element) => ({
      client: element.clientHeight,
      scroll: element.scrollHeight,
    }));
    expect(overflow.client).toBeLessThanOrEqual(34);
    expect(overflow.scroll).toBeGreaterThan(overflow.client);

    // The clamp tracks the row-height token rather than a hard-coded height.
    await page.locator(".statistics-table").evaluate((element: HTMLElement) => {
      element.style.setProperty("--statistics-row-height", "52px");
    });
    await expect(lines).toHaveCSS("max-height", "44px");
  });
});

test("the statistics sparkline paints with the palette accent", async ({ page }) => {
  await mount(page, '<svg class="statistics-sparkline"><polyline points="0,0" /></svg>', DESKTOP_WIDTH);

  // The polyline inherits --blue as currentColor, so an undefined token would
  // silently paint the SVG black. No page in either export renders a sparkline
  // -- it needs a server-supplied multivalue column -- so this contract and the
  // component baseline in `component-surfaces.visual.spec.ts` are the only
  // gates on its appearance.
  await expect(page.locator(".statistics-sparkline")).toHaveCSS("color", "rgb(40, 120, 168)");
  const polyline = page.locator(".statistics-sparkline polyline");
  await expect(polyline).toHaveCSS("stroke", "rgb(40, 120, 168)");
  await expect(polyline).toHaveCSS("fill", "none");
  await expect(polyline).toHaveCSS("stroke-linecap", "round");
});

const liveJobsMarkup = `
<div class="responsive-table-wrap live-jobs-table-wrap">
  <table class="product-table live-jobs-table">
    <thead>
      <tr>
        <th scope="col">Search</th><th scope="col">Status</th><th scope="col">Owner</th>
        <th scope="col">Runtime</th><th scope="col">Events</th><th scope="col">Started</th>
        <th scope="col">Actions</th>
      </tr>
    </thead>
    <tbody>
      <tr>
        <td data-label="Search"><code>index=main | stats count</code><small>admin</small></td>
        <td data-label="Status"><strong>Running</strong><small>48%</small></td>
        <td data-label="Owner">admin</td>
        <td data-label="Runtime">12s</td>
        <td data-label="Events"><strong>1,204</strong><small>scanned</small></td>
        <td data-label="Started">10:04:11</td>
        <td data-label="Actions"><button type="button">Cancel</button></td>
      </tr>
    </tbody>
  </table>
</div>`;

test.describe("activity job table contracts", () => {
  test("desktop job cells remain table cells so columns align with headers", async ({ page }) => {
    await mount(page, liveJobsMarkup, DESKTOP_WIDTH);

    const displays = await page.locator(".live-jobs-table tbody td").evaluateAll(
      (cells) => cells.map((cell) => globalThis.getComputedStyle(cell).display),
    );
    expect(displays).toEqual(Array.from({ length: 7 }, () => "table-cell"));

    // Header and body columns share the declared percentage widths.
    const tableWidth = await contentWidth(page, ".live-jobs-table");
    const firstCell = await contentWidth(page, ".live-jobs-table tbody td:first-child");
    const firstHeader = await contentWidth(page, ".live-jobs-table thead th:first-child");
    expect(firstCell).toBeCloseTo(firstHeader, 0);
    expect(firstCell / tableWidth).toBeCloseTo(0.3, 1);
  });

  test("mobile job cards override the more specific desktop column widths", async ({ page }) => {
    await mount(page, liveJobsMarkup, MOBILE_WIDTH);

    const displays = await page.locator(".live-jobs-table tbody td").evaluateAll(
      (cells) => cells.map((cell) => globalThis.getComputedStyle(cell).display),
    );
    expect(displays).toEqual(Array.from({ length: 7 }, () => "flex"));

    // The card is a two-column grid; the first, second, and last cells span it.
    const rowWidth = await contentWidth(page, ".live-jobs-table tbody tr");
    const first = await contentWidth(page, ".live-jobs-table tbody td:nth-child(1)");
    const second = await contentWidth(page, ".live-jobs-table tbody td:nth-child(2)");
    const third = await contentWidth(page, ".live-jobs-table tbody td:nth-child(3)");
    const fourth = await contentWidth(page, ".live-jobs-table tbody td:nth-child(4)");

    expect(second).toBeCloseTo(first, 0);
    expect(first / rowWidth).toBeGreaterThan(0.8);
    // Desktop pins these at 30% and 15% of the table; the card must beat that.
    expect(second / rowWidth).toBeGreaterThan(0.5);
    expect(third).toBeCloseTo(fourth, 0);
    expect(third / rowWidth).toBeLessThan(0.6);

    // The wrapper stops scrolling horizontally once rows become cards.
    await expect(page.locator(".live-jobs-table-wrap")).toHaveCSS("overflow-x", "visible");
  });
});

// Colour-token contracts. The two-tier layer in `app/styles/tokens-color.css`
// is only worth having if it resolves: a semantic token pointing at a primitive
// nothing declares yields an invalid value, and the browser falls back to
// `unset` -- black text on a transparent ground -- rather than failing a build.
// Each token is therefore resolved through a real element.

/** Resolves `var(--name)` for each token, as the browser computes it. */
async function resolveTokens(page: Page, names: readonly string[]): Promise<string[]> {
  return page.evaluate((tokens) => tokens.map((token) => {
    const probe = document.createElement("span");
    probe.style.color = `var(${token})`;
    document.body.append(probe);
    const value = globalThis.getComputedStyle(probe).color;
    probe.remove();
    return value;
  }), names);
}

// Every pre-refactor `:root` colour and the value it carried before the token
// layer existed, keyed by the role that carries it now. Phase 2 rewrote the
// call sites and deleted the aliases, so what is pinned here is the semantic
// name and the chain below it; drift anywhere along that chain moves pixels the
// page baselines catch only where a page happens to use the token. `--orange`
// and `--yellow` named no role, so the primitive each resolved to is pinned
// directly rather than dropped.
const PRE_REFACTOR_COLOUR_VALUES: ReadonlyArray<readonly [string, string]> = [
  ["--bg-inverse", "rgb(22, 27, 31)"],
  ["--chrome-bar", "rgb(30, 37, 43)"],
  ["--chrome-appbar", "rgb(63, 70, 76)"],
  ["--chrome-hover", "rgb(75, 83, 90)"],
  ["--bg-canvas", "rgb(246, 246, 244)"],
  ["--bg-surface", "rgb(255, 255, 255)"],
  ["--bg-subtle", "rgb(242, 243, 243)"],
  ["--bg-raised", "rgb(251, 251, 250)"],
  ["--border", "rgb(207, 212, 215)"],
  ["--border-strong", "rgb(174, 182, 187)"],
  ["--fg-text", "rgb(40, 52, 61)"],
  ["--fg-strong", "rgb(25, 37, 45)"],
  ["--fg-muted", "rgb(100, 113, 122)"],
  ["--fg-faint", "rgb(137, 148, 155)"],
  ["--accent", "rgb(71, 127, 43)"],
  ["--accent-hover", "rgb(55, 106, 32)"],
  ["--accent-soft", "rgb(232, 242, 225)"],
  ["--status-info", "rgb(40, 120, 168)"],
  ["--status-info-soft", "rgb(232, 243, 249)"],
  ["--orange-400", "rgb(217, 122, 35)"],
  ["--status-error", "rgb(201, 60, 55)"],
  ["--status-error-soft", "rgb(255, 240, 238)"],
  ["--amber-500", "rgb(210, 166, 0)"],
];

const SEMANTIC_COLOUR_TOKENS: readonly string[] = [
  "--bg-canvas",
  "--bg-surface",
  "--bg-subtle",
  "--bg-raised",
  "--bg-inverse",
  "--fg-text",
  "--fg-strong",
  "--fg-secondary",
  "--fg-muted",
  "--fg-faint",
  "--fg-inverse",
  "--fg-link",
  "--border",
  "--border-subtle",
  "--border-strong",
  "--border-focus",
  "--accent",
  "--accent-hover",
  "--accent-bright",
  "--accent-soft",
  "--accent-alt",
  "--accent-alt-soft",
  "--status-success",
  "--status-success-soft",
  "--status-info",
  "--status-info-soft",
  "--status-success-strong",
  "--status-info-strong",
  "--status-warning",
  "--status-warning-bright",
  "--status-warning-soft",
  "--status-warning-strong",
  "--status-error-strong",
  "--status-error",
  "--status-error-soft",
  "--status-neutral",
  "--status-neutral-soft",
  "--level-info",
  "--level-warn",
  "--level-error",
  "--level-debug",
  "--chart-series-1",
  "--chart-series-2",
  "--chart-series-3",
  "--chart-series-4",
  "--chart-series-5",
  "--chart-series-6",
  "--chart-series-7",
  "--chart-series-8",
  "--chart-series-9",
  "--chart-series-10",
  "--chart-series-11",
  "--chart-series-12",
  "--chart-neutral",
  "--chrome-bar",
  "--chrome-appbar",
  "--chrome-hover",
  "--chrome-fg",
  "--highlight",
  "--selection",
  "--focus-ring",
];

// The semantic tokens whose value is a contract rather than a choice, and the
// literal each one has to reproduce.
//
// `PRE_REFACTOR_COLOUR_VALUES` above pins twenty-three pre-refactor values, which
// left the rest of tier 2 checked only for "resolves to something". Every token
// below stands in for a literal `app/globals.css` or a component still ships --
// the four severity swatches (`.legend-info` and its peers, and the same four
// in `visualization-panel.tsx`), the categorical ramp against
// `TIME_SERIES_COLORS`, the search-term highlight, the selection wash, the
// focus outline and the chrome the product shell paints -- so a wrong primitive
// behind any of them would make Phase 2's substitution move pixels. Pinning the
// value here is what makes that substitution provably a no-op.
const EXPECTED_SEMANTIC_TOKENS: ReadonlyArray<readonly [string, string]> = [
  ["--bg-inverse", "rgb(22, 27, 31)"],
  ["--fg-inverse", "rgb(255, 255, 255)"],
  ["--fg-link", "rgb(40, 120, 168)"],
  ["--border-focus", "rgb(47, 138, 193)"],
  ["--status-warning", "rgb(168, 115, 0)"],
  ["--status-warning-soft", "rgb(255, 248, 233)"],
  ["--status-neutral", "rgb(116, 129, 136)"],
  ["--status-neutral-soft", "rgb(242, 243, 243)"],
  ["--level-info", "rgb(95, 156, 58)"],
  ["--level-warn", "rgb(221, 162, 41)"],
  ["--level-error", "rgb(200, 79, 72)"],
  ["--level-debug", "rgb(82, 144, 176)"],
  ["--chart-series-1", "rgb(95, 156, 58)"],
  ["--chart-series-2", "rgb(40, 120, 168)"],
  ["--chart-series-3", "rgb(221, 162, 41)"],
  ["--chart-series-4", "rgb(139, 103, 168)"],
  ["--chart-series-5", "rgb(200, 79, 72)"],
  ["--chart-series-6", "rgb(77, 154, 138)"],
  ["--chart-series-7", "rgb(199, 101, 148)"],
  ["--chart-series-8", "rgb(111, 127, 181)"],
  ["--chart-series-9", "rgb(165, 120, 53)"],
  ["--chart-series-10", "rgb(79, 143, 111)"],
  ["--chart-series-11", "rgb(138, 109, 85)"],
  ["--chart-series-12", "rgb(112, 143, 55)"],
  ["--chrome-bar", "rgb(30, 37, 43)"],
  ["--chrome-appbar", "rgb(63, 70, 76)"],
  ["--chrome-hover", "rgb(75, 83, 90)"],
  ["--chrome-fg", "rgb(255, 255, 255)"],
  ["--highlight", "rgb(255, 241, 168)"],
  ["--selection", "rgb(232, 243, 249)"],
  ["--focus-ring", "rgb(47, 138, 193)"],
];

/** WCAG 2.2 AA for text below 18.66px, which is every size this product ships. */
const AA_CONTRAST = 4.5;

/** WCAG relative luminance of a browser-serialised opaque paint. */
function luminance(paint: string): number {
  const parsed = /^rgba?\((\d+),\s*(\d+),\s*(\d+)/u.exec(paint);
  if (parsed === null) throw new Error(`unreadable paint ${paint}`);
  const [red, green, blue] = [parsed[1], parsed[2], parsed[3]].map((channel) => {
    const scaled = Number(channel) / 255;
    return scaled <= 0.040_45 ? scaled / 12.92 : ((scaled + 0.055) / 1.055) ** 2.4;
  });
  return 0.2126 * red + 0.7152 * green + 0.0722 * blue;
}

/** Contrast ratio between two paints, in either order. */
function contrastRatio(first: string, second: string): number {
  const [darker, lighter] = [luminance(first), luminance(second)].toSorted((a, b) => a - b);
  return (lighter + 0.05) / (darker + 0.05);
}

/** The pre-refactor alias names Phase 2 deleted, which may not come back. */
const RETIRED_ALIASES: readonly string[] = [
  "--app-bar",
  "--app-bar-hover",
  "--black",
  "--blue",
  "--blue-soft",
  "--border-dark",
  "--canvas",
  "--faint",
  "--green",
  "--green-soft",
  "--green-strong",
  "--muted",
  "--orange",
  "--product-bar",
  "--red",
  "--red-soft",
  "--surface",
  "--surface-raised",
  "--surface-subtle",
  "--text",
  "--text-strong",
  "--yellow",
];

test.describe("colour token contracts", () => {
  test("every role resolves to the value its pre-refactor name carried", async ({ page }) => {
    await mount(page, "", DESKTOP_WIDTH);

    const resolved = await resolveTokens(page, PRE_REFACTOR_COLOUR_VALUES.map(([name]) => name));
    expect(resolved).toEqual(PRE_REFACTOR_COLOUR_VALUES.map(([, value]) => value));
  });

  test("the deleted pre-refactor aliases stay deleted", async ({ page }) => {
    await mount(page, "", DESKTOP_WIDTH);

    // A `var()` with no fallback on a name nothing declares leaves the property
    // unset, and `color` then inherits; the fallback makes "undeclared" a value
    // this can read. A name that comes back would be a second spelling of a
    // role, which is exactly what the two-tier split exists to prevent.
    const revived = await page.evaluate((names) => names.filter((name) => {
      const probe = document.createElement("span");
      probe.style.color = `var(${name}, rgb(1, 2, 3))`;
      document.body.append(probe);
      const value = globalThis.getComputedStyle(probe).color;
      probe.remove();
      return value !== "rgb(1, 2, 3)";
    }), RETIRED_ALIASES);
    expect(revived, "pre-refactor alias names the token layer declares again").toEqual([]);
  });

  test("every semantic token resolves to a real colour", async ({ page }) => {
    await mount(page, "", DESKTOP_WIDTH);

    const resolved = await resolveTokens(page, SEMANTIC_COLOUR_TOKENS);
    // `unset` on `color` inherits the document default: opaque black on a bare
    // fixture, which is exactly what a broken var() chain produces. No semantic
    // token is meant to be pure black, so it doubles as the sentinel.
    const unresolved = SEMANTIC_COLOUR_TOKENS.filter((_, index) => (
      resolved[index] === undefined
      || resolved[index] === "rgb(0, 0, 0)"
      || resolved[index] === "rgba(0, 0, 0, 0)"
    ));
    expect(unresolved).toEqual([]);
    // Distinct roles must not have collapsed onto one primitive by accident.
    expect(new Set(resolved).size).toBeGreaterThan(SEMANTIC_COLOUR_TOKENS.length / 2);
  });

  test("every semantic token that stands in for a literal resolves to that literal", async ({ page }) => {
    await mount(page, "", DESKTOP_WIDTH);

    const resolved = await resolveTokens(page, EXPECTED_SEMANTIC_TOKENS.map(([name]) => name));
    expect(resolved).toEqual(EXPECTED_SEMANTIC_TOKENS.map(([, value]) => value));
  });

  // The five connected-backend state badges are a 13px bold glyph on a wash,
  // and every one of them is painted entirely from tokens: a sweep that swaps
  // the ink for the nearest *fill* role rather than the nearest *ink* role
  // silently takes them below AA, which is how `.backend-resource-state > span`
  // reached 3.6:1 during Phase 2. 13px bold is not WCAG "large text" -- that
  // starts at 18.66px bold -- so the floor is the full 4.5:1.
  test("every connected-backend state badge keeps AA contrast", async ({ page }) => {
    const modifiers = ["", "--error", "--unavailable", "--empty", "--loading"];
    await mount(
      page,
      modifiers
        .map((modifier) => (
          `<div class="backend-resource-state backend-resource-state${modifier}">`
          + `<span>i</span><div><strong>Index</strong><p>State</p></div></div>`
        ))
        .join(""),
      DESKTOP_WIDTH,
    );

    const painted = await page.evaluate(() => (
      [...document.querySelectorAll(".backend-resource-state > span")].map((glyph) => {
        const style = globalThis.getComputedStyle(glyph);
        return {
          badge: (glyph.parentElement as HTMLElement).className,
          ground: style.backgroundColor,
          ink: style.color,
        };
      })
    ));

    expect(painted.length, "no state badge mounted, so this assertion proves nothing").toEqual(5);
    const short = painted
      .map((badge) => ({ badge: badge.badge, ratio: contrastRatio(badge.ink, badge.ground) }))
      .filter((badge) => badge.ratio < AA_CONTRAST)
      .map((badge) => `${badge.badge} is ${badge.ratio.toFixed(2)}:1`);
    expect(short, "state badges whose glyph falls below WCAG AA on its own wash").toEqual([]);
  });

  test("the dark theme restates the semantic tier and is inert until it is selected", async ({ page }) => {
    await mount(page, "", DESKTOP_WIDTH);

    const roles = ["--bg-canvas", "--fg-text", "--border", "--chrome-bar"];
    const light = await resolveTokens(page, roles);
    expect(light).toEqual([
      "rgb(246, 246, 244)",
      "rgb(40, 52, 61)",
      "rgb(207, 212, 215)",
      "rgb(30, 37, 43)",
    ]);

    // Nothing sets `data-theme` yet, so the dark block must be unreachable in
    // the shipped render. Selecting it here proves it is wired correctly
    // without letting it near a screenshot baseline.
    await page.evaluate(() => document.documentElement.setAttribute("data-theme", "dark"));
    const dark = await resolveTokens(page, roles);
    for (const [index, role] of roles.entries()) {
      expect(dark[index], `${role} is unchanged in the dark theme`).not.toEqual(light[index]);
    }

    // Removing the attribute restores the light values, so the theme block
    // overrides tier 2 alone and leaves the palette underneath it untouched.
    await page.evaluate(() => document.documentElement.removeAttribute("data-theme"));
    expect(await resolveTokens(page, roles)).toEqual(light);
  });
});
