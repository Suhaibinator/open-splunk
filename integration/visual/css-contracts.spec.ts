// Computed-style contracts for app/globals.css.
//
// These replace unit assertions that matched the raw text of the stylesheet.
// Text matching pinned formatting (newline placement, single-line media-query
// bodies, declaration order) rather than behaviour, so any tokenising or
// reformatting pass broke them without changing a single rendered pixel.
// Here the stylesheet is loaded into a real browser against fixture markup that
// mirrors the production DOM, and the assertions read resolved values from
// getComputedStyle, which is what the rules actually promise.
import path from "node:path";

import { expect, test, type Page } from "@playwright/test";

const globalStylesheet = path.join(__dirname, "..", "..", "app", "globals.css");

const COMPACT_WIDTH = 980;
const MOBILE_WIDTH = 760;
const NARROW_WIDTH = 480;
const DESKTOP_WIDTH = 1280;

async function mount(page: Page, markup: string, width: number): Promise<void> {
  await page.setViewportSize({ height: 900, width });
  await page.setContent(`<main>${markup}</main>`);
  await page.addStyleTag({ path: globalStylesheet });
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
