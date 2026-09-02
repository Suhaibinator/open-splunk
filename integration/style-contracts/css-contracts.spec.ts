// Computed-style contracts for the application stylesheets.
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

async function contentHeight(page: Page, selector: string): Promise<number> {
  return page.evaluate((target) => {
    const element = document.querySelector(target);
    if (element === null) throw new Error(`fixture is missing ${target}`);
    return element.getBoundingClientRect().height;
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
  // `--focus-ring` at 28%, which Phase 5 rewrote from the literal
  // `rgb(42 120 158 / 28%)` to `color-mix(in srgb, var(--focus-ring) 28%,
  // transparent)`: no tier-2 token can carry an alpha, so the mix is how a ring
  // stays translucent and still moves with the theme. Chromium serializes a
  // `color-mix()` result in the `color(srgb …)` form rather than as `rgba()`,
  // which is why the expected string changed shape as well as value.
  await expect(control).toHaveCSS("outline-color", FOCUS_RING_28);
}

/** `--focus-ring` (`--blue-450`, #2f8ac1) at 28% alpha, as Chromium serializes it. */
const FOCUS_RING_28 = "color(srgb 0.184314 0.541176 0.756863 / 0.28)";

/** The same ring at 23%, which the knowledge-manager detail panel draws as a glow. */
const FOCUS_RING_23 = "color(srgb 0.184314 0.541176 0.756863 / 0.23)";

function expectEqualTracks(tracks: number[], count: number): void {
  expect(tracks).toHaveLength(count);
  for (const track of tracks) {
    expect(track).toBeCloseTo(tracks[0] ?? 0, 0);
    expect(track).toBeGreaterThan(0);
  }
}

test("desktop time picker stays right-anchored inside the viewport", async ({ page }) => {
  await mount(page, `
    <section class="search-composer">
      <div class="spl-editor"></div>
      <div class="time-picker-wrap">
        <button class="time-range-button" type="button">Last 24 hours</button>
        <dialog class="time-popover" open>
          <header class="time-popover-header"><strong>Select time range</strong></header>
          <div class="time-picker-layout">
            <aside class="time-picker-nav"></aside>
            <div class="time-picker-content">Common time ranges</div>
          </div>
          <footer class="time-popover-footer"><button type="button">Apply</button></footer>
        </dialog>
      </div>
      <button class="button run-button" type="button">Search</button>
    </section>
  `, DESKTOP_WIDTH);

  const geometry = await page.locator(".time-popover").evaluate((element) => {
    const dialog = element.getBoundingClientRect();
    const trigger = element.previousElementSibling?.getBoundingClientRect();
    if (trigger === undefined) throw new Error("fixture is missing the time range trigger");
    return {
      dialogLeft: dialog.left,
      dialogRight: dialog.right,
      documentWidth: document.documentElement.scrollWidth,
      triggerRight: trigger.right,
      viewportWidth: document.documentElement.clientWidth,
    };
  });

  expect(geometry.dialogLeft).toBeGreaterThanOrEqual(0);
  expect(geometry.dialogRight).toBeCloseTo(geometry.triggerRight, 0);
  expect(geometry.dialogRight).toBeLessThanOrEqual(geometry.viewportWidth);
  expect(geometry.documentWidth).toBe(geometry.viewportWidth);
});

test.describe("search workspace touch targets", () => {
  test.use({ hasTouch: true });

  const appMenuMarkup = `
    <div class="suite-shell splunk-shell">
      <header class="suite-product-bar">
        <button class="drawer-trigger" type="button" aria-label="Open product navigation"><span></span><span></span><span></span></button>
        <a class="wordmark" href="#"><span>open</span><b>&gt;</b><span>splunk</span></a>
        <div class="suite-menu-anchor">
          <button class="suite-app-switcher search-app-switcher" type="button" aria-expanded="true">App: <strong>Backend unavailable</strong></button>
          <div class="floating-menu app-menu" role="menu">
            <span class="menu-label">Server apps</span>
            <button role="menuitem" type="button"><span class="app-glyph">!</span><span><strong>Retry backend connection</strong><small>System bootstrap is unavailable</small></span></button>
            <button class="selected" role="menuitem" type="button"><span class="app-glyph">S</span><span><strong>Search &amp; Reporting</strong><small>Default indexes: gradethis, application, infrastructure</small></span><b>Selected</b></button>
            <div class="menu-separator"></div>
            <a role="menuitem" href="#"><span class="app-glyph">+</span><span><strong>Manage apps</strong><small>Open system administration and app permissions</small></span></a>
          </div>
        </div>
        <nav class="suite-utilities"><div class="suite-menu-anchor"><button class="suite-user-button" type="button"><span>A</span></button></div></nav>
      </header>
    </div>`;

  const eventRowMarkup = `
    <section class="event-list">
      <div class="event-head"><span></span><span>Time</span><span>Event</span><span class="event-row-actions-heading">Event Actions</span></div>
      <article class="event-row level-error">
        <button class="event-expander" type="button" aria-label="Expand event">›</button>
        <button class="event-time" type="button"><span>8/29/26</span><strong>12:58:04.754 PM</strong></button>
        <div class="event-content"><button class="event-raw" type="button">Email verification completion failed because the supplied verification token expired before the request completed.</button></div>
        <button class="event-copy-raw" type="button" aria-label="Copy raw event">Copy</button>
      </article>
    </section>`;

  test("coarse-pointer controls retain the 44px interaction floor", async ({ page }) => {
    await mount(page, `
      <div class="splunk-shell">
        <div class="suite-product-bar">
          <div class="suite-menu-anchor">
            <button class="suite-app-switcher search-app-switcher" type="button">App: Search</button>
          </div>
        </div>
        <header class="search-title-row">
          <div class="search-actions">
            <button class="search-action-save" type="button">Save</button>
          </div>
        </header>
        <section class="job-strip">
          <div class="job-controls">
            <button type="button">Job</button>
            <div class="header-menu-wrap"><button type="button">Smart Mode</button></div>
          </div>
        </section>
        <article><button class="event-copy-raw" type="button">Copy raw event</button></article>
        <div class="event-detail"><header><button class="event-detail-copy-raw" type="button">Copy raw</button></header></div>
      </div>
    `, MOBILE_WIDTH);

    expect(await page.evaluate(() => globalThis.matchMedia("(pointer: coarse)").matches)).toBe(true);
    await Promise.all([
      ".search-app-switcher",
      ".search-action-save",
      ".job-controls > button",
      ".job-controls .header-menu-wrap > button",
      ".event-copy-raw",
      ".event-detail-copy-raw",
    ].map(async (selector) => {
      const box = await page.locator(selector).evaluate((element) => {
        const bounds = element.getBoundingClientRect();
        return { height: bounds.height, width: bounds.width };
      });
      expect(box.height, `${selector} height`).toBeGreaterThanOrEqual(44);
      expect(box.width, `${selector} width`).toBeGreaterThanOrEqual(44);
    }));
  });

  test("the open Search app menu stays inside a narrow viewport", async ({ page }) => {
    async function mobileGeometry(width: number) {
      await mount(page, appMenuMarkup, width);
      return page.locator(".app-menu").evaluate((element) => {
        const bounds = element.getBoundingClientRect();
        const visibleDescendantOverflows = Array.from(element.querySelectorAll("*"))
          .filter((descendant) => {
            const style = globalThis.getComputedStyle(descendant);
            const descendantBounds = descendant.getBoundingClientRect();
            return style.display !== "none"
              && style.visibility !== "hidden"
              && descendantBounds.width > 0
              && descendantBounds.height > 0;
          })
          .flatMap((descendant) => {
            const descendantBounds = descendant.getBoundingClientRect();
            if (descendantBounds.left >= bounds.left - 0.5 && descendantBounds.right <= bounds.right + 0.5) return [];
            return [{
              className: descendant.getAttribute("class") ?? "",
              left: descendantBounds.left,
              right: descendantBounds.right,
              tagName: descendant.tagName,
            }];
          });
        return {
          documentWidth: document.documentElement.scrollWidth,
          left: bounds.left,
          position: globalThis.getComputedStyle(element).position,
          right: bounds.right,
          viewportWidth: document.documentElement.clientWidth,
          visibleDescendantOverflows,
        };
      });
    }

    const narrowGeometry = await mobileGeometry(320);
    const phoneGeometry = await mobileGeometry(390);
    for (const geometry of [narrowGeometry, phoneGeometry]) {
      expect(geometry.position).toBe("fixed");
      expect(geometry.left).toBeGreaterThanOrEqual(0);
      expect(geometry.right).toBeLessThanOrEqual(geometry.viewportWidth);
      expect(geometry.documentWidth).toBe(geometry.viewportWidth);
      expect(geometry.visibleDescendantOverflows).toEqual([]);
    }

    await mount(page, appMenuMarkup, DESKTOP_WIDTH);
    const desktopGeometry = await page.locator(".app-menu").evaluate((element) => {
      const bounds = element.getBoundingClientRect();
      const anchor = element.parentElement?.getBoundingClientRect();
      if (anchor === undefined) throw new Error("fixture is missing the app-menu anchor");
      return {
        anchorLeft: anchor.left,
        left: bounds.left,
        position: globalThis.getComputedStyle(element).position,
      };
    });
    expect(desktopGeometry.position).toBe("absolute");
    expect(desktopGeometry.left).toBeCloseTo(desktopGeometry.anchorLeft, 0);
  });

  test("event rows reserve the complete coarse-pointer action target", async ({ page }) => {
    async function eventGeometry(width: number, displayRaw = false) {
      await mount(page, eventRowMarkup, width);
      if (displayRaw) {
        await page.locator(".event-list").evaluate((element) => element.classList.add("display-raw"));
      }
      const rowTracks = await gridTracks(page, ".event-row");
      const headDisplay = await page.locator(".event-head").evaluate(
        (element) => globalThis.getComputedStyle(element).display,
      );
      const headTracks = headDisplay === "none" ? [] : await gridTracks(page, ".event-head");
      return page.locator(".event-copy-raw").evaluate((element, tracks) => {
        const bounds = element.getBoundingClientRect();
        const rowBounds = element.closest(".event-row")?.getBoundingClientRect();
        if (rowBounds === undefined) throw new Error("fixture is missing the event row");
        return {
          bounds: { height: bounds.height, left: bounds.left, right: bounds.right, width: bounds.width },
          documentWidth: document.documentElement.scrollWidth,
          headTracks: tracks.head,
          rowBounds: { left: rowBounds.left, right: rowBounds.right },
          rowTracks: tracks.row,
          viewportWidth: document.documentElement.clientWidth,
        };
      }, { head: headTracks, row: rowTracks });
    }

    const narrow = await eventGeometry(320);
    const phone = await eventGeometry(390);
    const folded = await eventGeometry(520);

    for (const geometry of [narrow, phone, folded]) {
      expect(geometry.bounds.height).toBeGreaterThanOrEqual(44);
      expect(geometry.bounds.width).toBeGreaterThanOrEqual(44);
      expect(geometry.bounds.left).toBeGreaterThanOrEqual(0);
      expect(geometry.bounds.right).toBeLessThanOrEqual(geometry.viewportWidth);
      expect(geometry.documentWidth).toBe(geometry.viewportWidth);
      expect(geometry.rowTracks.at(-1)).toBeCloseTo(44, 0);
    }
    expect(narrow.rowTracks).toHaveLength(3);
    expect(phone.rowTracks).toHaveLength(3);
    expect(folded.rowTracks).toHaveLength(4);
    expect(folded.headTracks).toHaveLength(4);
    expect(folded.headTracks.at(-1)).toBeCloseTo(44, 0);

    const rawDesktop = await eventGeometry(DESKTOP_WIDTH, true);
    const rawFolded = await eventGeometry(520, true);
    for (const geometry of [rawDesktop, rawFolded]) {
      expect(geometry.rowTracks).toHaveLength(4);
      expect(geometry.headTracks).toHaveLength(4);
      expect(geometry.rowTracks).toEqual(geometry.headTracks);
      expect(geometry.bounds.left).toBeCloseTo(geometry.rowBounds.right - 44, 0);
      expect(geometry.bounds.right).toBeLessThanOrEqual(geometry.rowBounds.right);
      expect(geometry.documentWidth).toBe(geometry.viewportWidth);
    }
  });
});

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
            <span data-label="State"><span class="status status--label"><i class="status status--dot status--success"></i>Active</span></span>
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
    await expect(detail).toHaveCSS("box-shadow", `${FOCUS_RING_23} 0px 0px 0px 3px`);
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

// The stacked presentation mirrors the panel: the cell carries the inline
// `max-width`/`overflow` the table cell renders with, and each member is its
// own line inside the fixed-height row.
const statisticsListMember = `/api/v1/${"resource-segment/".repeat(24)}index`;
const statisticsListMarkup = `
<table class="statistics-table statistics-table--fixed">
  <tbody>
    <tr class="statistics-plain-row">
      <td style="max-width: 420px; overflow: hidden">alpha</td>
    </tr>
    <tr class="statistics-list-row">
      <td style="max-width: 420px; overflow: hidden"><span class="statistics-multivalue-list"><span class="statistics-multivalue-item">${statisticsListMember}</span><span class="statistics-multivalue-item">${statisticsListMember}-two</span><button class="statistics-multivalue-more" type="button" aria-haspopup="dialog" aria-label="Show all 5 values for path">+3 more</button></span></td>
    </tr>
  </tbody>
</table>`;

test("Events Table headers keep the shared table paint", async ({ page }) => {
  await mount(page, `
    <div class="table-wrap events-table-wrap">
      <table class="table table--fixed events-table">
        <thead><tr><th scope="col">_time</th><th scope="col">host</th></tr></thead>
        <tbody><tr><td>2026-09-01</td><td>web-01</td></tr></tbody>
      </table>
    </div>
  `, DESKTOP_WIDTH);

  const [headerGround, border] = await resolveTokens(page, ["--bg-subtle", "--border"]);
  const header = page.locator(".events-table th").first();
  await expect(header).toHaveCSS("background-color", headerGround ?? "");
  await expect(header).toHaveCSS("border-bottom-color", border ?? "");
  await expect(header).toHaveCSS("white-space", "nowrap");
});

test("statistics col attributes and resize handles control column geometry", async ({ page }) => {
  await mount(page, `
    <div style="width: 360px">
      <table class="statistics-table statistics-table--fixed statistics-table--user-layout" width="360">
        <colgroup><col width="220" /><col width="140" /></colgroup>
        <thead><tr>
          <th scope="col">_time<span class="statistics-column-resizer"></span></th>
          <th scope="col">count<span class="statistics-column-resizer"></span></th>
        </tr></thead>
        <tbody><tr><td>2026-09-01</td><td>42</td></tr></tbody>
      </table>
    </div>
  `, DESKTOP_WIDTH);

  const widths = await page.locator(".statistics-table th").evaluateAll((headers) => (
    headers.map((header) => header.getBoundingClientRect().width)
  ));
  expect(widths[0]).toBeCloseTo(220, 0);
  expect(widths[1]).toBeCloseTo(140, 0);
  await expect(page.locator(".statistics-column-resizer").first()).toHaveCSS("width", "8px");
});

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

  test("stacked multivalue cells clip per line and keep the row height", async ({ page }) => {
    await mount(page, statisticsListMarkup, DESKTOP_WIDTH);

    const list = page.locator(".statistics-multivalue-list");
    await expect(list).toHaveCSS("display", "block");
    await expect(list).toHaveCSS("overflow-x", "hidden");
    await expect(list).toHaveCSS("overflow-y", "hidden");
    // calc(var(--statistics-row-height, 42px) - 8px) with the default row height.
    await expect(list).toHaveCSS("max-height", "34px");

    // Each member is its own clipped line rather than one joined string.
    const item = page.locator(".statistics-multivalue-item").first();
    await expect(item).toHaveCSS("display", "block");
    await expect(item).toHaveCSS("white-space", "nowrap");
    await expect(item).toHaveCSS("overflow-x", "hidden");
    await expect(item).toHaveCSS("text-overflow", "ellipsis");
    const itemOverflow = await item.evaluate((element) => ({
      client: element.clientWidth,
      scroll: element.scrollWidth,
    }));
    expect(itemOverflow.scroll).toBeGreaterThan(itemOverflow.client);

    // Two members plus the overflow button must not grow the virtual row past
    // the row token every other row is measured against.
    const [listRow, plainRow] = await Promise.all([
      contentHeight(page, ".statistics-list-row"),
      contentHeight(page, ".statistics-plain-row"),
    ]);
    expect(listRow).toBeCloseTo(plainRow, 0);
    expect(listRow).toBeLessThanOrEqual(42);

    // The clamp tracks the row-height token rather than a hard-coded height.
    await page.locator(".statistics-table").evaluate((element: HTMLElement) => {
      element.style.setProperty("--statistics-row-height", "52px");
    });
    await expect(list).toHaveCSS("max-height", "44px");
  });
});

test("the statistics sparkline paints with the palette accent", async ({ page }) => {
  await mount(page, '<svg class="statistics-sparkline"><polyline points="0,0" /></svg>', DESKTOP_WIDTH);

  // The polyline inherits --blue as currentColor, so an undefined token would
  // silently paint the SVG black. The sparkline needs a server-supplied
  // multivalue column, so this fixture is the direct gate on its palette.
  await expect(page.locator(".statistics-sparkline")).toHaveCSS("color", "rgb(40, 120, 168)");
  const polyline = page.locator(".statistics-sparkline polyline");
  await expect(polyline).toHaveCSS("stroke", "rgb(40, 120, 168)");
  await expect(polyline).toHaveCSS("fill", "none");
  await expect(polyline).toHaveCSS("stroke-linecap", "round");
});

test("time-series areas retain their series colour at the semantic fill opacity", async ({ page }) => {
  await mount(page, `
    <svg class="time-series-chart">
      <polygon class="time-series-chart__area time-series-chart__series" data-series-color="2" points="0,20 20,0 20,20"></polygon>
    </svg>
  `, DESKTOP_WIDTH);

  const area = page.locator(".time-series-chart__area");
  await expect(area).toHaveCSS("fill", "rgb(40, 120, 168)");
  await expect(area).toHaveCSS("fill-opacity", "0.24");
  await expect(area).toHaveCSS("stroke", "none");
});

test("stacked categorical slots overlap one shared track in both orientations", async ({ page }) => {
  await mount(page, `
    <span class="visualization-vertical-bars is-stacked">
      <span class="visualization-vertical-slot"></span>
    </span>
    <span class="visualization-horizontal-bars is-stacked">
      <span class="visualization-horizontal-slot"></span>
    </span>
  `, DESKTOP_WIDTH);

  await expect(page.locator(".visualization-vertical-bars")).toHaveCSS("display", "block");
  await expect(page.locator(".visualization-vertical-slot")).toHaveCSS("position", "absolute");
  await expect(page.locator(".visualization-horizontal-bars")).toHaveCSS("display", "block");
  await expect(page.locator(".visualization-horizontal-slot")).toHaveCSS("position", "absolute");
});

const liveJobsMarkup = `
<div class="table-wrap">
  <table class="table table--cards live-jobs-table">
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

    // The wrapper stops scrolling horizontally once rows become cards. The rule
    // is keyed off the table -- `.table-wrap:has(> .table--cards)` -- rather
    // than off a per-page wrapper class, so it reaches every card table.
    await expect(page.locator(".table-wrap")).toHaveCSS("overflow-x", "visible");
  });

  // A card has no column headers above it, so the only thing naming a value is
  // the `::before` the cell's own `data-label` fills. `app/activity`'s unit
  // test used to assert this by matching the stylesheet's characters, which
  // pinned the rule to whichever file held it; the promise is that the label
  // renders, and only a browser can say whether it does.
  test("mobile job cards print each cell's column name from its data-label", async ({ page }) => {
    await mount(page, liveJobsMarkup, MOBILE_WIDTH);

    const labels = await page.locator(".live-jobs-table tbody td").evaluateAll(
      (cells) => cells.map((cell) => globalThis.getComputedStyle(cell, "::before").content),
    );
    expect(labels).toEqual([
      '"Search"', '"Status"', '"Owner"', '"Runtime"', '"Events"', '"Started"', '"Actions"',
    ]);

    // The header row is what the label replaces, so it leaves the layout:
    // painting both would print every column name twice. It stays in the
    // accessibility tree rather than being display:none, which is why the
    // contract is on the clip rather than on visibility.
    const header = page.locator(".live-jobs-table thead");
    await expect(header).toHaveCSS("position", "absolute");
    await expect(header).toHaveCSS("clip-path", "inset(50%)");
    expect(await contentWidth(page, ".live-jobs-table thead")).toBeCloseTo(1, 0);
  });

  test("desktop job cells print no label, because the header row does", async ({ page }) => {
    await mount(page, liveJobsMarkup, DESKTOP_WIDTH);

    const labels = await page.locator(".live-jobs-table tbody td").evaluateAll(
      (cells) => cells.map((cell) => globalThis.getComputedStyle(cell, "::before").content),
    );
    expect(labels).toEqual(Array.from({ length: 7 }, () => "none"));
    await expect(page.locator(".live-jobs-table thead")).toHaveCSS("position", "static");
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

const SEMANTIC_COLOUR_TOKENS: readonly string[] = [
  "--bg-canvas",
  "--bg-surface",
  "--bg-subtle",
  "--bg-raised",
  "--bg-inverse",
  "--bg-inverse-tint",
  "--bg-scrim",
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
  "--skeleton-base",
  "--skeleton-highlight",
  "--chrome-bar",
  "--chrome-appbar",
  "--chrome-hover",
  "--chrome-fg",
  "--highlight",
  "--selection",
  "--focus-ring",
];

test("loading skeletons stop moving when reduced motion is requested", async ({ page }) => {
  await page.emulateMedia({ reducedMotion: "reduce" });
  await mount(page, '<span class="skeleton skeleton--line"></span>', DESKTOP_WIDTH);

  await expect(page.locator(".skeleton")).toHaveCSS("animation-name", "none");
});

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

test.describe("colour token contracts", () => {
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

  // The theme switch (lib/theme-preference.ts) writes `data-theme` on the
  // root and nothing else: this is the contract that the attribute alone
  // repaints the semantic tier, and that taking it away restores every value.
  test("the dark theme resolves when selected and restores when removed", async ({ page }) => {
    await mount(page, "", DESKTOP_WIDTH);

    const roles = ["--bg-canvas", "--fg-text", "--border", "--chrome-bar"];
    const light = await resolveTokens(page, roles);
    expect(light).toEqual([
      "rgb(246, 246, 244)",
      "rgb(40, 52, 61)",
      "rgb(207, 212, 215)",
      "rgb(30, 37, 43)",
    ]);
    expect(await colorScheme(page)).toEqual("light");

    await page.evaluate(() => document.documentElement.setAttribute("data-theme", "dark"));
    const dark = await resolveTokens(page, roles);
    for (const [index, role] of roles.entries()) {
      expect(dark[index], `${role} is unchanged in the dark theme`).not.toEqual(light[index]);
    }
    // Form controls, scrollbars and the canvas behind the page follow the
    // block's own `color-scheme`, so a dark page gets dark browser chrome.
    expect(await colorScheme(page)).toEqual("dark");

    // `data-theme="light"` is what the boot script writes for the light
    // theme; it must select nothing, so the light values are the defaults.
    await page.evaluate(() => document.documentElement.setAttribute("data-theme", "light"));
    expect(await resolveTokens(page, roles)).toEqual(light);
    expect(await colorScheme(page)).toEqual("light");

    // Removing the attribute restores the light values, so the theme block
    // overrides tier 2 alone and leaves the palette underneath it untouched.
    await page.evaluate(() => document.documentElement.removeAttribute("data-theme"));
    expect(await resolveTokens(page, roles)).toEqual(light);
  });

  // The three surfaces a search author stares at longest: the SPL editor,
  // its completion menu and the toast. Each reads only semantic tokens, so
  // the dark block must move every one of them, and the syntax inks -- which
  // the dark block lightens one step -- must still clear AA on the editor's
  // dark ground.
  test("the editor, completion menu and toast repaint in the dark theme with AA syntax inks", async ({ page }) => {
    await mount(
      page,
      '<div class="spl-editor"><div class="editor-highlight">'
      + '<span class="spl-command">stats</span> <span class="spl-field">host</span> <span class="spl-string">"web"</span>'
      + "</div></div>"
      + '<div class="completion-menu"><div class="completion-title"><span>Commands</span></div>'
      + '<button type="button" data-highlighted="true"><code>stats</code><span>Aggregate</span></button></div>'
      + '<div class="toast"><span>i</span><strong>Saved</strong></div>',
      DESKTOP_WIDTH,
    );

    const pairs: ReadonlyArray<readonly [string, "backgroundColor" | "color"]> = [
      [".spl-editor", "backgroundColor"],
      [".editor-highlight", "color"],
      [".completion-menu", "backgroundColor"],
      [".completion-menu > button", "backgroundColor"],
      [".completion-menu code", "color"],
      [".toast", "backgroundColor"],
      [".toast", "color"],
    ];
    const light = await paints(page, pairs);
    await page.evaluate(() => document.documentElement.setAttribute("data-theme", "dark"));
    const dark = await paints(page, pairs);
    for (const [index, [selector, property]] of pairs.entries()) {
      expect(dark[index], `${selector} ${property} is unchanged in the dark theme`).not.toEqual(light[index]);
    }

    const editorGround = dark[0] ?? "";
    const inks = await paints(page, [[".spl-command", "color"], [".spl-string", "color"], [".spl-field", "color"]]);
    const short = [".spl-command", ".spl-string", ".spl-field"]
      .map((selector, index) => ({ ratio: contrastRatio(inks[index] ?? "", editorGround), selector }))
      .filter((ink) => ink.ratio < AA_CONTRAST)
      .map((ink) => `${ink.selector} is ${ink.ratio.toFixed(2)}:1`);
    expect(short, "syntax inks below WCAG AA on the dark editor").toEqual([]);
  });
});

/** The root's computed `color-scheme`, which the theme block switches. */
async function colorScheme(page: Page): Promise<string> {
  return page.evaluate(() => globalThis.getComputedStyle(document.documentElement).colorScheme);
}

/** One computed paint per (selector, property) pair, in order. */
async function paints(
  page: Page,
  pairs: ReadonlyArray<readonly [string, "backgroundColor" | "color"]>,
): Promise<string[]> {
  return page.evaluate((targets) => targets.map(([selector, property]) => {
    const element = document.querySelector(selector);
    if (element === null) throw new Error(`fixture is missing ${selector}`);
    return globalThis.getComputedStyle(element)[property];
  }), pairs);
}

// Contracts for the five breakpoints Phase 4 folded onto the canon.
//
// docs/theming.md's fold table records what each fold changed. Each test below
// mounts the surface the table names at a width inside the folded band and
// asserts the promised layout, plus one width outside the band where the fold
// must have changed nothing.
const INSIDE_1120_FOLD = 1000;
const INSIDE_800_FOLD = 900;
const INSIDE_520_FOLD = 500;
const INSIDE_430_FOLD = 450;

const analyticsPanelsMarkup = `
<div class="analytics-primary-grid">
  <section class="suite-card"><header class="analytics-panel-header"><div><h2>Search performance</h2></div></header></section>
  <section class="suite-card"><header class="analytics-panel-header"><div><h2>Insights</h2></div></header></section>
</div>
<div class="analytics-secondary-grid">
  <section class="suite-card"><header class="analytics-panel-header"><div><h2>Field profile</h2></div></header></section>
  <section class="suite-card"><header class="analytics-panel-header"><div><h2>Slowest searches</h2></div></header></section>
</div>`;

const analyticsMetricsMarkup = `
<section class="analytics-metric-grid">
  <a href="#"><span>Searches run</span><strong>4,182</strong><small>up 8.4%</small><i>arrow</i></a>
  <a href="#"><span>Success rate</span><strong>99.4%</strong><small>26 failed</small><i>arrow</i></a>
  <a href="#"><span>Median runtime</span><strong>1.18 s</strong><small>p95 2.4 s</small><i>arrow</i></a>
  <a href="#"><span>Events scanned</span><strong>91K</strong><small>21.8 per result</small><i>arrow</i></a>
</section>
<div class="analytics-field-list">
  <div class="analytics-field-list-header"><span>Field</span><span>Coverage</span><span>Distinct</span><span>Example</span><span></span></div>
  <ul>
    <li>
      <div class="analytics-field-identity"><code>host</code><span class="analytics-field-type">string</span></div>
      <div class="analytics-coverage-cell"><span><i></i></span><strong>98%</strong></div>
      <span class="analytics-cardinality">42</span>
      <code class="analytics-example">web-01</code>
      <a href="#">Analyze</a>
    </li>
  </ul>
</div>`;

const analyticsContextMarkup = `
<section class="analytics-context-bar">
  <div><span class="analytics-context-icon">i</span><div><strong>Search workload</strong><small>Filters update the summary fixtures.</small></div></div>
  <label><span>Time range</span><select><option>Last 24 hours</option></select></label>
  <label><span>Environment</span><select><option>All</option></select></label>
</section>`;

const operationsHeaderMarkup = `
<header class="dashboard-title-row">
  <div><span class="suite-eyebrow">OPERATIONS</span><h1>Service overview</h1></div>
  <div class="operations-header-actions">
    <label class="operations-range-picker"><span>Metrics range</span><select><option>Last 24 hours</option></select></label>
    <span class="badge badge--outline operations-preview-badge">Preview data</span>
    <span class="operations-update-status">Fixture timestamp: Jul 21, 4:00 PM</span>
  </div>
</header>
<div class="operations-volume-chart">
  <div class="operations-volume-scroll">
    <div class="operations-volume-scroll-content">
      <fieldset class="operations-volume-plot">
        ${Array.from({ length: 24 }, (_, index) => `<button type="button" class="operations-volume-bar"><span class="operations-volume-fill" style="--bar-height: ${index === 12 ? "100%" : "30%"}"></span></button>`).join("")}
      </fieldset>
      <div class="operations-volume-axis"><span>Start</span><span>End</span></div>
    </div>
  </div>
  <span class="operations-volume-tooltip" hidden role="tooltip"><strong>Middle bucket</strong><span>84,219 events</span></span>
</div>`;

const knowledgeInspectionMarkup = `
<section class="workspace-dialog-knowledge-inspection">
  <h3>Knowledge authority</h3>
  <dl>
    <dt>Snapshot digest</dt><dd><code>2f9c11ab</code></dd>
    <dt>Catalog revision</dt><dd>18</dd>
    <dt>Applicable field objects</dt><dd>24</dd>
    <dt>Lookup assets</dt><dd>3</dd>
  </dl>
</section>`;

test.describe("folded breakpoint contracts", () => {
  test("the analytics panel rails stack from 980px, not from 1120px", async ({ page }) => {
    // The band the fold changed: 981px-1120px used to stack the rails and now
    // keeps both columns, which is the row docs/theming.md's fold table opens
    // with.
    await mount(page, analyticsPanelsMarkup, INSIDE_1120_FOLD);
    expect(await gridTracks(page, ".analytics-primary-grid")).toHaveLength(2);
    expect(await gridTracks(page, ".analytics-secondary-grid")).toHaveLength(2);

    // At the canon step itself they still stack, so the fold moved the boundary
    // rather than deleting the compact layout.
    await mount(page, analyticsPanelsMarkup, COMPACT_WIDTH);
    expect(await gridTracks(page, ".analytics-primary-grid")).toHaveLength(1);
    expect(await gridTracks(page, ".analytics-secondary-grid")).toHaveLength(1);
  });

  test("the metric grid and the field list fold at 980px, not at 800px", async ({ page }) => {
    await mount(page, analyticsMetricsMarkup, INSIDE_800_FOLD);
    expectEqualTracks(await gridTracks(page, ".analytics-metric-grid"), 2);
    await expect(page.locator(".analytics-field-list .analytics-example")).toHaveCSS("display", "none");
    await expect(page.locator(".analytics-field-list-header span:nth-child(4)")).toHaveCSS("display", "none");

    await mount(page, analyticsMetricsMarkup, DESKTOP_WIDTH);
    expectEqualTracks(await gridTracks(page, ".analytics-metric-grid"), 4);
    await expect(page.locator(".analytics-field-list .analytics-example")).not.toHaveCSS("display", "none");
  });

  test("the context bar and the metric numerals fold at 480px, not at 420px", async ({ page }) => {
    await mount(page, `${analyticsContextMarkup}${analyticsMetricsMarkup}`, INSIDE_430_FOLD);
    expect(await gridTracks(page, ".analytics-context-bar")).toHaveLength(1);
    // 18px and 21px until Phase 5, which put every font-size on a --type-* step
    // and landed both of them on --type-xxl -- one step, so the fold stopped
    // folding. The narrow numeral now reads --type-xl, which is the step below
    // it: the assertion is the same one, against the ramp the layer has.
    await expect(page.locator(".analytics-metric-grid strong").first()).toHaveCSS("font-size", "16px");

    // Just above the canon step the two-up context bar and the full numeral
    // survive: 421px-480px is the whole band this fold changed.
    await mount(page, `${analyticsContextMarkup}${analyticsMetricsMarkup}`, INSIDE_520_FOLD);
    expect(await gridTracks(page, ".analytics-context-bar")).toHaveLength(2);
    await expect(page.locator(".analytics-metric-grid strong").first()).toHaveCSS("font-size", "20px");
  });

  test("the operations header actions and volume plot fold at 480px, not at 430px", async ({ page }) => {
    await mount(page, operationsHeaderMarkup, INSIDE_430_FOLD);
    expectEqualTracks(await gridTracks(page, ".operations-header-actions"), 2);
    await expect(page.locator(".operations-volume-plot")).toHaveCSS("height", "196px");

    const bars = page.locator(".operations-volume-bar");
    await expect(bars).toHaveCount(24);
    const targets = await bars.evaluateAll((elements) => elements.map((element) => {
      const bounds = element.getBoundingClientRect();
      return { height: bounds.height, left: bounds.left, right: bounds.right, width: bounds.width };
    }));
    expect(targets.every((target) => target.width >= 44 && target.height >= 44)).toBe(true);
    expect(targets.every((target, index) => index === 0 || target.left >= targets[index - 1]!.right)).toBe(true);
    const scrollGeometry = await page.locator(".operations-volume-scroll").evaluate((element) => ({
      clientWidth: element.clientWidth,
      scrollWidth: element.scrollWidth,
    }));
    expect(scrollGeometry.scrollWidth).toBeGreaterThan(scrollGeometry.clientWidth);

    const middleBar = bars.nth(12);
    const scroll = page.locator(".operations-volume-scroll");
    await scroll.evaluate((element) => { element.scrollLeft = element.scrollWidth / 2; });
    await middleBar.evaluate((element) => { element.classList.add("operations-volume-bar--active"); });
    const tooltip = page.locator(".operations-volume-tooltip");
    await tooltip.evaluate((element: HTMLElement) => { element.hidden = false; });
    await expect(tooltip).toBeVisible();
    const tooltipGeometry = await tooltip.evaluate((element) => {
      const bounds = element.getBoundingClientRect();
      const chart = element.closest(".operations-volume-chart")?.getBoundingClientRect();
      if (chart === undefined) throw new Error("fixture is missing the volume chart");
      let visibleBottom = globalThis.innerHeight;
      let visibleLeft = 0;
      let visibleRight = globalThis.innerWidth;
      let visibleTop = 0;
      for (let ancestor = element.parentElement; ancestor !== null; ancestor = ancestor.parentElement) {
        const overflow = globalThis.getComputedStyle(ancestor);
        const ancestorBounds = ancestor.getBoundingClientRect();
        if (overflow.overflowX !== "visible") {
          visibleLeft = Math.max(visibleLeft, ancestorBounds.left);
          visibleRight = Math.min(visibleRight, ancestorBounds.right);
        }
        if (overflow.overflowY !== "visible") {
          visibleBottom = Math.min(visibleBottom, ancestorBounds.bottom);
          visibleTop = Math.max(visibleTop, ancestorBounds.top);
        }
      }
      return {
        chartBottom: chart.bottom,
        chartLeft: chart.left,
        chartRight: chart.right,
        chartTop: chart.top,
        tooltipBottom: bounds.bottom,
        tooltipLeft: bounds.left,
        tooltipRight: bounds.right,
        tooltipTop: bounds.top,
        visibleBottom,
        visibleLeft,
        visibleRight,
        visibleTop,
      };
    });
    expect(tooltipGeometry.tooltipLeft).toBeGreaterThanOrEqual(tooltipGeometry.chartLeft);
    expect(tooltipGeometry.tooltipRight).toBeLessThanOrEqual(tooltipGeometry.chartRight);
    expect(tooltipGeometry.tooltipTop).toBeGreaterThanOrEqual(tooltipGeometry.chartTop);
    expect(tooltipGeometry.tooltipBottom).toBeLessThanOrEqual(tooltipGeometry.chartBottom);
    expect(tooltipGeometry.tooltipLeft).toBeGreaterThanOrEqual(tooltipGeometry.visibleLeft);
    expect(tooltipGeometry.tooltipRight).toBeLessThanOrEqual(tooltipGeometry.visibleRight);
    expect(tooltipGeometry.tooltipTop).toBeGreaterThanOrEqual(tooltipGeometry.visibleTop);
    expect(tooltipGeometry.tooltipBottom).toBeLessThanOrEqual(tooltipGeometry.visibleBottom);

    // Between 481px and 760px the range picker still leads a `1fr auto` row and
    // the plot keeps its full height, so the fold reaches only the narrow band.
    await mount(page, operationsHeaderMarkup, INSIDE_520_FOLD);
    const wide = await gridTracks(page, ".operations-header-actions");
    expect(wide).toHaveLength(2);
    expect(wide[0]).toBeGreaterThan(wide[1] ?? 0);
    await expect(page.locator(".operations-volume-plot")).toHaveCSS("height", "215px");
  });

  test("the knowledge-inspection list keeps two columns down to 480px, not to 520px", async ({ page }) => {
    // The one fold that folds inward: this list is single-column from 480px
    // down where it used to be single-column from 520px down, so 481px-520px
    // is the 40px band that changed and the one this asserts.
    await mount(page, knowledgeInspectionMarkup, INSIDE_520_FOLD);
    expect(await gridTracks(page, ".workspace-dialog-knowledge-inspection dl")).toHaveLength(2);

    await mount(page, knowledgeInspectionMarkup, NARROW_WIDTH);
    expect(await gridTracks(page, ".workspace-dialog-knowledge-inspection dl")).toHaveLength(1);
  });
});

// A field in each of the two grids that state their own borders, valid and
// invalid, plus the sign-in card and the knowledge-manager filter grid, whose
// private invalid rules folded into the primitive. Every one of these is a
// place a feature rule loads after the primitive and could paint over it.
const fieldValidationMarkup = `
<div class="settings-form-grid">
  <label for="valid-limit">
    <span>Per-query memory</span>
    <input id="valid-limit" value="1 GiB" aria-describedby="valid-limit-note">
    <small id="valid-limit-note">1 MiB–64 GiB; default 1 GiB.</small>
  </label>
  <label for="invalid-limit">
    <span>Data read per query</span>
    <input id="invalid-limit" value="3 bytes" aria-invalid="true" aria-describedby="invalid-limit-note">
    <small class="field-error" id="invalid-limit-note">Enter 1 MiB–64 GiB.</small>
  </label>
</div>
<div class="admin-policy-grid">
  <label for="invalid-policy">
    <span>Maximum event size</span>
    <input id="invalid-policy" value="2 MiB" aria-invalid="true" aria-describedby="invalid-policy-note">
    <small class="field-error" id="invalid-policy-note">Enter 1 MiB or less.</small>
  </label>
  <label for="invalid-patterns">
    <span>Allowed host patterns</span>
    <textarea id="invalid-patterns" aria-invalid="true" aria-describedby="invalid-patterns-note"></textarea>
    <small class="field-error" id="invalid-patterns-note">Enter at most 16 unique patterns.</small>
  </label>
</div>
<div class="signin-card"><form><input aria-invalid="true" value="bad"></form></div>
<div class="knowledge-manager__advanced-filter-grid">
  <label><span>Owner ID</span><input aria-invalid="true" value="bad"></label>
</div>`;

test.describe("field validation contracts", () => {
  test("every invalid control is painted, whichever grid states its own border", async ({ page }) => {
    // The defect this pins: `aria-invalid` was set by five forms and painted by
    // two, so three of them marked the field for a screen reader and rendered
    // it identically to a field the form would accept.
    await mount(page, fieldValidationMarkup, DESKTOP_WIDTH);
    const [error, strong] = await resolveTokens(page, ["--status-error", "--border-strong"]);

    await Promise.all([
      "#invalid-limit",
      "#invalid-policy",
      "#invalid-patterns",
      ".signin-card input",
      ".knowledge-manager__advanced-filter-grid input",
    ].map((selector) => expect(page.locator(selector), selector).toHaveCSS("border-top-color", error!)));
    // The valid field in the same grid keeps the ordinary border, so the rule
    // above is reaching the attribute rather than the grid.
    await expect(page.locator("#valid-limit")).toHaveCSS("border-top-color", strong!);
  });

  test("the note that says why is not the same ink as the hint it replaced", async ({ page }) => {
    // Both rendered in `--fg-muted` before: the message a form showed to explain
    // a disabled button was styled exactly like the advice beside every other
    // field.
    await mount(page, fieldValidationMarkup, DESKTOP_WIDTH);
    const [error, muted] = await resolveTokens(page, ["--status-error", "--fg-muted"]);

    await expect(page.locator("#invalid-limit-note")).toHaveCSS("color", error!);
    await expect(page.locator("#invalid-policy-note")).toHaveCSS("color", error!);
    await expect(page.locator("#valid-limit-note")).toHaveCSS("color", muted!);
    expect(error).not.toEqual(muted);
  });
});

function composerMarkup(lines: number, completionOpen = false): string {
  const query = Array.from({ length: lines }, (_, index) => (index === 0 ? "index=main" : `| stage${index}`)).join("\n");
  const gutter = Array.from({ length: Math.max(2, lines) }, (_, index) => `<span>${index + 1}</span>`).join("");
  const menu = completionOpen
    ? `<div class="completion-menu" id="spl-completion-list" role="listbox" aria-label="SPL suggestions">
        <div class="completion-group" role="group" aria-labelledby="spl-completion-group-command">
          <div class="completion-title" id="spl-completion-group-command"><span>Commands</span><small>Enter a pipeline stage</small></div>
          <button class="completion-option" id="spl-completion-0" role="option" aria-selected="true" type="button"><code>stats</code><span>Aggregate</span><kbd>↵</kbd></button>
        </div>
        <div class="completion-group" role="group" aria-labelledby="spl-completion-group-field">
          <div class="completion-title" id="spl-completion-group-field"><span>Fields</span><small>Fields seen in results</small></div>
          <button class="completion-option" id="spl-completion-1" role="option" aria-selected="false" type="button"><code>status</code><span>Field</span><kbd></kbd></button>
        </div>
      </div>`
    : "";
  return `
    <section class="search-composer">
      <div class="spl-editor">
        <div class="editor-gutter" aria-hidden="true"><div class="editor-gutter-lines">${gutter}</div></div>
        <pre class="editor-highlight" aria-hidden="true">${query}</pre>
        <textarea aria-label="Search with SPL">${query}</textarea>
        <div class="editor-meta" id="editor-help"><span>SPL</span><span>Ctrl+Space for suggestions</span><span>⌘↵ to run</span></div>
        ${menu}
      </div>
      <div class="time-picker-wrap">
        <button class="time-range-button" type="button"><span></span><span><small>Time range</small><strong>Last 24 hours</strong></span><span></span></button>
      </div>
      <button class="button button--primary run-button" type="button"><span>⌕</span><strong>Search</strong></button>
    </section>`;
}

const diagnosticMarkup = `
  <section class="search-composer">
    <div class="spl-editor has-error">
      <div class="editor-gutter" aria-hidden="true"><div class="editor-gutter-lines">
        <span id="gutter-plain">1</span>
        <button class="editor-gutter-marker" data-severity="error" id="gutter-marker" tabindex="-1" type="button">2</button>
      </div></div>
      <pre class="editor-highlight" aria-hidden="true"><span class="spl-field">index</span>=main
| <span class="spl-command"><mark class="spl-diagnostic" data-severity="error" id="diagnostic-error">transaction</mark></span> <mark class="spl-diagnostic" data-severity="warning" id="diagnostic-warning">host</mark></pre>
      <textarea aria-label="Search with SPL" aria-invalid="true">index=main
| transaction host</textarea>
    </div>
  </section>`;

test.describe("SPL editor auto-grow", () => {
  test("the editor grows with the query up to its cap while the buttons keep their height", async ({ page }) => {
    // One line still fills the two-line composer row.
    await mount(page, composerMarkup(1), DESKTOP_WIDTH);
    expect(await contentHeight(page, ".spl-editor")).toBeCloseTo(62, 0);
    expect(await contentHeight(page, ".time-range-button")).toBeCloseTo(62, 0);
    expect(await contentHeight(page, ".run-button")).toBeCloseTo(62, 0);

    // Eight lines: 8 × 22px line height + 16px padding + 2px border.
    await mount(page, composerMarkup(8), DESKTOP_WIDTH);
    const eightLines = await contentHeight(page, ".spl-editor");
    expect(eightLines).toBeCloseTo(194, 0);
    expect(await contentHeight(page, ".spl-editor textarea")).toBeCloseTo(eightLines - 2, 0);
    expect(await contentHeight(page, ".editor-gutter")).toBeCloseTo(eightLines - 2, 0);
    expect(await contentHeight(page, ".time-range-button")).toBeCloseTo(62, 0);
    expect(await contentHeight(page, ".run-button")).toBeCloseTo(62, 0);

    // Thirty lines hit --search-editor-max-height and the mirror scrolls.
    await mount(page, composerMarkup(30), DESKTOP_WIDTH);
    const capped = await page.locator(".spl-editor").evaluate((element) => {
      const mirror = element.querySelector(".editor-highlight");
      if (!(mirror instanceof HTMLElement)) throw new Error("fixture is missing the highlight mirror");
      return {
        cap: Number.parseFloat(globalThis.getComputedStyle(element).getPropertyValue("--search-editor-max-height")),
        editorHeight: element.getBoundingClientRect().height,
        mirrorHeight: mirror.getBoundingClientRect().height,
        mirrorOverflows: mirror.scrollHeight > mirror.clientHeight,
      };
    });
    expect(capped.cap).toBeGreaterThan(eightLines);
    expect(capped.mirrorHeight).toBeCloseTo(capped.cap, 0);
    expect(capped.editorHeight).toBeCloseTo(capped.cap + 2, 0);
    expect(capped.mirrorOverflows).toBe(true);
    expect(await contentHeight(page, ".time-range-button")).toBeCloseTo(62, 0);
    expect(await contentHeight(page, ".run-button")).toBeCloseTo(62, 0);
  });

  test("the completion menu opens below the grown editor rather than over its last lines", async ({ page }) => {
    const menuGeometry = async (lines: number) => {
      await mount(page, composerMarkup(lines, true), DESKTOP_WIDTH);
      return page.locator(".spl-editor").evaluate((element) => {
        const menu = element.querySelector(".completion-menu");
        if (menu === null) throw new Error("fixture is missing the completion menu");
        return {
          editorBottom: element.getBoundingClientRect().bottom,
          menuTop: menu.getBoundingClientRect().top,
        };
      });
    };

    const twoLines = await menuGeometry(2);
    expect(twoLines.menuTop).toBeGreaterThanOrEqual(twoLines.editorBottom);
    const eightLines = await menuGeometry(8);
    expect(eightLines.menuTop).toBeGreaterThanOrEqual(eightLines.editorBottom);
  });

  test("a diagnostic squiggles the mirror without washing the token, and dots its gutter line", async ({ page }) => {
    // The mark nests inside a syntax token, so the browser's own <mark>
    // yellow would paint over the token's ink; the rule overrides it away
    // and leaves only a wavy underline in the severity's colour. The gutter
    // marker is the one gutter element the pointer may reach.
    await mount(page, diagnosticMarkup, DESKTOP_WIDTH);
    const [error, warning] = await resolveTokens(page, ["--status-error", "--status-warning"]);
    const errorMark = page.locator("#diagnostic-error");
    await expect(errorMark).toHaveCSS("text-decoration-line", "underline");
    await expect(errorMark).toHaveCSS("text-decoration-style", "wavy");
    await expect(errorMark).toHaveCSS("text-decoration-color", error!);
    await expect(errorMark).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
    await expect(page.locator("#diagnostic-warning")).toHaveCSS("text-decoration-color", warning!);
    expect(error).not.toEqual(warning);
    await expect(page.locator("#gutter-marker")).toHaveCSS("pointer-events", "auto");
    await expect(page.locator("#gutter-marker")).toHaveCSS("color", error!);
    await expect(page.locator("#gutter-plain")).toHaveCSS("pointer-events", "none");
  });

  test("the selected option is the one aria-selected names, not a private attribute", async ({ page }) => {
    // The menu is a listbox the textarea drives through aria-activedescendant,
    // so the highlight has to follow the same attribute assistive technology
    // reads. A second group in the fixture checks that grouping does not
    // change which surface the option sits on.
    await mount(page, composerMarkup(2, true), DESKTOP_WIDTH);
    const [selection, surface] = await resolveTokens(page, ["--selection", "--bg-surface"]);
    await expect(page.locator("#spl-completion-0")).toHaveCSS("background-color", selection!);
    await expect(page.locator("#spl-completion-1")).toHaveCSS("background-color", surface!);
    expect(selection).not.toEqual(surface);
  });
});
