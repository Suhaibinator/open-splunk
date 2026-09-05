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

import { type Palette, PALETTE_CONTRAST_FLOOR, PALETTES, resolvePalette } from "../../lib/palettes";
import { resolveTheme, THEME_BOOT_SCRIPT } from "../../lib/theme-preference";
import { addApplicationStyles } from "./application-stylesheets";
import { KNOB_CONSUMERS, SHELL_FIXTURE } from "./palette-fixture";

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

test("button modifiers preserve notice, toolbar, and dataset-toggle geometry", async ({ page }) => {
  await mount(page, `
    <output class="history-action-notice"><span>History updated</span><button class="button button--notice-dismiss" type="button">×</button></output>
    <output class="reports-action-notice"><span>Report updated</span><button class="button button--notice-dismiss" type="button">×</button></output>
    <div class="resource-toolbar"><button class="button button--toolbar" type="button"><svg class="app-icon app-icon--sm"></svg> Refresh</button></div>
    <fieldset class="dataset-view-toggle"><button class="button button--toolbar active" type="button"><svg class="app-icon app-icon--sm"></svg> Cards</button><button class="button button--toolbar" type="button"><span aria-hidden="true">☷</span> Table</button></fieldset>
    <article class="collector-card"><footer><span></span><small>72% of configured peak</small><button class="button button--link" type="button">Details</button></footer></article>
    <section class="field-inspector"><footer><span>Showing top values</span><button class="button button--link" type="button">New search</button></footer></section>
    <section class="pattern-table"><article><button class="button button--link pattern-action" type="button">View events</button></article></section>
  `, DESKTOP_WIDTH);

  await Promise.all([".history-action-notice .button", ".reports-action-notice .button"].map(async (selector) => {
    const dismiss = page.locator(selector);
    const [height, width] = await Promise.all([
      contentHeight(page, selector),
      contentWidth(page, selector),
    ]);
    await expect(dismiss).toHaveCSS("border-radius", "0px");
    await expect(dismiss).toHaveCSS("border-width", "0px");
    await expect(dismiss).toHaveCSS("font-weight", "400");
    await expect(dismiss).toHaveCSS("gap", "5px");
    await expect(dismiss).toHaveCSS("justify-content", "normal");
    await expect(dismiss).toHaveCSS("padding-bottom", "1px");
    await expect(dismiss).toHaveCSS("padding-left", "6px");
    await expect(dismiss).toHaveCSS("padding-right", "6px");
    await expect(dismiss).toHaveCSS("padding-top", "1px");
    expect(height).toBeCloseTo(28, 0);
    expect(width).toBeCloseTo(28, 0);
  }));

  const [surface, border, selection, subtle] = await resolveTokens(page, [
    "--bg-surface",
    "--border-strong",
    "--selection",
    "--bg-subtle",
  ]);
  const toolbar = page.locator(".resource-toolbar .button");
  await expect(toolbar).toHaveCSS("background-color", surface ?? "");
  await expect(toolbar).toHaveCSS("border-color", border ?? "");
  await expect(toolbar).toHaveCSS("border-radius", "0px");
  await expect(toolbar).toHaveCSS("display", "flex");
  await expect(toolbar).toHaveCSS("font-weight", "400");
  await expect(toolbar).toHaveCSS("gap", "5px");
  await expect(toolbar).toHaveCSS("height", "32px");
  await expect(toolbar).toHaveCSS("justify-content", "normal");
  await expect(toolbar).toHaveCSS("padding-left", "10px");
  await expect(toolbar).toHaveCSS("padding-right", "10px");
  expect(await contentWidth(page, ".resource-toolbar .button")).toBeCloseTo(76.02, 1);
  await toolbar.hover();
  await expect(toolbar).toHaveCSS("background-color", surface ?? "");

  const activeToggle = page.locator(".dataset-view-toggle .button").first();
  const inactiveToggle = page.locator(".dataset-view-toggle .button").last();
  await expect(activeToggle).toHaveCSS("border-radius", "0px");
  await expect(activeToggle).toHaveCSS("display", "flex");
  await expect(activeToggle).toHaveCSS("font-weight", "700");
  await expect(activeToggle).toHaveCSS("gap", "5px");
  await expect(inactiveToggle).toHaveCSS("border-radius", "0px");
  await expect(inactiveToggle).toHaveCSS("display", "block");
  await expect(inactiveToggle).toHaveCSS("font-weight", "400");
  await expect(inactiveToggle).toHaveCSS("gap", "normal");
  await expect(inactiveToggle).toHaveCSS("justify-content", "normal");
  await expect(inactiveToggle).toHaveCSS("border-left-width", "0px");
  expect(await contentWidth(page, ".dataset-view-toggle .button:first-of-type")).toBeCloseTo(69.36, 1);
  // The Unicode table glyph falls back to the platform's available font. Its
  // advance differs by a fraction of a pixel between macOS and Linux, while
  // the control retains the same visible geometry.
  const tableToggleWidth = await contentWidth(page, ".dataset-view-toggle .button:last-of-type");
  expect(tableToggleWidth).toBeGreaterThanOrEqual(56.5);
  expect(tableToggleWidth).toBeLessThanOrEqual(56.75);
  await activeToggle.hover();
  await expect(activeToggle).toHaveCSS("background-color", subtle ?? "");
  await inactiveToggle.hover();
  await expect(inactiveToggle).toHaveCSS("background-color", surface ?? "");

  await expect(page.locator(".collector-card footer .button")).toHaveCSS("display", "none");
  const fieldAction = page.locator(".field-inspector footer .button");
  await expect(fieldAction).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
  await expect(fieldAction).toHaveCSS("border-radius", "0px");
  await expect(fieldAction).toHaveCSS("border-width", "0px");
  await expect(fieldAction).toHaveCSS("display", "block");
  await expect(fieldAction).toHaveCSS("font-weight", "400");
  await expect(fieldAction).toHaveCSS("gap", "normal");
  await expect(fieldAction).toHaveCSS("min-height", "0px");
  await expect(fieldAction).toHaveCSS("padding-bottom", "1px");
  await expect(fieldAction).toHaveCSS("padding-left", "6px");
  await expect(fieldAction).toHaveCSS("padding-right", "6px");
  await expect(fieldAction).toHaveCSS("padding-top", "1px");
  expect(await contentHeight(page, ".field-inspector footer .button")).toBeCloseTo(14, 1);
  expect(await contentWidth(page, ".field-inspector footer .button")).toBeCloseTo(64.81, 1);

  const patternAction = page.locator(".pattern-action");
  await expect(patternAction).toHaveCSS("background-color", "rgba(0, 0, 0, 0)");
  await expect(patternAction).toHaveCSS("border-radius", "0px");
  await expect(patternAction).toHaveCSS("border-width", "0px");
  await expect(patternAction).toHaveCSS("display", "flex");
  await expect(patternAction).toHaveCSS("font-weight", "400");
  await expect(patternAction).toHaveCSS("gap", "5px");
  await expect(patternAction).toHaveCSS("min-height", "28px");
  await expect(patternAction).toHaveCSS("padding-left", "8px");
  await expect(patternAction).toHaveCSS("padding-right", "8px");
  await patternAction.hover();
  await expect(patternAction).toHaveCSS("background-color", selection ?? "");
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

  test("migrated buttons retain their established coarse-pointer dimensions", async ({ page }) => {
    await mount(page, `
      <output class="history-action-notice"><span>History updated</span><button class="button button--notice-dismiss" type="button">×</button></output>
      <output class="reports-action-notice"><span>Report updated</span><button class="button button--notice-dismiss" type="button">×</button></output>
      <div class="resource-toolbar"><button class="button button--toolbar" type="button"><svg class="app-icon app-icon--sm"></svg> Refresh</button></div>
      <fieldset class="dataset-view-toggle"><button class="button button--toolbar active" type="button"><svg class="app-icon app-icon--sm"></svg> Cards</button><button class="button button--toolbar" type="button"><span aria-hidden="true">☷</span> Table</button></fieldset>
      <section class="field-inspector"><footer><span>Showing top values</span><button class="button button--link" type="button">New search</button></footer></section>
      <section class="pattern-table"><article><button class="button button--link pattern-action" type="button">View events</button></article></section>
    `, MOBILE_WIDTH);

    expect(await contentHeight(page, ".history-action-notice .button")).toBeCloseTo(44, 0);
    expect(await contentWidth(page, ".history-action-notice .button")).toBeCloseTo(44, 0);
    expect(await contentHeight(page, ".reports-action-notice .button")).toBeCloseTo(28, 0);
    expect(await contentWidth(page, ".reports-action-notice .button")).toBeCloseTo(28, 0);
    expect(await contentHeight(page, ".resource-toolbar .button")).toBeCloseTo(32, 0);
    expect(await contentHeight(page, ".dataset-view-toggle .button:first-of-type")).toBeCloseTo(44, 0);
    expect(await contentHeight(page, ".dataset-view-toggle .button:last-of-type")).toBeCloseTo(44, 0);
    expect(await contentHeight(page, ".field-inspector footer .button")).toBeCloseTo(14, 0);
    expect(await contentHeight(page, ".pattern-action")).toBeCloseTo(28, 0);
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

const statisticsListMember = `/api/v1/${"resource-segment/".repeat(24)}index`;
const statisticsListMarkup = `
<table class="statistics-table statistics-table--fixed">
  <tbody>
    <tr class="statistics-plain-row">
      <td class="statistics-cell--single-line">${statisticsListMember}</td>
    </tr>
    <tr class="statistics-list-row">
      <td class="statistics-cell--multivalue"><span class="statistics-multivalue-list"><span class="statistics-multivalue-item">${statisticsListMember}</span><span class="statistics-multivalue-item">${statisticsListMember}-two</span><button class="statistics-multivalue-more" type="button" aria-haspopup="dialog" aria-label="Show all 5 values for path">+3 more</button></span></td>
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
    const listCell = page.locator(".statistics-cell--multivalue");
    const singleLineCell = page.locator(".statistics-cell--single-line");
    await expect(listCell).toHaveCSS("max-width", "420px");
    await expect(listCell).toHaveCSS("overflow-x", "hidden");
    await expect(listCell).toHaveCSS("overflow-y", "hidden");
    await expect(singleLineCell).toHaveCSS("max-width", "420px");
    await expect(singleLineCell).toHaveCSS("overflow-x", "hidden");
    await expect(singleLineCell).toHaveCSS("overflow-y", "hidden");
    await expect(singleLineCell).toHaveCSS("text-overflow", "ellipsis");
    await expect(singleLineCell).toHaveCSS("white-space", "nowrap");
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

/**
 * Resolves `var(--name)` as a length, in computed pixels. A colour probe
 * cannot read a radius or a spacing step: `color: 6px` is invalid and falls
 * back to the inherited ink, so a scale token is read off the one property
 * that accepts a bare length.
 */
async function resolveLengthToken(page: Page, name: string): Promise<string> {
  return page.evaluate((token) => {
    const probe = document.createElement("div");
    probe.style.width = `var(${token})`;
    document.body.append(probe);
    const value = globalThis.getComputedStyle(probe).width;
    probe.remove();
    return value;
  }, name);
}

/** Resolves `var(--name)` as a font-family list, as the browser serialises it. */
async function resolveFontToken(page: Page, name: string): Promise<string> {
  return page.evaluate((token) => {
    const probe = document.createElement("span");
    probe.style.fontFamily = `var(${token})`;
    document.body.append(probe);
    const value = globalThis.getComputedStyle(probe).fontFamily;
    probe.remove();
    return value;
  }, name);
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
  const [red, green, blue] = srgbChannels(paint).map((scaled) => (
    scaled <= 0.040_45 ? scaled / 12.92 : ((scaled + 0.055) / 1.055) ** 2.4
  ));
  return 0.2126 * red + 0.7152 * green + 0.0722 * blue;
}

/**
 * The three sRGB channels of a computed paint, each scaled to 0..1.
 *
 * Chromium serialises a plain colour as `rgb(r, g, b)` / `rgba(r, g, b, a)`
 * with 0..255 channels, and a `color-mix()` result -- every translucency knob
 * consumer, even at the inert 100% -- as `color(srgb r g b[ / a])` with 0..1
 * floats. Any alpha is ignored: a ratio is taken on the paint's own hue, as the
 * token invariants prove contrast on the opaque hex.
 */
function srgbChannels(paint: string): number[] {
  const byte = /^rgba?\((\d+),\s*(\d+),\s*(\d+)/u.exec(paint);
  if (byte !== null) return [byte[1], byte[2], byte[3]].map((channel) => Number(channel) / 255);
  const float = /^color\(srgb\s+(-?[\d.]+)\s+(-?[\d.]+)\s+(-?[\d.]+)/u.exec(paint);
  if (float !== null) return [float[1], float[2], float[3]].map((channel) => Math.min(1, Math.max(0, Number(channel))));
  throw new Error(`unreadable paint ${paint}`);
}

/** Contrast ratio between two paints, in either order. */
function contrastRatio(first: string, second: string): number {
  const [darker, lighter] = [luminance(first), luminance(second)].toSorted((a, b) => a - b);
  return (lighter + 0.05) / (darker + 0.05);
}

/** The SPL editor with three inks, its completion menu and a toast: the surfaces a search author stares at longest. */
const EDITOR_FIXTURE = '<div class="spl-editor"><div class="editor-highlight">'
  + '<span class="spl-command">stats</span> <span class="spl-field">host</span> <span class="spl-string">"web"</span>'
  + "</div></div>"
  + '<div class="completion-menu"><div class="completion-title"><span>Commands</span></div>'
  + '<button type="button" data-highlighted="true"><code>stats</code><span>Aggregate</span></button></div>'
  + '<div class="toast"><span>i</span><strong>Saved</strong></div>';

/** The paints a theme or palette block must move on `EDITOR_FIXTURE`. */
const EDITOR_PAINT_PAIRS: ReadonlyArray<readonly [string, "backgroundColor" | "color"]> = [
  [".spl-editor", "backgroundColor"],
  [".editor-highlight", "color"],
  [".completion-menu", "backgroundColor"],
  [".completion-menu > button", "backgroundColor"],
  [".completion-menu code", "color"],
  [".toast", "backgroundColor"],
  [".toast", "color"],
];

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
  // dark ground. The palette contracts ask the same of every palette on the
  // same fixture.
  test("the editor, completion menu and toast repaint in the dark theme with AA syntax inks", async ({ page }) => {
    await mount(page, EDITOR_FIXTURE, DESKTOP_WIDTH);

    const pairs = EDITOR_PAINT_PAIRS;
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

function composerMarkup(lines: number, completionOpen = false, fieldOptions = 1): string {
  const query = Array.from({ length: lines }, (_, index) => (index === 0 ? "index=main" : `| stage${index}`)).join("\n");
  const gutter = Array.from({ length: Math.max(2, lines) }, (_, index) => `<span>${index + 1}</span>`).join("");
  const fields = Array.from({ length: fieldOptions }, (_, index) => (
    `<button class="completion-option" id="spl-completion-${index + 1}" role="option" aria-selected="false" type="button"><code>field${index}</code><span>Field</span><kbd></kbd></button>`
  )).join("");
  const menu = completionOpen
    ? `<div class="completion-menu" id="spl-completion-list" role="listbox" aria-label="SPL suggestions">
        <div class="completion-group" role="group" aria-labelledby="spl-completion-group-command">
          <div class="completion-title" id="spl-completion-group-command"><span>Commands</span><small>Enter a pipeline stage</small></div>
          <button class="completion-option" id="spl-completion-0" role="option" aria-selected="true" type="button"><code>stats</code><span>Aggregate</span><kbd>↵</kbd></button>
        </div>
        <div class="completion-group" role="group" aria-labelledby="spl-completion-group-field">
          <div class="completion-title" id="spl-completion-group-field"><span>Fields</span><small>Fields seen in results</small></div>
          ${fields}
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

// Palette probes: every palette x mode, on one page that carries the shell,
// the editor, a table, every button and badge, the modal family, the drawer
// and the toast (integration/style-contracts/palette-fixture.ts). The token
// invariants prove contrast on the hex each token resolves to; these read the
// same promises off the live cascade, where a rule that reads the wrong role,
// a knob that leaks past its consumer, or a palette light block that outranks
// base dark would show up as a painted pixel rather than a token value.
type ThemeMode = "dark" | "light";

const THEME_MODES: readonly ThemeMode[] = ["light", "dark"];

/** Every palette x mode corner the cascade can land in. */
const PALETTE_SCOPES: ReadonlyArray<{ mode: ThemeMode; palette: Palette }> = PALETTES.flatMap((palette) => (
  THEME_MODES.map((mode) => ({ mode, palette }))
));

/** The contrast floor a palette promises: its `PALETTE_CONTRAST_FLOOR` entry, else AA. */
function contrastFloorOf(palette: Palette): number {
  return Object.hasOwn(PALETTE_CONTRAST_FLOOR, palette) ? PALETTE_CONTRAST_FLOOR[palette]! : AA_CONTRAST;
}

/** WCAG 2.2 non-text contrast: a focus indicator against what surrounds it. */
const NON_TEXT_CONTRAST = 3;

/** The alpha of a computed paint: `rgba(... , a)`, `color(srgb ... / a)`, else 1. */
function paintAlpha(paint: string): number {
  const byte = /^rgba\(\d+,\s*\d+,\s*\d+,\s*([\d.]+)\)$/u.exec(paint);
  if (byte !== null) return Number(byte[1]);
  const float = /^color\(srgb\s+[-\d.]+\s+[-\d.]+\s+[-\d.]+\s*\/\s*([\d.]+)\)$/u.exec(paint);
  if (float !== null) return Number(float[1]);
  return 1;
}

/** Two paints with the same hue, alpha aside, within a channel's rounding. */
function sameHue(first: string, second: string): boolean {
  const left = srgbChannels(first);
  const right = srgbChannels(second);
  return left.every((channel, index) => Math.abs(channel - (right[index] ?? Number.NaN)) < 1 / 255);
}

async function mountShell(page: Page, width: number): Promise<void> {
  await page.setViewportSize({ height: 900, width });
  await page.setContent(SHELL_FIXTURE);
  await addApplicationStyles(page);
  // The entry animations of the menus, the modal and the toast would leave
  // opacity mid-flight; the skeleton shimmer and the pulse never end.
  await page.evaluate(() => {
    for (const animation of document.getAnimations()) {
      const end = animation.effect?.getComputedTiming().endTime;
      if (end === undefined || end === Number.POSITIVE_INFINITY) animation.cancel();
      else animation.finish();
    }
  });
}

/** Writes the two attributes the boot script writes, exactly as it writes them. */
async function applyScope(page: Page, palette: Palette, mode: ThemeMode): Promise<void> {
  await page.evaluate(([nextPalette, nextMode]) => {
    document.documentElement.setAttribute("data-palette", nextPalette);
    document.documentElement.setAttribute("data-theme", nextMode);
  }, [palette, mode] as const);
}

/**
 * Runs `body` for each item in turn. The palette probes rewrite the root's
 * attributes and then read the page, so the scopes have to be visited one
 * after another on the one page rather than raced through `Promise.all`.
 */
async function sequentially<T>(items: readonly T[], body: (item: T) => Promise<void>): Promise<void> {
  const [head, ...rest] = items;
  if (items.length === 0) return;
  await body(head as T);
  await sequentially(rest, body);
}

/** `sequentially` over every palette x mode, with the scope applied before each visit. */
async function inEveryScope(page: Page, body: (scope: { mode: ThemeMode; palette: Palette }) => Promise<void>): Promise<void> {
  await sequentially(PALETTE_SCOPES, async (scope) => {
    await applyScope(page, scope.palette, scope.mode);
    await body(scope);
  });
}

/**
 * The ink of each element and the ground the eye meets it on: every
 * background from the body down to the element itself composited in order,
 * so a 9% wash over a bar, or a glass pane at 84% over the canvas, reads as
 * the colour it actually paints rather than as its own translucent value.
 *
 * With `selectors`, one entry per selector in order; without, every element
 * under the shell that carries text of its own (a text node, or a control).
 */
async function inkedElements(
  page: Page,
  selectors: readonly string[] | null = null,
): Promise<Array<{ ground: string; ink: string; label: string }>> {
  return page.evaluate((targets) => {
    // Chromium hands back `rgb()` / `rgba()` with byte channels, or the
    // `color(srgb …)` form with unit floats for a `color-mix()` paint.
    const byteForm = /^rgba?\((\d+),\s*(\d+),\s*(\d+)(?:,\s*([\d.]+))?\)$/u;
    const floatForm = /^color\(srgb\s+([-\d.]+)\s+([-\d.]+)\s+([-\d.]+)(?:\s*\/\s*([\d.]+))?\)$/u;
    function groundBehind(element: Element): string {
      const chain: Element[] = [];
      for (let node: Element | null = element; node !== null; node = node.parentElement) chain.unshift(node);
      let composite: [number, number, number] = [1, 1, 1];
      for (const node of chain) {
        const paint = globalThis.getComputedStyle(node).backgroundColor;
        const byte = byteForm.exec(paint);
        const float = floatForm.exec(paint);
        const scale = byte === null ? 1 : 255;
        const parsed = byte ?? float;
        if (parsed === null) continue;
        const alpha = parsed[4] === undefined ? 1 : Number(parsed[4]);
        if (alpha === 0) continue;
        const [red, green, blue] = [parsed[1], parsed[2], parsed[3]].map((channel) => Number(channel) / scale);
        composite = [
          alpha * red! + (1 - alpha) * composite[0],
          alpha * green! + (1 - alpha) * composite[1],
          alpha * blue! + (1 - alpha) * composite[2],
        ];
      }
      return `rgb(${composite.map((channel) => Math.round(channel * 255)).join(", ")})`;
    }
    if (targets !== null) {
      return targets.map((selector) => {
        const element = document.querySelector(selector);
        if (element === null) throw new Error(`fixture is missing ${selector}`);
        return { ground: groundBehind(element), ink: globalThis.getComputedStyle(element).color, label: selector };
      });
    }
    const found: Array<{ ground: string; ink: string; label: string }> = [];
    for (const element of document.querySelectorAll(".suite-shell *")) {
      const hasText = [...element.childNodes].some((node) => (
        node.nodeType === Node.TEXT_NODE && (node.textContent ?? "").trim() !== ""
      )) || element instanceof HTMLInputElement || element instanceof HTMLTextAreaElement;
      if (!hasText) continue;
      const style = globalThis.getComputedStyle(element);
      if (style.display === "none" || style.visibility === "hidden") continue;
      // The element and up to two ancestors, as `tag.class` steps.
      const trail: Element[] = [element];
      for (let parent = element.parentElement; parent !== null && trail.length < 3; parent = parent.parentElement) {
        trail.unshift(parent);
      }
      const label = trail
        .map((node) => `${node.tagName.toLowerCase()}${[...node.classList].map((name) => `.${name}`).join("")}`)
        .join(" > ");
      found.push({ ground: groundBehind(element), ink: style.color, label });
    }
    return found;
  }, selectors);
}

/**
 * The surfaces whose ink the AA sweep holds to the palette floor: the text a
 * user reads, not the decorative or deliberately faint. `--fg-faint` is
 * placeholder ink and a disabled button is dimmed by opacity, so neither is
 * a promise the token layer makes; everything here reads a role whose comment
 * names the ground it sits on.
 */
const READABLE_TEXT: readonly string[] = [
  ".body-copy",
  ".body-copy a",
  ".body-copy code",
  ".table th",
  ".table td",
  ".table a",
  ".button-row .button:not([aria-disabled])",
  ".button-row .button--primary",
  ".button-row .button--secondary",
  ".button-row .button--danger",
  ".button-row .button--ghost",
  ".badge",
  ".badge--success",
  ".badge--info",
  ".badge--warning",
  ".badge--error",
  ".badge--neutral",
  ".activity-count",
  ".suite-app-identity > span",
  ".suite-app-icon",
  ".user-summary > span",
  ".drawer .suite-user-avatar",
  ".suite-app-switcher",
  ".suite-primary-nav a",
  ".suite-primary-nav a.active",
  ".floating-menu button strong",
  ".floating-menu button.selected strong",
  ".suite-popover > a strong",
  ".completion-option code",
  '.completion-option[aria-selected="true"] code',
  ".time-range-button",
  ".time-picker-nav button",
  ".preset-grid button.selected",
  ".spl-command",
  ".spl-field",
  ".spl-string",
  ".spl-pipe",
  ".spl-function",
  ".spl-boolean",
  ".spl-operator",
  ".modal-header h2",
  ".modal-body",
  ".modal-footer .button--primary",
  ".drawer > a",
  ".drawer > a.active",
  ".drawer > header strong",
  ".toast strong",
  ".toast-success strong",
  ".form-stack > label > span",
  ".form-stack input",
  ".admin-sidebar > button.active strong",
  ".admin-sidebar > button.active small",
  ".admin-sidebar > button:not(.active) small",
  ".appearance-palette-options label.is-selected strong",
  ".appearance-palette-options label.is-selected small",
  ".appearance-palette-options label:not(.is-selected) small",
  ".knowledge-manager__readonly",
];

/**
 * The readable surfaces painted from the status ramp rather than the
 * foreground and ground roles. Status hues stay classic in every palette so a
 * state keeps its meaning, and graphite's 7:1 promise is made on its
 * monochrome text (the mandated pairs the token invariants hold), so these
 * are held to AA in every palette rather than to the palette's own floor.
 */
const STATE_COLOURED_TEXT: ReadonlySet<string> = new Set([
  ".button-row .button--danger",
  ".badge--info",
  ".badge--warning",
  ".badge--error",
]);

/**
 * The readable pairs the base pair itself renders under AA, in
 * `READABLE_TEXT` order. Empty: every surface in `READABLE_TEXT` clears AA in
 * classic light and dark, so each palette is held to its own floor on all of
 * them. The ledger stays so that a change to classic which drops a pair under
 * AA fails here by name rather than passing as an inherited shortfall, and
 * so that a deliberate regression has one place to be recorded and reviewed.
 *
 * The nine pairs that used to sit here were retired by retuning the tokens
 * behind them rather than the rules that read them: the link ink deepened one
 * step so it clears the striped row's `--bg-subtle`; the info and error
 * badges paint their `-strong` ink, the one the ramp already provides for
 * text on its own wash; and in dark the danger button's `--status-error`,
 * the accent wash `--accent-soft` and the selection wash `--selection` each
 * moved one primitive step so the ink the design lays on them reads.
 */
const CLASSIC_CONTRAST_SHORTFALLS: Readonly<Record<ThemeMode, readonly string[]>> = {
  dark: [],
  light: [],
};

test.describe("palette contracts", () => {
  test("no element in the shell paints its ink in its own ground, in any palette or mode", async ({ page }) => {
    await mountShell(page, DESKTOP_WIDTH);
    await inEveryScope(page, async ({ mode, palette }) => {
      const inked = await inkedElements(page);
      expect(inked.length, `${palette} ${mode}: no inked element found, so this proves nothing`).toBeGreaterThan(80);
      const invisible = inked
        .filter(({ ground, ink }) => paintAlpha(ink) > 0 && sameHue(ink, ground))
        .map(({ ground, ink, label }) => `${label}: ${ink} on ${ground}`);
      expect(invisible, `${palette} ${mode}: text painted in the colour of the ground behind it`).toEqual([]);
    });
  });

  test("readable text clears its palette's contrast floor on the live page, in every palette and mode", async ({ page }) => {
    // The token invariants hold the mandated pairs to the floor on the hex;
    // this holds the rendered ink of every surface a user reads to the same
    // floor, on whatever ground the cascade actually put behind it -- which
    // is how graphite's 7:1 is proved on the page rather than in the file.
    //
    // Classic is measured first and its shortfalls are a ledger, not a
    // floor: a pair recorded there sits under AA in the base pair, and a
    // palette that leaves it alone inherits the same ratio, so a palette may
    // not render it lower than classic does. Every other surface -- today,
    // every surface -- has to clear the palette's own floor, and a new
    // classic shortfall, or one that has been fixed, fails here until the
    // ledger is updated.
    await mountShell(page, MOBILE_WIDTH - 60);
    const classic = new Map<ThemeMode, Map<string, number>>();
    await inEveryScope(page, async ({ mode, palette }) => {
      const painted = await inkedElements(page, READABLE_TEXT);
      const ratios = new Map(painted.map(({ ground, ink, label }) => [label, contrastRatio(ink, ground)]));
      if (palette === "classic") {
        classic.set(mode, ratios);
        const short = [...ratios].filter(([, ratio]) => ratio < AA_CONTRAST).map(([label]) => label);
        expect(short, `classic ${mode}: the readable pairs under AA are not the ones the ledger records`)
          .toEqual(CLASSIC_CONTRAST_SHORTFALLS[mode]);
        return;
      }
      const floor = contrastFloorOf(palette);
      const short = [...ratios]
        .filter(([label, ratio]) => {
          const inherited = classic.get(mode)!.get(label)!;
          const required = STATE_COLOURED_TEXT.has(label) ? AA_CONTRAST : floor;
          return ratio < (inherited < AA_CONTRAST ? inherited - 0.005 : required);
        })
        .map(([label, ratio]) => `${label} is ${ratio.toFixed(2)}:1`);
      expect(short, `${palette} ${mode}: text below ${floor}:1 on the ground the cascade painted behind it`).toEqual([]);
    });
  });

  test("a keyboard-focused primary button shows a ring that clears 3:1 against its surround, in every palette and mode", async ({ page }) => {
    await mountShell(page, DESKTOP_WIDTH);
    const control = page.locator(".button-row .button--primary");
    await inEveryScope(page, async ({ mode, palette }) => {
      await control.evaluate((element: HTMLElement) => element.focus());
      await page.keyboard.press("Shift+Tab");
      await page.keyboard.press("Tab");
      await expect(control).toBeFocused();
      const ring = await control.evaluate((element) => {
        const style = globalThis.getComputedStyle(element);
        const surround = globalThis.getComputedStyle(element.parentElement as HTMLElement).backgroundColor;
        const canvas = globalThis.getComputedStyle(document.body).backgroundColor;
        return {
          colour: style.outlineColor,
          style: style.outlineStyle,
          surround: surround === "rgba(0, 0, 0, 0)" ? canvas : surround,
          width: Number.parseFloat(style.outlineWidth),
        };
      });
      expect(ring.style, `${palette} ${mode}: outline style`).toEqual("solid");
      expect(ring.width, `${palette} ${mode}: outline width`).toBeGreaterThanOrEqual(2);
      expect(paintAlpha(ring.colour), `${palette} ${mode}: the ring is translucent`).toEqual(1);
      const ratio = contrastRatio(ring.colour, ring.surround);
      expect(
        ratio,
        `${palette} ${mode}: focus ring ${ring.colour} on ${ring.surround} is ${ratio.toFixed(2)}:1`,
      ).toBeGreaterThanOrEqual(NON_TEXT_CONTRAST);
    });
  });

  test("the selected completion option draws a ring that clears 3:1 against the selection wash, in every palette and mode", async ({ page }) => {
    // The selection wash sits within 1.3:1 of the surface in every dark
    // scope, so the keyboard-selected option owes a cue that is not hue
    // alone: an inset ring in the focused-edge colour, absent from the
    // options around it, that clears non-text contrast on the wash itself.
    await mountShell(page, DESKTOP_WIDTH);
    const selected = page.locator('.completion-option[aria-selected="true"]');
    const unselected = page.locator('.completion-option[aria-selected="false"]');
    await inEveryScope(page, async ({ mode, palette }) => {
      await expect(unselected, `${palette} ${mode}: an unselected option carries the ring`).toHaveCSS("box-shadow", "none");
      const ring = await selected.evaluate((element) => {
        const style = globalThis.getComputedStyle(element);
        return { ground: style.backgroundColor, shadow: style.boxShadow };
      });
      const inset = /^(rgba?\([^)]+\)) 0px 0px 0px 1px inset$/u.exec(ring.shadow);
      expect(inset, `${palette} ${mode}: the selected option's box-shadow is ${ring.shadow}, not a 1px inset ring`).not.toBeNull();
      const colour = inset![1]!;
      expect(paintAlpha(colour), `${palette} ${mode}: the ring is translucent`).toEqual(1);
      expect(paintAlpha(ring.ground), `${palette} ${mode}: the selection wash is translucent`).toEqual(1);
      const ratio = contrastRatio(colour, ring.ground);
      expect(
        ratio,
        `${palette} ${mode}: selection ring ${colour} on ${ring.ground} is ${ratio.toFixed(2)}:1`,
      ).toBeGreaterThanOrEqual(NON_TEXT_CONTRAST);
    });
  });

  test("the two chrome bars are distinct from each other and stand off the canvas, in every palette and mode", async ({ page }) => {
    await mountShell(page, DESKTOP_WIDTH);
    await inEveryScope(page, async ({ mode, palette }) => {
      const [productBar, appBar, canvas] = await paints(page, [
        [".suite-product-bar", "backgroundColor"],
        [".suite-app-bar", "backgroundColor"],
        ["body", "backgroundColor"],
      ]);
      expect(sameHue(productBar!, appBar!), `${palette} ${mode}: both bars are ${productBar}`).toBe(false);
      expect(sameHue(appBar!, canvas!), `${palette} ${mode}: the app bar is the canvas, ${canvas}`).toBe(false);
      // Classic dark paints the product bar in the canvas's own deepest
      // neutral and lets the app bar draw the edge; a palette's dark block
      // may follow that arrangement, so only the light block owes a
      // product bar that stands off the page.
      if (mode === "light") {
        expect(sameHue(productBar!, canvas!), `${palette} light: the product bar is the canvas, ${canvas}`).toBe(false);
      }
    });
  });

  test("glass alone makes the raised surfaces translucent, and each one's opaque token still clears the text floor", async ({ page }) => {
    // The drawer's paint lives in the 760px fold, so the page is mounted
    // below it; every other consumer paints the same at any width.
    await mountShell(page, MOBILE_WIDTH - 60);
    const grounds: ReadonlyArray<readonly [string, string, string, boolean]> = [
      // consumer, ground token, ink token, takes the backdrop filter
      [".completion-menu", "--bg-surface", "--fg-text", true],
      [".drawer", "--bg-raised", "--fg-text", true],
      [".floating-menu", "--bg-surface", "--fg-text", true],
      [".modal-card", "--bg-surface", "--fg-text", true],
      [".suite-app-bar", "--chrome-appbar", "--chrome-fg", false],
      [".suite-product-bar", "--chrome-bar", "--chrome-fg", false],
      [".time-popover", "--bg-surface", "--fg-text", true],
      [".toast", "--bg-inverse", "--fg-inverse", true],
    ];
    expect(grounds.map(([consumer]) => consumer)).toEqual([...KNOB_CONSUMERS]);
    await inEveryScope(page, async ({ mode, palette }) => {
      const floor = contrastFloorOf(palette);
      const tokens = await resolveTokens(page, grounds.flatMap(([, ground, ink]) => [ground, ink]));
      const painted = await page.evaluate((consumers) => consumers.map((selector) => {
        const element = document.querySelector(selector);
        if (element === null) throw new Error(`fixture is missing ${selector}`);
        const style = globalThis.getComputedStyle(element);
        return { background: style.backgroundColor, filter: style.backdropFilter };
      }), grounds.map(([consumer]) => consumer));
      for (const [index, [consumer, ground, ink, filtered]] of grounds.entries()) {
        const { background, filter } = painted[index]!;
        const opaque = tokens[index * 2]!;
        const inkPaint = tokens[index * 2 + 1]!;
        const site = `${palette} ${mode} ${consumer}`;
        // The paint is the token's own hue whatever the alpha: a knob turns
        // opacity, never colour, so a consumer reading a different role
        // than its opaque fallback would show here.
        expect(sameHue(background, opaque), `${site}: paints ${background}, not its token ${opaque}`).toBe(true);
        if (palette === "glass") {
          expect(paintAlpha(background), `${site}: opaque under glass`).toBeLessThan(1);
          expect(paintAlpha(background), `${site}: below the 80% translucency floor`).toBeGreaterThanOrEqual(0.8);
          expect(filter !== "none", `${site}: backdrop-filter ${filter}`).toBe(filtered);
        } else {
          expect(paintAlpha(background), `${site}: translucent outside glass`).toEqual(1);
          expect(filter, `${site}: a backdrop filter outside glass`).toEqual("none");
        }
        const ratio = contrastRatio(inkPaint, opaque);
        expect(ratio, `${site}: ${ink} on the opaque ${ground} is ${ratio.toFixed(2)}:1`).toBeGreaterThanOrEqual(floor);
      }
    });
  });

  test("terminal alone sets the mono face on body text and squares every corner", async ({ page }) => {
    await mountShell(page, MOBILE_WIDTH - 60);
    // Each corner here is a full radius in classic at this width; the
    // composer's two controls square one edge on purpose to butt the editor,
    // and the modal card is the square phone sheet below 760px, so neither
    // is the shape terminal is asked to flatten.
    const cornered = [
      ".button-row .button",
      ".badge",
      ".suite-app-icon",
      ".activity-count",
      ".form-stack input",
    ];
    await inEveryScope(page, async ({ mode, palette }) => {
      const [mono] = await page.evaluate(() => {
        const probe = document.createElement("span");
        probe.style.fontFamily = "var(--font-mono)";
        document.body.append(probe);
        const family = globalThis.getComputedStyle(probe).fontFamily;
        probe.remove();
        return [family];
      });
      const [bodyFamily, tdFamily, buttonFamily, inputFamily] = await page.evaluate(() => (
        ["body", ".table td", ".button-row .button", ".form-stack input"].map((selector) => (
          globalThis.getComputedStyle(document.querySelector(selector) as Element).fontFamily
        ))
      ));
      const radii = await page.evaluate((selectors) => selectors.map((selector) => (
        globalThis.getComputedStyle(document.querySelector(selector) as Element).borderTopLeftRadius
      )), cornered);
      if (palette === "terminal") {
        expect(bodyFamily, `${mode}: body face`).toEqual(mono);
        expect(bodyFamily).toContain("monospace");
        expect(tdFamily, `${mode}: table cell face`).toEqual(mono);
        expect(buttonFamily, `${mode}: button face`).toEqual(mono);
        expect(inputFamily, `${mode}: input face`).toEqual(mono);
        expect(radii, `${mode}: a rounded corner survives under terminal`).toEqual(cornered.map(() => "0px"));
      } else {
        expect(bodyFamily, `${palette} ${mode}: body text in the mono face`).not.toEqual(mono);
        expect(bodyFamily).not.toContain("monospace");
        expect(
          cornered.filter((_, index) => radii[index] === "0px"),
          `${palette} ${mode}: a corner is square outside terminal (${radii.join(", ")})`,
        ).toEqual([]);
      }
    });
  });

  test("a palette light restatement never outranks the base dark block", async ({ page }) => {
    // `:root:where([data-palette])` keeps the light block at the base
    // block's specificity so source order alone lets it win in light and
    // base dark still beats it in dark. A token the palette restates only in
    // its light block therefore renders in dark exactly as classic dark does;
    // a light block written without `:where()` would leak its light grounds
    // under every dark page.
    await mount(page, "", DESKTOP_WIDTH);
    const lightOnly = LIGHT_ONLY_RESTATEMENTS;
    const tokens = [...new Set(lightOnly.flatMap(([, names]) => names))];
    await applyScope(page, "classic", "dark");
    const classicDark = await resolveTokens(page, tokens);
    await applyScope(page, "classic", "light");
    const classicLight = await resolveTokens(page, tokens);
    await sequentially(lightOnly, async ([palette, names]) => {
      await applyScope(page, palette, "light");
      const light = await resolveTokens(page, names);
      await applyScope(page, palette, "dark");
      const dark = await resolveTokens(page, names);
      for (const [index, token] of names.entries()) {
        const position = tokens.indexOf(token);
        expect(
          light[index],
          `${palette} light: ${token} is unchanged from classic, so this proves nothing`,
        ).not.toEqual(classicLight[position]);
        expect(dark[index], `${palette} dark: ${token} leaked from the palette's light block`).toEqual(classicDark[position]);
      }
    });
  });

  test("data-palette=\"classic\" selects nothing, in both modes", async ({ page }) => {
    // The boot script writes `data-palette="classic"` explicitly on every
    // load. Classic owns no palette file, so the attribute must select no
    // rule at all: the four literals the dark-theme contract pins, every
    // semantic colour, the radii, the body face and `color-scheme` read the
    // same with the attribute as without it, in light and in dark.
    await mount(page, "", DESKTOP_WIDTH);
    const pinned = ["--bg-canvas", "--fg-text", "--border", "--chrome-bar"];
    await sequentially(THEME_MODES, async (mode) => {
      await page.evaluate((next) => {
        document.documentElement.removeAttribute("data-palette");
        document.documentElement.setAttribute("data-theme", next);
      }, mode);
      const bare = await snapshotScope(page);
      expect(await colorScheme(page), `${mode}: color-scheme with no palette attribute`).toEqual(mode);
      if (mode === "light") {
        expect(await resolveTokens(page, pinned)).toEqual([
          "rgb(246, 246, 244)",
          "rgb(40, 52, 61)",
          "rgb(207, 212, 215)",
          "rgb(30, 37, 43)",
        ]);
      }
      await applyScope(page, "classic", mode);
      expect(await snapshotScope(page), `${mode}: data-palette="classic" changed a token`).toEqual(bare);
      expect(await colorScheme(page), `${mode}: color-scheme under data-palette="classic"`).toEqual(mode);
    });
  });

  test("every palette moves the accent and the chrome bar, follows the theme's color-scheme, and lets go cleanly", async ({ page }) => {
    await mount(page, "", DESKTOP_WIDTH);
    const identity = ["--accent", "--chrome-bar"] as const;
    const classic = new Map<ThemeMode, { identity: string[]; scope: ScopeSnapshot }>();
    await sequentially(THEME_MODES, async (mode) => {
      await applyScope(page, "classic", mode);
      classic.set(mode, { identity: await resolveTokens(page, identity), scope: await snapshotScope(page) });
    });
    await sequentially(PALETTES.filter((palette) => palette !== "classic"), async (palette) => {
      await applyScope(page, palette, "light");
      const light = await resolveTokens(page, identity);
      expect(await colorScheme(page), `${palette} light: color-scheme`).toEqual("light");
      for (const [index, token] of identity.entries()) {
        expect(light[index], `${palette} light: ${token} is classic's`).not.toEqual(classic.get("light")!.identity[index]);
      }

      await applyScope(page, palette, "dark");
      const dark = await resolveTokens(page, identity);
      expect(await colorScheme(page), `${palette} dark: color-scheme`).toEqual("dark");
      for (const [index, token] of identity.entries()) {
        const classicDark = classic.get("dark")!.identity[index];
        if (token === "--chrome-bar" && CHROME_STAYS_CLASSIC_IN_DARK.has(palette)) {
          expect(dark[index], `${palette} dark: ${token} no longer keeps classic dark's chrome; update the ledger`)
            .toEqual(classicDark);
          continue;
        }
        expect(dark[index], `${palette} dark: ${token} is classic dark's`).not.toEqual(classicDark);
        expect(dark[index], `${palette} dark: ${token} is the palette's own light value`).not.toEqual(light[index]);
      }

      // Taking the attribute away, in either mode, leaves classic exactly:
      // nothing a palette file declares survives outside its selector.
      await sequentially(THEME_MODES, async (mode) => {
        await applyScope(page, palette, mode);
        await page.evaluate(() => document.documentElement.removeAttribute("data-palette"));
        expect(await snapshotScope(page), `${palette} ${mode}: removing data-palette does not restore classic`)
          .toEqual(classic.get(mode)!.scope);
      });
    });
  });

  test("the editor, completion menu and toast repaint under every palette and mode, with syntax inks at the palette floor", async ({ page }) => {
    await mount(page, EDITOR_FIXTURE, DESKTOP_WIDTH);
    const syntax = ["--syntax-pipe", "--syntax-command", "--syntax-function", "--syntax-field", "--syntax-string", "--syntax-literal"];
    const seen = new Map<string, string[]>();
    await inEveryScope(page, async ({ mode, palette }) => {
      const painted = await paints(page, EDITOR_PAINT_PAIRS);
      seen.set(`${palette} ${mode}`, painted);
      for (const [index, [selector, property]] of EDITOR_PAINT_PAIRS.entries()) {
        expect(paintAlpha(painted[index]!), `${palette} ${mode}: ${selector} ${property} is transparent`).toBeGreaterThan(0);
      }
      // The editor ground is opaque in every palette: `.spl-editor` takes no
      // translucency knob, so the ratio is taken on the paint itself.
      const editorGround = painted[0]!;
      expect(paintAlpha(editorGround), `${palette} ${mode}: the editor ground is translucent`).toEqual(1);
      const floor = contrastFloorOf(palette);
      const inks = await resolveTokens(page, syntax);
      const short = syntax
        .map((token, index) => ({ ratio: contrastRatio(inks[index]!, editorGround), token }))
        .filter((ink) => ink.ratio < floor)
        .map((ink) => `${ink.token} is ${ink.ratio.toFixed(2)}:1`);
      expect(short, `${palette} ${mode}: syntax inks below ${floor}:1 on the editor ground ${editorGround}`).toEqual([]);
      const highlight = contrastRatio(painted[1]!, editorGround);
      expect(highlight, `${palette} ${mode}: the editor text is ${highlight.toFixed(2)}:1 on its ground`).toBeGreaterThanOrEqual(floor);
    });
    // Every palette's dark chain passes through base dark, so each of the
    // seven paints has to move between the palette's light and its dark. A
    // palette's light may leave these three surfaces on classic's paints
    // (ocean does: a white editor with classic inks under a cool canvas),
    // which is the "restate only what changes" rule rather than a defect.
    for (const palette of PALETTES) {
      const light = seen.get(`${palette} light`)!;
      const dark = seen.get(`${palette} dark`)!;
      for (const [index, [selector, property]] of EDITOR_PAINT_PAIRS.entries()) {
        expect(dark[index], `${palette}: ${selector} ${property} is unchanged between light and dark`).not.toEqual(light[index]);
      }
    }
  });

  test("the boot script, run as the inline head script it ships as, paints the cached palette and theme in a real browser", async ({ page }) => {
    // lib/theme-preference.test.ts binds fakes over `localStorage`,
    // `matchMedia` and `document` under node. This runs the same string as
    // app/layout.tsx does -- a classic inline script in `<head>`, before any
    // stylesheet -- against the browser's own storage and media query, from
    // an origin that has storage, so the pre-paint path is proved where it
    // runs rather than only where it is unit tested.
    const origin = "http://boot-script.localhost";
    await page.route(`${origin}/**`, (route) => route.fulfill({
      body: `<!doctype html><html><head><script>${THEME_BOOT_SCRIPT}</script></head><body></body></html>`,
      contentType: "text/html",
    }));
    const cached: ReadonlyArray<string | null> = [...PALETTES, "sepia", "", null];
    const stored: ReadonlyArray<string | null> = ["light", "dark", "system", null];
    const cases = cached.flatMap((palette) => stored.flatMap((theme) => (
      [true, false].map((prefersDark) => ({ palette, prefersDark, theme }))
    )));
    await page.goto(`${origin}/`);
    await sequentially(cases, async ({ palette, prefersDark, theme }) => {
      await page.emulateMedia({ colorScheme: prefersDark ? "dark" : "light" });
      await page.evaluate(([nextTheme, nextPalette]) => {
        localStorage.clear();
        if (nextTheme !== null) localStorage.setItem("open-splunk.theme", nextTheme);
        if (nextPalette !== null) localStorage.setItem("open-splunk.palette", nextPalette);
      }, [theme, palette] as const);
      await page.goto(`${origin}/`);
      const attributes = await page.evaluate(() => [
        document.documentElement.getAttribute("data-theme"),
        document.documentElement.getAttribute("data-palette"),
      ]);
      const label = `theme ${JSON.stringify(theme)}, palette ${JSON.stringify(palette)}, prefers ${prefersDark ? "dark" : "light"}`;
      expect(attributes, label).toEqual([resolveTheme(theme, prefersDark), resolvePalette(palette)]);
    });

    // And the attributes the script wrote are the ones the stylesheets read:
    // with the cascade added after the script, as the layout orders them,
    // a cached palette paints exactly what writing its attribute paints, and
    // an unknown cache paints classic.
    const identity = ["--accent", "--chrome-bar"];
    const booted = new Map<string, string[]>();
    await sequentially([...PALETTES, "sepia"], async (palette) => {
      await page.evaluate((nextPalette) => {
        localStorage.clear();
        localStorage.setItem("open-splunk.palette", nextPalette);
        localStorage.setItem("open-splunk.theme", "light");
      }, palette);
      await page.goto(`${origin}/`);
      await addApplicationStyles(page);
      booted.set(palette, await resolveTokens(page, identity));
    });
    // The routed page is compared with a page the script never ran on: the
    // fixture page every other palette contract reads, with the attributes
    // written by hand. Reading the routed page twice would compare it with
    // itself, since writing the attributes the script already wrote changes
    // nothing there.
    await mount(page, "", DESKTOP_WIDTH);
    await sequentially([...PALETTES, "sepia"], async (palette) => {
      await applyScope(page, resolvePalette(palette), "light");
      expect(booted.get(palette), `cached ${palette}: the boot script paints differently from its attribute on the fixture page`)
        .toEqual(await resolveTokens(page, identity));
    });
    expect(booted.get("sepia"), "an unknown cached palette paints classic").toEqual(booted.get("classic"));
    for (const palette of PALETTES.filter((name) => name !== "classic")) {
      expect(booted.get(palette), `cached ${palette} booted into classic's accent and chrome`).not.toEqual(booted.get("classic"));
    }
  });
});

/**
 * Every value a palette file may restate, read off the live cascade: each
 * semantic colour, the three radii and the body face. Two snapshots that are
 * equal mean the cascade has landed in the same place, whatever attributes
 * got it there.
 */
type ScopeSnapshot = { colours: string[]; face: string; radii: string[] };

async function snapshotScope(page: Page): Promise<ScopeSnapshot> {
  const radiusTokens = ["--radius-sm", "--radius-md", "--radius-lg"];
  const radii: string[] = [];
  await sequentially(radiusTokens, async (token) => {
    radii.push(await resolveLengthToken(page, token));
  });
  return {
    colours: await resolveTokens(page, SEMANTIC_COLOUR_TOKENS),
    face: await resolveFontToken(page, "--font-sans"),
    radii,
  };
}

/**
 * The tokens a palette restates in its light block and leaves alone in dark,
 * so that its dark renders them exactly as classic dark does. Graphite's
 * chrome is here because it paints its bars in the deepest neutral in both
 * modes ("colour is reserved for state and code"), which is also classic
 * dark's product bar: a dark restatement would be inert, and the invariant
 * that refuses inert restatements keeps it out of the file.
 *
 * This is the one ledger for the fact: a new palette whose dark chrome stays
 * classic's is registered here, under `--chrome-bar`, and the identity
 * contract below reads it from this table.
 */
const LIGHT_ONLY_RESTATEMENTS: ReadonlyArray<readonly [Palette, readonly string[]]> = [
  ["ocean", ["--bg-canvas", "--bg-subtle", "--border-subtle", "--skeleton-base"]],
  ["glass", ["--bg-canvas", "--bg-subtle", "--skeleton-base"]],
  ["terminal", ["--fg-secondary", "--fg-muted", "--fg-faint", "--border", "--border-subtle", "--border-strong"]],
  ["graphite", ["--chrome-bar", "--chrome-appbar", "--chrome-hover"]],
];

/** Palettes whose dark block leaves the chrome bar to classic dark on purpose, read off the ledger above. */
const CHROME_STAYS_CLASSIC_IN_DARK: ReadonlySet<Palette> = new Set(
  LIGHT_ONLY_RESTATEMENTS.filter(([, names]) => names.includes("--chrome-bar")).map(([palette]) => palette),
);

// The search workspace's stacking: the composer and the fields rail are
// siblings of one page, and the completion menu drops out of the composer over
// whatever sits below it.
const workspaceStackingMarkup = `
  ${composerMarkup(2, true, 5)}
  <div class="events-layout">
    <aside class="fields-rail" aria-label="Search fields">
      <div class="fields-topbar"><button type="button">Hide Fields</button><button type="button">All Fields</button></div>
      <div class="field-filter"><span>⌕</span><input aria-label="Filter fields" placeholder="Filter fields"></div>
    </aside>
    <section class="event-results" aria-label="Events">
      <div class="table-wrap">
        <table class="table">
          <thead><tr><th>Time</th><th>Event</th></tr></thead>
          <tbody><tr><td>7/21/26</td><td><code>index=main</code></td></tr></tbody>
        </table>
      </div>
    </section>
  </div>`;

test.describe("search workspace stacking", () => {
  test("the open completion menu paints over the fields rail, in classic and glass, both modes", async ({ page }) => {
    // `.search-composer` and `.fields-rail` both sit at --z-sticky, and the
    // rail is the later sibling: the menu, trapped in the composer's stacking
    // context however high its own z-index climbs, lost its lower rows to the
    // rail. The composer lifts to --z-dropdown for as long as it holds the
    // menu, the move the chrome bars make for a floating menu. Glass gives the
    // menu a backdrop-filter, which opens a stacking context on the menu
    // itself; that must not change which element the pointer reaches.
    await mount(page, workspaceStackingMarkup, DESKTOP_WIDTH);
    const scopes = (["classic", "glass"] as const).flatMap((palette) => THEME_MODES.map((mode) => ({ mode, palette })));
    await sequentially(scopes, async ({ mode, palette }) => {
      await applyScope(page, palette, mode);
      const probe = await page.evaluate(() => {
        const menu = document.querySelector(".completion-menu");
        const rail = document.querySelector(".fields-rail");
        const rows = document.querySelectorAll(".completion-menu .completion-option");
        const lowest = rows[rows.length - 1];
        if (menu === null || rail === null || lowest === undefined) throw new Error("fixture is missing the menu, its rows or the rail");
        const row = lowest.getBoundingClientRect();
        const box = rail.getBoundingClientRect();
        const overlap = {
          bottom: Math.min(row.bottom, box.bottom),
          left: Math.max(row.left, box.left),
          right: Math.min(row.right, box.right),
          top: Math.max(row.top, box.top),
        };
        if (overlap.right - overlap.left < 1 || overlap.bottom - overlap.top < 1) {
          throw new Error(`the lowest menu row does not overlap the rail: ${JSON.stringify({ rail: box, row })}`);
        }
        const x = (overlap.left + overlap.right) / 2;
        const y = (overlap.top + overlap.bottom) / 2;
        const hit = document.elementFromPoint(x, y);
        return {
          backdropFilter: globalThis.getComputedStyle(menu).backdropFilter,
          hitInMenu: hit !== null && hit.closest(".completion-menu") !== null,
          hitPath: hit === null ? null : [hit, hit.parentElement].map((node) => `${node?.tagName.toLowerCase()}.${node?.className}`).join(" < "),
          x,
          y,
        };
      });
      expect(probe.hitInMenu, `${palette} ${mode}: the pointer at (${probe.x}, ${probe.y}) reaches ${probe.hitPath}, not the menu`).toBe(true);
      // Glass is the palette that filters the menu; classic leaves it plain.
      expect(probe.backdropFilter === "none", `${palette} ${mode}: backdrop-filter is ${probe.backdropFilter}`).toBe(palette === "classic");
    });
  });
});
