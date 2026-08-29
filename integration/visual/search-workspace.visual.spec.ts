import { expect, test, type Page } from "@playwright/test";

import {
  awaitSettledSearchResults,
  expectDocumentRegionScreenshot,
  expectPageScreenshot,
  expectRegionScreenshot,
  expectViewportScreenshot,
  gotoVisualRoute,
  settleVisualPage,
} from "./visual-harness";

/**
 * Appearance of the search workspace, which owns the largest share of
 * `app/globals.css`: the job strip, timeline, fields rail, result tabs, the
 * statistics grid, the categorical chart, a modal surface, and the empty
 * state.
 *
 * The demo data mode ships fixed event, statistic, and timeline fixtures, so
 * every rendered number here is a constant rather than a sampled value.
 */

/** True on the narrow project, where the workspace collapses its toolbar. */
async function usesCompactToolbar(page: Page): Promise<boolean> {
  return page.locator("button.drawer-trigger").isVisible();
}

/** Runs a workspace action that the narrow layout moves into the overflow menu. */
async function runWorkspaceAction(page: Page, wide: string, compact: string): Promise<void> {
  if (await usesCompactToolbar(page)) {
    await page.getByRole("button", { name: "More", exact: true }).click();
    await page.getByRole("menuitem", { name: compact }).click();
  } else {
    await page.getByRole("button", { name: wide, exact: true }).click();
  }
  await settleVisualPage(page);
}

/** Reveals the fields rail, which the narrow layout keeps behind a drawer trigger. */
async function revealFieldsRail(page: Page): Promise<void> {
  if (await usesCompactToolbar(page)) {
    await page.locator("button.mobile-fields-button").click();
  }
  await expect(page.getByTestId("fields-rail")).toBeVisible();
  await settleVisualPage(page);
}

/**
 * Opens the workspace and waits for the exact result state the baselines record.
 *
 * The wait itself lives in the harness because the determinism gate needs the
 * same one: a capture taken before the workspace settles is a different page,
 * not a different rendering.
 */
async function openWorkspaceWithResults(page: Page): Promise<void> {
  await gotoVisualRoute(page, "/search/");
  await awaitSettledSearchResults(page);
}

test.describe("search workspace", () => {
  test("demo events with timeline and fields", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await expectPageScreenshot(page, "search-events");
  });

  test("event row controls and pagination", async ({ page }) => {
    const parameters = new URLSearchParams({
      earliest: "-24h",
      latest: "now",
      q: "index=gradethis",
      run: "1",
    });
    await gotoVisualRoute(page, `/search/?${parameters.toString()}`);
    await awaitSettledSearchResults(page);
    await page.context().grantPermissions(["clipboard-read", "clipboard-write"], {
      origin: new URL(page.url()).origin,
    });

    const rows = page.locator(".event-row");
    const rowCount = await rows.count();
    expect(rowCount).toBeGreaterThan(0);
    await expect(page.getByRole("button", { name: "Copy raw event" })).toHaveCount(rowCount);
    await page.getByRole("button", { name: "Copy raw event" }).first().click();
    await expect(page.getByTestId("toast")).toHaveText("Raw event copied.");

    const rowsMenu = page.getByRole("button", { name: /^Rows\b/u });
    await rowsMenu.click();
    await page.getByRole("menuitem", { name: /^Expand page\b/u }).click();
    await expect(page.locator(".event-row.expanded")).toHaveCount(rowCount);

    const rawPage = (await rows.locator(".event-raw").allTextContents())
      .map((raw) => raw.replaceAll("\u200b", ""))
      .join("\n");
    await rowsMenu.click();
    await page.getByRole("menuitem", { name: /^Copy page raw\b/u }).click();
    expect(await page.evaluate(() => navigator.clipboard.readText())).toBe(rawPage);

    await rowsMenu.click();
    await page.getByRole("menuitem", { name: /^Collapse page\b/u }).click();
    await expect(page.locator(".event-row.expanded")).toHaveCount(0);

    const pageInput = page.getByRole("spinbutton", { name: "Event page number" });
    await page.getByRole("button", { name: /^Next\b/u }).click();
    await expect(pageInput).toHaveValue("2");
    await expect(page.locator(".event-row.expanded")).toHaveCount(0);
    await page.getByRole("button", { name: "First", exact: true }).click();
    await expect(pageInput).toHaveValue("1");
    await pageInput.fill("3");
    await page.getByRole("button", { name: "Go", exact: true }).click();
    await expect(pageInput).toHaveValue("3");
    await page.getByRole("button", { name: /Prev/u }).click();
    await expect(pageInput).toHaveValue("2");

    const maximumPage = await pageInput.getAttribute("max");
    await pageInput.fill(String(Number(maximumPage) + 1));
    await page.getByRole("button", { name: "Go", exact: true }).click();
    await expect(pageInput).toHaveValue("2");
    await expect(page.getByTestId("toast")).toContainText("Choose a page from 1 to");

    await page.setViewportSize({ width: 375, height: 812 });
    await settleVisualPage(page);
    expect(await page.locator(".event-toolbar").evaluate((toolbar) => ({
      noHorizontalOverflow: toolbar.scrollWidth <= toolbar.clientWidth,
      controlsInsideToolbar: [...toolbar.querySelectorAll("button, input")].every((control) => {
        const controlBox = control.getBoundingClientRect();
        const toolbarBox = toolbar.getBoundingClientRect();
        return controlBox.left >= toolbarBox.left && controlBox.right <= toolbarBox.right;
      }),
    }))).toEqual({ noHorizontalOverflow: true, controlsInsideToolbar: true });
  });

  test("timeline", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await expectRegionScreenshot(page.getByTestId("timeline"), "search-timeline");
  });

  test("fields sidebar", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await revealFieldsRail(page);
    await expectDocumentRegionScreenshot(page.getByTestId("fields-rail"), "search-fields-rail");
  });

  test("statistics table", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await page.getByTestId("result-tab-statistics").click();
    await expect(page.locator("#panel-statistics")).toBeVisible();
    await settleVisualPage(page);
    await expectPageScreenshot(page, "search-statistics");
  });

  test("visualization chart", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await page.getByTestId("result-tab-visualization").click();
    await expect(page.getByTestId("visualization-chart")).toBeVisible();
    await settleVisualPage(page);
    await expectPageScreenshot(page, "search-visualization");
    await expectRegionScreenshot(page.getByTestId("categorical-chart"), "search-categorical-chart");
  });

  test("export dialog", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await runWorkspaceAction(page, "Export", "Export results");
    await expect(page.getByTestId("export-dialog")).toBeVisible();
    await settleVisualPage(page);
    await expectViewportScreenshot(page, "search-export-dialog");
  });

  test("navigation drawer", async ({ page, viewport }) => {
    // The workspace supplies its own app switcher, so it gets the shell
    // drawer's single-app branch rather than the per-app list every other page
    // offers. That is the one drawer in the product whose app section differs,
    // and it is the surface the header merge was most likely to get wrong.
    test.skip((viewport?.width ?? 0) > 760, "the drawer trigger only exists below the compact breakpoint");
    await gotoVisualRoute(page, "/search/");
    await page.locator("button.drawer-trigger").click();
    await expect(page.locator("dialog.drawer")).toBeVisible();
    await settleVisualPage(page);
    await expectViewportScreenshot(page, "search-drawer");
  });

  test("empty state", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await runWorkspaceAction(page, "Close", "Close search");
    await expect(page.getByTestId("job-empty-results")).toBeVisible();
    await expectPageScreenshot(page, "search-empty");
  });
});
