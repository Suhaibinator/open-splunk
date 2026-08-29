import { expect, test, type Page } from "@playwright/test";

import {
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
  return page.locator("button.search-mobile-trigger").isVisible();
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
 * The event list appears before the job settles, so waiting only for it accepts
 * more than one page shape. Two of them differ by roughly 370 pixels of height:
 * above the phone breakpoint the first event is expanded over its EVENT FIELDS
 * panel, and at or below it a mount effect collapses that event along with the
 * fields rail. A run that hydrates with the phone layout in effect therefore
 * screenshots a much shorter desktop page and fails on a surface nobody
 * touched. Pinning the finished status and the expansion the viewport implies
 * turns that race into a wait, and into an honest failure if it never resolves.
 */
async function openWorkspaceWithResults(page: Page): Promise<void> {
  await gotoVisualRoute(page, "/search/");
  await expect(page.getByTestId("search-workspace")).toBeVisible();
  await expect(page.getByTestId("event-list")).toBeVisible();

  const strip = page.getByTestId("job-strip");
  await expect(strip).toContainText("Completed");
  await expect(strip).toContainText("100%");

  // The same query the workspace itself uses to decide the phone layout.
  const phoneLayout = await page.evaluate(() => globalThis.matchMedia("(max-width: 760px)").matches);
  const firstEvent = page.locator(".event-row").first();
  if (phoneLayout) await expect(firstEvent).not.toHaveClass(/\bexpanded\b/u);
  else await expect(firstEvent).toHaveClass(/\bexpanded\b/u);

  await settleVisualPage(page);
}

test.describe("search workspace", () => {
  test("demo events with timeline and fields", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await expectPageScreenshot(page, "search-events");
  });

  test("timeline", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await expectRegionScreenshot(page.getByTestId("timeline"), "search-timeline");
  });

  test("fields sidebar", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await revealFieldsRail(page);
    await expectRegionScreenshot(page.getByTestId("fields-rail"), "search-fields-rail");
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

  test("empty state", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await runWorkspaceAction(page, "Close", "Close search");
    await expect(page.getByTestId("job-empty-results")).toBeVisible();
    await expectPageScreenshot(page, "search-empty");
  });
});
