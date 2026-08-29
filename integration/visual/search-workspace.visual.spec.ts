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

async function openWorkspaceWithResults(page: Page): Promise<void> {
  await gotoVisualRoute(page, "/search/");
  await expect(page.getByTestId("search-workspace")).toBeVisible();
  await expect(page.getByTestId("event-list")).toBeVisible();
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
    await expectViewportScreenshot(page, "search-export-dialog");
  });

  test("empty state", async ({ page }) => {
    await openWorkspaceWithResults(page);
    await runWorkspaceAction(page, "Close", "Close search");
    await expect(page.getByTestId("job-empty-results")).toBeVisible();
    await expectPageScreenshot(page, "search-empty");
  });
});
