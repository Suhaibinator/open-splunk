import { expect, test } from "@playwright/test";

import {
  expectPageScreenshot,
  expectViewportScreenshot,
  gotoVisualRoute,
  settleVisualPage,
} from "./visual-harness";

/**
 * Whole-page appearance for every exported product surface.
 *
 * These run against the demo data mode, so the rendered fixtures are literal
 * constants in the bundle and the only clock-dependent copy is pinned by the
 * shared frozen clock.
 */

test.describe("product pages", () => {
  test("home launcher", async ({ page }) => {
    await gotoVisualRoute(page, "/");
    await expect(page.getByRole("heading", { level: 1 })).toBeVisible();
    await expectPageScreenshot(page, "home");
  });

  test("sign in", async ({ page }) => {
    await gotoVisualRoute(page, "/signin/");
    await expect(page.getByRole("link", { name: "Continue to preview" })).toBeVisible();
    await expectPageScreenshot(page, "signin");
  });

  test("administration overview", async ({ page }) => {
    await gotoVisualRoute(page, "/admin/");
    await expect(page.getByRole("heading", { level: 2, name: "System overview" })).toBeVisible();
    await expectPageScreenshot(page, "admin-overview");
  });

  test("administration index table", async ({ page }) => {
    await gotoVisualRoute(page, "/admin/?section=indexes");
    await expect(page.getByRole("heading", { level: 2, name: "Indexes" })).toBeVisible();
    await expectPageScreenshot(page, "admin-indexes-table");
  });

  test("administration settings form", async ({ page }) => {
    await gotoVisualRoute(page, "/admin/?section=server");
    await expect(page.getByRole("heading", { level: 2, name: "Server settings" })).toBeVisible();
    await expectPageScreenshot(page, "admin-server-form");
  });

  test("administration index dialog", async ({ page }) => {
    await gotoVisualRoute(page, "/admin/?section=indexes");
    await page.getByRole("button", { name: "Simulate index" }).click();
    await expect(page.getByTestId("modal-layer")).toBeVisible();
    await settleVisualPage(page);
    await expectViewportScreenshot(page, "admin-index-dialog");
  });

  test("mobile navigation drawer", async ({ page, viewport }) => {
    // Below 760px the drawer *is* the product navigation, and this phase merged
    // the search workspace's own drawer into this one -- moving its identity
    // block, adding a section label and a rule, and renaming two aria labels.
    // Nothing else in the suite opens it, so every one of those changes was
    // invisible to every gate the phase ran.
    test.skip((viewport?.width ?? 0) > 760, "the drawer trigger only exists below the compact breakpoint");
    await gotoVisualRoute(page, "/reports/");
    await page.locator("button.drawer-trigger").click();
    await expect(page.locator("dialog.drawer")).toBeVisible();
    await settleVisualPage(page);
    await expectViewportScreenshot(page, "product-drawer");
  });

  test("activity console", async ({ page }) => {
    await gotoVisualRoute(page, "/activity/");
    await expect(page.getByRole("heading", { level: 1, name: "Activity" })).toBeVisible();
    await expectPageScreenshot(page, "activity");
  });

  test("datasets console", async ({ page }) => {
    await gotoVisualRoute(page, "/datasets/");
    await expect(page.getByRole("heading", { level: 1, name: "Datasets" })).toBeVisible();
    await expectPageScreenshot(page, "datasets");
  });

  test("dashboards", async ({ page }) => {
    await gotoVisualRoute(page, "/dashboards/");
    await expect(page.getByTestId("line-chart")).toBeVisible();
    await expectPageScreenshot(page, "dashboards");
  });

  test("reports library", async ({ page }) => {
    await gotoVisualRoute(page, "/reports/");
    await expect(page.getByRole("heading", { level: 2, name: "Report library" })).toBeVisible();
    await expectPageScreenshot(page, "reports");
  });

  test("analytics", async ({ page }) => {
    await gotoVisualRoute(page, "/analytics/");
    await expect(page.getByRole("heading", { level: 2, name: "Search performance" })).toBeVisible();
    await expectPageScreenshot(page, "analytics");
  });
});
