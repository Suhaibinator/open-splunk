import { expect, test } from "@playwright/test";

test("page aliases canonicalize without losing query parameters or hashes", async ({ page }) => {
  await page.goto("/activity/?appId=operations#recent");
  await expect(page).toHaveURL(/\/activity\/jobs\/\?appId=operations#recent$/u);

  await page.goto("/reports/?appId=operations#library");
  await expect(page).toHaveURL(/\/reports\/saved-searches\/\?appId=operations#library$/u);

  await page.goto("/search/?appId=operations#results");
  await expect(page).toHaveURL(/\/search\/events\/\?appId=operations#results$/u);
});

test("canonical Activity and Reports routes survive refresh and preserve unavailable deep links", async ({ page }) => {
  await page.goto("/activity/jobs/");
  await expect(page.getByRole("heading", { name: "Activity" })).toBeVisible();
  await page.reload();
  await expect(page).toHaveURL(/\/activity\/jobs\/$/u);
  await expect(page.getByRole("heading", { name: "Activity" })).toBeVisible();

  await page.goto("/reports/saved-searches/");
  await expect(page.getByRole("heading", { name: "Reports" })).toBeVisible();
  await page.reload();
  await expect(page).toHaveURL(/\/reports\/saved-searches\/$/u);
  await expect(page.getByRole("heading", { name: "Reports" })).toBeVisible();

  await page.goto("/activity/exports/");
  await expect(page).toHaveURL(/\/activity\/exports\/$/u);
  await expect(page.getByText("Activity view not found", { exact: true })).toBeVisible();

  await page.goto("/reports/alerts/");
  await expect(page).toHaveURL(/\/reports\/alerts\/$/u);
  await expect(page.getByText("Reports view not found", { exact: true })).toBeVisible();
});
