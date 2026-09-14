import { expect, test, type Locator } from "@playwright/test";

async function chooseOption(control: Locator, name: string): Promise<void> {
  await control.click();
  const listboxId = await control.getAttribute("aria-controls");
  expect(listboxId).not.toBeNull();
  await control.page().locator(`[id="${listboxId}"]`)
    .getByRole("option", { name, exact: true })
    .click();
}

test("analytics Select filters update the visible summary", async ({ page }) => {
  await page.goto("/analytics/");
  const summary = page.getByRole("region", { name: "Search analytics summary" });
  const searches = summary.getByText("Searches run").locator("..").locator("strong");
  await expect(searches).toHaveText("2,841");

  const range = page.getByRole("combobox", { name: "Time range" });
  await chooseOption(range, "Last 60 minutes");
  await expect(range).toContainText("Last 60 minutes");
  await expect(searches).toHaveText("142");

  const environment = page.getByRole("combobox", { name: "Environment" });
  await chooseOption(environment, "Staging");
  await expect(environment).toContainText("Staging");
  await expect(searches).toHaveText("18");
});

test("report Select filters narrow the real report table", async ({ page }) => {
  await page.goto("/reports/");
  const library = page.getByRole("region", { name: "Report library" });
  const table = page.getByRole("table");
  await expect(library).toContainText("7 reports");
  await expect(table.getByRole("row")).toHaveCount(8);

  await chooseOption(page.getByRole("combobox", { name: "Type" }), "Event lists");
  await expect(library).toContainText("2 reports");
  await expect(table.getByRole("row")).toHaveCount(3);
  await chooseOption(page.getByRole("combobox", { name: "Status" }), "Manual");
  await expect(library).toContainText("1 report");
  await expect(table.getByRole("row")).toHaveCount(2);
  await expect(table).toContainText("Clients near rate limit");
});

test("a Select inside the index modal contains Escape and stays one tab stop", async ({ page }) => {
  await page.goto("/admin/?section=indexes");
  await page.getByRole("button", { name: "Simulate index" }).click();
  const dialog = page.getByRole("dialog", { name: "Simulate index creation" });
  await expect(dialog).toBeVisible();

  const retention = dialog.getByRole("combobox", { name: "Retention" });
  await retention.click();
  await expect(retention).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("Escape");
  await expect(retention).toHaveAttribute("aria-expanded", "false");
  await expect(dialog).toBeVisible();

  await chooseOption(retention, "90 days");
  await expect(retention).toHaveAccessibleName("Retention");
  await expect(retention).toContainText("90 days");
  await expect(dialog).toBeVisible();

  await retention.click();
  const listboxId = await retention.getAttribute("aria-controls");
  expect(listboxId).not.toBeNull();
  const options = page.locator(`[id="${listboxId}"]`).getByRole("option");
  await expect(options).toHaveCount(4);
  expect(await options.evaluateAll((elements) => (
    elements.every((element) => element.getAttribute("tabindex") === "-1")
  ))).toBe(true);
  await retention.focus();
  await page.keyboard.press("Tab");
  await expect(retention).toHaveAttribute("aria-expanded", "false");
  await expect(dialog.getByRole("button", { name: "Cancel" })).toBeFocused();
  await expect(dialog).toBeVisible();
});

test("uncontrolled server settings Selects reset and keyboard browsing never submits", async ({ page }) => {
  await page.goto("/admin/?section=server");
  const form = page.locator("form.server-settings");
  const range = page.getByRole("combobox", { name: "Default time range" });
  const timezone = page.getByRole("combobox", { name: "Time zone" });
  const weekStart = page.getByRole("combobox", { name: "Week starts on" });

  await range.focus();
  await page.keyboard.type("l");
  await expect(range).toHaveAttribute("aria-expanded", "true");
  await page.keyboard.press("End");
  await page.keyboard.press("Space");
  await expect(range).toContainText("Last 7 days");
  await range.press("Space");
  await range.press("Home");
  await range.press("Escape");
  await expect(range).toHaveAttribute("aria-expanded", "false");
  await range.press("ArrowDown");
  await expect(range).toHaveAttribute("aria-expanded", "true");
  await range.press("Space");
  await expect(range).toContainText("Last 15 minutes");
  await expect(page.getByTestId("toast")).toHaveCount(0);

  await chooseOption(timezone, "UTC");
  await chooseOption(weekStart, "Monday");
  await form.evaluate((element) => (element as HTMLFormElement).reset());
  await expect(range).toContainText("Last 24 hours");
  await expect(timezone).toContainText("America/Los_Angeles");
  await expect(weekStart).toContainText("Sunday");
  await expect(page.getByTestId("toast")).toHaveCount(0);
});

test.describe("touch Select controls", () => {
  test.use({ hasTouch: true });

  test("analytics accepts a touch selection and updates its summary", async ({ page }) => {
    await page.goto("/analytics/");
    const environment = page.getByRole("combobox", { name: "Environment" });
    await environment.tap();
    const listboxId = await environment.getAttribute("aria-controls");
    expect(listboxId).not.toBeNull();
    await page.locator(`[id="${listboxId}"]`)
      .getByRole("option", { name: "Production", exact: true })
      .tap();
    await expect(environment).toContainText("Production");
    const summary = page.getByRole("region", { name: "Search analytics summary" });
    await expect(summary.getByText("Searches run").locator("..").locator("strong"))
      .toHaveText("2,528");
  });
});
