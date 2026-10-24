import { test } from "@playwright/test";

const dir = "/private/tmp/claude-501/-Users-suhaib-code-open-splunk/58e112a7-425e-40dc-9a30-a1bc69650f80/scratchpad";

test("shots", async ({ page }) => {
  await page.setViewportSize({ width: 1400, height: 900 });
  await page.goto("/admin/?section=users");
  await page.getByRole("table", { name: /^Users/u }).locator("tbody tr").first().waitFor();
  await page.screenshot({ fullPage: true, path: `${dir}/users.png` });
  await page.getByRole("button", { name: "Actions for Mark Mendez" }).click();
  await page.screenshot({ path: `${dir}/menu.png` });
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Actions for Ada Lovelace" }).click();
  await page.screenshot({ path: `${dir}/selfmenu.png` });
  await page.keyboard.press("Escape");
  await page.getByRole("button", { name: "Create user" }).first().click();
  await page.getByRole("dialog").getByRole("button", { name: "Create user" }).click();
  await page.screenshot({ path: `${dir}/create.png` });
  await page.keyboard.press("Escape");
  await page.goto("/admin/?section=organizations");
  await page.getByRole("table", { name: /^Organizations/u }).locator("tbody tr").first().waitFor();
  await page.screenshot({ fullPage: true, path: `${dir}/orgs.png` });
  await page.evaluate(() => document.documentElement.setAttribute("data-theme", "dark"));
  await page.screenshot({ fullPage: true, path: `${dir}/orgs-dark.png` });
  await page.setViewportSize({ width: 420, height: 900 });
  await page.goto("/admin/?section=users");
  await page.getByRole("table", { name: /^Users/u }).locator("tbody tr").first().waitFor();
  await page.screenshot({ path: `${dir}/users-mobile.png` });
});
