import { expect, test, type Page } from "@playwright/test";

// The demo export's identity is the in-memory fake (lib/demo/identity-data.ts):
// it starts signed in as the seeded site admin and keeps the session in this
// sessionStorage key, so a test acts as another seeded account by writing the
// entry before the page boots.
const SESSION_KEY = "open-splunk.demo-identity-session";
const MEMBER_USER_ID = "5"; // mark@open-splunk.test: member of Acme Operations and Globex Security

async function actAs(page: Page, userId: string) {
  await page.addInitScript(([key, id]) => {
    window.sessionStorage.setItem(key, JSON.stringify({ v: 1, userId: id, activeOrganizationId: null }));
  }, [SESSION_KEY, userId] as const);
}

function organizationTrigger(page: Page) {
  return page.getByRole("button", { name: /^Organization: / });
}

test("the organization switcher opens from the keyboard and the choice survives a reload", async ({ page }) => {
  await page.goto("/datasets/");
  const trigger = organizationTrigger(page);
  await expect(trigger).toContainText("Acme Operations");
  await expect(trigger).toHaveAttribute("aria-expanded", "false");

  await trigger.focus();
  await page.keyboard.press("ArrowDown");
  await expect(trigger).toHaveAttribute("aria-expanded", "true");
  const menu = page.getByRole("menu", { name: "Organizations" });
  const operations = menu.getByRole("menuitemradio", { name: /^Acme Operations/u });
  const platform = menu.getByRole("menuitemradio", { name: /^Acme Platform/u });
  await expect(operations).toBeFocused();
  await expect(operations).toHaveAttribute("aria-checked", "true");
  await expect(platform).toContainText("inherited from Acme Operations");

  await page.keyboard.press("ArrowDown");
  await expect(platform).toBeFocused();
  await page.keyboard.press("Enter");
  await expect(menu).toHaveCount(0);
  await expect(trigger).toContainText("Acme Platform");
  await expect(trigger).toBeFocused();
  await expect(page.locator(".suite-org-anchor [aria-live='polite']")).toHaveText("Switched to Acme Platform.");

  await page.reload();
  await expect(organizationTrigger(page)).toContainText("Acme Platform");
});

test("Escape closes the organization menu and returns focus to its trigger", async ({ page }) => {
  await page.goto("/datasets/");
  const trigger = organizationTrigger(page);
  await trigger.click();
  await expect(page.getByRole("menu", { name: "Organizations" })).toBeVisible();
  await page.keyboard.press("Escape");
  await expect(trigger).toHaveAttribute("aria-expanded", "false");
  await expect(trigger).toBeFocused();
});

test("the user menu names the signed-in account, and signing out sends later pages to sign in with a return path", async ({ page }) => {
  await page.goto("/datasets/");
  const userButton = page.getByRole("button", { name: "Ada Lovelace menu" });
  await userButton.click();
  const userMenu = page.locator('[data-suite-menu="user"]');
  await expect(userMenu).toContainText("ada@open-splunk.test");
  await expect(userMenu).toContainText("Owner · Acme Operations");
  await expect(userMenu).toContainText("Site admin");
  await userMenu.getByRole("menuitem", { name: "Sign out", exact: true }).click();
  await expect(page).toHaveURL(/\/signin\/$/u);

  await page.goto("/reports/saved-searches/?view=mine");
  await expect(page).toHaveURL(/\/signin\/\?returnTo=%2Freports%2Fsaved-searches%2F%3Fview%3Dmine$/u);

  await page.goto("/help/");
  await expect(page).toHaveURL(/\/help\/$/u);
  await expect(organizationTrigger(page)).toHaveCount(0);
});

test("a member is offered no administration, and opening it by URL explains what is missing", async ({ page }) => {
  await actAs(page, MEMBER_USER_ID);
  await page.goto("/datasets/");
  await expect(organizationTrigger(page)).toContainText("Acme Operations");
  const utilities = page.getByRole("navigation", { name: "Product utilities" });
  await expect(utilities.getByRole("link", { name: "Settings" })).toHaveCount(0);
  await expect(page.getByRole("link", { name: "Manage indexes" })).toHaveCount(0);
  await expect(page.getByRole("link", { name: "Search data" })).toBeVisible();

  await page.goto("/admin/?section=server");
  const heading = page.getByRole("heading", { level: 1, name: "You do not have access to Administration" });
  await expect(heading).toBeVisible();
  await expect(page.getByText("Your role in Acme Operations is Member.")).toBeVisible();
  await expect(page.getByText("Any one of these permissions is needed:")).toBeVisible();
  await expect(page.getByRole("link", { name: "Go to Search" })).toBeVisible();

  await page.getByRole("button", { name: "Switch organization" }).click();
  const menu = page.getByRole("menu", { name: "Organizations" });
  await expect(menu.getByRole("menuitemradio", { name: /^Acme Operations/u })).toBeFocused();
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("Enter");
  await expect(organizationTrigger(page)).toContainText("Globex Security");
  await expect(page.getByText("Your role in Globex Security is Member.")).toBeVisible();
});

test.describe("below the compact breakpoint", () => {
  test.use({ viewport: { width: 390, height: 844 } });

  test("the drawer lists the organizations and switches between them", async ({ page }) => {
    await page.goto("/datasets/");
    await expect(organizationTrigger(page)).toBeHidden();
    await page.getByRole("button", { name: "Open product navigation" }).click();
    const drawer = page.getByRole("dialog", { name: "Mobile product navigation" });
    const platform = drawer.getByRole("button", { name: /^Acme Platform/u });
    await expect(drawer.getByRole("button", { name: /^Acme Operations/u })).toHaveAttribute("aria-pressed", "true");
    await platform.click();
    await expect(drawer).toHaveCount(0);

    await page.getByRole("button", { name: "Open product navigation" }).click();
    await expect(page.getByRole("dialog", { name: "Mobile product navigation" }).getByRole("button", { name: /^Acme Platform/u }))
      .toHaveAttribute("aria-pressed", "true");
  });
});
