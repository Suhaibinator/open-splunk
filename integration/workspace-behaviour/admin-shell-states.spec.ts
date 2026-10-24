import { expect, test, type Page } from "@playwright/test";

/*
 * The shell and the administration console together on the demo export,
 * where a real browser matters: the modal's focus handling around a change
 * that moves the whole page, the browser's own `beforeunload` semantics
 * behind the organization switch's unsaved-work question, and a navigation
 * to sign-in. The demo's identity is the in-memory fake; its session lives in
 * sessionStorage under this key, which is how a test acts as another seeded
 * account. Directory changes last for one page load.
 */
const SESSION_KEY = "open-splunk.demo-identity-session";

async function actAs(page: Page, userId: string): Promise<void> {
  await page.addInitScript(([key, id]) => {
    window.sessionStorage.setItem(key, JSON.stringify({ v: 1, userId: id, activeOrganizationId: null }));
  }, [SESSION_KEY, userId] as const);
}

function organizationTrigger(page: Page) {
  return page.getByRole("button", { name: /^Organization: / });
}

test("leaving the only organization from Members takes the bar and the console with it", async ({ page }) => {
  await actAs(page, "3"); // alice@open-splunk.test: admin of Acme Operations, inherited in Acme Platform
  await page.goto("/admin/?section=members");
  await expect(organizationTrigger(page)).toContainText("Acme Operations");
  await page.getByRole("button", { name: "Leave organization" }).click();
  const dialog = page.getByRole("dialog", { name: "Leave Acme Operations?" });
  await dialog.getByRole("button", { name: "Leave organization" }).click();
  await expect(dialog).toBeHidden();
  await expect(organizationTrigger(page)).toContainText("No organization");
  await expect(page.getByRole("heading", { level: 1, name: "You are not in an organization yet" })).toBeVisible();
  await expect(page.getByRole("complementary", { name: "Administration navigation" })).toHaveCount(0);
});

test("a site-admin owner shares ownership, steps down, and the user menu shows the new role", async ({ page }) => {
  await page.goto("/admin/?section=members");
  await expect(page.getByRole("button", { name: "Change role for Ada Lovelace" })).toHaveAttribute("aria-disabled", "true");
  await page.getByRole("button", { name: "Change role for Owen Ortiz" }).click();
  const promote = page.getByRole("dialog", { name: "Change role for Owen Ortiz" });
  await promote.getByRole("radio", { name: /^Owner/u }).check();
  await promote.getByRole("button", { name: "Save role" }).click();
  await expect(promote).toBeHidden();

  const self = page.getByRole("button", { name: "Change role for Ada Lovelace" });
  await expect(self).not.toHaveAttribute("aria-disabled", "true");
  await self.click();
  const stepDown = page.getByRole("dialog", { name: "Change your role" });
  await stepDown.getByRole("radio", { name: /^Admin/u }).check();
  await stepDown.getByRole("button", { name: "Lower my role" }).click();
  await expect(stepDown).toBeHidden();

  await page.getByRole("button", { name: "Ada Lovelace menu" }).click();
  await expect(page.locator('[data-suite-menu="user"]')).toContainText("Admin · Acme Operations");
  await expect(page.locator('[data-suite-menu="user"]')).toContainText("Site admin");
});

test("switching organization with unsaved work asks the browser's question first", async ({ page }) => {
  await page.goto("/datasets/");
  await expect(organizationTrigger(page)).toContainText("Acme Operations");
  // A page with unsaved work cancels beforeunload, as the settings forms and the dashboard editor do.
  await page.evaluate(() => {
    window.addEventListener("beforeunload", (event) => event.preventDefault());
  });

  const asked: string[] = [];
  page.once("dialog", (question) => {
    asked.push(question.message());
    void question.dismiss();
  });
  await organizationTrigger(page).click();
  await page.getByRole("menuitemradio", { name: /^Acme Platform/u }).click();
  await expect.poll(() => asked.length).toBe(1);
  expect(asked[0]).toContain("Switching to Acme Platform discards them");
  await expect(organizationTrigger(page)).toContainText("Acme Operations");

  page.once("dialog", (question) => {
    void question.accept();
  });
  await page.getByRole("menuitemradio", { name: /^Acme Platform/u }).click();
  await expect(organizationTrigger(page)).toContainText("Acme Platform");
});

test("an organization admin who opens a site-admin section by URL is told why, and its navigation omits it", async ({ page }) => {
  await actAs(page, "3");
  await page.goto("/admin/?section=users");
  await expect(page.getByRole("heading", { level: 2, name: "You do not have access to Users" })).toBeVisible();
  await expect(page.getByText("Only a site administrator can open it.")).toBeVisible();
  const navigation = page.getByRole("complementary", { name: "Administration navigation" });
  await expect(navigation.getByRole("button", { name: /^Users Accounts/u })).toHaveCount(0);
  await expect(navigation.getByRole("button", { name: /Members & invitations/u })).toHaveCount(1);
  await expect(page.getByRole("table")).toHaveCount(0);
});

test("a site admin who signs out its own sessions from Users lands on sign-in with the way back", async ({ page }) => {
  await page.goto("/admin/?section=users");
  await page.getByRole("button", { name: "Actions for Ada Lovelace" }).click();
  await page.getByRole("menuitem", { name: /Sign out my sessions/u }).click();
  const dialog = page.getByRole("dialog");
  await dialog.locator('button[type="submit"]').click();
  await expect(page).toHaveURL(/\/signin\/\?returnTo=%2Fadmin%2F%3Fsection%3Dusers$/u);
});
