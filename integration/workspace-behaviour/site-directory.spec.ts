import { expect, test, type Locator, type Page } from "@playwright/test";

/*
 * The site-admin Users and Organizations panels on the demo export, which
 * starts signed in as the seeded site admin and backs both panels with the
 * in-memory identity fake. Every change lives for the page's lifetime only,
 * so each test drives one page from its first load.
 */

function usersTable(page: Page): Locator {
  return page.getByRole("table", { name: /^Users/u });
}

function bodyRows(table: Locator): Locator {
  return table.locator("tbody tr");
}

async function createUser(page: Page, email: string, username: string): Promise<void> {
  await page.getByRole("button", { name: "Create user" }).first().click();
  const dialog = page.getByRole("dialog", { name: "Create user" });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("textbox", { name: /^Email/u })).toBeFocused();
  await dialog.getByRole("textbox", { name: /^Email/u }).fill(email);
  await dialog.getByRole("textbox", { name: /^Username/u }).fill(username);
  await dialog.getByRole("button", { name: "Create user" }).click();
  await expect(dialog).toBeHidden();
}

test("the Users panel lists, searches, creates and pages the demo directory", async ({ page }) => {
  await page.goto("/admin/?section=users");
  await expect(page.getByRole("heading", { level: 2, name: "Users" })).toBeVisible();
  const table = usersTable(page);
  await expect(bodyRows(table)).toHaveCount(9);
  await expect(page.getByRole("status").filter({ hasText: "Showing 1–9 of 9 users" })).toBeVisible();
  await expect(bodyRows(table).first()).toContainText("Ada Lovelace");
  await expect(bodyRows(table).first()).toContainText("You");

  const searchBox = page.getByRole("searchbox", { name: "Search users" });
  await searchBox.fill("mona");
  await searchBox.press("Enter");
  await expect(bodyRows(table)).toHaveCount(1);
  await expect(table).toContainText("Mona Moreau");

  await searchBox.fill("nobody-matches-this");
  await searchBox.press("Enter");
  await expect(page.getByText("No users match")).toBeVisible();
  await page.getByRole("button", { name: "Clear search" }).click();
  await expect(bodyRows(table)).toHaveCount(9);
  await expect(searchBox).toBeFocused();

  // Validation names the field before anything is sent.
  await page.getByRole("button", { name: "Create user" }).first().click();
  const dialog = page.getByRole("dialog", { name: "Create user" });
  await dialog.getByRole("button", { name: "Create user" }).click();
  await expect(dialog.getByRole("alert")).toContainText("Fix the highlighted fields");
  await expect(dialog.getByRole("textbox", { name: /^Email/u })).toHaveAttribute("aria-invalid", "true");
  await dialog.getByRole("button", { name: "Cancel" }).click();
  await expect(dialog).toBeHidden();
  await expect(page.getByRole("button", { name: "Create user" }).first()).toBeFocused();

  await createUser(page, "pat@open-splunk.test", "pat");
  await expect(page.locator("output.toast")).toContainText("Created pat and sent an invitation to pat@open-splunk.test.");
  await createUser(page, "quinn@open-splunk.test", "quinn");
  await expect(page.getByRole("status").filter({ hasText: "Showing 1–10 of 11 users" })).toBeVisible();

  const pager = page.getByRole("navigation", { name: "Users pages" });
  await expect(pager).toContainText("Page 1 of 2");
  await pager.getByRole("button", { name: "Next" }).click();
  await expect(bodyRows(table)).toHaveCount(1);
  await expect(table).toContainText("quinn@open-splunk.test");
  await expect(pager.getByRole("button", { name: "Next" })).toBeDisabled();
});

test("a row action runs from the keyboard through its confirmation and returns focus", async ({ page }) => {
  await page.goto("/admin/?section=users");
  const table = usersTable(page);
  await expect(bodyRows(table)).toHaveCount(9);

  const trigger = page.getByRole("button", { name: "Actions for Mark Mendez" });
  await trigger.focus();
  await page.keyboard.press("Enter");
  const menu = page.getByRole("menu", { name: "Actions for Mark Mendez" });
  await expect(menu).toBeVisible();
  await expect(menu.getByRole("menuitem", { name: "Make site admin" })).toBeFocused();
  await page.keyboard.press("ArrowDown");
  await expect(menu.getByRole("menuitem", { name: "Ban user" })).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(menu).toBeHidden();
  await expect(trigger).toBeFocused();

  await page.keyboard.press("Enter");
  await page.keyboard.press("ArrowDown");
  await page.keyboard.press("Enter");
  const dialog = page.getByRole("dialog", { name: "Ban Mark Mendez?" });
  await expect(dialog).toBeVisible();
  await expect(dialog.getByRole("textbox", { name: /Reason/u })).toBeFocused();
  await dialog.getByRole("textbox", { name: /Reason/u }).fill("Shared credentials");
  await dialog.getByRole("button", { name: "Ban user" }).click();
  await expect(dialog).toBeHidden();
  await expect(page.locator("output.toast")).toContainText("Mark Mendez is banned.");
  await expect(page.locator('tr[data-user-id="5"]')).toContainText("Banned");
  await expect(trigger).toBeFocused();

  // The signed-in administrator's own row explains what it cannot do.
  await page.getByRole("button", { name: "Actions for Ada Lovelace" }).click();
  const selfMenu = page.getByRole("menu", { name: "Actions for Ada Lovelace" });
  await expect(selfMenu.getByRole("menuitem", { name: /Delete user/u })).toBeDisabled();
  await expect(selfMenu).toContainText("You cannot delete your own account.");
  await expect(selfMenu.getByRole("menuitem", { name: /^Remove site admin/u })).toBeFocused();
  await expect(selfMenu.getByRole("menuitem", { name: "Sign out my sessions" })).toBeEnabled();
  await page.keyboard.press("Escape");
});

test("delete asks for the typed email, then removes the row", async ({ page }) => {
  await page.goto("/admin/?section=users");
  const table = usersTable(page);
  await expect(bodyRows(table)).toHaveCount(9);
  await page.getByRole("button", { name: "Actions for Tess Takahashi" }).click();
  await page.getByRole("menuitem", { name: "Delete user" }).click();
  const dialog = page.getByRole("dialog", { name: "Delete Tess Takahashi?" });
  const confirm = dialog.getByRole("button", { name: "Delete user" });
  await expect(confirm).toBeDisabled();
  const typed = dialog.getByRole("textbox", { name: /Type tess@open-splunk\.test to confirm/u });
  await expect(typed).toBeFocused();
  await typed.fill("tess@open-splunk.test");
  await confirm.click();
  await expect(dialog).toBeHidden();
  await expect(bodyRows(table)).toHaveCount(8);
  await expect(table).not.toContainText("Tess Takahashi");
  await expect(page.getByRole("searchbox", { name: "Search users" })).toBeFocused();
});

test("the Organizations panel shows the hierarchy and creates a child organization", async ({ page }) => {
  await page.goto("/admin/?section=organizations");
  await expect(page.getByRole("heading", { level: 2, name: "Organizations" })).toBeVisible();
  const table = page.getByRole("table", { name: /^Organizations/u });
  await expect(bodyRows(table)).toHaveCount(3);
  const platform = page.locator('tr[data-organization-id="3"]');
  await expect(platform).toContainText("Child");
  await expect(platform.getByRole("list", { name: "Ancestors of Acme Platform" })).toContainText("Acme Operations");

  await page.getByRole("button", { name: "Create organization" }).click();
  const dialog = page.getByRole("dialog", { name: "Create organization" });
  const name = dialog.getByRole("textbox", { name: /^Name/u });
  await expect(name).toBeFocused();
  await name.fill("Acme");
  await expect(dialog.getByRole("textbox", { name: /^Slug/u })).toHaveValue("acme");
  await dialog.getByRole("button", { name: "Create organization" }).click();
  await expect(dialog.getByRole("textbox", { name: /^Slug/u })).toHaveAttribute("aria-invalid", "true");
  await expect(dialog).toContainText("That slug is already taken.");

  await name.fill("Acme Edge Europe");
  await expect(dialog.getByRole("textbox", { name: /^Slug/u })).toHaveValue("acme-edge-europe");
  const parent = dialog.getByRole("combobox", { name: /Parent organization/u });
  await parent.click();
  const listboxId = await parent.getAttribute("aria-controls");
  expect(listboxId).not.toBeNull();
  await page.locator(`[id="${listboxId}"]`).getByRole("option", { name: "Acme Platform", exact: true }).click();
  await expect(parent).toContainText("Acme Platform");
  await dialog.getByRole("button", { name: "Create organization" }).click();
  await expect(dialog).toBeHidden();
  await expect(page.locator("output.toast")).toContainText("Created Acme Edge Europe.");
  const created = bodyRows(table).filter({ hasText: "Acme Edge Europe" });
  await expect(created).toContainText("Acme Operations");
  await expect(created).toContainText("Acme Platform");
  await expect(bodyRows(table)).toHaveCount(4);
});

test("below 760px the users table folds into cards without a horizontal scroll", async ({ page }) => {
  await page.setViewportSize({ height: 900, width: 480 });
  await page.goto("/admin/?section=users");
  await expect(bodyRows(usersTable(page))).toHaveCount(9);
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(overflow).toBeLessThanOrEqual(0);
  await expect(bodyRows(usersTable(page)).first()).toHaveCSS("display", "grid");
});
