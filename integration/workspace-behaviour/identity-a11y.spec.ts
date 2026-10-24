import { expect, test, type Locator, type Page } from "@playwright/test";

/*
 * Keyboard-only and screen-reader paths through the identity screens that
 * need a real browser: layout-dependent Tab order, the modal and drawer
 * focus traps, and focus after a control the browser itself disables. The
 * demo export starts signed in as the seeded site admin (owner of Acme
 * Operations) and keeps every change for the page's lifetime only.
 */

const SESSION_KEY = "open-splunk.demo-identity-session";
const MEMBER_USER_ID = "5"; // mark@open-splunk.test: member of Acme Operations and Globex Security

async function actAs(page: Page, userId: string): Promise<void> {
  await page.addInitScript(([key, id]) => {
    window.sessionStorage.setItem(key, JSON.stringify({ v: 1, userId: id, activeOrganizationId: null }));
  }, [SESSION_KEY, userId] as const);
}

/** Presses `key` `times` times, asserting after each press that focus is still inside `scope`. */
async function tabStaysWithin(page: Page, scope: Locator, times: number, key: "Tab" | "Shift+Tab", pressed = 0): Promise<void> {
  if (pressed === times) return;
  await page.keyboard.press(key);
  expect(await scope.evaluate((element) => element.contains(document.activeElement)), `${key} ${pressed + 1} stays inside`).toBe(true);
  await tabStaysWithin(page, scope, times, key, pressed + 1);
}

async function createUserFromKeyboard(page: Page, email: string, username: string): Promise<void> {
  await page.getByRole("button", { name: "Create user" }).first().click();
  const dialog = page.getByRole("dialog", { name: "Create user" });
  await dialog.getByRole("textbox", { name: /^Email/u }).fill(email);
  await dialog.getByRole("textbox", { name: /^Username/u }).fill(username);
  await dialog.getByRole("textbox", { name: /^Username/u }).press("Enter");
  await expect(dialog).toBeHidden();
}

async function signOutFirst(page: Page): Promise<void> {
  await page.goto("/signin/");
  await expect(page.getByRole("heading", { name: "You're already signed in" })).toBeVisible();
  await page.getByRole("button", { name: "Sign in with a different account" }).click();
  await expect(page.getByRole("heading", { name: "Sign in", exact: true })).toBeVisible();
}

test("Enter in an empty sign-in email keeps focus there and announces why", async ({ page }) => {
  await signOutFirst(page);
  const form = page.getByRole("form", { name: "Sign in with email" });
  const email = form.getByLabel("Email", { exact: true });
  await expect(email).toBeFocused();
  await email.press("Enter");
  await expect(email).toBeFocused();
  await expect(email).toHaveAttribute("aria-invalid", "true");
  await expect(email).toHaveAccessibleDescription("Enter your email address.");
  await expect(form.locator('span[role="alert"]')).toHaveText("Enter your email address.");
  await email.pressSequentially("a");
  await expect(form.locator('span[role="alert"]')).toHaveText("");

  // From the button, focus moves to the field instead, and nothing is said twice.
  await email.fill("");
  await form.getByRole("button", { name: "Sign in", exact: true }).focus();
  await page.keyboard.press("Enter");
  await expect(email).toBeFocused();
  await expect(form.locator('span[role="alert"]')).toHaveText("");
});

test("the organization menu closes on Tab and moves focus to the next control in the bar", async ({ page }) => {
  await page.goto("/datasets/");
  const trigger = page.getByRole("button", { name: /^Organization: / });
  await trigger.focus();
  await page.keyboard.press("ArrowDown");
  const menu = page.getByRole("menu", { name: "Organizations" });
  await expect(menu).toBeVisible();
  await page.keyboard.press("End");
  await expect(menu.getByRole("menuitemradio").last()).toBeFocused();
  await page.keyboard.press("Home");
  await expect(menu.getByRole("menuitemradio").first()).toBeFocused();
  await page.keyboard.press("Tab");
  await expect(menu).toHaveCount(0);
  await expect(trigger).not.toBeFocused();
  // The menu moves focus on the next frame, so the check retries until it lands.
  await expect.poll(() => page.evaluate(() => {
    const active = document.activeElement;
    return active !== null && active !== document.body && (document.querySelector(".suite-product-bar")?.contains(active) ?? false);
  }), { message: "focus moves on within the bar" }).toBe(true);

  await trigger.focus();
  await page.keyboard.press("ArrowUp");
  await expect(menu.getByRole("menuitemradio").last()).toBeFocused();
  await page.keyboard.press("Shift+Tab");
  await expect(menu).toHaveCount(0);
  await expect(trigger).toBeFocused();
});

test("the user menu's sign-out entries are reachable with the arrows", async ({ page }) => {
  await page.goto("/datasets/");
  const userButton = page.getByRole("button", { name: "Ada Lovelace menu" });
  await userButton.focus();
  await page.keyboard.press("ArrowDown");
  const menu = page.locator('[data-suite-menu="user"]');
  await expect(menu.getByRole("menuitem").first()).toBeFocused();
  // Administration, Sign out, Sign out of all sessions: the theme radios follow.
  await page.keyboard.press("ArrowDown");
  await expect(menu.getByRole("menuitem", { name: "Sign out", exact: true })).toBeFocused();
  await page.keyboard.press("ArrowDown");
  await expect(menu.getByRole("menuitem", { name: "Sign out of all sessions" })).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(userButton).toBeFocused();
});

test.describe("below the compact breakpoint", () => {
  test.use({ viewport: { width: 390, height: 844 } });

  test("the drawer traps Tab, closes on Escape, and returns focus to its trigger", async ({ page }) => {
    await page.goto("/datasets/");
    const open = page.getByRole("button", { name: "Open product navigation" });
    await open.click();
    const drawer = page.getByRole("dialog", { name: "Mobile product navigation" });
    await expect(drawer).toBeVisible();
    await expect(drawer.getByRole("button", { name: "Close navigation" })).toBeFocused();
    await tabStaysWithin(page, drawer, 40, "Tab");
    await tabStaysWithin(page, drawer, 3, "Shift+Tab");
    await page.keyboard.press("Escape");
    await expect(drawer).toHaveCount(0);
    await expect(open).toBeFocused();
  });

  test("a drawer organization switch from the keyboard closes the drawer onto its trigger", async ({ page }) => {
    await page.goto("/datasets/");
    await page.getByRole("button", { name: "Open product navigation" }).click();
    const drawer = page.getByRole("dialog", { name: "Mobile product navigation" });
    await drawer.getByRole("button", { name: /^Acme Platform/u }).focus();
    await page.keyboard.press("Enter");
    await expect(drawer).toHaveCount(0);
    await expect(page.getByRole("button", { name: "Open product navigation" })).toBeFocused();
  });
});

test("the invite dialog traps focus, and a refused address gets focus back after sending", async ({ page }) => {
  await page.goto("/admin/?section=members");
  const invite = page.getByRole("button", { name: "Invite member" });
  await invite.click();
  const dialog = page.getByRole("dialog", { name: "Invite member" });
  const email = dialog.getByRole("textbox", { name: "Email address" });
  await expect(email).toBeFocused();
  await tabStaysWithin(page, dialog, 20, "Tab");
  await tabStaysWithin(page, dialog, 5, "Shift+Tab");

  // Enter in the empty field: focus cannot move, so the error is spoken.
  await email.focus();
  await email.press("Enter");
  await expect(email).toBeFocused();
  await expect(dialog.locator('span[role="alert"]')).toContainText("Enter the email address to invite.");

  // The field is disabled while the invitation is sent; the refusal brings focus back to it.
  await email.fill("nora@open-splunk.test");
  await email.press("Enter");
  await expect(email).toHaveAttribute("aria-invalid", "true");
  await expect(email).toBeFocused();

  await page.keyboard.press("Escape");
  await expect(dialog).toBeHidden();
  await expect(invite).toBeFocused();
});

test("a role choice is announced by its role, with its summary as the description", async ({ page }) => {
  await page.goto("/admin/?section=members");
  await page.getByRole("button", { name: "Change role for Owen Ortiz" }).click();
  const dialog = page.getByRole("dialog", { name: "Change role for Owen Ortiz" });
  const admin = dialog.getByRole("radio", { name: "Admin", exact: true });
  await expect(admin).toBeVisible();
  const description = await admin.evaluate((element) => document.getElementById(element.getAttribute("aria-describedby") ?? "")?.textContent ?? "");
  expect(description.length).toBeGreaterThan(0);
  await expect(admin).toHaveAccessibleDescription(description);
  await expect(admin).toHaveAccessibleName("Admin");
});

test("paging the users directory from the keyboard ends on Previous, not the document", async ({ page }) => {
  await page.goto("/admin/?section=users");
  await createUserFromKeyboard(page, "pat@open-splunk.test", "pat");
  await createUserFromKeyboard(page, "quinn@open-splunk.test", "quinn");
  const pager = page.getByRole("navigation", { name: "Users pages" });
  const next = pager.getByRole("button", { name: "Next" });
  await next.focus();
  await page.keyboard.press("Enter");
  await expect(pager).toContainText("Page 2 of 2");
  await expect(next).toBeDisabled();
  await expect(pager.getByRole("button", { name: "Previous" })).toBeFocused();
});

test("a member's forbidden page is one labelled region under one h1", async ({ page }) => {
  await actAs(page, MEMBER_USER_ID);
  await page.goto("/admin/?section=server");
  const region = page.getByRole("region", { name: "You do not have access to Administration" });
  await expect(region).toBeVisible();
  await expect(page.getByRole("heading", { level: 1 })).toHaveCount(1);
  await page.getByRole("button", { name: "Switch organization", exact: true }).click();
  const menu = page.getByRole("menu", { name: "Organizations" });
  await expect(menu.getByRole("menuitemradio").first()).toBeFocused();
  await page.keyboard.press("Escape");
  await expect(menu).toHaveCount(0);
});
