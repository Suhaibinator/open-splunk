import { expect, test, type Page } from "@playwright/test";

// The demo export's identity is the in-memory fake, starting signed in as the
// seeded site admin (owner of Acme Operations); its session lives in
// sessionStorage under this key, which is how a test signs in as someone else.
const DEMO_IDENTITY_STORAGE_KEY = "open-splunk.demo-identity-session";

async function signInAs(page: Page, userId: string): Promise<void> {
  await page.addInitScript(([key, value]) => {
    window.sessionStorage.setItem(key, value);
  }, [DEMO_IDENTITY_STORAGE_KEY, JSON.stringify({ v: 1, userId, activeOrganizationId: null })] as const);
}

async function openMembers(page: Page): Promise<void> {
  await page.goto("/admin/?section=members");
  await expect(page.getByRole("heading", { level: 2, name: "Members & invitations" })).toBeVisible();
}

test("the sidebar opens Members & invitations for the active organization", async ({ page }) => {
  await page.goto("/admin/");
  await page.getByRole("button", { name: /Members & invitations/u }).click();
  await expect(page).toHaveURL(/section=members/u);
  const members = page.getByRole("table", { name: "Members of Acme Operations" });
  await expect(members.getByRole("row")).toHaveCount(8);
  await expect(members.getByRole("row", { name: /Ada Lovelace \(you\)/u })).toContainText("Last owner");
  await expect(page.getByRole("button", { name: "Change role for Ada Lovelace" })).toHaveAttribute("aria-disabled", "true");
  const invitations = page.getByRole("table", { name: "Pending invitations" });
  await expect(invitations.getByRole("row", { name: /late@open-splunk\.test/u })).toContainText("Expired");
  await expect(invitations.getByRole("row", { name: /nora@open-splunk\.test/u })).toContainText("Pending");
});

test("an invitation is sent from the keyboard and lands in the pending list", async ({ page }) => {
  await openMembers(page);
  const invite = page.getByRole("button", { name: "Invite member" });
  await invite.click();
  const dialog = page.getByRole("dialog", { name: "Invite member" });
  const email = dialog.getByRole("textbox", { name: "Email address" });
  await expect(email).toBeFocused();

  await dialog.getByRole("button", { name: "Send invitation" }).click();
  await expect(email).toHaveAttribute("aria-invalid", "true");
  await expect(email).toBeFocused();
  await expect(dialog).toContainText("Enter the email address to invite.");

  await email.fill("new.analyst@example.com");
  await dialog.getByRole("radio", { name: /Moderator/u }).check();
  await email.press("Enter");
  await expect(dialog).toBeHidden();
  await expect(invite).toBeFocused();
  await expect(page.locator("[data-members-announcement]")).toHaveText("Invitation sent to new.analyst@example.com as moderator.");
  await expect(page.getByRole("table", { name: "Pending invitations" }).getByRole("row", { name: /new\.analyst@example\.com/u })).toContainText("Moderator");

  await invite.click();
  await dialog.getByRole("textbox", { name: "Email address" }).fill("new.analyst@example.com");
  await dialog.getByRole("button", { name: "Send invitation" }).click();
  await expect(dialog.getByRole("textbox", { name: "Email address" })).toHaveAccessibleDescription(/already has a pending invitation/u);
});

test("a role change names what it grants and updates the row", async ({ page }) => {
  await openMembers(page);
  await page.getByRole("button", { name: "Change role for Mona Moreau" }).click();
  const dialog = page.getByRole("dialog", { name: "Change role for Mona Moreau" });
  await expect(dialog.getByRole("radio", { name: /Moderator \(current\)/u })).toBeFocused();
  await expect(dialog.getByRole("button", { name: "Save role" })).toBeDisabled();
  await page.keyboard.press("ArrowDown");
  await expect(dialog.getByRole("radio", { name: /^Admin/u })).toBeChecked();
  await expect(dialog).toContainText("Manage members");
  await dialog.getByRole("button", { name: "Save role" }).click();
  await expect(dialog).toBeHidden();
  const row = page.getByRole("row", { name: /Mona Moreau/u });
  await expect(row.locator('td[data-label="Explicit role"]')).toHaveText("Admin");
  await expect(page.getByRole("button", { name: "Change role for Mona Moreau" })).toBeFocused();
});

test("removing a member is confirmed first and Escape backs out", async ({ page }) => {
  await openMembers(page);
  const remove = page.getByRole("button", { name: "Remove Mark Mendez" });
  await remove.click();
  const dialog = page.getByRole("dialog", { name: "Remove Mark Mendez?" });
  await expect(dialog).toContainText("loses their member role in Acme Operations immediately");
  await page.keyboard.press("Escape");
  await expect(dialog).toBeHidden();
  await expect(remove).toBeFocused();

  await remove.click();
  await dialog.getByRole("button", { name: "Remove member" }).click();
  await expect(dialog).toBeHidden();
  await expect(page.getByRole("row", { name: /Mark Mendez/u })).toHaveCount(0);
  await expect(page.getByRole("heading", { level: 3, name: "Members" })).toBeFocused();
  await expect(page.locator("[data-members-announcement]")).toHaveText("Mark Mendez was removed from Acme Operations.");
});

test("a pending invitation is revoked after confirmation", async ({ page }) => {
  await openMembers(page);
  await page.getByRole("button", { name: "Revoke invitation for late@open-splunk.test" }).click();
  const dialog = page.getByRole("dialog", { name: "Revoke invitation?" });
  await dialog.getByRole("button", { name: "Revoke invitation" }).click();
  await expect(dialog).toBeHidden();
  await expect(page.getByRole("row", { name: /late@open-splunk\.test/u })).toHaveCount(0);
  await expect(page.getByRole("heading", { level: 3, name: "Pending invitations" })).toBeFocused();
});

test("a member without members.admin sees why the panel is closed", async ({ page }) => {
  await signInAs(page, "5");
  await openMembers(page);
  await expect(page.locator('[data-members-state="forbidden"]')).toBeVisible();
  await expect(page.getByText("Members are managed by admins")).toBeVisible();
  await expect(page.getByRole("button", { name: "Invite member" })).toHaveCount(0);
  await expect(page.getByRole("table")).toHaveCount(0);
});

test("an admin cannot offer a role above its own", async ({ page }) => {
  await signInAs(page, "3");
  await openMembers(page);
  await page.getByRole("button", { name: "Invite member" }).click();
  const dialog = page.getByRole("dialog", { name: "Invite member" });
  await expect(dialog.getByRole("radio", { name: /^Admin/u })).toBeDisabled();
  await expect(dialog.getByRole("radio", { name: /^Owner/u })).toBeDisabled();
  await expect(dialog.getByRole("radio", { name: /^Moderator/u })).toBeEnabled();
});

test("the members tables fold into cards without a horizontal scroll on a phone", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await openMembers(page);
  await expect(page.getByRole("table", { name: "Members of Acme Operations" })).toBeVisible();
  const overflow = await page.locator(".members-panel").evaluate((element) => element.scrollWidth - element.clientWidth);
  expect(overflow).toBeLessThanOrEqual(0);
  const display = await page.locator(".members-table").first().evaluate((element) => getComputedStyle(element).display);
  expect(display).toBe("block");
});
