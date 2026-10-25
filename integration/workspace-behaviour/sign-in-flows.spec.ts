import { expect, test, type Page } from "@playwright/test";

// The demo export signs every tab in as the seeded site admin, so each flow
// starts by signing out from the sign-in page. The demo directory lives in the
// page realm: every step below moves by client-side navigation (a link, a
// button, a redirect the page makes), never by `page.goto`, until the flow is
// finished, or the emailed tokens the demo minted are gone.

const PASSWORD = "open-splunk-demo";
const OTP_CODE = "123456";

async function signOutFirst(page: Page, path = "/signin/"): Promise<void> {
  await page.goto(path);
  await expect(page.getByRole("heading", { name: "You're already signed in" })).toBeVisible();
  await page.getByRole("button", { name: "Sign in with a different account" }).click();
  await expect(page.getByRole("heading", { name: "Sign in", exact: true })).toBeVisible();
}

async function fillCredentials(page: Page, email: string, password: string): Promise<void> {
  await page.getByLabel("Email", { exact: true }).fill(email);
  await page.getByLabel("Password", { exact: true }).fill(password);
}

test("a wrong password is announced and the right one returns to the page that asked", async ({ page }) => {
  await signOutFirst(page, "/signin/?returnTo=%2Fsearch%2Fevents%2F");
  await fillCredentials(page, "alice@open-splunk.test", "not-the-password");
  await page.getByLabel("Password", { exact: true }).press("Enter");
  const alert = page.getByRole("alert").filter({ hasText: "Email or password is incorrect" });
  await expect(alert).toBeVisible();
  await expect(alert.getByRole("link", { name: "Reset your password" })).toBeVisible();
  await expect(page.getByLabel("Password", { exact: true })).toBeFocused();
  await expect(page.getByLabel("Password", { exact: true })).toHaveValue("");

  await page.getByLabel("Password", { exact: true }).fill(PASSWORD);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page).toHaveURL(/\/search\/events\/$/u);
});

test("an off-site returnTo is ignored after sign-in", async ({ page, baseURL }) => {
  await signOutFirst(page, "/signin/?returnTo=%2F%2Fevil.example%2Fsteal");
  await fillCredentials(page, "mona@open-splunk.test", PASSWORD);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  // The product root, on this origin.
  await expect.poll(() => new URL(page.url()).pathname).toBe("/");
  expect(new URL(page.url()).origin).toBe(new URL(baseURL ?? "http://127.0.0.1").origin);
  expect(page.url()).not.toContain("evil.example");
});

test("an account with an authenticator asks for the code on a second step", async ({ page }) => {
  await signOutFirst(page, "/signin/?returnTo=%2Freports%2F");
  await fillCredentials(page, "tess@open-splunk.test", PASSWORD);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page.getByRole("heading", { name: "Two-step verification" })).toBeVisible();
  const code = page.getByLabel("Authenticator code");
  await expect(code).toBeFocused();
  await code.fill("000000");
  await code.press("Enter");
  // The field note shows the refusal and the form's alert announces it.
  await expect(code).toHaveAccessibleDescription(/That code was not accepted/u);
  await expect(page.getByRole("form", { name: "Verification code" }).getByRole("alert")).toContainText("That code was not accepted");
  await code.fill(OTP_CODE);
  await page.getByRole("button", { name: "Verify and sign in" }).click();
  await expect(page).toHaveURL(/\/reports\/$/u);
});

test("a provider button runs the OAuth round trip through the callback page", async ({ page }) => {
  await signOutFirst(page, "/signin/?returnTo=%2Fdashboards%2F");
  await page.getByRole("button", { name: "Continue with GitHub" }).click();
  await expect(page).toHaveURL(/\/dashboards\/$/u);
});

test("an OAuth callback carrying a state nobody issued says the link is spent", async ({ page }) => {
  await page.goto("/auth/oauth/callback/?code=fake-oauth-code-github&state=never-issued");
  await expect(page.getByRole("heading", { name: "Sign-in did not finish" })).toBeVisible();
  await expect(page.getByText("This sign-in link has expired")).toBeVisible();
  await expect(page.getByRole("link", { name: "Back to sign in" })).toBeVisible();
});

test("a forgotten password is reset from the emailed link and the new one signs in", async ({ page }) => {
  await signOutFirst(page);
  await page.getByLabel("Email", { exact: true }).fill("alice@open-splunk.test");
  await page.getByRole("link", { name: "Forgot password?" }).click();
  await expect(page.getByRole("heading", { name: "Reset your password" })).toBeVisible();
  await expect(page.getByLabel("Email", { exact: true })).toHaveValue("alice@open-splunk.test");
  await page.getByRole("button", { name: "Send reset link" }).click();
  await expect(page.getByRole("heading", { name: "Check your email" })).toBeVisible();
  await page.getByRole("link", { name: "Open the reset link" }).click();

  await expect(page.getByRole("heading", { name: "Choose a new password" })).toBeVisible();
  await page.getByLabel("New password", { exact: true }).fill("short");
  await page.getByLabel("Confirm password", { exact: true }).fill("short");
  await page.getByRole("button", { name: "Save new password" }).click();
  await expect(page.getByText("Passwords need at least 8 characters.")).toBeVisible();
  await page.getByLabel("New password", { exact: true }).fill("a-long-new-passphrase");
  await page.getByLabel("Confirm password", { exact: true }).fill("a-long-new-passphrase");
  await page.getByRole("button", { name: "Save new password" }).click();
  await expect(page.getByRole("heading", { name: "Password updated" })).toBeVisible();

  await page.getByRole("link", { name: "Sign in", exact: true }).click();
  await fillCredentials(page, "alice@open-splunk.test", "a-long-new-passphrase");
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect.poll(() => new URL(page.url()).pathname).toBe("/");
});

test("an invited address with no account registers, verifies and accepts", async ({ page }) => {
  await signOutFirst(page);
  await page.getByText("Demo accounts and flows").click();
  await page.getByRole("link", { name: "Accept an invitation as a new user" }).click();
  await expect(page.getByRole("heading", { name: "You're invited to Open Splunk" })).toBeVisible();
  await page.getByRole("button", { name: /create an account/u }).click();

  await expect(page.getByLabel("Email", { exact: true })).toHaveValue("new.analyst@open-splunk.test");
  await expect(page.getByLabel("First name")).toBeFocused();
  await page.getByLabel("First name").fill("Nia");
  await page.getByLabel("Last name").fill("Analyst");
  await page.getByLabel("Password", { exact: true }).fill("a-long-new-passphrase");
  await page.getByRole("button", { name: "Create account" }).click();
  await expect(page.getByRole("heading", { name: "Verify your email" })).toBeVisible();
  await page.getByRole("link", { name: "Open the verification link" }).click();

  await expect(page.getByRole("heading", { name: "Join Globex Security" })).toBeVisible();
  await page.getByRole("button", { name: "Accept invitation" }).click();
  await expect(page.getByRole("heading", { name: "You joined Globex Security" })).toBeVisible();
  await page.getByRole("button", { name: "Open Globex Security" }).click();
  await expect(page).not.toHaveURL(/\/auth\//u);
});

test("an invitation for a user with no organization is accepted after signing in", async ({ page }) => {
  await signOutFirst(page);
  await fillCredentials(page, "nora@open-splunk.test", PASSWORD);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
  await expect(page.getByRole("heading", { name: "You're not in an organization yet" })).toBeVisible();
  await page.getByRole("button", { name: /Review/u }).click();
  await expect(page.getByRole("heading", { name: "Join Acme Operations" })).toBeVisible();
  await page.getByRole("button", { name: "Decline" }).click();
  await expect(page.getByRole("heading", { name: "Invitation declined" })).toBeVisible();
});

test("the sign-in card works from the keyboard alone at phone width", async ({ page }) => {
  await page.setViewportSize({ width: 390, height: 844 });
  await signOutFirst(page);
  const overflow = await page.evaluate(() => document.documentElement.scrollWidth - document.documentElement.clientWidth);
  expect(overflow).toBeLessThanOrEqual(0);
  const email = page.getByLabel("Email", { exact: true });
  await email.focus();
  await page.keyboard.type("mark@open-splunk.test");
  await page.keyboard.press("Tab");
  await expect(page.getByLabel("Password", { exact: true })).toBeFocused();
  await page.keyboard.type(PASSWORD);
  await page.keyboard.press("Enter");
  await expect.poll(() => new URL(page.url()).pathname).toBe("/");
});
