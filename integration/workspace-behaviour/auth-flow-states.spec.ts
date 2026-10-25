import { expect, test, type Page } from "@playwright/test";

// The hard states of the sign-in flows in a real browser: return paths the
// URL parser and the App Router read for themselves, history after a one-time
// step, and the provider callback opened by hand. The demo export signs every
// tab in as the seeded site admin; a flow that needs a signed-out tab signs
// out from the sign-in page first and then moves only by client-side
// navigation, so the in-page demo directory survives.

const PASSWORD = "open-splunk-demo";

async function signOutFirst(page: Page, path = "/signin/"): Promise<void> {
  await page.goto(path);
  await expect(page.getByRole("heading", { name: "You're already signed in" })).toBeVisible();
  await page.getByRole("button", { name: "Sign in with a different account" }).click();
  await expect(page.getByRole("heading", { name: "Sign in", exact: true })).toBeVisible();
}

async function signIn(page: Page, email: string): Promise<void> {
  await page.getByLabel("Email", { exact: true }).fill(email);
  await page.getByLabel("Password", { exact: true }).fill(PASSWORD);
  await page.getByRole("button", { name: "Sign in", exact: true }).click();
}

/** Enters `count` wrong authenticator codes one after another, each refused in place. */
async function refuseCodes(page: Page, count: number): Promise<void> {
  if (count === 0) return;
  const code = page.getByLabel("Authenticator code");
  await code.fill("000000");
  await code.press("Enter");
  await expect(code).toHaveAccessibleDescription(/That code was not accepted/u);
  await refuseCodes(page, count - 1);
}

/** Every JavaScript dialog the page opens, dismissed; a test expects none. */
function watchDialogs(page: Page): string[] {
  const opened: string[] = [];
  page.on("dialog", (dialog) => {
    opened.push(dialog.message());
    void dialog.dismiss();
  });
  return opened;
}

for (const returnTo of [
  "javascript:alert(document.domain)",
  "/\\evil.example/steal",
  "/\t/evil.example/steal",
  "https:evil.example",
  "/%2e%2e//evil.example",
  "/auth//oauth/callback/?code=fake-oauth-code-github&state=x",
  "/x/..%2fsignin/",
]) {
  test(`returnTo ${JSON.stringify(returnTo)} lands on the product root of this origin`, async ({ page, baseURL }) => {
    const dialogs = watchDialogs(page);
    await signOutFirst(page, `/signin/?returnTo=${encodeURIComponent(returnTo)}`);
    await signIn(page, "mona@open-splunk.test");
    await expect.poll(() => new URL(page.url()).pathname).toBe("/");
    expect(new URL(page.url()).origin).toBe(new URL(baseURL ?? "http://127.0.0.1").origin);
    expect(page.url()).not.toContain("evil.example");
    expect(dialogs).toEqual([]);
  });
}

// An encoded backslash is not a separator to the URL parser, so it stays a
// literal path segment on this origin (return-to-hostile.test.ts holds the
// same contract); it never names evil.example as a host.
test("returnTo \"/%5Cevil.example/steal\" stays a path on this origin", async ({ page, baseURL }) => {
  const dialogs = watchDialogs(page);
  await signOutFirst(page, `/signin/?returnTo=${encodeURIComponent("/%5Cevil.example/steal")}`);
  await signIn(page, "mona@open-splunk.test");
  await expect.poll(() => new URL(page.url()).pathname).toBe("/%5Cevil.example/steal");
  const landed = new URL(page.url());
  expect(landed.origin).toBe(new URL(baseURL ?? "http://127.0.0.1").origin);
  expect(landed.host).not.toContain("evil.example");
  expect(dialogs).toEqual([]);
});

test("Back after a password sign-in returns to the page before sign-in, not to the sign-in card", async ({ page }) => {
  await page.goto("/help/");
  // /datasets/ renders where it lands; /reports/ would canonicalise itself to
  // /reports/saved-searches/ and race the URL assertion.
  await signOutFirst(page, "/signin/?returnTo=%2Fdatasets%2F");
  await signIn(page, "mona@open-splunk.test");
  await expect(page).toHaveURL(/\/datasets\/$/u);
  await page.goBack();
  await expect(page).toHaveURL(/\/help\/$/u);
});

test("Back after the OAuth round trip never reopens the spent callback", async ({ page }) => {
  await signOutFirst(page, "/signin/?returnTo=%2Fdashboards%2F");
  await page.getByRole("button", { name: "Continue with GitHub" }).click();
  await expect(page).toHaveURL(/\/dashboards\/$/u);
  await page.goBack();
  expect(new URL(page.url()).pathname).not.toBe("/auth/oauth/callback/");
  await expect(page.getByRole("heading", { name: "Sign-in did not finish" })).toHaveCount(0);
});

test("a provider error opened by hand explains itself without echoing the link's text", async ({ page }) => {
  const dialogs = watchDialogs(page);
  const description = "<img src=x onerror=alert(1)> Call 555-0100 to restore access";
  await page.goto(`/auth/oauth/callback/?error=access_denied&state=x&error_description=${encodeURIComponent(description)}`);
  await expect(page.getByRole("heading", { name: "Sign-in did not finish" })).toBeVisible();
  await expect(page.getByText("The provider did not sign you in")).toBeVisible();
  await expect(page.getByText("555-0100")).toHaveCount(0);
  await expect(page.locator("img[src='x']")).toHaveCount(0);
  expect(dialogs).toEqual([]);
});

test("a bare callback opened by a signed-in tab offers to continue with the session it still has", async ({ page }) => {
  await page.goto("/auth/oauth/callback/");
  await expect(page.getByRole("heading", { name: "Sign-in did not finish" })).toBeVisible();
  await expect(page.getByText("You are still signed in.")).toBeVisible();
  await expect(page.getByText("ada@open-splunk.test")).toBeVisible();
  await page.getByRole("link", { name: "Continue to Open Splunk" }).click();
  await expect.poll(() => new URL(page.url()).pathname).toBe("/");
});

test("too many wrong authenticator codes hold sign-in with a focusable countdown", async ({ page }) => {
  await signOutFirst(page);
  await signIn(page, "tess@open-splunk.test");
  // The demo discards a challenge on its fifth wrong code.
  await refuseCodes(page, 4);
  const code = page.getByLabel("Authenticator code");
  await code.fill("000000");
  await code.press("Enter");
  await expect(page.getByRole("alert").filter({ hasText: "Too many codes tried" })).toBeVisible();
  const held = page.getByRole("button", { name: /Try again in 1 minute/u });
  await expect(held).toHaveAttribute("aria-disabled", "true");
  // aria-disabled, not the native attribute: Playwright reads either as
  // "disabled", so the native one is checked directly to prove the countdown
  // keeps its place in the Tab order.
  expect(await held.evaluate((button) => (button as HTMLButtonElement).disabled), "the held button is not natively disabled").toBe(false);
  await held.focus();
  await expect(held).toBeFocused();
  await held.press("Enter");
  // The held button sends nothing: the same card and countdown remain.
  await expect(page.getByRole("button", { name: /Try again in/u })).toHaveAttribute("aria-disabled", "true");
  await expect(page.getByLabel("Authenticator code")).toHaveCount(0);
});

test("an invitation link with no id says it is incomplete", async ({ page }) => {
  await page.goto("/auth/invitation/");
  await expect(page.getByRole("heading", { name: "This invitation link is incomplete" })).toBeVisible();
  await expect(page.getByRole("link", { name: "Go to sign in" })).toBeVisible();
});
