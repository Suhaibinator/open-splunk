import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the harness installs the fake browser before
// `react-dom/client` is evaluated anywhere in this process.
import {
  clickButton,
  findByText,
  focusedId,
  linkHref,
  mount,
  settle,
  submitForm,
  typeInto,
} from "@/lib/testing/react-mount";
import { StrictMode } from "react";

import { IdentityError, type IdentityClient, type OAuthCallbackParams } from "@/lib/api/identity";
import {
  FAKE_IDENTITY_OTP_CODE,
  FAKE_IDENTITY_PASSWORD,
  FAKE_LOGIN_ATTEMPT_LIMIT,
  createFakeIdentityClient,
  type FakeIdentityClient,
} from "@/lib/api/identity-fake";

import { IdentityProvider } from "../_components/identity-provider";
import { IdentitySignIn } from "./identity-sign-in";
import { OAuthCallback } from "./oauth-callback";

const SIGN_IN_FORM = "Sign in with email";
const OTP_FORM = "Verification code";

function recorder(): { readonly visits: string[]; readonly navigate: (url: string) => void } {
  const visits: string[] = [];
  return { visits, navigate: (url) => { visits.push(url); } };
}

async function mountSignIn(client: IdentityClient, returnTo = "/search/", demo = false) {
  const route = recorder();
  const view = await mount(
    <IdentityProvider client={client}>
      <IdentitySignIn demo={demo} navigate={route.navigate} returnTo={returnTo} />
    </IdentityProvider>,
  );
  await settle();
  return { ...view, ...route, text: () => view.container.textContent };
}

async function signInWith(view: Awaited<ReturnType<typeof mountSignIn>>, email: string, password: string): Promise<void> {
  await typeInto(view.container, "signin-email", email);
  await typeInto(view.container, "signin-password", password);
  await submitForm(view.container, SIGN_IN_FORM);
}

test("the credentials step checks its fields in place before it asks the server", async () => {
  const client = createFakeIdentityClient();
  let logins = 0;
  const counting = Object.create(client) as IdentityClient;
  counting.loginWithEmail = (credentials) => { logins += 1; return client.loginWithEmail(credentials); };
  const view = await mountSignIn(counting);
  assert.equal(focusedId(), "signin-email", "the step opens on the empty email field");

  await submitForm(view.container, SIGN_IN_FORM);
  assert.equal(view.text().includes("Enter your email address."), true);
  assert.equal(view.text().includes("Enter your password."), true);
  assert.equal(focusedId(), "signin-email", "focus goes to the first field to fix");
  assert.equal(view.container.querySelector("#signin-email")?.getAttribute("aria-invalid"), "true");

  await typeInto(view.container, "signin-email", "not-an-address");
  await submitForm(view.container, SIGN_IN_FORM);
  assert.equal(view.text().includes("Enter a valid email address"), true);
  assert.equal(logins, 0, "nothing invalid reached the server");
  await view.unmount();
});

test("a wrong password is reported, cleared and offers a reset; the right one goes to returnTo", async () => {
  const view = await mountSignIn(createFakeIdentityClient(), "/dashboards/?id=7");
  await signInWith(view, "alice@open-splunk.test", "wrong-password");
  assert.equal(view.text().includes("Email or password is incorrect"), true);
  assert.equal(findByText(view.container, "[role=\"alert\"]", "Email or password is incorrect") === null, false, "the failure is announced");
  assert.equal(view.container.querySelector("#signin-password")?.value, "");
  assert.equal(focusedId(), "signin-password");
  assert.equal(linkHref(view.container, "Reset your password"), "/signin/reset?email=alice%40open-splunk.test");
  assert.equal(view.visits.length, 0);

  await typeInto(view.container, "signin-password", FAKE_IDENTITY_PASSWORD);
  await submitForm(view.container, SIGN_IN_FORM);
  assert.deepEqual(view.visits, ["/dashboards/?id=7"]);
  assert.equal(view.text().includes("Welcome, Alice Abara"), true);
  await view.unmount();
});

test("a suspended account and an unverified address each say what to do", async () => {
  const client = createFakeIdentityClient();
  const view = await mountSignIn(client);
  await signInWith(view, "bart@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  assert.equal(view.text().includes("This account is suspended"), true);
  assert.equal(linkHref(view.container, "Reset your password"), null, "a reset would not help a suspended account");

  await client.registerWithEmail({ email: "fresh@open-splunk.test", password: "long-enough-1", username: "fresh", firstName: "Fresh", lastName: "Face" });
  await signInWith(view, "fresh@open-splunk.test", "long-enough-1");
  assert.equal(view.text().includes("Verify your email address first"), true);
  assert.equal(view.visits.length, 0);
  await view.unmount();
});

test("a rate-limited sign-in holds the button with a countdown instead of disabling it", async () => {
  const client = createFakeIdentityClient();
  const failures = Array.from({ length: FAKE_LOGIN_ATTEMPT_LIMIT }, () => client.loginWithEmail({ email: "mona@open-splunk.test", password: "wrong" }));
  const settled = await Promise.allSettled(failures);
  assert.equal(settled.every((outcome) => outcome.status === "rejected"), true, "the earlier attempts all failed");
  const view = await mountSignIn(client);
  await signInWith(view, "mona@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  assert.equal(view.text().includes("Too many attempts"), true);
  const button = findByText(view.container, "button", "Try again in");
  assert.equal(button === null, false, "the submit button counts down");
  assert.equal(button?.getAttribute("aria-disabled"), "true");
  assert.equal(button?.hasAttribute("disabled"), false, "a disabled button would drop keyboard focus");
  await view.unmount();
});

test("a network failure keeps the form and says the service could not be reached", async () => {
  const client = createFakeIdentityClient();
  const view = await mountSignIn(client);
  client.failNext("loginWithEmail");
  await signInWith(view, "alice@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  assert.equal(view.text().includes("The identity service could not be reached"), true);
  assert.equal(view.container.querySelector("#signin-email")?.value, "alice@open-splunk.test");
  await submitForm(view.container, SIGN_IN_FORM);
  assert.deepEqual(view.visits, ["/search/"], "the same form succeeds once the service answers");
  await view.unmount();
});

test("the second step asks for the authenticator code, rejects a wrong one and signs in on the right one", async () => {
  const view = await mountSignIn(createFakeIdentityClient(), "/reports/");
  await signInWith(view, "tess@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  assert.equal(view.text().includes("Two-step verification"), true);
  assert.equal(view.text().includes("tess@open-splunk.test"), true);
  assert.equal(focusedId(), "signin-otp");

  await typeInto(view.container, "signin-otp", "12");
  await submitForm(view.container, OTP_FORM);
  assert.equal(view.text().includes("Enter the 6-digit code"), true);

  await typeInto(view.container, "signin-otp", "000000");
  await submitForm(view.container, OTP_FORM);
  assert.equal(view.text().includes("That code was not accepted"), true);
  assert.equal(view.container.querySelector("#signin-otp")?.value, "", "a refused code is cleared");
  assert.equal(focusedId(), "signin-otp");

  await typeInto(view.container, "signin-otp", ` ${FAKE_IDENTITY_OTP_CODE.slice(0, 3)} ${FAKE_IDENTITY_OTP_CODE.slice(3)}`);
  await submitForm(view.container, OTP_FORM);
  assert.deepEqual(view.visits, ["/reports/"]);
  await view.unmount();
});

test("leaving the second step returns to the credentials with the address kept", async () => {
  const view = await mountSignIn(createFakeIdentityClient());
  await signInWith(view, "tess@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  await clickButton(view.container, "Use a different account");
  assert.equal(view.container.querySelector("#signin-email")?.value, "tess@open-splunk.test");
  assert.equal(view.container.querySelector("#signin-otp") === null, true);
  await view.unmount();
});

test("an expired second-factor challenge sends the user back to sign in again", async () => {
  const client = createFakeIdentityClient();
  const view = await mountSignIn(client);
  await signInWith(view, "tess@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  client.failNext("verifyLoginOtp", new IdentityError("invalid", "This sign-in attempt has expired."));
  await typeInto(view.container, "signin-otp", FAKE_IDENTITY_OTP_CODE);
  await submitForm(view.container, OTP_FORM);
  assert.equal(view.container.querySelector("#signin-email") === null, false, "back on the credentials step");
  assert.equal(view.text().includes("This sign-in attempt expired"), true);
  await view.unmount();
});

test("provider buttons start OAuth with the validated return path; none configured renders none", async () => {
  const client = createFakeIdentityClient();
  const view = await mountSignIn(client, "/alerts/");
  assert.equal(findByText(view.container, "button", "Continue with GitHub") === null, false);
  assert.equal(findByText(view.container, "button", "Continue with Okta") === null, false);
  await clickButton(view.container, "Continue with GitHub");
  assert.equal(view.visits.length, 1);
  const target = new URL(view.visits[0] ?? "", "https://example.invalid");
  assert.equal(target.pathname, "/auth/oauth/callback/");
  assert.equal(target.searchParams.get("state")?.startsWith("fake-oauth-state-"), true);
  await view.unmount();

  const none = Object.create(createFakeIdentityClient()) as IdentityClient;
  none.listOAuthProviders = async () => [];
  const bare = await mountSignIn(none);
  assert.equal(findByText(bare.container, "button", "Continue with") === null, true);
  assert.equal(bare.text().includes("or use email"), false, "no divider promises a provider");
  await bare.unmount();
});

test("a provider list that fails to load says so and retries", async () => {
  const client = createFakeIdentityClient();
  client.failNext("listOAuthProviders");
  const view = await mountSignIn(client);
  assert.equal(view.text().includes("Single sign-on providers could not be loaded"), true);
  assert.equal(view.container.querySelector("#signin-email") === null, false, "email sign-in still works");
  await clickButton(view.container, "Try again");
  assert.equal(findByText(view.container, "button", "Continue with GitHub") === null, false);
  await view.unmount();
});

test("a signed-in visitor continues or switches accounts", async () => {
  const view = await mountSignIn(createFakeIdentityClient({ initialSignedInEmail: "mona@open-splunk.test" }), "/search/events/");
  assert.equal(view.text().includes("You're already signed in"), true);
  assert.equal(view.text().includes("mona@open-splunk.test"), true);
  await clickButton(view.container, "Continue as Mona");
  assert.deepEqual(view.visits, ["/search/events/"]);
  await view.unmount();

  const other = await mountSignIn(createFakeIdentityClient({ initialSignedInEmail: "mona@open-splunk.test" }));
  await clickButton(other.container, "Sign in with a different account");
  assert.equal(other.container.querySelector("#signin-email") === null, false);
  assert.equal(other.visits.length, 0);
  await other.unmount();
});

test("a user with no organization sees their pending invitations instead of an empty product", async () => {
  const view = await mountSignIn(createFakeIdentityClient());
  await signInWith(view, "nora@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  assert.equal(view.visits.length, 0);
  assert.equal(view.text().includes("You're not in an organization yet"), true);
  assert.equal(view.text().includes("Acme Operations"), true);
  await clickButton(view.container, "Review");
  assert.deepEqual(view.visits, ["/auth/invitation/?id=1&email=nora%40open-splunk.test"]);
  await view.unmount();
});

test("an identity service that cannot be reached offers a retry", async () => {
  const client = createFakeIdentityClient();
  client.failNext("getPrincipal");
  const view = await mountSignIn(client);
  assert.equal(view.text().includes("Sign-in is unavailable"), true);
  await clickButton(view.container, "Try again");
  assert.equal(view.container.querySelector("#signin-email") === null, false);
  await view.unmount();
});

test("demo accounts fill the form; a connected server shows none", async () => {
  const view = await mountSignIn(createFakeIdentityClient(), "/", true);
  assert.equal(view.text().includes("Demo accounts and flows"), true);
  await clickButton(view.container, "Use");
  assert.equal(view.container.querySelector("#signin-email")?.value, "ada@open-splunk.test");
  assert.equal(view.container.querySelector("#signin-password")?.value, FAKE_IDENTITY_PASSWORD);
  await view.unmount();

  const connected = await mountSignIn(createFakeIdentityClient());
  assert.equal(connected.text().includes("Demo accounts"), false);
  await connected.unmount();
});

// --------------------------------------------------------------------------
// The OAuth callback page
// --------------------------------------------------------------------------

async function startOAuth(client: FakeIdentityClient, slug: string, returnTo: string): Promise<OAuthCallbackParams> {
  const { authorizationUrl } = await client.beginOAuth(slug, returnTo);
  const query = new URL(authorizationUrl, "https://example.invalid").searchParams;
  return { code: query.get("code"), state: query.get("state"), error: null, errorDescription: null };
}

async function mountCallback(client: IdentityClient, params: OAuthCallbackParams) {
  const route = recorder();
  const view = await mount(
    <StrictMode>
      <IdentityProvider client={client}>
        <OAuthCallback navigate={route.navigate} params={params} />
      </IdentityProvider>
    </StrictMode>,
  );
  await settle();
  return { ...view, ...route, text: () => view.container.textContent };
}

test("the callback exchanges its code exactly once, even under StrictMode, and continues to returnTo", async () => {
  const client = createFakeIdentityClient();
  let exchanges = 0;
  const counting = Object.create(client) as IdentityClient;
  counting.completeOAuthCallback = (params) => { exchanges += 1; return client.completeOAuthCallback(params); };
  const params = await startOAuth(client, "github", "/dashboards/?id=7");
  const view = await mountCallback(counting, params);
  assert.equal(exchanges, 1, "a one-time code reaches the server once");
  assert.deepEqual(view.visits, ["/dashboards/?id=7"]);
  assert.equal(view.text().includes("Welcome, Mark Mendez"), true);
  await view.unmount();

  const replay = await mountCallback(client, params);
  assert.equal(replay.text().includes("This sign-in link has expired"), true);
  assert.equal(linkHref(replay.container, "Back to sign in"), "/signin");
  assert.equal(replay.visits.length, 0);
  await replay.unmount();
});

test("the callback re-validates the return path the server hands back", async () => {
  const client = createFakeIdentityClient();
  const hostile = Object.create(client) as IdentityClient;
  hostile.completeOAuthCallback = async (params) => ({ ...(await client.completeOAuthCallback(params)), returnTo: "//evil.example/steal" });
  const view = await mountCallback(hostile, await startOAuth(client, "github", "/search/"));
  assert.deepEqual(view.visits, ["/"]);
  await view.unmount();
});

test("a provider refusal is explained without echoing the callback's query text", async () => {
  const view = await mountCallback(createFakeIdentityClient(), {
    code: null,
    state: "fake-oauth-state-1",
    error: "access_denied",
    errorDescription: "Call 555-0100 to unlock your account",
  });
  assert.equal(view.text().includes("The provider did not sign you in"), true);
  assert.equal(view.text().includes("555-0100"), false);
  assert.equal(view.visits.length, 0);
  await view.unmount();
});

test("a provider account linked to someone else, and a rate limit, each have their own copy", async () => {
  const client = createFakeIdentityClient();
  client.failNext("completeOAuthCallback", new IdentityError("conflict", "This provider account is linked to a different user."));
  const conflict = await mountCallback(client, await startOAuth(client, "github", "/"));
  assert.equal(conflict.text().includes("This provider account belongs to someone else"), true);
  await conflict.unmount();

  client.failNext("completeOAuthCallback", new IdentityError("rate_limited", "slow down", { retryAfterMs: 30_000 }));
  const limited = await mountCallback(client, await startOAuth(client, "github", "/"));
  assert.equal(limited.text().includes("Too many sign-in attempts"), true);
  assert.equal(limited.text().includes("30 seconds"), true);
  await limited.unmount();
});

test("a first provider sign-in with no organization lands on the invitations card", async () => {
  const client = createFakeIdentityClient();
  const view = await mountCallback(client, await startOAuth(client, "okta", "/search/"));
  assert.equal(view.visits.length, 0);
  assert.equal(view.text().includes("You're not in an organization yet"), true);
  assert.equal(view.text().includes("No pending invitations"), true);
  await view.unmount();
});
