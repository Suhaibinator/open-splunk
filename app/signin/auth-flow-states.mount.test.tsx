import assert from "node:assert/strict";
import test from "node:test";

// Load-bearing order: the harness installs the fake browser before
// `react-dom/client` is evaluated anywhere in this process.
import {
  browser,
  clickButton,
  findByText,
  focusedId,
  linkHref,
  mount,
  settle,
  submitForm,
  typeInto,
} from "@/lib/testing/react-mount";
import { StrictMode, act, type ReactNode } from "react";

import { IdentityError, type IdentityClient, type OAuthCallbackParams } from "@/lib/api/identity";
import {
  FAKE_ACCOUNT_INVITATION_TOKEN,
  FAKE_IDENTITY_OTP_CODE,
  FAKE_IDENTITY_PASSWORD,
  FAKE_OAUTH_STATE_TTL_MS,
  FAKE_OTP_ATTEMPT_LIMIT,
  FAKE_PASSWORD_RESET_TTL_MS,
  FAKE_PENDING_INVITATION_ID,
  createFakeIdentityClient,
  type FakeIdentityClient,
} from "@/lib/api/identity-fake";

import { IdentityProvider, createIdentitySessionStore } from "../_components/identity-provider";
import type { AuthNavigate } from "./auth-hooks";
import { EmailVerification } from "./email-verification";
import { IdentitySignIn } from "./identity-sign-in";
import { OAuthCallback } from "./oauth-callback";
import { OrganizationInvitationFlow } from "./organization-invitation";
import { PasswordResetRequest } from "./password-reset-request";
import { organizationInvitationHref, rememberPendingReturn, safeReturnTo } from "./return-to";
import { TokenPasswordFlow } from "./token-password-flow";

// The hard states of the sign-in flows: lockouts, spent and forged links,
// sessions that end halfway, failures in transit and what retrying does.
// Every assertion compares primitives; nothing hands a DOM node to a diff.

const SIGN_IN_FORM = "Sign in with email";
const OTP_FORM = "Verification code";
const NORA = "nora@open-splunk.test";
const NEW_PASSWORD = "a-long-new-passphrase";
const UNAUTHENTICATED = () => new IdentityError("unauthenticated", "Sign in to continue.");

function recorder(): { readonly visits: string[]; readonly replaced: boolean[]; readonly navigate: AuthNavigate } {
  const visits: string[] = [];
  const replaced: boolean[] = [];
  return {
    visits,
    replaced,
    navigate: (url, options) => {
      visits.push(url);
      replaced.push(options?.replace === true);
    },
  };
}

async function mountWith(client: IdentityClient, node: ReactNode) {
  const view = await mount(<IdentityProvider client={client}>{node}</IdentityProvider>);
  await settle();
  return { ...view, text: () => view.container.textContent };
}

/** Waits `milliseconds` inside `act`, for a client with latency. */
async function wait(milliseconds: number): Promise<void> {
  await act(async () => {
    await new Promise((resolve) => {
      globalThis.setTimeout(resolve, milliseconds);
    });
  });
}

/** A client whose `method` calls are counted. */
function counting<K extends keyof IdentityClient>(client: IdentityClient, method: K): { readonly client: IdentityClient; calls(): number } {
  let calls = 0;
  const wrapped = Object.create(client) as IdentityClient;
  const original = client[method] as (...parameters: unknown[]) => unknown;
  (wrapped as unknown as Record<string, unknown>)[method] = (...parameters: unknown[]) => {
    calls += 1;
    return original.apply(client, parameters);
  };
  return { client: wrapped, calls: () => calls };
}

function buttonState(container: Awaited<ReturnType<typeof mountWith>>["container"], label: string): string {
  const button = findByText(container, "button", label);
  return button === null ? "missing" : button.getAttribute("aria-disabled") === "true" ? "blocked" : "ready";
}

async function mountSignIn(client: IdentityClient, returnTo = "/search/") {
  const route = recorder();
  const view = await mountWith(client, <IdentitySignIn navigate={route.navigate} returnTo={returnTo} />);
  return { ...view, ...route };
}

async function signInWith(view: Awaited<ReturnType<typeof mountSignIn>>, email: string, password: string): Promise<void> {
  await typeInto(view.container, "signin-email", email);
  await typeInto(view.container, "signin-password", password);
  await submitForm(view.container, SIGN_IN_FORM);
}

async function enterCode(view: Awaited<ReturnType<typeof mountSignIn>>, code: string): Promise<void> {
  await typeInto(view.container, "signin-otp", code);
  await submitForm(view.container, OTP_FORM);
}

/** Enters `count` wrong codes one after another, each refused in place. */
async function refuseCodes(view: Awaited<ReturnType<typeof mountSignIn>>, count: number): Promise<void> {
  if (count === 0) return;
  await enterCode(view, "000000");
  assert.equal(view.text().includes("That code was not accepted"), true, `${count} refusals to go`);
  await refuseCodes(view, count - 1);
}

// --------------------------------------------------------------------------
// The second factor
// --------------------------------------------------------------------------

test("too many wrong codes discard the challenge and hold sign-in for the server's Retry-After", async () => {
  const fake = createFakeIdentityClient();
  const logins = counting(fake, "loginWithEmail");
  const view = await mountSignIn(logins.client);
  await signInWith(view, "tess@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  await refuseCodes(view, FAKE_OTP_ATTEMPT_LIMIT - 1);
  await enterCode(view, "000000");
  assert.equal(view.container.querySelector("#signin-otp") === null, true, "the spent challenge is gone");
  assert.equal(view.container.querySelector("#signin-email")?.value, "tess@open-splunk.test", "the address is kept");
  assert.equal(view.text().includes("Too many codes tried"), true);
  assert.equal(view.text().includes("Wait 1 minute"), true, "the server's 60-second Retry-After is named");
  assert.equal(buttonState(view.container, "Try again in 1 minute"), "blocked");

  await typeInto(view.container, "signin-password", FAKE_IDENTITY_PASSWORD);
  await submitForm(view.container, SIGN_IN_FORM);
  assert.equal(logins.calls(), 1, "nothing is sent while the countdown runs");
  assert.equal(view.visits.length, 0);
  await view.unmount();
});

test("a rate limit with no Retry-After still holds the button for a default wait", async () => {
  const client = createFakeIdentityClient();
  const view = await mountSignIn(client);
  await signInWith(view, "tess@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  client.failNext("verifyLoginOtp", new IdentityError("rate_limited", "Too many requests."));
  await enterCode(view, FAKE_IDENTITY_OTP_CODE);
  assert.equal(view.text().includes("Wait a moment, then sign in again"), true);
  assert.equal(buttonState(view.container, "Try again in 30 seconds"), "blocked");
  await view.unmount();
});

test("a rate limit whose Retry-After has already passed does not hold the button", async () => {
  const client = createFakeIdentityClient();
  const view = await mountSignIn(client);
  client.failNext("loginWithEmail", new IdentityError("rate_limited", "Too many requests.", { retryAfterMs: 0 }));
  await signInWith(view, "alice@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  assert.equal(view.text().includes("Too many attempts"), true);
  assert.equal(buttonState(view.container, "Try again in"), "missing");
  await submitForm(view.container, SIGN_IN_FORM);
  assert.deepEqual(view.visits, ["/search/"]);
  await view.unmount();
});

test("a code lost in transit keeps the challenge and the code, and the retry signs in", async () => {
  const client = createFakeIdentityClient();
  const view = await mountSignIn(client, "/reports/");
  await signInWith(view, "tess@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  client.failNext("verifyLoginOtp");
  await enterCode(view, FAKE_IDENTITY_OTP_CODE);
  assert.equal(view.text().includes("The identity service could not be reached"), true);
  assert.equal(view.container.querySelector("#signin-otp")?.value, FAKE_IDENTITY_OTP_CODE, "a code that was never judged is not cleared");
  await submitForm(view.container, OTP_FORM);
  assert.deepEqual(view.visits, ["/reports/"]);
  await view.unmount();
});

test("an account suspended, or a session revoked, between password and code goes back to the password step", async () => {
  const client = createFakeIdentityClient();
  const view = await mountSignIn(client);
  await signInWith(view, "tess@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  client.failNext("verifyLoginOtp", new IdentityError("unauthenticated", "User account is banned"));
  await enterCode(view, FAKE_IDENTITY_OTP_CODE);
  assert.equal(view.container.querySelector("#signin-email") === null, false);
  assert.equal(view.text().includes("This account is suspended"), true);

  await signInWith(view, "tess@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  client.failNext("verifyLoginOtp", new IdentityError("unauthenticated", "Invalid credentials"));
  await enterCode(view, FAKE_IDENTITY_OTP_CODE);
  assert.equal(view.text().includes("This sign-in attempt is no longer valid"), true);
  assert.equal(view.visits.length, 0);
  await view.unmount();
});

test("a second Enter while a code is being checked sends nothing more", async () => {
  const fake = createFakeIdentityClient({ latencyMs: 40 });
  const verifications = counting(fake, "verifyLoginOtp");
  const view = await mountSignIn(verifications.client);
  await wait(60);
  await typeInto(view.container, "signin-email", "tess@open-splunk.test");
  await typeInto(view.container, "signin-password", FAKE_IDENTITY_PASSWORD);
  await submitForm(view.container, SIGN_IN_FORM);
  await wait(60);
  assert.equal(focusedId(), "signin-otp");
  await typeInto(view.container, "signin-otp", FAKE_IDENTITY_OTP_CODE);
  await submitForm(view.container, OTP_FORM);
  await submitForm(view.container, OTP_FORM);
  assert.equal(buttonState(view.container, "Verifying"), "blocked");
  await wait(60);
  assert.equal(verifications.calls(), 1, "one code, one request");
  assert.deepEqual(view.visits, ["/search/"]);
  await view.unmount();
});

// --------------------------------------------------------------------------
// Starting a provider sign-in
// --------------------------------------------------------------------------

test("a provider start that fails in transit frees the buttons and the retry goes", async () => {
  const client = createFakeIdentityClient();
  const view = await mountSignIn(client);
  client.failNext("beginOAuth");
  await clickButton(view.container, "Continue with GitHub");
  assert.equal(view.text().includes("The identity service could not be reached"), true);
  assert.equal(buttonState(view.container, "Continue with GitHub"), "ready");
  assert.equal(buttonState(view.container, "Continue with Okta"), "ready");
  assert.equal(view.visits.length, 0);
  await clickButton(view.container, "Continue with GitHub");
  assert.equal(view.visits.length, 1);
  assert.equal(view.text().includes("could not be reached"), false, "the old failure is cleared");
  await view.unmount();
});

test("coming back from the provider through the back-forward cache frees the pending buttons", async () => {
  const view = await mountSignIn(createFakeIdentityClient());
  await clickButton(view.container, "Continue with GitHub");
  assert.equal(view.visits.length, 1);
  assert.equal(buttonState(view.container, "Opening GitHub"), "blocked", "the page is leaving");
  assert.equal(buttonState(view.container, "Continue with Okta"), "blocked");

  await act(async () => { browser.dispatchWindowEvent("pageshow", { persisted: false }); });
  assert.equal(buttonState(view.container, "Opening GitHub"), "blocked", "an ordinary load changes nothing");

  await act(async () => { browser.dispatchWindowEvent("pageshow", { persisted: true }); });
  assert.equal(buttonState(view.container, "Continue with GitHub"), "ready");
  assert.equal(buttonState(view.container, "Continue with Okta"), "ready");
  await clickButton(view.container, "Continue with Okta");
  assert.equal(view.visits.length, 2, "another provider can be chosen");
  await view.unmount();
});

// --------------------------------------------------------------------------
// The OAuth callback
// --------------------------------------------------------------------------

async function startOAuth(client: FakeIdentityClient, slug: string, returnTo: string): Promise<OAuthCallbackParams> {
  const { authorizationUrl } = await client.beginOAuth(slug, returnTo);
  const query = new URL(authorizationUrl, "https://example.invalid").searchParams;
  return { code: query.get("code"), state: query.get("state"), error: null, errorDescription: null };
}

async function mountCallback(client: IdentityClient, params: OAuthCallbackParams) {
  const route = recorder();
  const view = await mountWith(client, (
    <StrictMode>
      <OAuthCallback navigate={route.navigate} params={params} />
    </StrictMode>
  ));
  return { ...view, ...route };
}

const NO_PARAMS: OAuthCallbackParams = { code: null, state: null, error: null, errorDescription: null };

for (const params of [
  NO_PARAMS,
  { ...NO_PARAMS, code: "fake-oauth-code-github" },
  { ...NO_PARAMS, state: "fake-oauth-state-1" },
  { ...NO_PARAMS, code: "", state: "" },
]) {
  test(`a callback with code ${JSON.stringify(params.code)} and state ${JSON.stringify(params.state)} is a spent link and signs nobody in`, async () => {
    const client = createFakeIdentityClient();
    const view = await mountCallback(client, params);
    assert.equal(view.text().includes("This sign-in link has expired"), true);
    assert.equal(linkHref(view.container, "Back to sign in"), "/signin");
    assert.equal(view.visits.length, 0);
    assert.equal(await client.getPrincipal(), null);
    await view.unmount();
  });
}

test("a state nobody issued, or one past its lifetime, is refused even with a good code", async () => {
  const forged = createFakeIdentityClient();
  const real = await startOAuth(forged, "github", "/search/");
  const view = await mountCallback(forged, { ...real, state: "fake-oauth-state-999" });
  assert.equal(view.text().includes("This sign-in link has expired"), true);
  assert.equal(await forged.getPrincipal(), null);
  await view.unmount();

  let clock = Date.parse("2026-10-24T12:00:00.000Z");
  const slow = createFakeIdentityClient({ now: () => clock });
  const params = await startOAuth(slow, "github", "/search/");
  clock += FAKE_OAUTH_STATE_TTL_MS + 1;
  const late = await mountCallback(slow, params);
  assert.equal(late.text().includes("This sign-in link has expired"), true);
  assert.equal(late.visits.length, 0);
  await late.unmount();
});

test("a good state with the wrong code is a refusal, and the state cannot be tried again", async () => {
  const client = createFakeIdentityClient();
  const params = await startOAuth(client, "github", "/search/");
  const wrong = await mountCallback(client, { ...params, code: "fake-oauth-code-okta" });
  assert.equal(wrong.text().includes("The provider did not sign you in"), true);
  await wrong.unmount();

  const replay = await mountCallback(client, params);
  assert.equal(replay.text().includes("This sign-in link has expired"), true, "the state was spent by the failed attempt");
  assert.equal(replay.visits.length, 0);
  await replay.unmount();
});

test("a provider error wins over a code beside it, spends the state, and echoes none of its text", async () => {
  const client = createFakeIdentityClient();
  const params = await startOAuth(client, "github", "/search/");
  const refused = await mountCallback(client, {
    ...params,
    error: "access_denied",
    errorDescription: "<img src=x onerror=alert(1)> Visit evil.example to restore access",
  });
  assert.equal(refused.text().includes("The provider did not sign you in"), true);
  assert.equal(refused.text().includes("evil.example"), false);
  assert.equal(refused.text().includes("onerror"), false);
  assert.equal(refused.container.querySelector("img") === null, true);
  assert.equal(refused.visits.length, 0);
  assert.equal(await client.getPrincipal(), null, "an error with a code beside it signs nobody in");
  await refused.unmount();

  const replay = await mountCallback(client, params);
  assert.equal(replay.text().includes("This sign-in link has expired"), true);
  await replay.unmount();
});

test("a callback that fails in transit offers a fresh start, never a resend of the one-time code", async () => {
  const client = createFakeIdentityClient();
  const exchanges = counting(client, "completeOAuthCallback");
  client.failNext("completeOAuthCallback");
  const view = await mountCallback(exchanges.client, await startOAuth(client, "github", "/search/"));
  assert.equal(view.text().includes("The identity service could not be reached"), true);
  assert.equal(findByText(view.container, "button", "Try again") === null, true);
  assert.equal(linkHref(view.container, "Back to sign in"), "/signin");
  assert.equal(exchanges.calls(), 1);
  await view.unmount();
});

for (const returnTo of [
  "javascript:alert(document.cookie)",
  "JaVaScRiPt:alert(1)",
  "https://evil.example/",
  "//evil.example/",
  "/\\evil.example",
  "/\t/evil.example",
  "/%2e%2e//evil.example",
  "/auth/oauth/callback/?code=x&state=y",
  "/auth//oauth/callback/?code=x&state=y",
  "/x/..%2fsignin/",
  "",
]) {
  test(`a server return path of ${JSON.stringify(returnTo)} sends the callback to the product root`, async () => {
    const client = createFakeIdentityClient();
    const hostile = Object.create(client) as IdentityClient;
    hostile.completeOAuthCallback = async (params) => ({ ...(await client.completeOAuthCallback(params)), returnTo });
    const view = await mountCallback(hostile, await startOAuth(client, "github", "/search/"));
    assert.deepEqual(view.visits, ["/"]);
    assert.equal(linkHref(view.container, "continue"), "/");
    await view.unmount();
  });
}

test("a first provider sign-in whose session is gone before its invitations load signs in again", async () => {
  const client = createFakeIdentityClient();
  const params = await startOAuth(client, "okta", "/search/");
  // Every read is refused: StrictMode reads twice, and a gone session stays gone.
  const ended = Object.create(client) as IdentityClient;
  ended.listMyInvitations = () => Promise.reject(UNAUTHENTICATED());
  const view = await mountCallback(ended, params);
  assert.equal(view.text().includes("Your session has ended"), true);
  assert.equal(findByText(view.container, "button", "Try again") === null, true, "a retry would only be refused again");
  await clickButton(view.container, "Sign in again");
  assert.deepEqual(view.visits, ["/signin/"]);
  await view.unmount();
});

// --------------------------------------------------------------------------
// History: Back after a one-time step
// --------------------------------------------------------------------------

test("a finished sign-in replaces the sign-in entry; starting a provider sign-in does not", async () => {
  const view = await mountSignIn(createFakeIdentityClient(), "/dashboards/");
  await clickButton(view.container, "Continue with GitHub");
  assert.deepEqual(view.replaced, [false], "Back from the provider returns to this card");
  await view.unmount();

  const password = await mountSignIn(createFakeIdentityClient(), "/dashboards/");
  await signInWith(password, "alice@open-splunk.test", FAKE_IDENTITY_PASSWORD);
  assert.deepEqual(password.visits, ["/dashboards/"]);
  assert.deepEqual(password.replaced, [true], "Back from the destination skips the finished sign-in");
  await password.unmount();
});

test("the callback and the verification link replace themselves, so Back cannot replay their token", async () => {
  const client = createFakeIdentityClient();
  const callback = await mountCallback(client, await startOAuth(client, "github", "/alerts/"));
  assert.deepEqual(callback.visits, ["/alerts/"]);
  assert.deepEqual(callback.replaced, [true]);
  await callback.unmount();

  const fresh = createFakeIdentityClient({ initialSignedInEmail: null });
  await fresh.registerWithEmail({ email: "back@open-splunk.test", password: NEW_PASSWORD, username: "back", firstName: "Bea", lastName: "Back" });
  const token = fresh.lastMessage("email_verification", "back@open-splunk.test")?.token ?? "";
  const invitation = organizationInvitationHref(FAKE_PENDING_INVITATION_ID, "back@open-splunk.test");
  rememberPendingReturn(invitation);
  const route = recorder();
  const verified = await mountWith(fresh, <EmailVerification navigate={route.navigate} token={token} />);
  assert.deepEqual(route.visits, [invitation], "verification resumes the invitation that started it");
  assert.deepEqual(route.replaced, [true], "the spent verification link leaves history");
  await verified.unmount();

  // With nothing to resume, the new account has nowhere to go yet; leaving replaces the link too.
  await fresh.logout();
  await fresh.registerWithEmail({ email: "nowhere@open-splunk.test", password: NEW_PASSWORD, username: "nowhere", firstName: "No", lastName: "Where" });
  const second = fresh.lastMessage("email_verification", "nowhere@open-splunk.test")?.token ?? "";
  const stranded = recorder();
  const card = await mountWith(fresh, <EmailVerification navigate={stranded.navigate} token={second} />);
  assert.equal(card.text().includes("You're not in an organization yet"), true);
  await clickButton(card.container, "Sign in with a different account");
  assert.deepEqual(stranded.visits, ["/signin/"]);
  assert.deepEqual(stranded.replaced, [true]);
  await card.unmount();
});

test("a spent callback reopened by a signed-in browser says the session is fine", async () => {
  const client = createFakeIdentityClient();
  const params = await startOAuth(client, "github", "/search/");
  const first = await mountCallback(client, params);
  await first.unmount();

  const reopened = await mountCallback(client, params);
  assert.equal(reopened.text().includes("This sign-in link has expired"), true, "what happened to the link is still said");
  assert.equal(reopened.text().includes("You are still signed in"), true);
  assert.equal(reopened.text().includes("mark@open-splunk.test"), true);
  assert.equal(linkHref(reopened.container, "Continue to Open Splunk"), "/");
  assert.equal(reopened.visits.length, 0, "nothing navigates on its own");
  await reopened.unmount();

  // Signed out, the same link is only a dead link.
  await client.logout();
  const signedOut = await mountCallback(client, params);
  assert.equal(signedOut.text().includes("This sign-in link has expired"), true);
  assert.equal(signedOut.text().includes("still signed in"), false);
  assert.equal(linkHref(signedOut.container, "Continue to Open Splunk"), null);
  await signedOut.unmount();

  // A refusal that is not a spent link is never dressed up as success.
  const refused = createFakeIdentityClient({ initialSignedInEmail: "mark@open-splunk.test" });
  refused.failNext("completeOAuthCallback", new IdentityError("conflict", "This provider account is linked to a different user."));
  const conflict = await mountCallback(refused, await startOAuth(refused, "github", "/"));
  assert.equal(conflict.text().includes("This provider account belongs to someone else"), true);
  assert.equal(linkHref(conflict.container, "Continue to Open Splunk"), null);
  await conflict.unmount();
});

// --------------------------------------------------------------------------
// A session that ends halfway
// --------------------------------------------------------------------------

test("signing out of a session the server already ended counts as signed out", async () => {
  const client = createFakeIdentityClient({ initialSignedInEmail: "ada@open-splunk.test" });
  const expired = Object.create(client) as IdentityClient;
  expired.logout = () => Promise.reject(UNAUTHENTICATED());
  const store = createIdentitySessionStore(expired, { principal: await client.getPrincipal() });
  await store.signOut();
  assert.equal(store.getSnapshot().status, "signed_out");

  const refusing = Object.create(client) as IdentityClient;
  refusing.logout = () => Promise.reject(new IdentityError("forbidden", "Reauthentication required"));
  const kept = createIdentitySessionStore(refusing, { principal: await client.getPrincipal() });
  await assert.rejects(kept.signOut());
  assert.equal(kept.getSnapshot().status, "signed_in", "a refusal that is not about the session keeps it");
});

test("a user with no organization whose session ends before the invitations load goes back to sign in", async () => {
  const fake = createFakeIdentityClient();
  // go-common answers a logout without a session with 401.
  const client = Object.create(fake) as IdentityClient;
  client.logout = () => Promise.reject(UNAUTHENTICATED());
  fake.failNext("listMyInvitations", UNAUTHENTICATED());
  const view = await mountSignIn(client);
  await signInWith(view, NORA, FAKE_IDENTITY_PASSWORD);
  assert.equal(view.text().includes("You're not in an organization yet"), true);
  assert.equal(view.text().includes("Your session has ended"), true);
  assert.equal(findByText(view.container, "button", "Try again") === null, true);
  await clickButton(view.container, "Sign in again");
  assert.equal(view.container.querySelector("#signin-email") === null, false, "back on the credentials step");
  assert.equal(view.text().includes("You're not in an organization yet"), false);
  assert.equal(view.visits.length, 0);
  await view.unmount();
});

async function mountInvitation(client: IdentityClient, invitationId: string | null, invitedEmail: string | null, now?: () => number) {
  const route = recorder();
  const view = await mountWith(client, (
    <OrganizationInvitationFlow invitationId={invitationId} invitedEmail={invitedEmail} navigate={route.navigate} now={now} />
  ));
  return { ...view, ...route };
}

function signInTarget(visit: string | undefined): { readonly pathname: string; readonly returnTo: string | null; readonly email: string | null } {
  const url = new URL(visit ?? "", "https://example.invalid");
  return { pathname: url.pathname, returnTo: url.searchParams.get("returnTo"), email: url.searchParams.get("email") };
}

test("an invitation whose session ends before Accept drops to sign-in that comes back to it", async () => {
  const client = createFakeIdentityClient({ initialSignedInEmail: NORA });
  const view = await mountInvitation(client, FAKE_PENDING_INVITATION_ID, NORA);
  assert.equal(view.text().includes("Join Acme Operations"), true);
  await client.logout();
  await clickButton(view.container, "Accept invitation");
  assert.equal(view.text().includes("You're invited to Open Splunk"), true);
  assert.equal(view.text().includes("Your session has ended"), true);
  assert.equal(findByText(view.container, "button", "Accept invitation") === null, true, "no button the server will refuse again");
  await clickButton(view.container, "Sign in to accept");
  const target = signInTarget(view.visits[0]);
  const href = organizationInvitationHref(FAKE_PENDING_INVITATION_ID, NORA);
  assert.equal(target.pathname, "/signin/");
  assert.equal(target.returnTo, href);
  assert.equal(target.email, NORA);
  assert.equal(safeReturnTo(target.returnTo), href, "the sign-in page will honour the return");
  await view.unmount();
});

test("an invitation lookup refused as unauthenticated shows the signed-out card, not a retry loop", async () => {
  const client = createFakeIdentityClient({ initialSignedInEmail: NORA });
  client.failNext("listMyInvitations", UNAUTHENTICATED());
  const view = await mountInvitation(client, FAKE_PENDING_INVITATION_ID, null);
  assert.equal(view.text().includes("Your session has ended"), true);
  assert.equal(findByText(view.container, "button", "Sign in to accept") === null, false);
  assert.equal(findByText(view.container, "button", "Try again") === null, true);
  await view.unmount();
});

test("a session that ends between accepting and opening the organization signs in again", async () => {
  const client = createFakeIdentityClient({ initialSignedInEmail: NORA });
  const view = await mountInvitation(client, FAKE_PENDING_INVITATION_ID, null);
  await clickButton(view.container, "Accept invitation");
  assert.equal(view.text().includes("You joined Acme Operations"), true);
  await client.logout();
  await clickButton(view.container, "Open Acme Operations");
  assert.equal(view.visits.length, 0);
  assert.equal(view.text().includes("Your session has ended"), true);
  assert.equal(findByText(view.container, "button", "Sign in to accept") === null, false);
  await view.unmount();
});

/** The page's clock in the expiry test; the server's runs on separately. */
function pageNow(): number {
  return Date.parse("2026-10-24T12:00:00.000Z");
}

test("an invitation that expires while the page is open says so when accepted", async () => {
  let serverNow = pageNow();
  const expiring = createFakeIdentityClient({ initialSignedInEmail: NORA, now: () => serverNow });
  const view = await mountInvitation(expiring, FAKE_PENDING_INVITATION_ID, null, pageNow);
  assert.equal(view.text().includes("Join Acme Operations"), true);
  serverNow = Date.parse("2099-01-02T00:00:00.000Z");
  await clickButton(view.container, "Accept invitation");
  assert.equal(view.text().includes("This invitation has expired"), true);
  assert.equal(findByText(view.container, "button", "Accept invitation") === null, true);
  await view.unmount();
});

for (const [method, label] of [["acceptInvitation", "Accept invitation"], ["declineInvitation", "Decline"]] as const) {
  test(`an invitation revoked while the page is open is unavailable when the ${label} button answers it`, async () => {
    const client = createFakeIdentityClient({ initialSignedInEmail: NORA });
    const revoked = await mountInvitation(client, FAKE_PENDING_INVITATION_ID, null);
    client.failNext(method, new IdentityError("not_found", "Invitation not found"));
    await clickButton(revoked.container, label);
    assert.equal(revoked.text().includes("This invitation isn't available"), true);
    assert.equal(findByText(revoked.container, "button", "Sign in with a different account") === null, false);
    await revoked.unmount();
  });
}

test("someone else's invitation switches accounts and returns to it with the invited address", async () => {
  const client = createFakeIdentityClient({ initialSignedInEmail: "ada@open-splunk.test" });
  const view = await mountInvitation(client, FAKE_PENDING_INVITATION_ID, NORA);
  assert.equal(view.text().includes("This invitation isn't available"), true);
  assert.equal(view.text().includes("ada@open-splunk.test"), true);
  await clickButton(view.container, "Sign in with a different account");
  assert.equal(view.text().includes("This invitation was sent to"), true);
  assert.equal(view.text().includes("Your session has ended"), false, "a chosen sign-out is not a lost session");
  await clickButton(view.container, "Sign in to accept");
  const target = signInTarget(view.visits[0]);
  assert.equal(target.email, NORA);
  assert.equal(safeReturnTo(target.returnTo), organizationInvitationHref(FAKE_PENDING_INVITATION_ID, NORA));
  await view.unmount();
});

test("an invitation id full of path syntax stays a query value on the invitation page", async () => {
  const id = "../../signin/?returnTo=//evil.example#x";
  const view = await mountInvitation(createFakeIdentityClient(), id, null);
  await clickButton(view.container, "Sign in to accept");
  const target = signInTarget(view.visits[0]);
  const returnTo = safeReturnTo(target.returnTo);
  const resolved = new URL(returnTo, "https://app.invalid");
  assert.equal(resolved.origin, "https://app.invalid");
  assert.equal(resolved.pathname, "/auth/invitation/");
  assert.equal(resolved.searchParams.get("id"), id);
  assert.equal(resolved.hash, "");
  await view.unmount();

  const signedIn = await mountInvitation(createFakeIdentityClient({ initialSignedInEmail: NORA }), id, null);
  assert.equal(signedIn.text().includes("This invitation isn't available"), true);
  await signedIn.unmount();
});

// --------------------------------------------------------------------------
// Emailed links
// --------------------------------------------------------------------------

function resetFlow(client: IdentityClient, token: string | null) {
  return (
    <TokenPasswordFlow
      flow="reset_complete"
      intro="Choose a password."
      pendingLabel="Saving…"
      recovery={{ href: "/signin/reset/", label: "Request a new link" }}
      run={(code, newPassword) => client.completePasswordReset({ verificationCode: code, newPassword })}
      submitLabel="Save new password"
      successDetail="Your password was changed."
      successTitle="Password updated"
      title="Choose a new password"
      token={token}
    />
  );
}

async function choosePassword(view: Awaited<ReturnType<typeof mountWith>>, form: string): Promise<void> {
  await typeInto(view.container, "new-password", NEW_PASSWORD);
  await typeInto(view.container, "confirm-password", NEW_PASSWORD);
  await submitForm(view.container, form);
}

test("a reset link past its lifetime is dead, and one used in another tab is dead too", async () => {
  let clock = Date.parse("2026-10-24T12:00:00.000Z");
  const client = createFakeIdentityClient({ now: () => clock });
  await client.requestPasswordReset("alice@open-splunk.test");
  const code = client.lastMessage("password_reset", "alice@open-splunk.test")?.token ?? "";
  clock += FAKE_PASSWORD_RESET_TTL_MS + 1;
  const expired = await mountWith(client, resetFlow(client, code));
  await choosePassword(expired, "Choose a new password");
  assert.equal(expired.text().includes("This reset link no longer works"), true);
  assert.equal(linkHref(expired.container, "Request a new link"), "/signin/reset");
  assert.equal(expired.container.querySelector("#new-password") === null, true, "no password can revive it");
  await expired.unmount();

  clock = Date.parse("2026-10-24T13:00:00.000Z");
  await client.requestPasswordReset("mona@open-splunk.test");
  const fresh = client.lastMessage("password_reset", "mona@open-splunk.test")?.token ?? "";
  const first = await mountWith(client, resetFlow(client, fresh));
  const second = await mountWith(client, resetFlow(client, fresh));
  await choosePassword(first, "Choose a new password");
  assert.equal(first.text().includes("Password updated"), true);
  await choosePassword(second, "Choose a new password");
  assert.equal(second.text().includes("This reset link no longer works"), true);
  await first.unmount();
  await second.unmount();
});

test("a reset lost in transit keeps the form, a rate limit counts down, and the retry saves once", async () => {
  const client = createFakeIdentityClient();
  await client.requestPasswordReset("alice@open-splunk.test");
  const code = client.lastMessage("password_reset", "alice@open-splunk.test")?.token ?? "";
  const resets = counting(client, "completePasswordReset");
  const view = await mountWith(client, resetFlow(resets.client, code));
  client.failNext("completePasswordReset");
  await choosePassword(view, "Choose a new password");
  assert.equal(view.text().includes("The identity service could not be reached"), true);
  assert.equal(view.container.querySelector("#new-password")?.value, NEW_PASSWORD);

  client.failNext("completePasswordReset", new IdentityError("rate_limited", "slow down", { retryAfterMs: 45_000 }));
  await submitForm(view.container, "Choose a new password");
  assert.equal(view.text().includes("Wait 45 seconds"), true);
  assert.equal(buttonState(view.container, "Try again in 45 seconds"), "blocked");
  await submitForm(view.container, "Choose a new password");
  assert.equal(resets.calls(), 2, "nothing is sent during the countdown");
  await view.unmount();
});

test("an account invitation link works once; a second tab finds it spent", async () => {
  const client = createFakeIdentityClient();
  const flow = (
    <TokenPasswordFlow
      flow="account_invitation"
      intro="Set a password."
      pendingLabel="Setting up…"
      run={async (token, password) => { await client.acceptInvitation({ kind: "account", token, password }); }}
      submitLabel="Set password"
      successDetail="Your account is ready."
      successTitle="Account ready"
      title="Set up your account"
      token={FAKE_ACCOUNT_INVITATION_TOKEN}
    />
  );
  const first = await mountWith(client, flow);
  const second = await mountWith(client, flow);
  await choosePassword(first, "Set up your account");
  assert.equal(first.text().includes("Account ready"), true);
  await choosePassword(second, "Set up your account");
  assert.equal(second.text().includes("This invitation link no longer works"), true);
  assert.equal(linkHref(second.container, "Back to sign in"), "/signin");
  await first.unmount();
  await second.unmount();
});

test("a reset request lost in transit can be sent again", async () => {
  const client = createFakeIdentityClient();
  client.failNext("requestPasswordReset");
  const view = await mountWith(client, <PasswordResetRequest initialEmail="alice@open-splunk.test" />);
  await submitForm(view.container, "Request a password reset");
  assert.equal(view.text().includes("The identity service could not be reached"), true);
  assert.equal(view.container.querySelector("#reset-email")?.value, "alice@open-splunk.test");
  await submitForm(view.container, "Request a password reset");
  assert.equal(view.text().includes("Check your email"), true);
  assert.equal(client.sentMessages().filter((message) => message.kind === "password_reset").length, 1);
  await view.unmount();
});
