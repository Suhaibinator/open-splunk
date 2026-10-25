import assert from "node:assert/strict";
import test from "node:test";

import { CAPABILITIES, IdentityError, isIdentityError, type IdentityErrorKind, type Principal } from "./identity";
import {
  FAKE_ACCOUNT_INVITATION_TOKEN,
  FAKE_EXPIRED_INVITATION_ID,
  FAKE_IDENTITY_ACCOUNTS,
  FAKE_IDENTITY_OTP_CODE,
  FAKE_IDENTITY_PASSWORD,
  FAKE_LOGIN_ATTEMPT_LIMIT,
  FAKE_LOGIN_WINDOW_MS,
  FAKE_OAUTH_CALLBACK_LIMIT,
  FAKE_OAUTH_CALLBACK_PATH,
  FAKE_OAUTH_CONFLICT_CODE,
  FAKE_OAUTH_STATE_TTL_MS,
  FAKE_OTP_ATTEMPT_LIMIT,
  FAKE_OTP_CHALLENGE_TTL_MS,
  FAKE_PASSWORD_RESET_COOLDOWN_MS,
  FAKE_PASSWORD_RESET_TTL_MS,
  FAKE_PENDING_INVITATION_ID,
  FakeIdentityClient,
  createFakeIdentityClient,
  isSafeReturnPath,
  type FakeIdentityOptions,
  type FakeIdentityStorage,
} from "./identity-fake";

const START = Date.parse("2026-10-24T12:00:00.000Z");

function clock(): { now: () => number; advance: (milliseconds: number) => void } {
  let at = START;
  return { now: () => at, advance: (milliseconds) => { at += milliseconds; } };
}

function fake(options: FakeIdentityOptions = {}): FakeIdentityClient & { advance: (milliseconds: number) => void } {
  const time = clock();
  return Object.assign(createFakeIdentityClient({ now: time.now, ...options }), { advance: time.advance });
}

async function rejectsWith(promise: Promise<unknown>, kind: IdentityErrorKind): Promise<IdentityError> {
  try {
    await promise;
  } catch (error) {
    assert.ok(isIdentityError(error), `expected an IdentityError, got ${String(error)}`);
    assert.equal(error.kind, kind, error.message);
    return error;
  }
  assert.fail(`expected a ${kind} rejection`);
}

async function signIn(client: FakeIdentityClient, username: string): Promise<Principal> {
  const result = await client.loginWithEmail({ email: `${username}@open-splunk.test`, password: FAKE_IDENTITY_PASSWORD });
  assert.equal(result.status, "signed_in");
  return result.principal;
}

/** Runs `step` once per index, each after the previous settles: a rate limit counts them in order. */
async function sequentially(count: number, step: (index: number) => Promise<void>, index = 0): Promise<void> {
  if (index >= count) return;
  await step(index);
  await sequentially(count, step, index + 1);
}

function storageStub(): FakeIdentityStorage & { entries: Map<string, string> } {
  const entries = new Map<string, string>();
  return {
    entries,
    getItem: (key) => entries.get(key) ?? null,
    setItem: (key, value) => { entries.set(key, value); },
    removeItem: (key) => { entries.delete(key); },
  };
}

test("a fresh fake is signed out and seeds the documented accounts", async () => {
  const client = fake();
  assert.equal(await client.getPrincipal(), null);
  const admin = await signIn(client, "ada");
  const users = await client.adminListUsers({}, { page: 1, limit: 100 });
  for (const account of FAKE_IDENTITY_ACCOUNTS) {
    assert.ok(users.items.some((entry) => entry.user.email === account.email), account.email);
  }
  assert.equal(admin.user.email, "ada@open-splunk.test");
});

test("email sign-in resolves the principal, its organizations and capabilities", async () => {
  const client = fake();
  const principal = await signIn(client, "mark");
  assert.equal(principal.user.id, "5");
  assert.equal(principal.siteAdmin, false);
  assert.equal(principal.activeOrganization?.slug, "acme", "the first organization by name is active");
  assert.equal(principal.role, "member");
  assert.deepEqual(principal.capabilities, ["search", "objects.own"]);
  assert.deepEqual(principal.organizations.map((entry) => [entry.organization.slug, entry.effectiveRole]), [
    ["acme", "member"],
    ["acme-platform", "member"],
    ["globex", "member"],
  ]);
  assert.deepEqual(await client.getPrincipal(), principal);
});

test("roles inherited from a parent organization report explicit and effective roles", async () => {
  const client = fake();
  const alice = await signIn(client, "alice");
  const platform = alice.organizations.find((entry) => entry.organization.slug === "acme-platform");
  assert.ok(platform);
  assert.equal(platform.explicitRole, null);
  assert.equal(platform.effectiveRole, "admin");
  assert.equal(platform.inheritedFromOrganizationId, "1");
  assert.equal(platform.effectiveRoleDepth, 0);
  assert.deepEqual(platform.organization.lineage, ["1"]);

  const mona = await signIn(client, "mona");
  const monaPlatform = mona.organizations.find((entry) => entry.organization.slug === "acme-platform");
  assert.ok(monaPlatform);
  assert.equal(monaPlatform.explicitRole, "admin", "an explicit role above the inherited one wins");
  assert.equal(monaPlatform.effectiveRole, "admin");
  assert.equal(monaPlatform.inheritedFromOrganizationId, undefined);
  assert.equal(monaPlatform.effectiveRoleDepth, 1);
});

test("sign-in rejects malformed input, wrong passwords, banned and password-less accounts", async () => {
  const client = fake();
  await rejectsWith(client.loginWithEmail({ email: "not-an-email", password: "x" }), "invalid");
  await rejectsWith(client.loginWithEmail({ email: "mark@open-splunk.test", password: "" }), "invalid");
  await rejectsWith(client.loginWithEmail({ email: "mark@open-splunk.test", password: "wrong-password" }), "unauthenticated");
  await rejectsWith(client.loginWithEmail({ email: "nobody@open-splunk.test", password: "wrong-password" }), "unauthenticated");
  const banned = await rejectsWith(client.loginWithEmail({ email: "bart@open-splunk.test", password: FAKE_IDENTITY_PASSWORD }), "unauthenticated");
  assert.match(banned.message, /banned/u);
  await rejectsWith(client.loginWithEmail({ email: "iris@open-splunk.test", password: FAKE_IDENTITY_PASSWORD }), "unauthenticated");
  assert.equal(await client.getPrincipal(), null);
  const principal = await client.loginWithEmail({ email: "  MARK@Open-Splunk.TEST ", password: FAKE_IDENTITY_PASSWORD });
  assert.equal(principal.status, "signed_in", "addresses are normalized");
});

test("repeated sign-in failures rate-limit the address until the window passes", async () => {
  const client = fake();
  await sequentially(FAKE_LOGIN_ATTEMPT_LIMIT, async () => {
    await rejectsWith(client.loginWithEmail({ email: "mark@open-splunk.test", password: "wrong-password" }), "unauthenticated");
    client.advance(1_000);
  });
  const limited = await rejectsWith(client.loginWithEmail({ email: "mark@open-splunk.test", password: FAKE_IDENTITY_PASSWORD }), "rate_limited");
  assert.equal(limited.retryAfterMs, FAKE_LOGIN_WINDOW_MS - FAKE_LOGIN_ATTEMPT_LIMIT * 1_000);
  assert.equal((await client.loginWithEmail({ email: "mona@open-splunk.test", password: FAKE_IDENTITY_PASSWORD })).status, "signed_in", "other addresses are unaffected");
  client.advance(limited.retryAfterMs ?? 0);
  assert.equal((await client.loginWithEmail({ email: "mark@open-splunk.test", password: FAKE_IDENTITY_PASSWORD })).status, "signed_in");
});

test("a second factor interrupts sign-in and accepts only the current code", async () => {
  const client = fake();
  const result = await client.loginWithEmail({ email: "tess@open-splunk.test", password: FAKE_IDENTITY_PASSWORD });
  assert.equal(result.status, "otp_required");
  const { challenge } = result;
  assert.equal(challenge.email, "tess@open-splunk.test");
  assert.equal(await client.getPrincipal(), null, "no session before the code");
  await rejectsWith(client.verifyLoginOtp(challenge, "12ab"), "invalid");
  await rejectsWith(client.verifyLoginOtp(challenge, "000000"), "otp_required");
  const principal = await client.verifyLoginOtp(challenge, ` ${FAKE_IDENTITY_OTP_CODE} `);
  assert.equal(principal.user.username, "tess");
  await rejectsWith(client.verifyLoginOtp(challenge, FAKE_IDENTITY_OTP_CODE), "invalid");
});

test("a second-factor challenge rate-limits rejected codes and expires", async () => {
  const client = fake();
  const first = await client.loginWithEmail({ email: "tess@open-splunk.test", password: FAKE_IDENTITY_PASSWORD });
  assert.equal(first.status, "otp_required");
  await sequentially(FAKE_OTP_ATTEMPT_LIMIT - 1, async () => {
    await rejectsWith(client.verifyLoginOtp(first.challenge, "000000"), "otp_required");
  });
  const limited = await rejectsWith(client.verifyLoginOtp(first.challenge, "000000"), "rate_limited");
  assert.ok((limited.retryAfterMs ?? 0) > 0);
  await rejectsWith(client.verifyLoginOtp(first.challenge, FAKE_IDENTITY_OTP_CODE), "invalid");

  const second = await client.loginWithEmail({ email: "tess@open-splunk.test", password: FAKE_IDENTITY_PASSWORD });
  assert.equal(second.status, "otp_required");
  client.advance(FAKE_OTP_CHALLENGE_TTL_MS + 1);
  await rejectsWith(client.verifyLoginOtp(second.challenge, FAKE_IDENTITY_OTP_CODE), "invalid");
  await rejectsWith(client.verifyLoginOtp({ id: "forged", email: "tess@open-splunk.test" }, FAKE_IDENTITY_OTP_CODE), "invalid");
});

test("logout ends the session and is idempotent", async () => {
  const client = fake();
  await signIn(client, "mark");
  await client.logout();
  assert.equal(await client.getPrincipal(), null);
  await client.logout();
  await rejectsWith(client.listMembers("1"), "unauthenticated");
});

test("switching organizations recomputes role and capabilities", async () => {
  const client = fake();
  await signIn(client, "owen");
  const globex = await client.setActiveOrganization("2");
  assert.equal(globex.role, "owner");
  assert.ok(globex.capabilities.includes("org.admin"));
  const acme = await client.setActiveOrganization("1");
  assert.equal(acme.role, "member");
  assert.ok(!acme.capabilities.includes("members.admin"));
  await rejectsWith(client.setActiveOrganization("999"), "not_found");
  assert.equal((await client.getPrincipal())?.activeOrganization?.id, "1");

  await signIn(client, "nora");
  const nora = await client.getPrincipal();
  assert.equal(nora?.activeOrganization, null);
  assert.equal(nora?.role, null);
  assert.deepEqual(nora?.capabilities, []);
  await rejectsWith(client.setActiveOrganization("1"), "forbidden");
});

test("a site admin may act in an organization it is not a member of", async () => {
  const client = fake();
  await signIn(client, "ada");
  const principal = await client.setActiveOrganization("2");
  assert.equal(principal.activeOrganization?.slug, "globex");
  assert.equal(principal.role, null);
  assert.ok(principal.siteAdmin);
  assert.deepEqual(principal.capabilities, CAPABILITIES);
  assert.ok(!principal.organizations.some((entry) => entry.organization.id === "2"));
});

test("OAuth sign-in round-trips through the simulated provider redirect", async () => {
  const client = fake();
  assert.deepEqual((await client.listOAuthProviders()).map((provider) => provider.slug), ["github", "okta"]);
  const { authorizationUrl } = await client.beginOAuth("github", "/search/?q=error");
  assert.ok(authorizationUrl.startsWith(`${FAKE_OAUTH_CALLBACK_PATH}?`));
  const query = new URL(authorizationUrl, "https://workspace.test").searchParams;
  const result = await client.completeOAuthCallback({ code: query.get("code"), state: query.get("state") });
  assert.equal(result.principal.user.username, "mark", "GitHub links the existing account by verified email");
  assert.equal(result.accountCreated, false);
  assert.equal(result.returnTo, "/search/?q=error");
  assert.equal(result.providerSlug, "github");
  await rejectsWith(client.completeOAuthCallback({ code: query.get("code"), state: query.get("state") }), "invalid");

  const okta = new URL((await client.beginOAuth("okta", "/")).authorizationUrl, "https://workspace.test").searchParams;
  const created = await client.completeOAuthCallback({ code: okta.get("code"), state: okta.get("state") });
  assert.equal(created.accountCreated, true);
  assert.equal(created.principal.activeOrganization, null);
  const again = new URL((await client.beginOAuth("okta", "/")).authorizationUrl, "https://workspace.test").searchParams;
  assert.equal((await client.completeOAuthCallback({ code: again.get("code"), state: again.get("state") })).accountCreated, false);
});

test("OAuth rejects unsafe destinations, unknown providers and every bad callback", async () => {
  const client = fake();
  await rejectsWith(client.beginOAuth("myspace", "/"), "invalid");
  await Promise.all(["//evil.test/", "https://evil.test/", "search", "/\\evil.test", "/a\tb", ""].map(async (destination) => {
    assert.ok(!isSafeReturnPath(destination), destination);
    await rejectsWith(client.beginOAuth("github", destination), "invalid");
  }));
  const begin = async () => new URL((await client.beginOAuth("github", "/")).authorizationUrl, "https://workspace.test").searchParams;

  const denied = await begin();
  await rejectsWith(client.completeOAuthCallback({ state: denied.get("state"), error: "access_denied", errorDescription: "The user said no" }), "unauthenticated");
  await rejectsWith(client.completeOAuthCallback({ code: denied.get("code"), state: denied.get("state") }), "invalid");

  const incomplete = await begin();
  await rejectsWith(client.completeOAuthCallback({ state: incomplete.get("state") }), "invalid");
  await rejectsWith(client.completeOAuthCallback({ code: incomplete.get("code"), state: "forged" }), "invalid");

  const conflicting = await begin();
  await rejectsWith(client.completeOAuthCallback({ code: FAKE_OAUTH_CONFLICT_CODE, state: conflicting.get("state") }), "conflict");

  const tampered = await begin();
  await rejectsWith(client.completeOAuthCallback({ code: "fake-oauth-code-okta", state: tampered.get("state") }), "unauthenticated");

  const stale = await begin();
  client.advance(FAKE_OAUTH_STATE_TTL_MS + 1);
  await rejectsWith(client.completeOAuthCallback({ code: stale.get("code"), state: stale.get("state") }), "invalid");
  assert.equal(await client.getPrincipal(), null);
});

test("OAuth callbacks are rate limited per minute", async () => {
  const client = fake();
  await sequentially(FAKE_OAUTH_CALLBACK_LIMIT, async () => {
    await rejectsWith(client.completeOAuthCallback({ code: "x", state: "forged" }), "invalid");
  });
  const limited = await rejectsWith(client.completeOAuthCallback({ code: "x", state: "forged" }), "rate_limited");
  assert.equal(limited.retryAfterMs, 60_000);
  client.advance(60_000);
  await rejectsWith(client.completeOAuthCallback({ code: "x", state: "forged" }), "invalid");
});

test("password reset sends a single-use code and never reveals unknown addresses", async () => {
  const client = fake();
  await rejectsWith(client.requestPasswordReset("nope"), "invalid");
  await client.requestPasswordReset("nobody@open-splunk.test");
  assert.equal(client.lastMessage("password_reset", "nobody@open-splunk.test"), undefined);

  await client.requestPasswordReset("Mark@open-splunk.test");
  const limited = await rejectsWith(client.requestPasswordReset("mark@open-splunk.test"), "rate_limited");
  assert.equal(limited.retryAfterMs, FAKE_PASSWORD_RESET_COOLDOWN_MS);
  const message = client.lastMessage("password_reset", "mark@open-splunk.test");
  assert.ok(message);

  await rejectsWith(client.completePasswordReset({ verificationCode: "forged", newPassword: "long-enough-password" }), "invalid");
  await rejectsWith(client.completePasswordReset({ verificationCode: message.token, newPassword: "short" }), "invalid");
  await client.completePasswordReset({ verificationCode: message.token, newPassword: "a-new-password" });
  assert.equal(await client.getPrincipal(), null, "completing a reset does not sign in");
  await rejectsWith(client.completePasswordReset({ verificationCode: message.token, newPassword: "another-password" }), "invalid");
  await rejectsWith(client.loginWithEmail({ email: "mark@open-splunk.test", password: FAKE_IDENTITY_PASSWORD }), "unauthenticated");
  assert.equal((await client.loginWithEmail({ email: "mark@open-splunk.test", password: "a-new-password" })).status, "signed_in");

  client.advance(FAKE_PASSWORD_RESET_COOLDOWN_MS);
  await client.requestPasswordReset("mark@open-splunk.test");
  const expiring = client.lastMessage("password_reset", "mark@open-splunk.test");
  assert.ok(expiring && expiring.token !== message.token);
  client.advance(FAKE_PASSWORD_RESET_TTL_MS + 1);
  const expired = await rejectsWith(client.completePasswordReset({ verificationCode: expiring.token, newPassword: "a-third-password" }), "invalid");
  assert.match(expired.message, /expired/u);
});

test("an admin-created account sets its password through its invitation token", async () => {
  const client = fake();
  await rejectsWith(client.acceptInvitation({ kind: "account", token: "forged", password: "long-enough" }), "invalid");
  await rejectsWith(client.acceptInvitation({ kind: "account", token: FAKE_ACCOUNT_INVITATION_TOKEN, password: "short" }), "invalid");
  assert.deepEqual(await client.acceptInvitation({ kind: "account", token: FAKE_ACCOUNT_INVITATION_TOKEN, password: "iris-password" }), { kind: "account" });
  assert.equal(await client.getPrincipal(), null, "accepting creates no session");
  await rejectsWith(client.acceptInvitation({ kind: "account", token: FAKE_ACCOUNT_INVITATION_TOKEN, password: "iris-password" }), "invalid");
  assert.equal((await client.loginWithEmail({ email: "iris@open-splunk.test", password: "iris-password" })).status, "signed_in");
});

test("an organization invitation joins only its addressee, before it expires", async () => {
  const client = fake();
  await rejectsWith(client.acceptInvitation({ kind: "organization", invitationId: FAKE_PENDING_INVITATION_ID }), "unauthenticated");
  await signIn(client, "mark");
  await rejectsWith(client.acceptInvitation({ kind: "organization", invitationId: FAKE_PENDING_INVITATION_ID }), "forbidden");
  await rejectsWith(client.acceptInvitation({ kind: "organization", invitationId: "404" }), "not_found");

  await signIn(client, "nora");
  const accepted = await client.acceptInvitation({ kind: "organization", invitationId: FAKE_PENDING_INVITATION_ID });
  assert.equal(accepted.kind, "organization");
  assert.equal(accepted.kind === "organization" ? accepted.membership.effectiveRole : null, "member");
  const nora = await client.getPrincipal();
  assert.equal(nora?.activeOrganization?.slug, "acme");
  await rejectsWith(client.acceptInvitation({ kind: "organization", invitationId: FAKE_PENDING_INVITATION_ID }), "not_found");

  await signIn(client, "ada");
  const invitation = await client.inviteMember("2", { email: "nora@open-splunk.test", role: "moderator" });
  client.advance(8 * 24 * 60 * 60_000);
  await signIn(client, "nora");
  await rejectsWith(client.acceptInvitation({ kind: "organization", invitationId: invitation.id }), "invalid");
});

test("member management needs members.admin in that organization", async () => {
  const client = fake();
  await signIn(client, "mark");
  await rejectsWith(client.listMembers("1"), "forbidden");
  await rejectsWith(client.inviteMember("1", { email: "x@open-splunk.test", role: "member" }), "forbidden");
  await rejectsWith(client.listInvitations("1"), "forbidden");

  await signIn(client, "alice");
  await rejectsWith(client.listMembers("2"), "forbidden");
  await rejectsWith(client.listMembers("999"), "not_found");
  const members = await client.listMembers("1", { page: 1, limit: 100 });
  assert.deepEqual(members.items.map((member) => member.user.displayPrimary), [
    "Ada Lovelace", "Alice Abara", "Bart Becker", "Mark Mendez", "Mona Moreau", "Owen Ortiz", "Tess Takahashi",
  ]);
  const bart = members.items.find((member) => member.userId === "8");
  assert.equal(bart?.isActive, false);
  assert.equal(members.page.totalItems, 7);

  const first = await client.listMembers("1", { page: 1, limit: 3 });
  assert.equal(first.items.length, 3);
  assert.ok(first.page.hasNextPage);
  assert.ok(!first.page.hasPrevPage);
  assert.equal(first.page.totalPages, 3);
  const last = await client.listMembers("1", { page: 3, limit: 3 });
  assert.equal(last.items.length, 1);
  assert.ok(!last.page.hasNextPage);
  await rejectsWith(client.listMembers("1", { page: 0, limit: 3 }), "invalid");
  await rejectsWith(client.listMembers("1", { page: 1, limit: 101 }), "invalid");

  const platform = await client.listMembers("3", { page: 1, limit: 100 });
  const inheritedAda = platform.items.find((member) => member.userId === "1");
  assert.equal(inheritedAda?.explicitRole, null);
  assert.equal(inheritedAda?.effectiveRole, "owner");
  assert.equal(inheritedAda?.inheritedFromOrganizationId, "1");
});

test("role changes respect the owner rules and keep an owner", async () => {
  const client = fake();
  await signIn(client, "alice");
  const promoted = await client.changeMemberRole("1", "5", "moderator");
  assert.equal(promoted.explicitRole, "moderator");
  await rejectsWith(client.changeMemberRole("1", "5", "owner"), "forbidden");
  await rejectsWith(client.changeMemberRole("1", "1", "member"), "forbidden");
  await rejectsWith(client.changeMemberRole("1", "5", "root" as never), "invalid");
  await rejectsWith(client.changeMemberRole("1", "404", "member"), "not_found");
  await rejectsWith(client.changeMemberRole("1", "7", "member"), "not_found");
  const explicit = await client.changeMemberRole("3", "5", "moderator");
  assert.equal(explicit.explicitRole, "moderator", "an inherited member gains an explicit role");

  await signIn(client, "ada");
  await rejectsWith(client.changeMemberRole("1", "1", "admin"), "conflict");
  await client.changeMemberRole("1", "2", "owner");
  const demoted = await client.changeMemberRole("1", "1", "admin");
  assert.equal(demoted.effectiveRole, "admin");
  assert.equal((await client.getPrincipal())?.role, "admin");
});

test("removing members respects inheritance and the last owner", async () => {
  const client = fake();
  await signIn(client, "alice");
  await client.removeMember("1", "5");
  assert.ok(!(await client.listMembers("1")).items.some((member) => member.userId === "5"));
  await rejectsWith(client.removeMember("1", "5"), "not_found");
  await rejectsWith(client.removeMember("3", "1"), "conflict");
  await rejectsWith(client.removeMember("1", "1"), "forbidden");
  await client.removeMember("3", "4");

  await signIn(client, "ada");
  await rejectsWith(client.removeMember("1", "1"), "conflict");
});

test("invitations normalize addresses, refuse duplicates and can be revoked", async () => {
  const client = fake();
  await signIn(client, "alice");
  await rejectsWith(client.inviteMember("1", { email: "nope", role: "member" }), "invalid");
  await rejectsWith(client.inviteMember("1", { email: "new@open-splunk.test", role: "owner" }), "forbidden");
  await rejectsWith(client.inviteMember("1", { email: "mark@open-splunk.test", role: "member" }), "conflict");
  const created = await client.inviteMember("1", { email: " New.Person@Open-Splunk.TEST ", role: "moderator" });
  assert.equal(created.email, "new.person@open-splunk.test");
  assert.equal(created.invitedByUserId, "3");
  assert.equal(created.organization.slug, "acme");
  assert.equal(created.expiresAt, "2026-10-31T12:00:00.000Z");
  assert.equal(client.lastMessage("organization_invitation", "new.person@open-splunk.test")?.token, created.id);
  await rejectsWith(client.inviteMember("1", { email: "new.person@open-splunk.test", role: "member" }), "conflict");

  const pending = await client.listInvitations("1");
  assert.deepEqual(pending.items.map((entry) => entry.id), [FAKE_PENDING_INVITATION_ID, FAKE_EXPIRED_INVITATION_ID, created.id]);
  await client.revokeInvitation("1", created.id);
  await rejectsWith(client.revokeInvitation("1", created.id), "not_found");
  await rejectsWith(client.revokeInvitation("2", FAKE_PENDING_INVITATION_ID), "forbidden");
  assert.equal((await client.listInvitations("1")).items.length, 2);
});

test("the user console is site-admin only and filters the directory", async () => {
  const client = fake();
  await rejectsWith(client.adminListUsers(), "unauthenticated");
  await signIn(client, "owen");
  await rejectsWith(client.adminListUsers(), "forbidden");
  await rejectsWith(client.adminListOrganizations(), "forbidden");
  await rejectsWith(client.adminCreateOrganization({ name: "X", slug: "x" }), "forbidden");

  await signIn(client, "ada");
  assert.equal((await client.adminListUsers()).page.totalItems, 9);
  assert.deepEqual((await client.adminListUsers({ search: "MONA" })).items.map((entry) => entry.user.id), ["4"]);
  assert.deepEqual((await client.adminListUsers({ search: "5" })).items.map((entry) => entry.user.username), ["mark"]);
  assert.deepEqual((await client.adminListUsers({ banned: true })).items.map((entry) => entry.user.username), ["bart"]);
  assert.deepEqual((await client.adminListUsers({ siteAdmin: true })).items.map((entry) => entry.user.username), ["ada"]);
  const iris = (await client.adminListUsers({ search: "iris" })).items[0];
  assert.equal(iris?.invitationPending, true);
  assert.deepEqual(iris?.authMethods, []);
  const mark = (await client.adminListUsers({ search: "mark" })).items[0];
  assert.deepEqual(mark?.authMethods, ["password", "github"]);
});

test("site admins create accounts in each access mode", async () => {
  const client = fake();
  await signIn(client, "ada");
  const base = { firstName: "Paul", lastName: "Park", siteAdmin: false } as const;
  await rejectsWith(client.adminCreateUser({ ...base, email: "mark@open-splunk.test", username: "paul", accessMode: "oauth_only" }), "conflict");
  await rejectsWith(client.adminCreateUser({ ...base, email: "paul@open-splunk.test", username: "MARK", accessMode: "oauth_only" }), "conflict");
  await rejectsWith(client.adminCreateUser({ ...base, email: "paul@open-splunk.test", username: "p", accessMode: "oauth_only" }), "invalid");
  await rejectsWith(client.adminCreateUser({ ...base, email: "paul", username: "paul", accessMode: "oauth_only" }), "invalid");
  await rejectsWith(client.adminCreateUser({ ...base, email: "paul@open-splunk.test", username: "paul", accessMode: "password" }), "invalid");
  await rejectsWith(client.adminCreateUser({ ...base, email: "paul@open-splunk.test", username: "paul", accessMode: "oauth_only", password: "long-enough" }), "invalid");

  const withPassword = await client.adminCreateUser({ ...base, email: "paul@open-splunk.test", username: "paul", accessMode: "password", password: "paul-password" });
  assert.equal(withPassword.emailVerified, true);
  assert.equal(withPassword.user.userType, "regular");
  const invited = await client.adminCreateUser({ ...base, email: "quinn@open-splunk.test", username: "quinn", accessMode: "invitation", siteAdmin: true });
  assert.equal(invited.invitationPending, true);
  assert.equal(invited.user.userType, "admin");
  const link = client.lastMessage("account_invitation", "quinn@open-splunk.test");
  assert.ok(link);
  const oauthOnly = await client.adminCreateUser({ ...base, email: "rae@open-splunk.test", username: "rae", accessMode: "oauth_only" });
  assert.equal(oauthOnly.emailVerified, false);

  assert.equal((await client.loginWithEmail({ email: "paul@open-splunk.test", password: "paul-password" })).status, "signed_in");
  await client.acceptInvitation({ kind: "account", token: link.token, password: "quinn-password" });
  const quinn = await client.loginWithEmail({ email: "quinn@open-splunk.test", password: "quinn-password" });
  assert.equal(quinn.status === "signed_in" ? quinn.principal.siteAdmin : null, true);
});

test("site admins grant site admin, ban, revoke sessions and delete — never on themselves", async () => {
  const client = fake();
  await signIn(client, "ada");
  await rejectsWith(client.adminSetSiteAdmin("1", false), "conflict");
  await rejectsWith(client.adminBanUser("1"), "conflict");
  await rejectsWith(client.adminDeleteUser("1"), "conflict");
  await rejectsWith(client.adminBanUser("404"), "not_found");

  assert.equal((await client.adminSetSiteAdmin("5", true)).user.userType, "admin");
  const banned = await client.adminBanUser("4");
  assert.equal(banned.user.banned, true);
  assert.equal(banned.activeSessions, 0);
  assert.equal((await client.adminUnbanUser("8")).user.banned, false);
  assert.equal(await client.adminRevokeUserSessions("2"), 1);
  assert.equal(await client.adminRevokeUserSessions("2"), 0);
  await client.adminDeleteUser("6");
  assert.equal((await client.adminListUsers()).page.totalItems, 8);

  await rejectsWith(client.loginWithEmail({ email: "mona@open-splunk.test", password: FAKE_IDENTITY_PASSWORD }), "unauthenticated");
  await rejectsWith(client.loginWithEmail({ email: "tess@open-splunk.test", password: FAKE_IDENTITY_PASSWORD }), "unauthenticated");
  assert.equal((await client.loginWithEmail({ email: "bart@open-splunk.test", password: FAKE_IDENTITY_PASSWORD })).status, "signed_in");
  const mark = await signIn(client, "mark");
  assert.ok(mark.siteAdmin);
  assert.deepEqual(mark.capabilities, CAPABILITIES);
  assert.ok(!(await client.listMembers("1")).items.some((member) => member.userId === "6"), "deleting removes memberships");

  const revoked = await client.adminRevokeUserSessions("5");
  assert.ok(revoked >= 1);
  assert.equal(await client.getPrincipal(), null, "revoking your own sessions signs you out");
});

test("site admins list and create organizations", async () => {
  const client = fake();
  await signIn(client, "ada");
  assert.deepEqual((await client.adminListOrganizations({ search: "ACME" })).items.map((entry) => entry.slug), ["acme", "acme-platform"]);
  await rejectsWith(client.adminCreateOrganization({ name: "Initech", slug: "Initech" }), "invalid");
  await rejectsWith(client.adminCreateOrganization({ name: " ", slug: "initech" }), "invalid");
  await rejectsWith(client.adminCreateOrganization({ name: "Acme again", slug: "acme" }), "conflict");
  await rejectsWith(client.adminCreateOrganization({ name: "Orphan", slug: "orphan", parentOrganizationId: "404" }), "not_found");
  const child = await client.adminCreateOrganization({ name: "Acme Platform SRE", slug: "acme-platform-sre", description: " On call ", parentOrganizationId: "3" });
  assert.deepEqual(child.lineage, ["1", "3"]);
  assert.equal(child.parentOrganizationId, "3");
  assert.equal(child.description, "On call");
  assert.equal(child.createdAt, "2026-10-24T12:00:00.000Z");
  const alice = await signIn(client, "alice");
  assert.ok(alice.organizations.some((entry) => entry.organization.id === child.id && entry.inheritedFromOrganizationId === "1"), "roles inherit through every ancestor");
});

test("failNext injects a network failure into exactly the next call", async () => {
  const client = fake();
  client.failNext("getPrincipal");
  client.failNext("loginWithEmail", new IdentityError("rate_limited", "slow down", { retryAfterMs: 5 }));
  await rejectsWith(client.getPrincipal(), "network");
  assert.equal(await client.getPrincipal(), null);
  const limited = await rejectsWith(client.loginWithEmail({ email: "mark@open-splunk.test", password: FAKE_IDENTITY_PASSWORD }), "rate_limited");
  assert.equal(limited.retryAfterMs, 5);
  assert.equal((await client.loginWithEmail({ email: "mark@open-splunk.test", password: FAKE_IDENTITY_PASSWORD })).status, "signed_in");
});

test("results are copies that cannot reach back into the directory", async () => {
  const client = fake();
  const principal = await signIn(client, "mark") as unknown as { user: { email: string }; capabilities: string[] };
  principal.user.email = "forged@open-splunk.test";
  principal.capabilities.push("server.admin");
  const fresh = await client.getPrincipal();
  assert.equal(fresh?.user.email, "mark@open-splunk.test");
  assert.ok(!fresh?.capabilities.includes("server.admin"));
});

test("the session survives a new instance through storage, including a sign-out", async () => {
  const storage = storageStub();
  const first = fake({ storage });
  await signIn(first, "owen");
  await first.setActiveOrganization("2");

  const second = fake({ storage });
  const restored = await second.getPrincipal();
  assert.equal(restored?.user.username, "owen");
  assert.equal(restored?.activeOrganization?.slug, "globex");

  await second.logout();
  const third = fake({ storage, initialSignedInEmail: "ada@open-splunk.test" });
  assert.equal(await third.getPrincipal(), null, "a remembered sign-out beats the initial account");

  const fresh = fake({ storage: storageStub(), initialSignedInEmail: "ada@open-splunk.test" });
  assert.equal((await fresh.getPrincipal())?.user.username, "ada");

  const damaged = storageStub();
  damaged.setItem("open-splunk.fake-identity-session", "{not json");
  assert.equal(await fake({ storage: damaged }).getPrincipal(), null);
  damaged.setItem("open-splunk.fake-identity-session", JSON.stringify({ v: 1, userId: "8", activeOrganizationId: null }));
  assert.equal(await fake({ storage: damaged }).getPrincipal(), null, "a stored banned account is not restored");
});

test("blocked storage costs only persistence", async () => {
  const blocked: FakeIdentityStorage = {
    getItem: () => { throw new DOMException("denied", "SecurityError"); },
    setItem: () => { throw new DOMException("denied", "SecurityError"); },
    removeItem: () => { throw new DOMException("denied", "SecurityError"); },
  };
  const client = fake({ storage: blocked, initialSignedInEmail: "ada@open-splunk.test" });
  assert.equal((await client.getPrincipal())?.user.username, "ada");
  await client.logout();
  assert.equal(await client.getPrincipal(), null);
});

test("latency delays settlement without changing results", async () => {
  const client = createFakeIdentityClient({ latencyMs: 5 });
  let settled = false;
  const pending = client.getPrincipal().then((principal) => {
    settled = true;
    return principal;
  });
  await Promise.resolve();
  assert.equal(settled, false);
  assert.equal(await pending, null);
});
