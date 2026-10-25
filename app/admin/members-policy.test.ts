import assert from "node:assert/strict";
import test from "node:test";

import {
  IDENTITY_ERROR_KINDS,
  IdentityError,
  ORG_ROLES,
  type OrganizationMember,
  type Page,
} from "@/lib/api/identity";
import { FAKE_EXPIRED_INVITATION_ID, FAKE_PENDING_INVITATION_ID, createFakeIdentityClient } from "@/lib/api/identity-fake";

import {
  actorHasTopRole,
  canManageMemberHolding,
  capabilityChange,
  countExplicitOwners,
  describeMembersFailure,
  grantableRoles,
  invitationState,
  inviteEmailError,
  memberPolicy,
  roleChangeNotices,
  roleWithArticle,
  type MembersActor,
  type MembersContext,
} from "./members-policy";

const ACME = "1";
const PLATFORM = "3";
const NAMES: Record<string, string> = { 1: "Acme Operations", 2: "Globex Security", 3: "Acme Platform" };
const NOW = Date.parse("2026-10-24T12:00:00.000Z");

const ada: MembersActor = { userId: "1", role: "owner", siteAdmin: true };
const owen: MembersActor = { userId: "2", role: "owner", siteAdmin: false };
const alice: MembersActor = { userId: "3", role: "admin", siteAdmin: false };
const mona: MembersActor = { userId: "4", role: "moderator", siteAdmin: false };

async function membersOf(organizationId: string, limit = 100): Promise<Page<OrganizationMember>> {
  const client = createFakeIdentityClient({ initialSignedInEmail: "ada@open-splunk.test", now: () => NOW });
  return client.listMembers(organizationId, { page: 1, limit });
}

function find(page: Page<OrganizationMember>, userId: string): OrganizationMember {
  const member = page.items.find((item) => item.userId === userId);
  assert.ok(member, `user ${userId} is not listed`);
  return member;
}

function context(page: Page<OrganizationMember>): MembersContext {
  return {
    explicitOwners: countExplicitOwners(page),
    organizationName: (id) => (id === undefined ? undefined : NAMES[id]) ?? "a parent organization",
  };
}

test("grantable roles are strictly below the actor's band, or every role from the top", () => {
  assert.deepEqual(grantableRoles(ada), ORG_ROLES);
  assert.deepEqual(grantableRoles(owen), ORG_ROLES);
  assert.deepEqual(grantableRoles(alice), ["member", "moderator"]);
  assert.deepEqual(grantableRoles(mona), ["member"]);
  assert.deepEqual(grantableRoles({ userId: "9", role: "member", siteAdmin: false }), []);
  assert.deepEqual(grantableRoles({ userId: "9", role: null, siteAdmin: false }), []);
  // A site admin acting in an organization it holds no role in still grants everything.
  assert.deepEqual(grantableRoles({ userId: "1", role: null, siteAdmin: true }), ORG_ROLES);
  assert.equal(actorHasTopRole({ userId: "1", role: null, siteAdmin: true }), true);
  assert.equal(actorHasTopRole(alice), false);
});

test("an actor manages only members holding a role below its own", () => {
  assert.equal(canManageMemberHolding(alice, "moderator"), true);
  assert.equal(canManageMemberHolding(alice, "admin"), false);
  assert.equal(canManageMemberHolding(alice, "owner"), false);
  assert.equal(canManageMemberHolding(owen, "owner"), true);
  assert.equal(canManageMemberHolding({ userId: "9", role: null, siteAdmin: false }, "member"), false);
});

test("explicit owners are counted only when the page holds the whole organization", async () => {
  assert.equal(countExplicitOwners(await membersOf(ACME)), 1);
  assert.equal(countExplicitOwners(await membersOf(ACME, 2)), null);
  // Acme Platform's owner is inherited, so it has no explicit owner at all.
  assert.equal(countExplicitOwners(await membersOf(PLATFORM)), 0);
});

test("the last explicit owner can be neither demoted nor removed, even by itself", async () => {
  const page = await membersOf(ACME);
  const policy = memberPolicy(ada, find(page, "1"), context(page));
  assert.equal(policy.self, true);
  assert.equal(policy.lastOwner, true);
  assert.equal(policy.roleChange.allowed, false);
  assert.equal(policy.removal.allowed, false);
  assert.match(policy.roleChange.allowed ? "" : policy.roleChange.reason, /last owner.*another member an owner first/u);
  assert.match(policy.removal.allowed ? "" : policy.removal.reason, /last owner/u);
});

test("with a second owner, a site-admin owner may demote or remove itself", async () => {
  const page = await membersOf(ACME);
  const policy = memberPolicy(ada, find(page, "1"), { ...context(page), explicitOwners: 2 });
  assert.equal(policy.lastOwner, false);
  assert.equal(policy.roleChange.allowed, true);
  assert.deepEqual(policy.removal, { allowed: true, self: true });
});

test("an owner without site admin cannot change its own role, but may leave once another owner exists", async () => {
  // go-common checks the target's current role against the actor's, so an owner never outranks itself.
  const page = await membersOf("2");
  const self = find(page, "2");
  const alone = memberPolicy(owen, self, context(page));
  assert.equal(alone.lastOwner, true);
  assert.deepEqual(alone.roleChange, { allowed: false, reason: "The last owner cannot be demoted or removed. Make another member an owner first." });
  assert.equal(alone.removal.allowed, false);

  const withPeer = memberPolicy(owen, self, { ...context(page), explicitOwners: 2 });
  assert.deepEqual(withPeer.roleChange, {
    allowed: false,
    reason: "Owners cannot change their own role. Make another member an owner, then leave, or ask a site admin.",
  });
  assert.deepEqual(withPeer.removal, { allowed: true, self: true });
});

test("an owner is offered another owner's role and removal, leaving a tier refusal to the server", async () => {
  const page = await membersOf(ACME);
  const otherOwner: OrganizationMember = { ...find(page, "1"), explicitRole: "owner", effectiveRole: "owner" };
  const policy = memberPolicy(owen, otherOwner, { ...context(page), explicitOwners: 2 });
  assert.equal(policy.roleChange.allowed, true);
  assert.deepEqual(policy.removal, { allowed: true, self: false });
});

test("a site admin with no role in the organization manages every member", async () => {
  const page = await membersOf(ACME);
  const visitor: MembersActor = { userId: "99", role: null, siteAdmin: true };
  const policy = memberPolicy(visitor, find(page, "3"), context(page));
  assert.ok(policy.roleChange.allowed);
  assert.deepEqual(policy.roleChange.options.filter((option) => option.disabled), []);
  assert.deepEqual(policy.removal, { allowed: true, self: false });
});

test("an unknown owner count leaves the last-owner guard to the server", async () => {
  const page = await membersOf(ACME);
  const policy = memberPolicy(ada, find(page, "1"), { ...context(page), explicitOwners: null });
  assert.equal(policy.lastOwner, false);
  assert.equal(policy.roleChange.allowed, true);
});

test("an admin can change a moderator only to roles below admin", async () => {
  const page = await membersOf(ACME);
  const policy = memberPolicy(alice, find(page, "4"), context(page));
  assert.ok(policy.roleChange.allowed);
  assert.equal(policy.roleChange.current, "moderator");
  assert.deepEqual(policy.roleChange.options.map((option) => [option.role, option.current, option.disabled]), [
    ["member", false, false],
    ["moderator", true, false],
    ["admin", false, true],
    ["owner", false, true],
  ]);
  assert.equal(policy.roleChange.options[2]?.reason, "Above what your role can grant.");
  assert.deepEqual(policy.removal, { allowed: true, self: false });
});

test("an admin cannot touch an owner or another admin, nor change its own role, but may leave", async () => {
  const page = await membersOf(ACME);
  const onOwner = memberPolicy(alice, find(page, "1"), context(page));
  assert.deepEqual(onOwner.roleChange, { allowed: false, reason: "Only a role above owner can change an owner." });
  assert.deepEqual(onOwner.removal, { allowed: false, reason: "Only a role above owner can remove an owner." });
  const peer: OrganizationMember = { ...find(page, "4"), explicitRole: "admin", effectiveRole: "admin" };
  const onPeer = memberPolicy(alice, peer, context(page));
  assert.deepEqual(onPeer.roleChange, { allowed: false, reason: "Only a role above admin can change an admin." });
  assert.deepEqual(onPeer.removal, { allowed: false, reason: "Only a role above admin can remove an admin." });
  const onSelf = memberPolicy(alice, find(page, "3"), context(page));
  assert.deepEqual(onSelf.roleChange, { allowed: false, reason: "Ask an owner to change your own role." });
  // go-common lets anyone who may remove members remove themselves.
  assert.deepEqual(onSelf.removal, { allowed: true, self: true });
});

test("an owner may grant the owner role", async () => {
  const page = await membersOf(ACME);
  const policy = memberPolicy(ada, find(page, "3"), context(page));
  assert.ok(policy.roleChange.allowed);
  assert.deepEqual(policy.roleChange.options.filter((option) => option.disabled), []);
});

test("an inherited-only member has no membership row here to change or remove", async () => {
  // go-common updates and deletes the membership row; with none here the request would fail.
  const page = await membersOf(PLATFORM);
  const inherited = find(page, "3");
  assert.equal(inherited.explicitRole, null);
  assert.equal(inherited.effectiveRole, "admin");
  assert.equal(inherited.inheritedFromOrganizationId, ACME);
  const policy = memberPolicy(ada, inherited, context(page));
  assert.deepEqual(policy.removal, { allowed: false, reason: "Inherited from Acme Operations; remove them there." });
  assert.deepEqual(policy.roleChange, { allowed: false, reason: "Inherited from Acme Operations; change it there." });
});

test("an inherited role nobody here can outrank is locked with a pointer to its organization", async () => {
  const page = await membersOf(PLATFORM);
  // Ada holds owner in Acme Platform only through Acme Operations.
  const policy = memberPolicy(owen, find(page, "1"), context(page));
  assert.deepEqual(policy.roleChange, { allowed: false, reason: "Inherited from Acme Operations; change it there." });
});

test("removing an explicit role below an inherited one keeps the inherited role", async () => {
  const page = await membersOf(PLATFORM);
  const member: OrganizationMember = {
    ...find(page, "4"),
    explicitRole: "member",
    effectiveRole: "admin",
    inheritedFromOrganizationId: ACME,
  };
  const policy = memberPolicy(ada, member, context(page));
  assert.deepEqual(policy.removal, { allowed: true, self: false, keepsInheritedRole: "admin", inheritedFromOrganizationId: ACME });
  assert.ok(policy.roleChange.allowed);
  assert.equal(policy.roleChange.current, "member");
  // The explicit row may still change below the inherited role; the dialog warns that it outranks.
  assert.deepEqual(policy.roleChange.options.filter((option) => option.disabled), []);
  assert.deepEqual(roleChangeNotices(ada, member, "moderator"), [{ kind: "inherited_floor", role: "admin", organizationId: ACME }]);
});

test("role-change notices warn about granting ownership and lowering one's own role", async () => {
  const page = await membersOf(ACME);
  assert.deepEqual(roleChangeNotices(ada, find(page, "3"), "owner"), [{ kind: "owner_grant" }]);
  assert.deepEqual(roleChangeNotices(ada, find(page, "1"), "admin"), [{ kind: "self_demotion", siteAdmin: true }]);
  assert.deepEqual(roleChangeNotices(owen, { ...find(page, "2"), explicitRole: "owner", effectiveRole: "owner" }, "member"), [{ kind: "self_demotion", siteAdmin: false }]);
  assert.deepEqual(roleChangeNotices(ada, find(page, "3"), "admin"), []);
  assert.deepEqual(roleChangeNotices(ada, find(page, "3"), "member"), []);
});

test("a role change names the capabilities gained and lost", () => {
  assert.deepEqual(capabilityChange("member", "moderator"), {
    gained: ["knowledge.write", "lookups.write", "alerts.write", "reports.write"],
    lost: [],
  });
  const demotion = capabilityChange("owner", "admin");
  assert.deepEqual(demotion, { gained: [], lost: ["org.admin"] });
  assert.deepEqual(capabilityChange("admin", "admin"), { gained: [], lost: [] });
  assert.ok(capabilityChange("admin", "member").lost.includes("members.admin"));
});

test("an invitation past its expiry reads as expired", async () => {
  const client = createFakeIdentityClient({ initialSignedInEmail: "ada@open-splunk.test", now: () => NOW });
  const { items } = await client.listInvitations(ACME);
  const expired = items.find((invitation) => invitation.id === FAKE_EXPIRED_INVITATION_ID);
  const pending = items.find((invitation) => invitation.id === FAKE_PENDING_INVITATION_ID);
  assert.ok(expired && pending);
  assert.equal(invitationState(expired, NOW), "expired");
  assert.equal(invitationState(pending, NOW), "pending");
  assert.equal(invitationState({ ...pending, expiresAt: new Date(NOW).toISOString() }, NOW), "expired");
  assert.equal(invitationState({ ...pending, expiresAt: "not a date" }, NOW), "pending");
});

test("the invite form refuses an empty or malformed address before sending", () => {
  assert.equal(inviteEmailError(""), "Enter the email address to invite.");
  assert.equal(inviteEmailError("   "), "Enter the email address to invite.");
  assert.match(inviteEmailError("not-an-address") ?? "", /valid email/u);
  assert.match(inviteEmailError("two@@example.com") ?? "", /valid email/u);
  assert.match(inviteEmailError("space in@example.com") ?? "", /valid email/u);
  assert.equal(inviteEmailError(" new.person@example.com "), null);
});

test("every identity error kind has a description, and only the transient ones are retryable", () => {
  for (const kind of IDENTITY_ERROR_KINDS) {
    const failure = describeMembersFailure(new IdentityError(kind, `server said ${kind}`), "load members");
    assert.ok(failure.title.length > 0 && failure.message.length > 0, kind);
    assert.equal(failure.retryable, kind === "rate_limited" || kind === "network", kind);
    assert.equal(failure.signedOut, kind === "unauthenticated" || kind === "otp_required", kind);
  }
  assert.equal(describeMembersFailure(new IdentityError("conflict", "An organization needs at least one owner."), "change the role").message, "An organization needs at least one owner.");
  assert.equal(describeMembersFailure(new IdentityError("rate_limited", "slow", { retryAfterMs: 1500 }), "x").message, "Wait 2 seconds and try again.");
  assert.equal(describeMembersFailure(new IdentityError("rate_limited", "slow", { retryAfterMs: 200 }), "x").message, "Wait 1 second and try again.");
  assert.equal(describeMembersFailure(new IdentityError("rate_limited", "slow"), "x").message, "Wait a moment and try again.");
  assert.match(describeMembersFailure(new IdentityError("not_found", "Invitation not found"), "revoke").message, /^Invitation not found\. The list has been refreshed\.$/u);
  const unknown = describeMembersFailure(new Error("boom"), "load members");
  assert.equal(unknown.title, "Could not load members");
  assert.equal(unknown.retryable, true);
});

test("roles read naturally in running text", () => {
  assert.equal(roleWithArticle("member"), "a member");
  assert.equal(roleWithArticle("moderator"), "a moderator");
  assert.equal(roleWithArticle("admin"), "an admin");
  assert.equal(roleWithArticle("owner"), "an owner");
});
