import assert from "node:assert/strict";
import test from "node:test";

import { IDENTITY_ERROR_KINDS, IdentityError, type AdminUser, type IdentityOrganization } from "@/lib/api/identity";

import {
  EMPTY_CREATE_ORGANIZATION_DRAFT,
  EMPTY_CREATE_USER_DRAFT,
  adminUserStatus,
  asIdentityError,
  authMethodsLabel,
  createOrganizationErrorField,
  createOrganizationInput,
  createUserErrorField,
  createUserInput,
  createUserSuccessMessage,
  hasErrors,
  isUserFilter,
  organizationAncestry,
  organizationKind,
  organizationNames,
  pageAfterRemoval,
  pagePositionLabel,
  pageRangeLabel,
  presentDirectoryError,
  presentMutationError,
  retryAfterSeconds,
  slugFromName,
  userActionCopy,
  userActionSuccessMessage,
  userActionsFor,
  userDisplayName,
  userQueryFor,
  validateCreateOrganizationDraft,
  validateCreateUserDraft,
  withOrganizationName,
} from "./site-directory-data";

function admin(overrides: Partial<AdminUser["user"]> = {}, extra: Partial<Omit<AdminUser, "user">> = {}): AdminUser {
  return {
    activeSessions: 2,
    authMethods: ["password"],
    createdAt: "2026-01-05T09:00:00.000Z",
    emailVerified: true,
    invitationPending: false,
    lastLogin: "2026-10-20T08:15:00.000Z",
    totalLogins: 3,
    updatedAt: "2026-01-05T09:00:00.000Z",
    user: {
      banned: false,
      email: "dana@example.test",
      firstName: "Dana",
      id: "42",
      isBot: false,
      lastName: "Diaz",
      userType: "regular",
      username: "dana",
      ...overrides,
    },
    ...extra,
  };
}

function organization(id: string, name: string, overrides: Partial<IdentityOrganization> = {}): IdentityOrganization {
  return {
    createdAt: "2026-01-05T09:00:00.000Z",
    id,
    lineage: [],
    name,
    slug: slugFromName(name),
    updatedAt: "2026-01-05T09:00:00.000Z",
    ...overrides,
  };
}

test("every identity error kind presents a titled state with exactly one next step", () => {
  for (const kind of IDENTITY_ERROR_KINDS) {
    for (const subject of ["users", "organizations"] as const) {
      const presentation = presentDirectoryError(new IdentityError(kind, `detail for ${kind}`), subject);
      assert.ok(presentation.title.length > 0, kind);
      assert.ok(presentation.message.length > 0, kind);
      assert.ok(["retry", "sign_in", "clear_search", "none"].includes(presentation.action), kind);
    }
    assert.ok(presentMutationError(new IdentityError(kind, `detail for ${kind}`)).length > 0, kind);
  }
});

function nextStepFor(kind: (typeof IDENTITY_ERROR_KINDS)[number]) {
  return presentDirectoryError(new IdentityError(kind, "x"), "users").action;
}

test("each kind maps to the action an administrator can take about it", () => {
  assert.equal(nextStepFor("unauthenticated"), "sign_in");
  assert.equal(nextStepFor("otp_required"), "sign_in");
  assert.equal(nextStepFor("forbidden"), "none");
  assert.equal(nextStepFor("not_configured"), "none");
  assert.equal(nextStepFor("invalid"), "clear_search");
  assert.equal(nextStepFor("network"), "retry");
  assert.equal(nextStepFor("rate_limited"), "retry");
  assert.equal(presentDirectoryError(new IdentityError("not_configured", "x"), "users").kind, "unavailable");
  assert.equal(presentDirectoryError(new IdentityError("network", "x"), "users").kind, "error");
});

test("a rate limit carries its wait; a mutation's message names it in whole seconds", () => {
  const limited = new IdentityError("rate_limited", "slow down", { retryAfterMs: 4_200 });
  assert.equal(presentDirectoryError(limited, "organizations").retryAfterMs, 4_200);
  assert.equal(presentDirectoryError(new IdentityError("rate_limited", "slow down"), "users").retryAfterMs, undefined);
  assert.match(presentMutationError(limited), /5 s/u);
  assert.equal(retryAfterSeconds(1), 1);
  assert.equal(retryAfterSeconds(0), 1);
  assert.equal(retryAfterSeconds(2_000), 2);
  assert.equal(retryAfterSeconds(2_001), 3);
});

test("conflict and invalid mutations quote the service; the rest explain in product words", () => {
  assert.equal(presentMutationError(new IdentityError("conflict", "You cannot ban yourself.")), "You cannot ban yourself.");
  assert.equal(presentMutationError(new IdentityError("invalid", "Bad slug")), "Bad slug");
  assert.match(presentMutationError(new IdentityError("not_found", "User not found")), /no longer exists/u);
  assert.match(presentMutationError(new IdentityError("network", "socket")), /Nothing was changed/u);
});

test("a non-identity rejection becomes a network error that keeps its message and cause", () => {
  const cause = new TypeError("Failed to fetch");
  const error = asIdentityError(cause);
  assert.equal(error.kind, "network");
  assert.equal(error.message, "Failed to fetch");
  assert.equal(error.cause, cause);
  const original = new IdentityError("forbidden", "no");
  assert.equal(asIdentityError(original), original);
  assert.equal(asIdentityError("boom").kind, "network");
});

test("the search box and filter compose the contract query", () => {
  assert.deepEqual(userQueryFor("", "all"), {});
  assert.deepEqual(userQueryFor("  ada  ", "all"), { search: "ada" });
  assert.deepEqual(userQueryFor("", "site_admins"), { siteAdmin: true });
  assert.deepEqual(userQueryFor("b", "banned"), { banned: true, search: "b" });
  assert.equal(isUserFilter("banned"), true);
  assert.equal(isUserFilter("everyone"), false);
});

test("display names fall back from the full name to the username to the email", () => {
  assert.equal(userDisplayName(admin().user), "Dana Diaz");
  assert.equal(userDisplayName(admin({ firstName: "", lastName: "" }).user), "dana");
  assert.equal(userDisplayName(admin({ firstName: " ", lastName: "", username: "" }).user), "dana@example.test");
});

test("account status reports the most severe state first", () => {
  assert.deepEqual(adminUserStatus(admin({ banned: true }, { invitationPending: true })), { label: "Banned", tone: "error" });
  assert.deepEqual(adminUserStatus(admin({}, { invitationPending: true })), { label: "Invitation pending", tone: "warning" });
  const neverSignedIn = admin();
  delete (neverSignedIn as { lastLogin?: string }).lastLogin;
  assert.deepEqual(adminUserStatus(neverSignedIn), { label: "Never signed in", tone: "neutral" });
  assert.deepEqual(adminUserStatus(admin()), { label: "Active", tone: "success" });
});

test("sign-in methods read as product names, once each", () => {
  assert.equal(authMethodsLabel([]), "No sign-in method yet");
  assert.equal(authMethodsLabel(["password", "github"]), "Password · GitHub");
  assert.equal(authMethodsLabel(["email", "password", "azure"]), "Password · Azure");
});

test("row actions flip with the account's state, and the self row disables what go-common refuses", () => {
  const other = userActionsFor(admin(), "1");
  assert.deepEqual(other.map((item) => item.action), ["grant_site_admin", "ban", "revoke_sessions", "delete"]);
  assert.ok(other.every((item) => item.disabledReason === null));

  const bannedAdmin = userActionsFor(admin({ banned: true, userType: "admin" }), "1");
  assert.deepEqual(bannedAdmin.map((item) => item.action), ["revoke_site_admin", "unban", "revoke_sessions", "delete"]);

  const self = userActionsFor(admin({ id: "1", userType: "admin" }), "1");
  assert.deepEqual(self.map((item) => [item.action, item.disabledReason !== null]), [
    ["revoke_site_admin", true],
    ["ban", true],
    ["revoke_sessions", false],
    ["delete", true],
  ]);
  assert.equal(self.find((item) => item.action === "revoke_sessions")?.label, "Sign out my sessions");

  const idle = userActionsFor(admin({}, { activeSessions: 0 }), "1");
  assert.equal(idle.find((item) => item.action === "revoke_sessions")?.disabledReason, "This account has no active sessions.");
  assert.equal(other.find((item) => item.action === "ban")?.danger, true);
  assert.equal(other.find((item) => item.action === "grant_site_admin")?.danger, false);
});

test("only delete asks for the email to be typed; destructive actions are dangerous", () => {
  const target = admin();
  for (const action of ["grant_site_admin", "revoke_site_admin", "ban", "unban", "revoke_sessions", "delete"] as const) {
    const copy = userActionCopy(action, target, false);
    assert.ok(copy.title.includes("Dana Diaz") || action === "revoke_sessions", action);
    assert.equal(copy.typedConfirmation, action === "delete" ? "dana@example.test" : null, action);
    assert.equal(copy.danger, !["grant_site_admin", "unban"].includes(action), action);
  }
  assert.match(userActionCopy("ban", target, false).description, /2 active sessions/u);
  assert.match(userActionCopy("ban", admin({}, { activeSessions: 1 }), false).description, /1 active session /u);
  assert.match(userActionCopy("revoke_sessions", target, true).description, /including this one/u);
});

test("success messages name the user and the revoked count", () => {
  const target = admin();
  assert.equal(userActionSuccessMessage("ban", target), "Dana Diaz is banned.");
  assert.equal(userActionSuccessMessage("revoke_sessions", target, 1), "Revoked 1 session for Dana Diaz.");
  assert.equal(userActionSuccessMessage("revoke_sessions", target, 3), "Revoked 3 sessions for Dana Diaz.");
  assert.equal(userActionSuccessMessage("delete", target), "Dana Diaz was deleted.");
});

test("a create-user draft is validated per field, with the password only for the password mode", () => {
  assert.deepEqual(validateCreateUserDraft(EMPTY_CREATE_USER_DRAFT), { email: "Enter an email address.", username: "Enter a username." });
  const draft = { ...EMPTY_CREATE_USER_DRAFT, email: "dana@example.test", username: "dana" };
  assert.deepEqual(validateCreateUserDraft(draft), {});
  assert.equal(hasErrors(validateCreateUserDraft(draft)), false);
  assert.ok(validateCreateUserDraft({ ...draft, email: "dana@" }).email);
  assert.ok(validateCreateUserDraft({ ...draft, username: "da" }).username);
  assert.ok(validateCreateUserDraft({ ...draft, username: "dana diaz" }).username);
  assert.ok(validateCreateUserDraft({ ...draft, firstName: "x".repeat(101) }).firstName);
  assert.ok(validateCreateUserDraft({ ...draft, accessMode: "password", password: "short" }).password);
  assert.deepEqual(validateCreateUserDraft({ ...draft, accessMode: "password", password: "long enough" }), {});
  assert.deepEqual(validateCreateUserDraft({ ...draft, accessMode: "invitation", password: "x" }), {});
});

test("the create-user input trims, lowercases the email, and sends a password only with its mode", () => {
  const draft = {
    ...EMPTY_CREATE_USER_DRAFT,
    email: " Dana@Example.TEST ",
    firstName: " Dana ",
    password: "long enough",
    reason: "  ",
    username: " dana ",
  };
  assert.deepEqual(createUserInput(draft), {
    accessMode: "invitation",
    email: "dana@example.test",
    firstName: "Dana",
    lastName: "",
    siteAdmin: false,
    username: "dana",
  });
  assert.deepEqual(createUserInput({ ...draft, accessMode: "password", reason: "onboarding", siteAdmin: true }), {
    accessMode: "password",
    email: "dana@example.test",
    firstName: "Dana",
    lastName: "",
    password: "long enough",
    reason: "onboarding",
    siteAdmin: true,
    username: "dana",
  });
});

test("server rejections land on the field they name", () => {
  assert.equal(createUserErrorField(new IdentityError("conflict", "Email already exists")), "email");
  assert.equal(createUserErrorField(new IdentityError("conflict", "User already exists")), "username");
  assert.equal(createUserErrorField(new IdentityError("invalid", "Usernames are 3–32 letters")), "username");
  assert.equal(createUserErrorField(new IdentityError("invalid", "Passwords need at least 8 characters.")), "password");
  assert.equal(createUserErrorField(new IdentityError("network", "Email server down")), null);
  assert.equal(createUserErrorField(new IdentityError("conflict", "Something else")), null);
  assert.equal(createOrganizationErrorField(new IdentityError("conflict", "That slug is already taken.")), "slug");
  assert.equal(createOrganizationErrorField(new IdentityError("invalid", "Organization names are 1–100 characters.")), "name");
  assert.equal(createOrganizationErrorField(new IdentityError("forbidden", "slug")), null);
});

test("the creation toast says how the new account gets in", () => {
  const created = admin();
  assert.match(createUserSuccessMessage(created, "invitation"), /sent an invitation to dana@example\.test/u);
  assert.match(createUserSuccessMessage(created, "password"), /Share the password/u);
  assert.match(createUserSuccessMessage(created, "oauth_only"), /single sign-on/u);
});

test("organizations are personal at any depth, else top level or child", () => {
  assert.equal(organizationKind(organization("1", "Acme")), "top_level");
  assert.equal(organizationKind(organization("3", "Platform", { lineage: ["1"], parentOrganizationId: "1" })), "child");
  assert.equal(organizationKind(organization("9", "Mine", { lineage: ["8"], parentOrganizationId: "8", personalOwnerUserId: "5" })), "personal");
});

test("ancestry reads names root first, and an ancestor not loaded reads as its ID", () => {
  const names = organizationNames([organization("1", "Acme")], [organization("3", "Platform")]);
  const grandchild = organization("7", "Edge", { lineage: ["1", "3"], parentOrganizationId: "3" });
  assert.deepEqual(organizationAncestry(grandchild, names), [{ id: "1", name: "Acme" }, { id: "3", name: "Platform" }]);
  assert.deepEqual(organizationAncestry(organization("8", "Lost", { lineage: ["99"], parentOrganizationId: "99" }), names), [
    { id: "99", name: "Organization 99" },
  ]);
  // A server that sends only the parent still shows one step.
  assert.deepEqual(organizationAncestry(organization("4", "Only parent", { parentOrganizationId: "1" }), names), [{ id: "1", name: "Acme" }]);
  assert.deepEqual(organizationAncestry(organization("1", "Acme"), names), []);
  assert.equal(organizationNames([organization("1", "Old")], [organization("1", "New")]).get("1"), "New");
});

test("a slug is suggested from the name until the slug is edited by hand", () => {
  assert.equal(slugFromName("Acme Ops (EU)"), "acme-ops-eu");
  assert.equal(slugFromName("  Café — Zürich  "), "cafe-zurich");
  assert.equal(slugFromName("!!!"), "");
  assert.equal(slugFromName("a".repeat(70)).length, 63);
  assert.equal(slugFromName(`${"a".repeat(62)} b`), "a".repeat(62));
  const named = withOrganizationName(EMPTY_CREATE_ORGANIZATION_DRAFT, "Acme Ops");
  assert.equal(named.slug, "acme-ops");
  const edited = withOrganizationName({ ...named, slug: "ops", slugEdited: true }, "Acme Operations");
  assert.equal(edited.slug, "ops");
  assert.equal(edited.name, "Acme Operations");
});

test("an organization draft needs a name and a well-formed slug", () => {
  assert.deepEqual(validateCreateOrganizationDraft(EMPTY_CREATE_ORGANIZATION_DRAFT), { name: "Enter a name.", slug: "Enter a slug." });
  const draft = withOrganizationName(EMPTY_CREATE_ORGANIZATION_DRAFT, "Acme Ops");
  assert.deepEqual(validateCreateOrganizationDraft(draft), {});
  assert.ok(validateCreateOrganizationDraft({ ...draft, slug: "Acme" }).slug);
  assert.ok(validateCreateOrganizationDraft({ ...draft, slug: "acme--ops" }).slug);
  assert.ok(validateCreateOrganizationDraft({ ...draft, slug: "a".repeat(64) }).slug);
  assert.ok(validateCreateOrganizationDraft({ ...draft, name: "x".repeat(101) }).name);
  assert.ok(validateCreateOrganizationDraft({ ...draft, description: "x".repeat(501) }).description);
  assert.deepEqual(createOrganizationInput({ ...draft, description: "  " }), { name: "Acme Ops", slug: "acme-ops" });
  assert.deepEqual(createOrganizationInput({ ...draft, description: " Ops ", parentId: "1" }), {
    description: "Ops",
    name: "Acme Ops",
    parentOrganizationId: "1",
    slug: "acme-ops",
  });
});

test("page labels count from the page's first row, with or without a total", () => {
  const info = { currentPage: 2, hasNextPage: true, hasPrevPage: true, itemsPerPage: 10, totalItems: 23, totalPages: 3 };
  assert.equal(pageRangeLabel(info, 10, "users"), "Showing 11–20 of 23 users");
  assert.equal(pageRangeLabel({ ...info, totalItems: undefined }, 10, "users"), "Showing 11–20 users");
  assert.equal(pageRangeLabel(info, 0, "users"), "No users");
  assert.equal(pagePositionLabel(info), "Page 2 of 3");
  assert.equal(pagePositionLabel({ ...info, totalPages: undefined }), "Page 2");
  assert.equal(pageAfterRemoval({ ...info, currentPage: 3 }, 0), 2);
  assert.equal(pageAfterRemoval({ ...info, currentPage: 3 }, 2), 3);
  assert.equal(pageAfterRemoval({ ...info, currentPage: 1 }, 0), 1);
});
