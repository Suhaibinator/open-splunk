import assert from "node:assert/strict";
import test from "node:test";

import { IDENTITY_ERROR_KINDS, IdentityError } from "@/lib/api/identity";

import { describeAuthError, formatWait, toIdentityError, type AuthFlow } from "./auth-error-copy";

const FLOWS: readonly AuthFlow[] = [
  "session",
  "sign_in",
  "otp",
  "providers",
  "oauth_start",
  "oauth_callback",
  "reset_request",
  "reset_complete",
  "account_invitation",
  "register",
  "verify_email",
  "invitations",
  "accept_invitation",
  "decline_invitation",
];

test("every error kind has a title and keeps its kind on every surface", () => {
  for (const kind of IDENTITY_ERROR_KINDS) {
    for (const flow of FLOWS) {
      const copy = describeAuthError(new IdentityError(kind, "server said"), flow);
      assert.equal(copy.kind, kind, `${kind} on ${flow}`);
      assert.ok(copy.title.length > 0, `${kind} on ${flow} has a title`);
    }
  }
});

test("anything a client throws that is not typed is a network failure", () => {
  const typed = toIdentityError(new TypeError("Failed to fetch"));
  assert.equal(typed.kind, "network");
  assert.equal(typed.message, "Failed to fetch");
  assert.equal(toIdentityError("boom").kind, "network");
  const copy = describeAuthError(new Error("socket hang up"), "sign_in");
  assert.equal(copy.title, "The identity service could not be reached");
  assert.equal(copy.detail, "Check your connection and try again.", "the raw transport message is not shown");
});

test("sign-in tells a wrong password, a suspended account and an unverified address apart", () => {
  assert.equal(describeAuthError(new IdentityError("unauthenticated", "Invalid credentials"), "sign_in").title, "Email or password is incorrect");
  assert.equal(describeAuthError(new IdentityError("unauthenticated", "User account is banned"), "sign_in").title, "This account is suspended");
  assert.equal(describeAuthError(new IdentityError("forbidden", "Email not verified"), "sign_in").title, "Verify your email address first");
});

test("a rate limit says how long to wait, rounded up", () => {
  const copy = describeAuthError(new IdentityError("rate_limited", "slow down", { retryAfterMs: 1_200 }), "sign_in");
  assert.equal(copy.retryAfterMs, 1_200);
  assert.match(copy.detail, /2 seconds/u);
  assert.match(describeAuthError(new IdentityError("rate_limited", "x"), "reset_request").detail, /a moment/u);
  assert.equal(describeAuthError(new IdentityError("rate_limited", "x"), "reset_request").retryAfterMs, null);
  assert.equal(formatWait(0), "1 second");
  assert.equal(formatWait(1_000), "1 second");
  assert.equal(formatWait(59_001), "1 minute");
  assert.equal(formatWait(61_000), "2 minutes");
});

test("a spent link or attempt is marked dead so the surface offers a fresh start", () => {
  const dead: Array<[AuthFlow, string]> = [
    ["otp", "This sign-in attempt has expired."],
    ["oauth_callback", "This sign-in link has expired or was already used."],
    ["reset_complete", "Token has already been used"],
    ["verify_email", "Invalid token"],
    ["account_invitation", "This invitation link is invalid or was already used."],
    ["accept_invitation", "This invitation has expired."],
  ];
  for (const [flow, message] of dead) {
    assert.equal(describeAuthError(new IdentityError("invalid", message), flow).deadLink, true, flow);
  }
  // A password the policy refuses is fixable in place, not a dead link.
  const policy = describeAuthError(new IdentityError("invalid", "Passwords need at least 8 characters."), "reset_complete");
  assert.equal(policy.deadLink, false);
  assert.equal(policy.detail, "Passwords need at least 8 characters.");
});

test("registration conflicts name the field that clashed", () => {
  assert.equal(describeAuthError(new IdentityError("conflict", "Username is already taken"), "register").title, "That username is taken");
  assert.equal(describeAuthError(new IdentityError("conflict", "Email already exists"), "register").title, "An account already uses this email");
});

test("invitation answers distinguish another address, an answered invitation and an existing membership", () => {
  assert.equal(describeAuthError(new IdentityError("forbidden", "different email"), "accept_invitation").title, "This invitation is for another address");
  assert.equal(describeAuthError(new IdentityError("not_found", "Invitation not found"), "decline_invitation").title, "This invitation is no longer available");
  assert.equal(describeAuthError(new IdentityError("conflict", "already a member"), "accept_invitation").title, "You are already a member");
});

test("an OAuth identity linked to someone else is explained, not echoed", () => {
  const copy = describeAuthError(new IdentityError("conflict", "This provider account is linked to a different user."), "oauth_callback");
  assert.equal(copy.title, "This provider account belongs to someone else");
});
