import assert from "node:assert/strict";
import test from "node:test";

import {
  CAPABILITIES,
  IDENTITY_ERROR_KINDS,
  IdentityError,
  ORG_ROLE_CAPABILITIES,
  capabilitiesFor,
  compareOrgRoles,
  createGoCommonIdentityClient,
  identityErrorFromHttpStatus,
  isCapability,
  isIdentityError,
  isOrgRole,
  isSiteAdminUserType,
  orgRoleAtLeast,
  orgRoleFromGoCommon,
  orgRoleToGoCommon,
  parseRetryAfterMs,
  principalHasCapability,
  userTypeBandFromGoCommon,
} from "./identity";

test("the capability table carries the canonical ids, cumulatively by role", () => {
  assert.deepEqual(ORG_ROLE_CAPABILITIES.member, ["search", "objects.own"]);
  assert.deepEqual(ORG_ROLE_CAPABILITIES.moderator, [
    "search", "objects.own",
    "knowledge.write", "lookups.write", "alerts.write", "reports.write",
  ]);
  assert.deepEqual(ORG_ROLE_CAPABILITIES.admin, [
    "search", "objects.own",
    "knowledge.write", "lookups.write", "alerts.write", "reports.write",
    "indexes.admin", "ingest_tokens.admin", "collectors.admin", "hec.admin",
    "apps.admin", "audit.read", "knowledge.admin", "members.admin",
  ]);
  assert.deepEqual(ORG_ROLE_CAPABILITIES.owner, [...ORG_ROLE_CAPABILITIES.admin, "org.admin"]);
  assert.deepEqual(CAPABILITIES, [...ORG_ROLE_CAPABILITIES.owner, "server.admin", "users.admin", "orgs.admin"]);
  assert.equal(new Set(CAPABILITIES).size, CAPABILITIES.length, "capability ids are unique");
});

test("a site admin holds every capability in any organization, with or without a role", () => {
  assert.deepEqual(capabilitiesFor(null, true), CAPABILITIES);
  assert.deepEqual(capabilitiesFor("member", true), CAPABILITIES);
  assert.deepEqual(capabilitiesFor(null, false), []);
  assert.ok(!capabilitiesFor("owner", false).includes("server.admin"));
  assert.ok(capabilitiesFor("owner", false).includes("org.admin"));
  assert.ok(!capabilitiesFor("admin", false).includes("org.admin"));

  const returned = capabilitiesFor("member", false) as string[];
  returned.push("server.admin");
  assert.deepEqual(capabilitiesFor("member", false), ["search", "objects.own"], "callers get a copy of the table");
});

test("role ordering and guards", () => {
  assert.ok(compareOrgRoles("member", "owner") < 0);
  assert.equal(compareOrgRoles("admin", "admin"), 0);
  assert.ok(orgRoleAtLeast("owner", "admin"));
  assert.ok(!orgRoleAtLeast("moderator", "admin"));
  assert.ok(!orgRoleAtLeast(null, "member"));
  assert.ok(isOrgRole("moderator"));
  assert.ok(!isOrgRole("superuser"));
  assert.ok(!isOrgRole(3));
  assert.ok(isCapability("hec.admin"));
  assert.ok(!isCapability("hec"));
});

test("go-common role and user-type bands collapse to open-splunk roles", () => {
  const expectations: Array<[number, string | null]> = [
    [0, null], [1, "member"], [11, "member"], [12, "moderator"], [22, "moderator"],
    [23, "admin"], [33, "admin"], [34, "owner"], [44, "owner"], [45, null], [-1, null], [1.5, null],
  ];
  for (const [value, role] of expectations) {
    assert.equal(orgRoleFromGoCommon(value), role, `OrganizationRole ${value}`);
  }
  for (const role of ["member", "moderator", "admin", "owner"] as const) {
    assert.equal(orgRoleFromGoCommon(orgRoleToGoCommon(role)), role);
  }
  assert.deepEqual([orgRoleToGoCommon("member"), orgRoleToGoCommon("owner")], [1, 34]);
  assert.equal(userTypeBandFromGoCommon(12), "regular");
  assert.equal(userTypeBandFromGoCommon(0), null);
  assert.ok(isSiteAdminUserType(34));
  assert.ok(isSiteAdminUserType(44));
  assert.ok(!isSiteAdminUserType(33));
  assert.ok(!isSiteAdminUserType(0));
});

test("HTTP failures classify by go-common's status table", () => {
  const expectations: Array<[number, string]> = [
    [400, "invalid"], [401, "unauthenticated"], [403, "forbidden"], [404, "not_found"],
    [409, "conflict"], [410, "invalid"], [413, "invalid"], [429, "rate_limited"],
    [500, "network"], [502, "network"], [503, "network"],
  ];
  for (const [status, kind] of expectations) {
    const error = identityErrorFromHttpStatus(status, "message", 2_000);
    assert.equal(error.kind, kind, `HTTP ${status}`);
    assert.equal(error.status, status);
    assert.equal(error.retryAfterMs, kind === "rate_limited" ? 2_000 : undefined);
  }
  assert.equal(identityErrorFromHttpStatus(429, "slow down").retryAfterMs, undefined);
  assert.ok(IDENTITY_ERROR_KINDS.includes("otp_required"));
});

test("Retry-After reads delta-seconds and HTTP dates", () => {
  const now = Date.parse("2026-10-24T12:00:00Z");
  assert.equal(parseRetryAfterMs("30", now), 30_000);
  assert.equal(parseRetryAfterMs(" 0 ", now), 0);
  assert.equal(parseRetryAfterMs("Sat, 24 Oct 2026 12:00:05 GMT", now), 5_000);
  assert.equal(parseRetryAfterMs("Sat, 24 Oct 2026 11:00:00 GMT", now), 0, "a past date means retry now");
  assert.equal(parseRetryAfterMs("soon", now), null);
  assert.equal(parseRetryAfterMs("", now), null);
  assert.equal(parseRetryAfterMs(null, now), null);
  assert.equal(parseRetryAfterMs("-5", now), null);
  assert.equal(parseRetryAfterMs("1.5", now), null);
});

test("identity errors are typed", () => {
  const error = new IdentityError("rate_limited", "wait", { retryAfterMs: 10, status: 429 });
  assert.ok(error instanceof Error);
  assert.equal(error.name, "IdentityError");
  assert.ok(isIdentityError(error));
  assert.ok(isIdentityError(error, "rate_limited"));
  assert.ok(!isIdentityError(error, "network"));
  assert.ok(!isIdentityError(new Error("plain")));
});

test("the server binding refuses with not_configured until the routes exist", () => {
  assert.throws(createGoCommonIdentityClient, (error: unknown) => isIdentityError(error, "not_configured"));
});

test("principalHasCapability treats a missing principal as holding nothing", () => {
  assert.ok(!principalHasCapability(null, "search"));
});
