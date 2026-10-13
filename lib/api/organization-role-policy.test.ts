import assert from "node:assert/strict";
import test from "node:test";
import { canManageOrganizationRole, grantableOrganizationRoles } from "./organization-role-policy";

test("Admin membership controls expose Viewers while Owner controls include Admins and Owners", () => {
  assert.deepEqual(grantableOrganizationRoles(23, false).map((role) => role.value), [1]);
  assert.equal(canManageOrganizationRole(23, 23, false), false);
  assert.equal(canManageOrganizationRole(23, 44, false), false);
  assert.deepEqual(grantableOrganizationRoles(44, false).map((role) => role.value), [1, 23, 44]);
});

test("site administrators can manage recognized team roles without a selected membership", () => {
  assert.deepEqual(grantableOrganizationRoles(0, true).map((role) => role.value), [1, 23, 44]);
  assert.equal(canManageOrganizationRole(0, 24, true), false);
});

test("Viewer and unsupported role types do not expose membership mutation controls", () => {
  assert.deepEqual(grantableOrganizationRoles(1, false), []);
  assert.deepEqual(grantableOrganizationRoles(999, false), []);
  assert.equal(canManageOrganizationRole(23, 24, false), false);
});
