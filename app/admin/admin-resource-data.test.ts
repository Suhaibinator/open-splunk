import assert from "node:assert/strict";
import test from "node:test";

import { definitionFromForm, splitIndexNames } from "./admin-resource-data";
import { adminSectionPath, resolveAdminSection } from "./admin-navigation";

const adminSections = ["overview", "indexes", "collectors"] as const;

test("splitIndexNames normalizes comma-separated app defaults", () => {
  assert.deepEqual(
    splitIndexNames(" security, main,security,  archive "),
    ["archive", "main", "security"],
  );
});

test("definitionFromForm omits empty optional values", () => {
  assert.deepEqual(definitionFromForm({
    slug: "  Security_Ops ",
    displayName: " Security Operations ",
    description: "  ",
    indexNames: "security, main",
    hasTimeRange: false,
    earliest: "-24h",
    latest: "now",
    timezone: "UTC",
  }), {
    slug: "security_ops",
    displayName: "Security Operations",
    description: undefined,
    defaultIndexNames: ["main", "security"],
    defaultTimeRange: undefined,
  });
});

test("definitionFromForm preserves an explicit app time range", () => {
  assert.deepEqual(definitionFromForm({
    slug: "ops",
    displayName: "Operations",
    description: "Shared operational searches",
    indexNames: "main",
    hasTimeRange: true,
    earliest: " -7d ",
    latest: " now ",
    timezone: " America/Los_Angeles ",
  }).defaultTimeRange, {
    earliest: "-7d",
    latest: "now",
    timezone: "America/Los_Angeles",
  });
});

test("resolveAdminSection accepts only known section values", () => {
  assert.equal(resolveAdminSection("indexes", adminSections, "overview"), "indexes");
  assert.equal(resolveAdminSection("unknown", adminSections, "overview"), "overview");
  assert.equal(resolveAdminSection(["collectors", "indexes"], adminSections, "overview"), "collectors");
  assert.equal(resolveAdminSection(undefined, adminSections, "overview"), "overview");
});

test("adminSectionPath preserves unrelated URL state", () => {
  assert.equal(
    adminSectionPath("https://example.test/admin/?mode=compact#status", "indexes"),
    "/admin/?mode=compact&section=indexes#status",
  );
  assert.equal(
    adminSectionPath("https://example.test/admin/?section=overview", "collectors"),
    "/admin/?section=collectors",
  );
});
