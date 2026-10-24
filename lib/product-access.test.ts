import assert from "node:assert/strict";
import test from "node:test";

import {
  CAPABILITIES,
  IdentityError,
  ORG_ROLE_CAPABILITIES,
  capabilitiesFor,
  type Capability,
  type IdentityOrganization,
  type IdentityUser,
  type OrganizationWithRole,
  type Principal,
} from "@/lib/api/identity";

import {
  ADMIN_CONSOLE_ACCESS,
  ADMIN_CONSOLE_CAPABILITIES,
  CAPABILITY_LABELS,
  PRODUCT_SECTION_ACCESS,
  activeRoleLabel,
  identityUnavailableMessage,
  isPersonalOrganization,
  isSameOriginPath,
  lowestRoleMeeting,
  meetsRequirement,
  missingCapabilities,
  organizationChoices,
  organizationSwitchErrorMessage,
  requireAll,
  requireAny,
  retryAfterPhrase,
  returnPathFor,
  signInHref,
  signOutErrorMessage,
  userDisplayName,
  userInitials,
  type ProductSection,
} from "./product-access";

const USER: IdentityUser = {
  id: "5",
  username: "mark",
  email: "mark@open-splunk.test",
  firstName: "Mark",
  lastName: "Mendez",
  userType: "regular",
  banned: false,
  isBot: false,
};

function organization(id: string, name: string, extra: Partial<IdentityOrganization> = {}): IdentityOrganization {
  return {
    id,
    name,
    slug: name.toLowerCase().replaceAll(" ", "-"),
    createdAt: "2026-01-01T00:00:00.000Z",
    updatedAt: "2026-01-01T00:00:00.000Z",
    lineage: [],
    ...extra,
  };
}

const ACME = organization("1", "Acme Operations");
const PLATFORM = organization("3", "Acme Platform", { parentOrganizationId: "1", lineage: ["1"] });
const MINE = organization("40", "Mark's workspace", { personalOwnerUserId: "5", slug: "personal-5" });
const THEIRS = organization("41", "Ada's workspace", { personalOwnerUserId: "1", slug: "personal-1" });
const GLOBEX = organization("2", "Globex Security");

function principal(overrides: Partial<Principal> = {}): Principal {
  const organizations: OrganizationWithRole[] = [
    { organization: ACME, explicitRole: "member", effectiveRole: "member" },
    { organization: PLATFORM, explicitRole: null, effectiveRole: "admin", inheritedFromOrganizationId: "1", effectiveRoleDepth: 0 },
    { organization: MINE, explicitRole: "owner", effectiveRole: "owner" },
    { organization: THEIRS, explicitRole: null, effectiveRole: "member", inheritedFromOrganizationId: "999" },
  ];
  const base: Principal = {
    user: USER,
    siteAdmin: false,
    activeOrganization: ACME,
    role: "member",
    capabilities: capabilitiesFor("member", false),
    organizations,
  };
  return { ...base, ...overrides };
}

function holder(capabilities: readonly Capability[]): (capability: Capability) => boolean {
  const held = new Set(capabilities);
  return (capability) => held.has(capability);
}

test("every product section but Help needs search; administration needs any admin capability", () => {
  const sections: ProductSection[] = ["home", "search", "analytics", "datasets", "reports", "dashboards", "activity"];
  for (const section of sections) assert.deepEqual(PRODUCT_SECTION_ACCESS[section], requireAll("search"), section);
  assert.equal(PRODUCT_SECTION_ACCESS.help, null, "bundled documentation stays open without a session");
  assert.equal(PRODUCT_SECTION_ACCESS.admin, ADMIN_CONSOLE_ACCESS);
  assert.equal(ADMIN_CONSOLE_ACCESS.match, "any");
  assert.ok(!ADMIN_CONSOLE_CAPABILITIES.includes("search"));
  assert.ok(!ADMIN_CONSOLE_CAPABILITIES.includes("audit.read"), "audit journals live on Activity");
  assert.equal(new Set(ADMIN_CONSOLE_CAPABILITIES).size, ADMIN_CONSOLE_CAPABILITIES.length);
});

test("role capability sets reach exactly the surfaces the role table grants", () => {
  const member = holder(ORG_ROLE_CAPABILITIES.member);
  const moderator = holder(ORG_ROLE_CAPABILITIES.moderator);
  const admin = holder(ORG_ROLE_CAPABILITIES.admin);
  const nobody = holder([]);
  const siteAdmin = holder(capabilitiesFor(null, true));

  assert.ok(meetsRequirement(PRODUCT_SECTION_ACCESS.search, member));
  assert.ok(!meetsRequirement(PRODUCT_SECTION_ACCESS.admin, member), "a member has no administration");
  assert.ok(meetsRequirement(PRODUCT_SECTION_ACCESS.admin, moderator), "knowledge and lookups open the console for a moderator");
  assert.ok(meetsRequirement(PRODUCT_SECTION_ACCESS.admin, admin));
  assert.ok(!meetsRequirement(PRODUCT_SECTION_ACCESS.search, nobody), "no role, no organization data");
  assert.ok(meetsRequirement(PRODUCT_SECTION_ACCESS.help, nobody));
  assert.ok(meetsRequirement(requireAll("server.admin", "orgs.admin"), siteAdmin));
  assert.ok(!meetsRequirement(requireAll("server.admin"), admin));
});

test("requirements match all or any, and an empty requirement never grants", () => {
  const some = holder(["search", "hec.admin"]);
  assert.ok(meetsRequirement(requireAny("indexes.admin", "hec.admin"), some));
  assert.ok(!meetsRequirement(requireAll("indexes.admin", "hec.admin"), some));
  assert.ok(!meetsRequirement(requireAll(), holder(CAPABILITIES)));
  assert.ok(!meetsRequirement(requireAny(), holder(CAPABILITIES)));
  assert.deepEqual(missingCapabilities(requireAll("search", "indexes.admin", "hec.admin"), some), ["indexes.admin"]);
  assert.deepEqual(missingCapabilities(null, some), []);
});

test("the lowest role meeting a requirement drives the forbidden advice", () => {
  const table = (role: keyof typeof ORG_ROLE_CAPABILITIES) => ORG_ROLE_CAPABILITIES[role];
  assert.equal(lowestRoleMeeting(requireAll("search"), table), "member");
  assert.equal(lowestRoleMeeting(PRODUCT_SECTION_ACCESS.admin, table), "moderator");
  assert.equal(lowestRoleMeeting(requireAll("indexes.admin"), table), "admin");
  assert.equal(lowestRoleMeeting(requireAll("org.admin"), table), "owner");
  assert.equal(lowestRoleMeeting(requireAll("server.admin"), table), null, "site capabilities come from no organization role");
});

test("every capability has a plain-language label", () => {
  for (const capability of CAPABILITIES) {
    assert.ok(CAPABILITY_LABELS[capability].length > 0, capability);
  }
});

test("the sign-in redirect carries only a same-origin return path", () => {
  assert.equal(signInHref("/search/events/?q=index%3Dmain#results"), "/signin/?returnTo=%2Fsearch%2Fevents%2F%3Fq%3Dindex%253Dmain%23results");
  assert.equal(new URLSearchParams(signInHref("/admin/?section=server").split("?")[1]).get("returnTo"), "/admin/?section=server");
  assert.equal(signInHref("/"), "/signin/?returnTo=%2F");
  for (const unsafe of ["https://evil.test/", "//evil.test/", "/\\evil.test", "search/", "", "/a\nb", "/a\u007fb"]) {
    assert.equal(signInHref(unsafe), "/signin/", JSON.stringify(unsafe));
  }
  for (const loop of ["/signin", "/signin/", "/signin/?returnTo=%2F", "/signin?x=1", "/signin#top"]) {
    assert.equal(signInHref(loop), "/signin/", `${loop} would loop`);
  }
  assert.equal(signInHref("/signins/"), "/signin/?returnTo=%2Fsignins%2F", "only the sign-in page itself is dropped");
  assert.equal(signInHref(null), "/signin/");
  assert.equal(signInHref(undefined), "/signin/");
  assert.ok(isSameOriginPath("/datasets/"));
  assert.ok(!isSameOriginPath("javascript:alert(1)"));
  assert.equal(returnPathFor({ pathname: "/reports/saved-searches/", search: "?view=mine", hash: "#top" }), "/reports/saved-searches/?view=mine#top");
});

test("organization choices mark personal workspaces, inheritance and the active organization", () => {
  const choices = organizationChoices(principal());
  assert.deepEqual(choices.map((choice) => choice.id), ["1", "3", "40", "41"], "the principal's order is kept");
  const [acme, platform, mine, theirs] = choices;
  assert.equal(acme?.active, true);
  assert.equal(acme?.detail, "Member");
  assert.equal(acme?.personal, false);
  assert.equal(platform?.active, false);
  assert.equal(platform?.inherited, true);
  assert.equal(platform?.detail, "Admin · inherited from Acme Operations");
  assert.equal(mine?.personal, true);
  assert.equal(mine?.ownWorkspace, true);
  assert.equal(mine?.detail, "Owner · Your personal workspace");
  assert.equal(theirs?.ownWorkspace, false);
  assert.equal(theirs?.detail, "Member · inherited · Personal workspace", "an unknown ancestor is not named");
  assert.ok(choices.every((choice) => !choice.siteAdminAccess));
  assert.equal(acme?.initial, "A");
  assert.ok(isPersonalOrganization(MINE));
  assert.ok(!isPersonalOrganization(ACME));
  assert.ok(!isPersonalOrganization(organization("9", "personal-9", { slug: "personal-9" })), "a slug is never the classifier");
});

test("a site admin acting outside its memberships sees that organization first, marked", () => {
  const choices = organizationChoices(principal({
    siteAdmin: true,
    activeOrganization: GLOBEX,
    role: null,
    capabilities: capabilitiesFor(null, true),
  }));
  assert.equal(choices[0]?.id, "2");
  assert.equal(choices[0]?.siteAdminAccess, true);
  assert.equal(choices[0]?.active, true);
  assert.equal(choices[0]?.roleLabel, "Site admin");
  assert.equal(choices.filter((choice) => choice.active).length, 1);
  assert.equal(activeRoleLabel(principal({ siteAdmin: true, activeOrganization: GLOBEX, role: null })), "Site admin");
  assert.equal(activeRoleLabel(principal({ activeOrganization: null, role: null })), null);
  assert.deepEqual(organizationChoices(principal({ organizations: [], activeOrganization: null, role: null })), []);
});

test("display names and initials fall back from name to username to email", () => {
  assert.equal(userDisplayName(USER), "Mark Mendez");
  assert.equal(userInitials(USER), "MM");
  const nameless = { ...USER, firstName: "", lastName: " " };
  assert.equal(userDisplayName(nameless), "mark");
  assert.equal(userInitials(nameless), "M");
  const bare = { ...nameless, username: "" };
  assert.equal(userDisplayName(bare), "mark@open-splunk.test");
  assert.equal(userInitials({ ...bare, email: "" }), "?");
});

test("every typed identity error has its own switcher, sign-out and session copy", () => {
  const name = "Globex Security";
  assert.match(organizationSwitchErrorMessage(new IdentityError("unauthenticated", "x"), name), /session has ended/u);
  assert.equal(organizationSwitchErrorMessage(new IdentityError("forbidden", "x"), name), "You no longer have access to Globex Security.");
  assert.equal(organizationSwitchErrorMessage(new IdentityError("not_found", "x"), name), "Globex Security no longer exists.");
  assert.equal(
    organizationSwitchErrorMessage(new IdentityError("rate_limited", "x", { retryAfterMs: 30_000 }), name),
    "Too many requests. Try switching again in 30 seconds.",
  );
  assert.match(organizationSwitchErrorMessage(new IdentityError("network", "x"), name), /Could not reach the identity service/u);
  assert.match(organizationSwitchErrorMessage(new IdentityError("not_configured", "x"), name), /not available/u);
  assert.equal(organizationSwitchErrorMessage(new IdentityError("conflict", "Already switching"), name), "Could not switch to Globex Security: Already switching");
  assert.equal(organizationSwitchErrorMessage(new Error("boom"), name), "Could not switch to Globex Security. Try again.");

  assert.match(signOutErrorMessage(new IdentityError("network", "x")), /still signed in/u);
  assert.match(signOutErrorMessage(new IdentityError("rate_limited", "x")), /again shortly/u);
  assert.equal(signOutErrorMessage("nope"), "Could not sign out. Try again.");

  assert.equal(identityUnavailableMessage(new IdentityError("network", "x")).title, "Identity service unreachable");
  assert.match(identityUnavailableMessage(new IdentityError("rate_limited", "x", { retryAfterMs: 120_000 })).message, /in 2 minutes/u);
  assert.equal(identityUnavailableMessage(new IdentityError("forbidden", "x")).title, "Access was refused");
  assert.equal(identityUnavailableMessage(new IdentityError("invalid", "Bad cookie")).message, "Bad cookie");
  assert.equal(identityUnavailableMessage(null).title, "Could not load your session");
});

test("retry delays read as whole seconds or minutes", () => {
  assert.equal(retryAfterPhrase(undefined), "shortly");
  assert.equal(retryAfterPhrase(0), "shortly");
  assert.equal(retryAfterPhrase(Number.NaN), "shortly");
  assert.equal(retryAfterPhrase(1), "in 1 second");
  assert.equal(retryAfterPhrase(59_000), "in 59 seconds");
  assert.equal(retryAfterPhrase(59_001), "in 1 minute");
  assert.equal(retryAfterPhrase(60_000), "in 1 minute");
  assert.equal(retryAfterPhrase(61_000), "in 2 minutes");
});
