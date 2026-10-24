/**
 * What the product shell lets a principal reach, and how it describes who the
 * principal is acting as.
 *
 * Every rule here gates on a capability id from `lib/api/identity.ts`, never
 * on a role: the role table there is the one place that says what a role may
 * do, so a screen asks "may this principal manage indexes?" and the table
 * answers. The shell hides navigation a principal cannot use, and shows a
 * forbidden state when a URL is opened directly; the server still enforces
 * every capability on its own routes, so nothing here is a security boundary.
 */

import {
  ORG_ROLES,
  isIdentityError,
  type Capability,
  type IdentityError,
  type IdentityOrganization,
  type IdentityUser,
  type OrgRole,
  type OrganizationId,
  type Principal,
} from "@/lib/api/identity";

// --------------------------------------------------------------------------
// Sections and their requirements
// --------------------------------------------------------------------------

/** The product's top-level surfaces, one per `ProductShell` page. */
export type ProductSection =
  | "home"
  | "search"
  | "analytics"
  | "datasets"
  | "reports"
  | "dashboards"
  | "activity"
  | "admin"
  | "help";

/**
 * The capabilities a surface needs: every one of them (`all`), or at least
 * one (`any`). An empty `all` list would admit everyone, so a requirement
 * always names at least one capability; a surface anyone may open, signed in
 * or not, has no requirement at all (`null`).
 */
export interface AccessRequirement {
  readonly capabilities: readonly Capability[];
  readonly match: "all" | "any";
}

export function requireAll(...capabilities: Capability[]): AccessRequirement {
  return { capabilities, match: "all" };
}

export function requireAny(...capabilities: Capability[]): AccessRequirement {
  return { capabilities, match: "any" };
}

/**
 * Holding any one of these opens the administration console; each section of
 * it then asks for its own (`ADMIN_SECTION_ACCESS` in
 * `app/admin/admin-navigation.ts`). `audit.read` is not among them because the
 * audit journals live on the Activity page, and `alerts.write` /
 * `reports.write` because alerts and scheduled reports live under Reports.
 */
export const ADMIN_CONSOLE_CAPABILITIES: readonly Capability[] = [
  "apps.admin",
  "indexes.admin",
  "collectors.admin",
  "ingest_tokens.admin",
  "hec.admin",
  "knowledge.write",
  "knowledge.admin",
  "lookups.write",
  "members.admin",
  "org.admin",
  "server.admin",
  "users.admin",
  "orgs.admin",
];

export const ADMIN_CONSOLE_ACCESS: AccessRequirement = requireAny(...ADMIN_CONSOLE_CAPABILITIES);

/**
 * Every product surface but Help reads organization data, which takes the
 * `search` capability every role holds; Help is the bundled documentation and
 * stays open without a session.
 */
export const PRODUCT_SECTION_ACCESS: Readonly<Record<ProductSection, AccessRequirement | null>> = {
  home: requireAll("search"),
  search: requireAll("search"),
  analytics: requireAll("search"),
  datasets: requireAll("search"),
  reports: requireAll("search"),
  dashboards: requireAll("search"),
  activity: requireAll("search"),
  admin: ADMIN_CONSOLE_ACCESS,
  help: null,
};

/** How the forbidden state names a surface. */
export const PRODUCT_SECTION_LABELS: Readonly<Record<ProductSection, string>> = {
  home: "Home",
  search: "Search",
  analytics: "Analytics",
  datasets: "Datasets",
  reports: "Reports",
  dashboards: "Dashboards",
  activity: "Activity",
  admin: "Administration",
  help: "Documentation",
};

export function meetsRequirement(
  requirement: AccessRequirement | null,
  hasCapability: (capability: Capability) => boolean,
): boolean {
  if (requirement === null) return true;
  const { capabilities, match } = requirement;
  if (capabilities.length === 0) return false;
  return match === "any" ? capabilities.some(hasCapability) : capabilities.every(hasCapability);
}

/** The capabilities of a requirement the principal does not hold. */
export function missingCapabilities(
  requirement: AccessRequirement | null,
  hasCapability: (capability: Capability) => boolean,
): readonly Capability[] {
  if (requirement === null) return [];
  return requirement.capabilities.filter((capability) => !hasCapability(capability));
}

/** Plain-language names for the capabilities a forbidden state lists. */
export const CAPABILITY_LABELS: Readonly<Record<Capability, string>> = {
  "search": "Run searches",
  "objects.own": "Own saved searches and dashboards",
  "knowledge.write": "Edit knowledge objects",
  "lookups.write": "Edit lookup tables",
  "alerts.write": "Manage alerts",
  "reports.write": "Manage scheduled reports",
  "indexes.admin": "Manage indexes",
  "ingest_tokens.admin": "Manage ingestion tokens",
  "collectors.admin": "Manage collectors",
  "hec.admin": "Manage HTTP Event Collector",
  "apps.admin": "Manage apps",
  "audit.read": "Read audit journals",
  "knowledge.admin": "Administer knowledge objects",
  "members.admin": "Manage members and invitations",
  "org.admin": "Administer the organization",
  "server.admin": "Administer the server",
  "users.admin": "Manage user accounts",
  "orgs.admin": "Manage organizations",
};

// --------------------------------------------------------------------------
// Sign-in redirect
// --------------------------------------------------------------------------

export const SIGN_IN_PATH = "/signin/";
/** The query parameter the sign-in page reads to send the user back. */
export const RETURN_TO_PARAMETER = "returnTo";

/** A same-origin absolute path: one leading slash, no scheme, host, backslash or control character. */
export function isSameOriginPath(value: string): boolean {
  return value.startsWith("/")
    && !value.startsWith("//")
    && !value.includes("\\")
    && ![...value].some((character) => {
      const code = character.codePointAt(0) ?? 0;
      return code < 0x20 || code === 0x7f;
    });
}

/** The path, query and fragment of a location, as the place to come back to. */
export function returnPathFor(location: { readonly pathname: string; readonly search: string; readonly hash: string }): string {
  return `${location.pathname}${location.search}${location.hash}`;
}

/**
 * The sign-in page, carrying where to come back to. A return path that is not
 * same-origin, or is the sign-in page itself, is dropped rather than echoed.
 */
export function signInHref(returnTo?: string | null): string {
  if (
    returnTo === undefined
    || returnTo === null
    || !isSameOriginPath(returnTo)
    || /^\/signin(?:[/?#]|$)/u.test(returnTo)
  ) return SIGN_IN_PATH;
  return `${SIGN_IN_PATH}?${new URLSearchParams({ [RETURN_TO_PARAMETER]: returnTo }).toString()}`;
}

// --------------------------------------------------------------------------
// Who the principal is acting as
// --------------------------------------------------------------------------

export const ORG_ROLE_LABELS: Readonly<Record<OrgRole, string>> = {
  member: "Member",
  moderator: "Moderator",
  admin: "Admin",
  owner: "Owner",
};

/** go-common marks every organization of a personal workspace tree with its owner; slugs are not a classifier. */
export function isPersonalOrganization(organization: IdentityOrganization): boolean {
  return organization.personalOwnerUserId !== undefined && organization.personalOwnerUserId !== "";
}

/**
 * The name to show for a user: first and last name, else the username, else
 * the email. A name made only of invisible characters (a lone bidi mark, a
 * zero-width space) counts as no name, or the row would show a blank.
 */
export function userDisplayName(user: IdentityUser): string {
  const name = `${user.firstName} ${user.lastName}`.trim();
  if (leadingCharacter(name).length > 0) return name;
  const username = user.username.trim();
  if (leadingCharacter(username).length > 0) return username;
  return user.email;
}

/**
 * What a name may lead with that draws nothing on its own: white space, the
 * invisible format characters (bidi marks, zero-width spaces and joiners, the
 * byte-order mark) and a combining mark with no letter to sit on. An avatar
 * built from one of those would be blank, or a dotted circle.
 */
const INVISIBLE_LEAD = /^[\p{White_Space}\p{Cf}\p{M}]+/u;

/** Splits text where a reader sees one character end and the next begin, where the runtime can. */
const GRAPHEME_SEGMENTER = typeof Intl === "object" && typeof Intl.Segmenter === "function"
  ? new Intl.Segmenter(undefined, { granularity: "grapheme" })
  : null;

/**
 * The first character a reader would see in `text`, whole: an accented
 * letter with its combining marks, an emoji with its modifiers and joiners,
 * a flag's two regional indicators. `charAt(0)` hands back half of a
 * surrogate pair for anything outside the Basic Multilingual Plane, which
 * renders as a replacement glyph. Without a segmenter this falls back to the
 * first code point, which is still never half of one.
 */
export function leadingCharacter(text: string): string {
  const visible = text.replace(INVISIBLE_LEAD, "");
  if (visible.length === 0) return "";
  if (GRAPHEME_SEGMENTER !== null) {
    for (const { segment } of GRAPHEME_SEGMENTER.segment(visible)) return segment;
  }
  return Array.from(visible)[0] ?? "";
}

/**
 * `text`'s first visible character as a capital, or "" when it has none. A
 * character whose capital is more than one character (`ß` is `SS`, `ﬁ` is
 * `FI`) keeps its own form: an initial is one character.
 */
export function initialOf(text: string): string {
  const character = leadingCharacter(text);
  const capital = character.toUpperCase();
  return leadingCharacter(capital) === capital ? capital : character;
}

/** Up to two characters for an avatar: first and last name, else the first of the display name. */
export function userInitials(user: IdentityUser): string {
  const first = initialOf(user.firstName);
  const last = initialOf(user.lastName);
  const initials = first.length > 0 || last.length > 0 ? `${first}${last}` : initialOf(userDisplayName(user));
  return initials || "?";
}

export function organizationInitial(organization: IdentityOrganization): string {
  return initialOf(organization.name) || initialOf(organization.slug) || "?";
}

/** One row of the organization switcher. */
export interface OrganizationChoice {
  readonly id: OrganizationId;
  readonly name: string;
  readonly initial: string;
  /** The effective role's label, or "Site admin" for break-glass access without a membership. */
  readonly roleLabel: string;
  /** The line under the name: role, inheritance and workspace kind. */
  readonly detail: string;
  readonly personal: boolean;
  /** The personal workspace belongs to the signed-in user. */
  readonly ownWorkspace: boolean;
  readonly inherited: boolean;
  readonly active: boolean;
  /** A site admin acting in an organization it holds no role in. */
  readonly siteAdminAccess: boolean;
}

function workspaceDetail(organization: IdentityOrganization, viewer: IdentityUser): string | null {
  if (!isPersonalOrganization(organization)) return null;
  return organization.personalOwnerUserId === viewer.id ? "Your personal workspace" : "Personal workspace";
}

/**
 * The switcher's rows, in the principal's order. When a site admin is acting
 * in an organization it holds no role in, that organization leads the list,
 * marked as site-admin access, so the switcher always shows where the
 * principal is.
 */
export function organizationChoices(principal: Principal): readonly OrganizationChoice[] {
  const activeId = principal.activeOrganization?.id ?? null;
  const names = new Map(principal.organizations.map((entry) => [entry.organization.id, entry.organization.name]));
  const choices: OrganizationChoice[] = principal.organizations.map((entry) => {
    const { organization } = entry;
    const inherited = entry.explicitRole === null;
    const roleLabel = ORG_ROLE_LABELS[entry.effectiveRole];
    const inheritedFrom = entry.inheritedFromOrganizationId === undefined
      ? undefined
      : names.get(entry.inheritedFromOrganizationId);
    const parts = [
      inherited
        ? inheritedFrom === undefined ? `${roleLabel} · inherited` : `${roleLabel} · inherited from ${inheritedFrom}`
        : roleLabel,
      workspaceDetail(organization, principal.user),
    ].filter((part): part is string => part !== null);
    return {
      id: organization.id,
      name: organization.name,
      initial: organizationInitial(organization),
      roleLabel,
      detail: parts.join(" · "),
      personal: isPersonalOrganization(organization),
      ownWorkspace: isPersonalOrganization(organization) && organization.personalOwnerUserId === principal.user.id,
      inherited,
      active: organization.id === activeId,
      siteAdminAccess: false,
    };
  });
  const active = principal.activeOrganization;
  if (active !== null && !choices.some((choice) => choice.id === active.id)) {
    const workspace = workspaceDetail(active, principal.user);
    choices.unshift({
      id: active.id,
      name: active.name,
      initial: organizationInitial(active),
      roleLabel: "Site admin",
      detail: ["Site admin access · not a member", workspace].filter((part): part is string => part !== null).join(" · "),
      personal: isPersonalOrganization(active),
      ownWorkspace: false,
      inherited: false,
      active: true,
      siteAdminAccess: true,
    });
  }
  return choices;
}

/** The role the principal acts with in its active organization, for the shell's summaries. */
export function activeRoleLabel(principal: Principal): string | null {
  if (principal.role !== null) return ORG_ROLE_LABELS[principal.role];
  if (principal.siteAdmin && principal.activeOrganization !== null) return "Site admin";
  return null;
}

/** Roles from lowest to highest that would satisfy a requirement, for the forbidden state's advice. */
export function lowestRoleMeeting(
  requirement: AccessRequirement | null,
  capabilitiesOf: (role: OrgRole) => readonly Capability[],
): OrgRole | null {
  for (const role of ORG_ROLES) {
    const held = new Set(capabilitiesOf(role));
    if (meetsRequirement(requirement, (capability) => held.has(capability))) return role;
  }
  return null;
}

// --------------------------------------------------------------------------
// Error copy
// --------------------------------------------------------------------------

/** "in 30 seconds", "in 2 minutes", or "shortly" when the server named no delay. */
export function retryAfterPhrase(retryAfterMs: number | undefined): string {
  if (retryAfterMs === undefined || !Number.isFinite(retryAfterMs) || retryAfterMs <= 0) return "shortly";
  const seconds = Math.ceil(retryAfterMs / 1000);
  if (seconds < 60) return `in ${seconds} ${seconds === 1 ? "second" : "seconds"}`;
  const minutes = Math.ceil(seconds / 60);
  return `in ${minutes} ${minutes === 1 ? "minute" : "minutes"}`;
}

function asKnownError(error: unknown): IdentityError | null {
  return isIdentityError(error) ? error : null;
}

/** Why switching to `organizationName` failed, per typed error. */
export function organizationSwitchErrorMessage(error: unknown, organizationName: string): string {
  const known = asKnownError(error);
  if (known === null) return `Could not switch to ${organizationName}. Try again.`;
  switch (known.kind) {
    case "unauthenticated":
      return "Your session has ended. Sign in again to switch organizations.";
    case "forbidden":
      return `You no longer have access to ${organizationName}.`;
    case "not_found":
      return `${organizationName} no longer exists.`;
    case "rate_limited":
      return `Too many requests. Try switching again ${retryAfterPhrase(known.retryAfterMs)}.`;
    case "not_configured":
      return "Organization switching is not available on this server.";
    case "network":
      return `Could not reach the identity service to switch to ${organizationName}. Check your connection and try again.`;
    default:
      return `Could not switch to ${organizationName}: ${known.message}`;
  }
}

/** Why signing out failed, per typed error. */
export function signOutErrorMessage(error: unknown): string {
  const known = asKnownError(error);
  if (known === null) return "Could not sign out. Try again.";
  switch (known.kind) {
    case "rate_limited":
      return `Too many requests. Try signing out again ${retryAfterPhrase(known.retryAfterMs)}.`;
    case "network":
      return "Could not reach the identity service, so this session is still signed in. Try again.";
    case "not_configured":
      return "Signing out is not available on this server.";
    default:
      return `Could not sign out: ${known.message}`;
  }
}

/** Why the principal could not be read at all, per typed error. */
export function identityUnavailableMessage(error: unknown): { readonly title: string; readonly message: string } {
  const known = asKnownError(error);
  if (known === null) {
    return { title: "Could not load your session", message: "Something went wrong while confirming who is signed in. Try again." };
  }
  switch (known.kind) {
    case "rate_limited":
      return {
        title: "Too many requests",
        message: `The identity service is limiting requests. Try again ${retryAfterPhrase(known.retryAfterMs)}.`,
      };
    case "forbidden":
      return { title: "Access was refused", message: "The identity service refused to read this session. Sign in again or contact an administrator." };
    case "network":
      return { title: "Identity service unreachable", message: "Could not confirm who is signed in. Check your connection and try again." };
    case "not_configured":
      return { title: "Sign-in is not configured", message: "This server does not have identity configured." };
    default:
      return { title: "Could not load your session", message: known.message };
  }
}
