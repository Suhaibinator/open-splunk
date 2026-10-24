/**
 * What the Members & invitations panel lets an organization admin do, decided
 * before a request is sent.
 *
 * go-common is the authority: every rule here is one it enforces, so the panel
 * only avoids offering a change the server would refuse and explains why. The
 * rules come from `OrganizationHandler.UpdateMemberRole` / `RemoveMember`,
 * `CanPromoteToRole` / `CanManageRole` and the persistence last-owner guards:
 *
 * - a manager may grant, and may change or remove a member holding, only a
 *   role strictly below its own effective role; a site admin (the
 *   `PermissionAdminManageOrganizationMembers` shortcut) may do anything;
 * - a role change also checks the target's current role against the actor's,
 *   so nobody but a site admin changes their own role; an owner hands over
 *   ownership by making someone else an owner and then leaving;
 * - anyone who may remove members may remove themselves (leave), whatever
 *   their role, and removal also drops their memberships in descendant
 *   organizations;
 * - the last explicit owner can be neither demoted nor removed
 *   (`ErrOrganizationLastOwner`);
 * - an inherited role belongs to the ancestor organization that grants it:
 *   with no membership row here there is nothing to change or remove.
 *
 * The browser sees only the role band, not go-common's tier inside it, so
 * "strictly below" is read band against band. Below the owner band that is the
 * conservative reading (two admins in different tiers may be allowed more by
 * the server than the panel offers, never less). The owner band is read
 * optimistically: owners are granted at the band's lowest tier while creators
 * hold a higher one, so an owner usually outranks the owners it made; when it
 * does not, the server answers `forbidden` and the panel says so.
 */

import {
  ORG_ROLES,
  ORG_ROLE_CAPABILITIES,
  compareOrgRoles,
  isIdentityError,
  type Capability,
  type IdentityError,
  type OrgCapability,
  type OrgRole,
  type OrganizationId,
  type OrganizationInvitation,
  type OrganizationMember,
  type Page,
  type UserId,
} from "@/lib/api/identity";

export const ROLE_LABELS: Readonly<Record<OrgRole, string>> = {
  member: "Member",
  moderator: "Moderator",
  admin: "Admin",
  owner: "Owner",
};

/** One line per role: what it adds to the role below it. */
export const ROLE_SUMMARIES: Readonly<Record<OrgRole, string>> = {
  member: "Runs searches and owns their saved searches and dashboards.",
  moderator: "Also edits knowledge objects, lookups, alerts and scheduled reports.",
  admin: "Also manages indexes, tokens, collectors, HEC, apps, the audit log and members.",
  owner: "Also administers the organization itself, including its other owners.",
};

export const CAPABILITY_LABELS: Readonly<Record<OrgCapability, string>> = {
  "search": "Run searches",
  "objects.own": "Own saved searches and dashboards",
  "knowledge.write": "Edit knowledge objects",
  "lookups.write": "Edit lookups",
  "alerts.write": "Edit alerts",
  "reports.write": "Edit scheduled reports",
  "indexes.admin": "Manage indexes",
  "ingest_tokens.admin": "Manage ingestion tokens",
  "collectors.admin": "Manage collectors",
  "hec.admin": "Manage HEC",
  "apps.admin": "Manage apps",
  "audit.read": "Read the audit log",
  "knowledge.admin": "Administer knowledge objects",
  "members.admin": "Manage members",
  "org.admin": "Administer the organization",
};

export const MEMBERS_CAPABILITY: Capability = "members.admin";

/** Who is acting, reduced to what the rules read. */
export interface MembersActor {
  readonly userId: UserId;
  /** Effective role in the organization being managed; null for a site admin acting without one. */
  readonly role: OrgRole | null;
  readonly siteAdmin: boolean;
}

/** The owner band and a site admin may grant every role and manage every member. */
export function actorHasTopRole(actor: MembersActor): boolean {
  return actor.siteAdmin || actor.role === "owner";
}

/** The roles an actor may grant: strictly below its own, or all of them from the top. */
export function grantableRoles(actor: MembersActor): readonly OrgRole[] {
  if (actorHasTopRole(actor)) return ORG_ROLES;
  const own = actor.role;
  if (own === null) return [];
  return ORG_ROLES.filter((role) => compareOrgRoles(role, own) < 0);
}

/** Whether an actor may change or remove a member currently holding `role`. */
export function canManageMemberHolding(actor: MembersActor, role: OrgRole): boolean {
  if (actorHasTopRole(actor)) return true;
  return actor.role !== null && compareOrgRoles(actor.role, role) > 0;
}

/**
 * Explicit owners on a members page, or null when the page does not hold the
 * whole organization and the count would be a guess. A null count never
 * blocks anything here: the server's last-owner guard still answers.
 */
export function countExplicitOwners(page: Page<OrganizationMember>): number | null {
  const { currentPage, hasNextPage, hasPrevPage } = page.page;
  if (currentPage !== 1 || hasNextPage || hasPrevPage) return null;
  return page.items.filter((member) => member.explicitRole === "owner").length;
}

export interface MembersContext {
  readonly explicitOwners: number | null;
  /** The display name of an organization, for inherited-role explanations. */
  readonly organizationName: (organizationId: OrganizationId | undefined) => string;
}

export interface RoleOption {
  readonly role: OrgRole;
  readonly current: boolean;
  readonly disabled: boolean;
  /** Why a disabled option is disabled. */
  readonly reason?: string;
}

export type RoleChangePolicy =
  | { readonly allowed: true; readonly current: OrgRole; readonly options: readonly RoleOption[] }
  | { readonly allowed: false; readonly reason: string };

export type RemovalPolicy =
  | {
    readonly allowed: true;
    readonly self: boolean;
    /** The role the member keeps through an ancestor once the explicit membership is gone. */
    readonly keepsInheritedRole?: OrgRole;
    readonly inheritedFromOrganizationId?: OrganizationId;
  }
  | { readonly allowed: false; readonly reason: string };

export interface MemberPolicy {
  readonly self: boolean;
  readonly lastOwner: boolean;
  readonly roleChange: RoleChangePolicy;
  readonly removal: RemovalPolicy;
}

function article(role: OrgRole): string {
  return role === "admin" || role === "owner" ? "an" : "a";
}

/** "a member", "an admin": a role in running text. */
export function roleWithArticle(role: OrgRole): string {
  return `${article(role)} ${ROLE_LABELS[role].toLowerCase()}`;
}

/** The role a member's membership row holds, or the inherited one when there is no row. */
export function displayedRole(member: OrganizationMember): OrgRole {
  return member.explicitRole ?? member.effectiveRole;
}

export function memberPolicy(actor: MembersActor, member: OrganizationMember, context: MembersContext): MemberPolicy {
  const self = member.userId === actor.userId;
  const lastOwner = member.explicitRole === "owner" && context.explicitOwners === 1;
  const inheritedFrom = member.inheritedFromOrganizationId;
  const inheritedName = context.organizationName(inheritedFrom);
  const manageable = canManageMemberHolding(actor, member.effectiveRole);
  const held = ROLE_LABELS[member.effectiveRole].toLowerCase();
  const outranked = (verb: string) => `Only a role above ${held} can ${verb} ${article(member.effectiveRole)} ${held}.`;
  const lastOwnerReason = "The last owner cannot be demoted or removed. Make another member an owner first.";
  const ownRoleReason = actor.role === "owner"
    ? "Owners cannot change their own role. Make another member an owner, then leave, or ask a site admin."
    : "Ask an owner to change your own role.";

  let roleChange: RoleChangePolicy;
  if (member.explicitRole === null) {
    roleChange = { allowed: false, reason: `Inherited from ${inheritedName}; change it there.` };
  } else if (self && !actor.siteAdmin) {
    roleChange = { allowed: false, reason: lastOwner ? lastOwnerReason : ownRoleReason };
  } else if (!manageable) {
    roleChange = { allowed: false, reason: outranked("change") };
  } else if (lastOwner) {
    roleChange = { allowed: false, reason: lastOwnerReason };
  } else {
    const grantable = grantableRoles(actor);
    const current = member.explicitRole;
    const options = ORG_ROLES.map((role): RoleOption => {
      if (role === current) return { role, current: true, disabled: false };
      if (!grantable.includes(role)) {
        return { role, current: false, disabled: true, reason: "Above what your role can grant." };
      }
      return { role, current: false, disabled: false };
    });
    roleChange = options.some((option) => !option.current && !option.disabled)
      ? { allowed: true, current, options }
      : { allowed: false, reason: "Your role cannot grant any other role to this member." };
  }

  let removal: RemovalPolicy;
  if (member.explicitRole === null) {
    removal = { allowed: false, reason: `Inherited from ${inheritedName}; remove them there.` };
  } else if (!self && !manageable) {
    // Checked before the last-owner guard, as for a role change: an actor who
    // is outranked could not remove this member whatever the owner count.
    removal = { allowed: false, reason: outranked("remove") };
  } else if (lastOwner) {
    removal = { allowed: false, reason: lastOwnerReason };
  } else {
    removal = {
      allowed: true,
      self,
      ...(inheritedFrom === undefined ? {} : { keepsInheritedRole: member.effectiveRole, inheritedFromOrganizationId: inheritedFrom }),
    };
  }

  return { self, lastOwner, roleChange, removal };
}

/** Capabilities a role change adds and takes away, in table order. */
export function capabilityChange(from: OrgRole, to: OrgRole): { readonly gained: readonly OrgCapability[]; readonly lost: readonly OrgCapability[] } {
  const before = ORG_ROLE_CAPABILITIES[from];
  const after = ORG_ROLE_CAPABILITIES[to];
  return {
    gained: after.filter((capability) => !before.includes(capability)),
    lost: before.filter((capability) => !after.includes(capability)),
  };
}

export type RoleChangeNotice =
  | { readonly kind: "owner_grant" }
  | { readonly kind: "self_demotion"; readonly siteAdmin: boolean }
  | { readonly kind: "inherited_floor"; readonly role: OrgRole; readonly organizationId: OrganizationId };

/** What the role dialog warns about before a chosen change is saved. */
export function roleChangeNotices(actor: MembersActor, member: OrganizationMember, next: OrgRole): readonly RoleChangeNotice[] {
  const current = displayedRole(member);
  if (next === current) return [];
  const notices: RoleChangeNotice[] = [];
  if (next === "owner") notices.push({ kind: "owner_grant" });
  if (member.userId === actor.userId && compareOrgRoles(next, current) < 0) {
    notices.push({ kind: "self_demotion", siteAdmin: actor.siteAdmin });
  }
  if (member.inheritedFromOrganizationId !== undefined && compareOrgRoles(next, member.effectiveRole) <= 0) {
    notices.push({ kind: "inherited_floor", role: member.effectiveRole, organizationId: member.inheritedFromOrganizationId });
  }
  return notices;
}

export type InvitationState = "pending" | "expired";

export function invitationState(invitation: OrganizationInvitation, now: number): InvitationState {
  const expiresAt = Date.parse(invitation.expiresAt);
  return Number.isFinite(expiresAt) && expiresAt <= now ? "expired" : "pending";
}

const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/u;

/** The invite form's own check; the server still validates and may refuse. */
export function inviteEmailError(email: string): string | null {
  const trimmed = email.trim();
  if (trimmed.length === 0) return "Enter the email address to invite.";
  if (!EMAIL_PATTERN.test(trimmed)) return "Enter a valid email address, such as name@example.com.";
  return null;
}

export interface FailureDescription {
  readonly title: string;
  readonly message: string;
  /** Repeating the same request may succeed. */
  readonly retryable: boolean;
  /** The session ended; the provider should re-read the principal. */
  readonly signedOut: boolean;
}

function seconds(milliseconds: number | undefined): string {
  if (milliseconds === undefined) return "a moment";
  const value = Math.max(1, Math.ceil(milliseconds / 1000));
  return value === 1 ? "1 second" : `${value} seconds`;
}

/**
 * Says what went wrong in terms of what the admin can do about it. `action`
 * completes "Could not …", such as "load members" or "send the invitation".
 */
export function describeMembersFailure(error: unknown, action: string): FailureDescription {
  if (!isIdentityError(error)) {
    return { title: `Could not ${action}`, message: "Something unexpected went wrong. Try again.", retryable: true, signedOut: false };
  }
  const identityError: IdentityError = error;
  switch (identityError.kind) {
    case "unauthenticated":
      return { title: "Your session ended", message: "Sign in again to keep managing members.", retryable: false, signedOut: true };
    case "otp_required":
      return { title: "Confirm it is you", message: "Sign in again with your authenticator code to continue.", retryable: false, signedOut: true };
    case "forbidden":
      return { title: `Could not ${action}`, message: identityError.message || "Your role does not allow this change.", retryable: false, signedOut: false };
    case "rate_limited":
      return { title: "Too many requests", message: `Wait ${seconds(identityError.retryAfterMs)} and try again.`, retryable: true, signedOut: false };
    case "invalid":
      return { title: `Could not ${action}`, message: identityError.message || "The request was not accepted.", retryable: false, signedOut: false };
    case "not_found":
      return { title: "It is no longer there", message: `${identityError.message || "That record was not found"}. The list has been refreshed.`, retryable: false, signedOut: false };
    case "conflict":
      return { title: `Could not ${action}`, message: identityError.message || "That conflicts with the organization's current state.", retryable: false, signedOut: false };
    case "network":
      return { title: `Could not ${action}`, message: "The identity service could not be reached. Check your connection and try again.", retryable: true, signedOut: false };
    case "not_configured":
      return { title: "Identity is not configured", message: identityError.message || "This server does not provide accounts and organizations yet.", retryable: false, signedOut: false };
  }
}
