/**
 * The logic behind the site-admin Users and Organizations panels, kept free
 * of React so every rule a screen applies -- what an error tells the
 * administrator to do, which row actions an account offers, what a draft must
 * hold before it is sent -- is unit-tested on its own.
 *
 * Everything reads the identity contract in `lib/api/identity.ts`; nothing
 * here knows whether the client behind it is go-common or the demo fake.
 */

import {
  IdentityError,
  isIdentityError,
  type AdminCreateOrganizationInput,
  type AdminCreateUserInput,
  type AdminUser,
  type AdminUserAccessMode,
  type AdminUserQuery,
  type IdentityOrganization,
  type IdentityUser,
  type OrganizationId,
  type PageInfo,
  type UserId,
} from "@/lib/api/identity";
import type { StatusTone } from "../_components/status";

/** Rows per directory page. Small enough that a demo directory pages at all. */
export const SITE_DIRECTORY_PAGE_SIZE = 10;
/** The organization picker reads one page this large; go-common caps a page at 100. */
export const ORGANIZATION_PICKER_LIMIT = 100;

/** Any rejection as an `IdentityError`; a client that threw something else failed like the network. */
export function asIdentityError(error: unknown): IdentityError {
  if (isIdentityError(error)) return error;
  return new IdentityError("network", error instanceof Error ? error.message : "The identity service failed.", { cause: error });
}

// --------------------------------------------------------------------------
// Errors
// --------------------------------------------------------------------------

/** What the panel offers beside an error. */
export type DirectoryErrorAction = "retry" | "sign_in" | "clear_search" | "none";

export interface DirectoryErrorPresentation {
  readonly title: string;
  readonly message: string;
  readonly action: DirectoryErrorAction;
  /** `unavailable` paints the warning-edged state; `error` the failure state. */
  readonly kind: "error" | "unavailable";
  /** Set for `rate_limited`: how long the retry stays disabled. */
  readonly retryAfterMs?: number;
}

export type DirectorySubject = "users" | "organizations";

const SUBJECT_NOUN: Readonly<Record<DirectorySubject, string>> = {
  organizations: "organizations",
  users: "users",
};

/** Whole seconds left before a rate-limited retry, never below one. */
export function retryAfterSeconds(milliseconds: number): number {
  return Math.max(1, Math.ceil(milliseconds / 1000));
}

/** The panel-level state for a list that failed to load, by error kind. */
export function presentDirectoryError(error: IdentityError, subject: DirectorySubject): DirectoryErrorPresentation {
  const noun = SUBJECT_NOUN[subject];
  switch (error.kind) {
    case "unauthenticated":
      return {
        action: "sign_in",
        kind: "unavailable",
        message: "Your session ended or was revoked. Sign in again to keep managing this server.",
        title: "Sign in again",
      };
    case "otp_required":
      return {
        action: "sign_in",
        kind: "unavailable",
        message: "This action needs a fresh sign-in with your authenticator code.",
        title: "Confirm it is you",
      };
    case "forbidden":
      return {
        action: "none",
        kind: "unavailable",
        message: `Your account no longer holds site administrator access, so the ${noun} directory is hidden. Ask another site administrator to restore it.`,
        title: "Site administrator access required",
      };
    case "rate_limited":
      return {
        action: "retry",
        kind: "error",
        message: `The identity service is limiting requests. Wait a moment before loading ${noun} again.`,
        title: "Too many requests",
        ...(error.retryAfterMs === undefined ? {} : { retryAfterMs: error.retryAfterMs }),
      };
    case "invalid":
      return {
        action: "clear_search",
        kind: "error",
        message: `The identity service rejected this search: ${error.message}`,
        title: `Could not search ${noun}`,
      };
    case "not_found":
      return {
        action: "retry",
        kind: "error",
        message: `The ${noun} directory was not found on this server. ${error.message}`,
        title: `The ${noun} directory is missing`,
      };
    case "conflict":
      return {
        action: "retry",
        kind: "error",
        message: error.message,
        title: `Could not load ${noun}`,
      };
    case "not_configured":
      return {
        action: "none",
        kind: "unavailable",
        message: "This server does not have user accounts configured yet, so there is no directory to manage.",
        title: "Identity is not configured",
      };
    case "network":
      return {
        action: "retry",
        kind: "error",
        message: `The identity service could not be reached. Check the connection and try again. (${error.message})`,
        title: `Could not load ${noun}`,
      };
  }
}

/** One sentence for a dialog whose mutation failed. */
export function presentMutationError(error: IdentityError): string {
  switch (error.kind) {
    case "unauthenticated":
      return "Your session ended. Sign in again, then retry.";
    case "otp_required":
      return "This change needs a fresh sign-in with your authenticator code.";
    case "forbidden":
      return "Your account is not allowed to make this change.";
    case "rate_limited":
      return error.retryAfterMs === undefined
        ? "Too many requests. Wait a moment and try again."
        : `Too many requests. Try again in ${retryAfterSeconds(error.retryAfterMs)} s.`;
    case "not_found":
      return "It no longer exists; another administrator may have removed it. Close this dialog to refresh the list.";
    case "not_configured":
      return "Identity is not configured on this server.";
    case "network":
      return "The identity service could not be reached. Nothing was changed; try again.";
    case "invalid":
    case "conflict":
      return error.message;
  }
}

// --------------------------------------------------------------------------
// Users
// --------------------------------------------------------------------------

export type UserFilter = "all" | "site_admins" | "banned";

export const USER_FILTERS: readonly { readonly value: UserFilter; readonly label: string }[] = [
  { label: "All users", value: "all" },
  { label: "Site admins", value: "site_admins" },
  { label: "Banned", value: "banned" },
];

export function isUserFilter(value: unknown): value is UserFilter {
  return USER_FILTERS.some((filter) => filter.value === value);
}

/** The contract query a search box and a filter ask for. */
export function userQueryFor(search: string, filter: UserFilter): AdminUserQuery {
  const trimmed = search.trim();
  return {
    ...(trimmed.length === 0 ? {} : { search: trimmed }),
    ...(filter === "site_admins" ? { siteAdmin: true } : {}),
    ...(filter === "banned" ? { banned: true } : {}),
  };
}

export function userDisplayName(user: IdentityUser): string {
  const name = `${user.firstName} ${user.lastName}`.trim();
  if (name.length > 0) return name;
  return user.username.length > 0 ? user.username : user.email;
}

export function isSiteAdmin(user: IdentityUser): boolean {
  return user.userType === "admin";
}

export interface UserStatus {
  readonly label: string;
  readonly tone: StatusTone;
}

/** The account's one-word state, most severe first. */
export function adminUserStatus(admin: AdminUser): UserStatus {
  if (admin.user.banned) return { label: "Banned", tone: "error" };
  if (admin.invitationPending) return { label: "Invitation pending", tone: "warning" };
  if (admin.lastLogin === undefined) return { label: "Never signed in", tone: "neutral" };
  return { label: "Active", tone: "success" };
}

const AUTH_METHOD_LABELS: Readonly<Record<string, string>> = {
  email: "Password",
  github: "GitHub",
  google: "Google",
  okta: "Okta",
  password: "Password",
};

/** "Password · GitHub", or a plain statement that nothing is linked. */
export function authMethodsLabel(methods: readonly string[]): string {
  if (methods.length === 0) return "No sign-in method yet";
  const labels = methods.map((method) => AUTH_METHOD_LABELS[method] ?? method.charAt(0).toUpperCase() + method.slice(1));
  return [...new Set(labels)].join(" · ");
}

export type UserAction = "grant_site_admin" | "revoke_site_admin" | "ban" | "unban" | "revoke_sessions" | "delete";

export interface UserActionItem {
  readonly action: UserAction;
  readonly label: string;
  /** Why the action cannot be taken on this row; null when it can. */
  readonly disabledReason: string | null;
  readonly danger: boolean;
}

/**
 * The row menu for one account. The signed-in administrator's own row keeps
 * "Revoke sessions" (it signs them out) and disables the three changes
 * go-common refuses on oneself, saying why rather than hiding them.
 */
export function userActionsFor(admin: AdminUser, selfId: UserId | null): readonly UserActionItem[] {
  const self = admin.user.id === selfId;
  const selfReason = (what: string) => (self ? `You cannot ${what} your own account.` : null);
  const siteAdmin = isSiteAdmin(admin.user);
  return [
    siteAdmin
      ? { action: "revoke_site_admin", danger: false, disabledReason: selfReason("remove site administrator access from"), label: "Remove site admin" }
      : { action: "grant_site_admin", danger: false, disabledReason: selfReason("change"), label: "Make site admin" },
    admin.user.banned
      ? { action: "unban", danger: false, disabledReason: selfReason("unban"), label: "Unban user" }
      : { action: "ban", danger: true, disabledReason: selfReason("ban"), label: "Ban user" },
    {
      action: "revoke_sessions",
      danger: false,
      disabledReason: admin.activeSessions === 0 ? "This account has no active sessions." : null,
      label: self ? "Sign out my sessions" : "Revoke sessions",
    },
    { action: "delete", danger: true, disabledReason: selfReason("delete"), label: "Delete user" },
  ];
}

export interface UserActionCopy {
  readonly title: string;
  readonly description: string;
  readonly confirmLabel: string;
  readonly pendingLabel: string;
  readonly danger: boolean;
  /** Delete asks the administrator to type the account's email first. */
  readonly typedConfirmation: string | null;
}

export function userActionCopy(action: UserAction, admin: AdminUser, self: boolean): UserActionCopy {
  const name = userDisplayName(admin.user);
  const sessions = admin.activeSessions === 1 ? "1 active session" : `${admin.activeSessions} active sessions`;
  switch (action) {
    case "grant_site_admin":
      return {
        confirmLabel: "Make site admin",
        danger: false,
        description: `${name} will hold every capability in every organization, and can manage users, organizations and server settings.`,
        pendingLabel: "Granting…",
        title: `Make ${name} a site admin?`,
        typedConfirmation: null,
      };
    case "revoke_site_admin":
      return {
        confirmLabel: "Remove site admin",
        danger: true,
        description: `${name} keeps their organization memberships but loses deployment-wide access, including this page.`,
        pendingLabel: "Removing…",
        title: `Remove site admin from ${name}?`,
        typedConfirmation: null,
      };
    case "ban":
      return {
        confirmLabel: "Ban user",
        danger: true,
        description: `${name} is signed out of ${sessions} and cannot sign in again until unbanned. Their saved searches and memberships are kept.`,
        pendingLabel: "Banning…",
        title: `Ban ${name}?`,
        typedConfirmation: null,
      };
    case "unban":
      return {
        confirmLabel: "Unban user",
        danger: false,
        description: `${name} can sign in again with their existing sign-in methods.`,
        pendingLabel: "Unbanning…",
        title: `Unban ${name}?`,
        typedConfirmation: null,
      };
    case "revoke_sessions":
      return self
        ? {
          confirmLabel: "Sign out everywhere",
          danger: true,
          description: `This ends all ${sessions} of yours, including this one. You will need to sign in again.`,
          pendingLabel: "Signing out…",
          title: "Sign out all of your sessions?",
          typedConfirmation: null,
        }
        : {
          confirmLabel: "Revoke sessions",
          danger: true,
          description: `${name} is signed out of ${sessions} on every device. They can sign in again.`,
          pendingLabel: "Revoking…",
          title: `Revoke ${name}'s sessions?`,
          typedConfirmation: null,
        };
    case "delete":
      return {
        confirmLabel: "Delete user",
        danger: true,
        description: `${name}'s account, memberships and sign-in methods are removed permanently. This cannot be undone.`,
        pendingLabel: "Deleting…",
        title: `Delete ${name}?`,
        typedConfirmation: admin.user.email,
      };
  }
}

/** The toast after a confirmed action; `revoked` is the count `adminRevokeUserSessions` returned. */
export function userActionSuccessMessage(action: UserAction, admin: AdminUser, revoked = 0): string {
  const name = userDisplayName(admin.user);
  switch (action) {
    case "grant_site_admin":
      return `${name} is now a site admin.`;
    case "revoke_site_admin":
      return `${name} is no longer a site admin.`;
    case "ban":
      return `${name} is banned.`;
    case "unban":
      return `${name} can sign in again.`;
    case "revoke_sessions":
      return revoked === 1 ? `Revoked 1 session for ${name}.` : `Revoked ${revoked} sessions for ${name}.`;
    case "delete":
      return `${name} was deleted.`;
  }
}

// ---- create user ----------------------------------------------------------

export interface CreateUserDraft {
  readonly email: string;
  readonly username: string;
  readonly firstName: string;
  readonly lastName: string;
  readonly accessMode: AdminUserAccessMode;
  readonly password: string;
  readonly siteAdmin: boolean;
  readonly reason: string;
}

export const EMPTY_CREATE_USER_DRAFT: CreateUserDraft = {
  accessMode: "invitation",
  email: "",
  firstName: "",
  lastName: "",
  password: "",
  reason: "",
  siteAdmin: false,
  username: "",
};

export const ACCESS_MODE_OPTIONS: readonly { readonly value: AdminUserAccessMode; readonly label: string; readonly description: string }[] = [
  { description: "Emails a link where they choose their own password.", label: "Send an invitation", value: "invitation" },
  { description: "You choose the first password and share it with them.", label: "Set a password", value: "password" },
  { description: "No password; they sign in through a configured identity provider.", label: "Single sign-on only", value: "oauth_only" },
];

export type CreateUserField = "email" | "username" | "firstName" | "lastName" | "password";
export type CreateUserErrors = Partial<Record<CreateUserField, string>>;

const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/u;
const USERNAME_PATTERN = /^[A-Za-z0-9._-]{3,32}$/u;
export const MINIMUM_PASSWORD_LENGTH = 8;
const NAME_LIMIT = 100;

/** The fields a draft cannot be sent with, each with the sentence its note shows. */
export function validateCreateUserDraft(draft: CreateUserDraft): CreateUserErrors {
  const errors: { -readonly [K in CreateUserField]?: string } = {};
  const email = draft.email.trim();
  if (email.length === 0) errors.email = "Enter an email address.";
  else if (!EMAIL_PATTERN.test(email)) errors.email = "Enter a valid email address, such as dana@example.com.";
  const username = draft.username.trim();
  if (username.length === 0) errors.username = "Enter a username.";
  else if (!USERNAME_PATTERN.test(username)) errors.username = "Use 3–32 letters, digits, dots, dashes or underscores.";
  if (draft.firstName.trim().length > NAME_LIMIT) errors.firstName = `Keep it under ${NAME_LIMIT} characters.`;
  if (draft.lastName.trim().length > NAME_LIMIT) errors.lastName = `Keep it under ${NAME_LIMIT} characters.`;
  if (draft.accessMode === "password" && draft.password.length < MINIMUM_PASSWORD_LENGTH) {
    errors.password = `Use at least ${MINIMUM_PASSWORD_LENGTH} characters.`;
  }
  return errors;
}

export function hasErrors(errors: Readonly<Record<string, string | undefined>>): boolean {
  return Object.values(errors).some((value) => value !== undefined);
}

/** The contract input; the password rides only with the password access mode. */
export function createUserInput(draft: CreateUserDraft): AdminCreateUserInput {
  const reason = draft.reason.trim();
  return {
    accessMode: draft.accessMode,
    email: draft.email.trim().toLowerCase(),
    firstName: draft.firstName.trim(),
    lastName: draft.lastName.trim(),
    siteAdmin: draft.siteAdmin,
    username: draft.username.trim(),
    ...(draft.accessMode === "password" ? { password: draft.password } : {}),
    ...(reason.length === 0 ? {} : { reason }),
  };
}

/**
 * Which field a server rejection belongs to, so the note under it says so;
 * null keeps the message at the top of the form.
 */
export function createUserErrorField(error: IdentityError): CreateUserField | null {
  if (error.kind !== "conflict" && error.kind !== "invalid") return null;
  const message = error.message.toLowerCase();
  if (message.includes("email")) return "email";
  if (message.includes("username") || message.includes("user already exists")) return "username";
  if (message.includes("password")) return "password";
  return null;
}

export function createUserSuccessMessage(created: AdminUser, accessMode: AdminUserAccessMode): string {
  const name = userDisplayName(created.user);
  if (accessMode === "invitation") return `Created ${name} and sent an invitation to ${created.user.email}.`;
  if (accessMode === "password") return `Created ${name}. Share the password with them securely.`;
  return `Created ${name}. They can sign in with single sign-on.`;
}

// --------------------------------------------------------------------------
// Organizations
// --------------------------------------------------------------------------

export type OrganizationKind = "personal" | "child" | "top_level";

export function organizationKind(organization: IdentityOrganization): OrganizationKind {
  if (organization.personalOwnerUserId !== undefined) return "personal";
  return organization.parentOrganizationId === undefined ? "top_level" : "child";
}

export const ORGANIZATION_KIND_LABELS: Readonly<Record<OrganizationKind, string>> = {
  child: "Child",
  personal: "Personal",
  top_level: "Top level",
};

/** The names of an organization's ancestors, root first; an unknown ancestor reads as its ID. */
export function organizationAncestry(
  organization: IdentityOrganization,
  names: ReadonlyMap<OrganizationId, string>,
): readonly { readonly id: OrganizationId; readonly name: string }[] {
  const lineage = organization.lineage.length > 0
    ? organization.lineage
    : organization.parentOrganizationId === undefined ? [] : [organization.parentOrganizationId];
  return lineage.map((id) => ({ id, name: names.get(id) ?? `Organization ${id}` }));
}

/** Name lookup for every organization seen so far, later lists winning. */
export function organizationNames(...lists: readonly (readonly IdentityOrganization[])[]): ReadonlyMap<OrganizationId, string> {
  const names = new Map<OrganizationId, string>();
  for (const list of lists) for (const organization of list) names.set(organization.id, organization.name);
  return names;
}

/** go-common's slug grammar: lowercase letters and digits joined by single dashes. */
export const SLUG_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/u;
const SLUG_LIMIT = 63;
const ORGANIZATION_NAME_LIMIT = 100;
const DESCRIPTION_LIMIT = 500;

/** A slug the name suggests: "Acme Ops (EU)" becomes "acme-ops-eu". */
export function slugFromName(name: string): string {
  return name
    .normalize("NFKD")
    .replaceAll(/[\u0300-\u036F]/gu, "")
    .toLowerCase()
    .replaceAll(/[^a-z0-9]+/gu, "-")
    .replaceAll(/^-+|-+$/gu, "")
    .slice(0, SLUG_LIMIT)
    .replace(/-+$/u, "");
}

export interface CreateOrganizationDraft {
  readonly name: string;
  readonly slug: string;
  /** Once the administrator types a slug, the name stops rewriting it. */
  readonly slugEdited: boolean;
  readonly description: string;
  /** Empty for a top-level organization. */
  readonly parentId: OrganizationId | "";
}

export const EMPTY_CREATE_ORGANIZATION_DRAFT: CreateOrganizationDraft = {
  description: "",
  name: "",
  parentId: "",
  slug: "",
  slugEdited: false,
};

/** Applies a name edit, carrying the suggested slug along until the slug was edited by hand. */
export function withOrganizationName(draft: CreateOrganizationDraft, name: string): CreateOrganizationDraft {
  return { ...draft, name, slug: draft.slugEdited ? draft.slug : slugFromName(name) };
}

export type CreateOrganizationField = "name" | "slug" | "description";
export type CreateOrganizationErrors = Partial<Record<CreateOrganizationField, string>>;

export function validateCreateOrganizationDraft(draft: CreateOrganizationDraft): CreateOrganizationErrors {
  const errors: { -readonly [K in CreateOrganizationField]?: string } = {};
  const name = draft.name.trim();
  if (name.length === 0) errors.name = "Enter a name.";
  else if (name.length > ORGANIZATION_NAME_LIMIT) errors.name = `Keep it under ${ORGANIZATION_NAME_LIMIT} characters.`;
  if (draft.slug.length === 0) errors.slug = "Enter a slug.";
  else if (draft.slug.length > SLUG_LIMIT) errors.slug = `Keep it under ${SLUG_LIMIT} characters.`;
  else if (!SLUG_PATTERN.test(draft.slug)) errors.slug = "Use lowercase letters and digits separated by single dashes.";
  if (draft.description.trim().length > DESCRIPTION_LIMIT) errors.description = `Keep it under ${DESCRIPTION_LIMIT} characters.`;
  return errors;
}

export function createOrganizationInput(draft: CreateOrganizationDraft): AdminCreateOrganizationInput {
  const description = draft.description.trim();
  return {
    name: draft.name.trim(),
    slug: draft.slug,
    ...(description.length === 0 ? {} : { description }),
    ...(draft.parentId === "" ? {} : { parentOrganizationId: draft.parentId }),
  };
}

export function createOrganizationErrorField(error: IdentityError): CreateOrganizationField | null {
  if (error.kind !== "conflict" && error.kind !== "invalid") return null;
  const message = error.message.toLowerCase();
  if (message.includes("slug")) return "slug";
  if (message.includes("name")) return "name";
  if (message.includes("description")) return "description";
  return null;
}

// --------------------------------------------------------------------------
// Paging
// --------------------------------------------------------------------------

/** "Showing 11–20 of 23 users", or without a total when the server did not count. */
export function pageRangeLabel(page: PageInfo, count: number, noun: string): string {
  if (count === 0) return `No ${noun}`;
  const first = (page.currentPage - 1) * page.itemsPerPage + 1;
  const last = first + count - 1;
  return page.totalItems === undefined
    ? `Showing ${first}–${last} ${noun}`
    : `Showing ${first}–${last} of ${page.totalItems} ${noun}`;
}

export function pagePositionLabel(page: PageInfo): string {
  return page.totalPages === undefined ? `Page ${page.currentPage}` : `Page ${page.currentPage} of ${page.totalPages}`;
}

/** The page to show after rows were removed: one back when the current page emptied. */
export function pageAfterRemoval(page: PageInfo, remainingOnPage: number): number {
  return remainingOnPage === 0 && page.currentPage > 1 ? page.currentPage - 1 : page.currentPage;
}
