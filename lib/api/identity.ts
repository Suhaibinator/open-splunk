/**
 * The browser's identity contract.
 *
 * Users, organizations, memberships, invitations, OAuth providers and sessions
 * come from go-common (`proto/user.proto`, `organization.proto`,
 * `authentication.proto`, `admin_api.proto`, `pagination.proto`). These types
 * mirror those messages in the shape screens consume: identifiers are decimal
 * strings (go-common user IDs are uint64 and organization IDs int64, both past
 * the safe-integer range), timestamps are ISO-8601 strings, and go-common's
 * tiered role and user-type enums collapse to the four organization roles and
 * the site-admin flag open-splunk authorizes with.
 *
 * Capabilities are open-splunk's own static RBAC table. The ids are the same
 * strings the Go authorizer uses; a screen gates on a capability, never on a
 * role, so the table stays the one place that says what a role may do.
 */

// --------------------------------------------------------------------------
// Roles and capabilities
// --------------------------------------------------------------------------

/** go-common OrganizationRole bands, lowest first. */
export const ORG_ROLES = ["member", "moderator", "admin", "owner"] as const;
export type OrgRole = (typeof ORG_ROLES)[number];

/** Capabilities a Member holds in its organization. */
export const MEMBER_CAPABILITIES = ["search", "objects.own"] as const;
/** Capabilities a Moderator adds to a Member's. */
export const MODERATOR_CAPABILITIES = [
  "knowledge.write",
  "lookups.write",
  "alerts.write",
  "reports.write",
] as const;
/** Capabilities an Admin adds to a Moderator's. */
export const ADMIN_CAPABILITIES = [
  "indexes.admin",
  "ingest_tokens.admin",
  "collectors.admin",
  "hec.admin",
  "apps.admin",
  "audit.read",
  "knowledge.admin",
  "members.admin",
] as const;
/** Capabilities an Owner adds to an Admin's. */
export const OWNER_CAPABILITIES = ["org.admin"] as const;
/** Deployment-wide capabilities only a site admin (go-common UserType Admin band) holds. */
export const SITE_CAPABILITIES = ["server.admin", "users.admin", "orgs.admin"] as const;

export type OrgCapability =
  | (typeof MEMBER_CAPABILITIES)[number]
  | (typeof MODERATOR_CAPABILITIES)[number]
  | (typeof ADMIN_CAPABILITIES)[number]
  | (typeof OWNER_CAPABILITIES)[number];
export type SiteCapability = (typeof SITE_CAPABILITIES)[number];
export type Capability = OrgCapability | SiteCapability;

/** Every organization capability, in role order. */
export const ORG_CAPABILITIES: readonly OrgCapability[] = [
  ...MEMBER_CAPABILITIES,
  ...MODERATOR_CAPABILITIES,
  ...ADMIN_CAPABILITIES,
  ...OWNER_CAPABILITIES,
];
/** Every capability, in role order. */
export const CAPABILITIES: readonly Capability[] = [...ORG_CAPABILITIES, ...SITE_CAPABILITIES];

/** The cumulative capability set each organization role holds. */
export const ORG_ROLE_CAPABILITIES: Readonly<Record<OrgRole, readonly OrgCapability[]>> = {
  member: [...MEMBER_CAPABILITIES],
  moderator: [...MEMBER_CAPABILITIES, ...MODERATOR_CAPABILITIES],
  admin: [...MEMBER_CAPABILITIES, ...MODERATOR_CAPABILITIES, ...ADMIN_CAPABILITIES],
  owner: [...ORG_CAPABILITIES],
};

export function isOrgRole(value: unknown): value is OrgRole {
  return typeof value === "string" && (ORG_ROLES as readonly string[]).includes(value);
}

export function isCapability(value: unknown): value is Capability {
  return typeof value === "string" && (CAPABILITIES as readonly string[]).includes(value);
}

/** Orders roles: negative when `left` is lower than `right`. */
export function compareOrgRoles(left: OrgRole, right: OrgRole): number {
  return ORG_ROLES.indexOf(left) - ORG_ROLES.indexOf(right);
}

export function orgRoleAtLeast(role: OrgRole | null, minimum: OrgRole): boolean {
  return role !== null && compareOrgRoles(role, minimum) >= 0;
}

/**
 * The capabilities a principal holds in its active organization. A site admin
 * holds every organization capability in any organization (break-glass) plus
 * the deployment-wide ones; anyone else holds exactly its effective role's set,
 * and nothing without a role.
 */
export function capabilitiesFor(role: OrgRole | null, siteAdmin: boolean): readonly Capability[] {
  if (siteAdmin) return [...CAPABILITIES];
  return role === null ? [] : [...ORG_ROLE_CAPABILITIES[role]];
}

// go-common enum values: four bands of eleven tiers each, starting at 1.
const GO_COMMON_BAND_WIDTH = 11;
const GO_COMMON_BAND_ORIGIN = 1;
/** go-common `UserType` bands, lowest first. */
export const USER_TYPE_BANDS = ["lite", "regular", "premium", "admin"] as const;
export type UserTypeBand = (typeof USER_TYPE_BANDS)[number];

function bandIndex(value: number): number | null {
  if (!Number.isSafeInteger(value) || value < GO_COMMON_BAND_ORIGIN) return null;
  const index = Math.floor((value - GO_COMMON_BAND_ORIGIN) / GO_COMMON_BAND_WIDTH);
  return index < ORG_ROLES.length ? index : null;
}

/** Maps a go-common `OrganizationRole` value to its band; UNSPECIFIED and unknown values are null. */
export function orgRoleFromGoCommon(value: number): OrgRole | null {
  const index = bandIndex(value);
  return index === null ? null : ORG_ROLES[index] ?? null;
}

/** The tier-0 go-common `OrganizationRole` value for a band (MEMBER_TIER_0 = 1 … OWNER_TIER_0 = 34). */
export function orgRoleToGoCommon(role: OrgRole): number {
  return GO_COMMON_BAND_ORIGIN + ORG_ROLES.indexOf(role) * GO_COMMON_BAND_WIDTH;
}

/** Maps a go-common `UserType` value to its band; UNSPECIFIED and unknown values are null. */
export function userTypeBandFromGoCommon(value: number): UserTypeBand | null {
  const index = bandIndex(value);
  return index === null ? null : USER_TYPE_BANDS[index] ?? null;
}

/** Site admin is go-common's UserType Admin band, at any tier. */
export function isSiteAdminUserType(value: number): boolean {
  return userTypeBandFromGoCommon(value) === "admin";
}

// --------------------------------------------------------------------------
// go-common messages
// --------------------------------------------------------------------------

/** Decimal go-common user ID (uint64). It is also open-splunk's owner_id. */
export type UserId = string;
/** Decimal go-common organization ID (int64). */
export type OrganizationId = string;
/** ISO-8601 instant. */
export type Timestamp = string;

/** go-common `User`, with the user type reduced to its band. */
export interface IdentityUser {
  readonly id: UserId;
  readonly username: string;
  readonly email: string;
  readonly firstName: string;
  readonly lastName: string;
  readonly avatarUrl?: string;
  readonly userType: UserTypeBand | null;
  readonly banned: boolean;
  readonly isBot: boolean;
}

/** go-common `PublicUser`: what one member may see of another. */
export interface PublicIdentityUser {
  readonly id: UserId;
  readonly displayPrimary: string;
  readonly displaySecondary?: string;
  readonly avatarUrl?: string;
  readonly userType: UserTypeBand | null;
  readonly banned: boolean;
  readonly isBot: boolean;
}

/** go-common `Organization`. */
export interface IdentityOrganization {
  readonly id: OrganizationId;
  readonly name: string;
  readonly slug: string;
  readonly description?: string;
  readonly avatarUrl?: string;
  readonly createdAt: Timestamp;
  readonly updatedAt: Timestamp;
  readonly parentOrganizationId?: OrganizationId;
  /** Ancestor organization IDs, root first. */
  readonly lineage: readonly OrganizationId[];
  readonly personalOwnerUserId?: UserId;
}

/**
 * The role fields go-common repeats on every membership shape. `explicitRole`
 * is null when the user holds no membership row in this organization and the
 * role is inherited; `effectiveRole` is what authorization uses.
 */
export interface MembershipRole {
  readonly explicitRole: OrgRole | null;
  readonly effectiveRole: OrgRole;
  readonly inheritedFromOrganizationId?: OrganizationId;
  /** Absolute depth of the organization that supplied `effectiveRole` (0 = root). */
  readonly effectiveRoleDepth?: number;
}

/** go-common `OrganizationUser`. */
export interface OrganizationMembership extends MembershipRole {
  readonly organizationId: OrganizationId;
  readonly userId: UserId;
  readonly createdAt: Timestamp;
  readonly updatedAt: Timestamp;
}

/** go-common `OrganizationWithUserRole`. */
export interface OrganizationWithRole extends MembershipRole {
  readonly organization: IdentityOrganization;
}

/** go-common `OrganizationMemberDetails`. */
export interface OrganizationMember extends OrganizationMembership {
  readonly user: PublicIdentityUser;
  /** The account is not banned; this is not online presence. */
  readonly isActive: boolean;
  readonly lastActivity?: Timestamp;
}

/** go-common `OrganizationInvitation`. */
export interface OrganizationInvitation {
  readonly id: string;
  readonly organizationId: OrganizationId;
  readonly email: string;
  readonly role: OrgRole;
  readonly invitedByUserId: UserId;
  readonly createdAt: Timestamp;
  readonly expiresAt: Timestamp;
  readonly acceptedAt?: Timestamp;
  readonly declinedAt?: Timestamp;
  readonly organization: IdentityOrganization;
  /** True for the invite flow; false for an auto-add applied once the user registers. */
  readonly requiresAcceptance: boolean;
}

/** go-common `OAuthProviderDescriptor`. */
export interface OAuthProvider {
  readonly slug: string;
  readonly displayName: string;
  /** Frontend icon key; empty selects the generic icon. */
  readonly iconKey: string;
  /** The provider answers with response_mode=form_post through the backend relay. */
  readonly formPost: boolean;
}

/** go-common `AuthProvider`. */
export type AuthMethod = "email" | "phone" | "oauth" | "unspecified";

/** go-common `ActiveSession`. */
export interface ActiveSession {
  readonly sessionId: string;
  readonly current: boolean;
  readonly authMethod: AuthMethod;
  readonly ipAddress: string;
  readonly userAgent: string;
  readonly deviceId: string;
  readonly issuedAt: Timestamp;
  readonly expiresAt: Timestamp;
  readonly lastUsedAt: Timestamp;
  readonly oauthProviderSlug?: string;
}

/** go-common `AdminUserDetails`. */
export interface AdminUser {
  readonly user: IdentityUser;
  readonly createdAt: Timestamp;
  readonly updatedAt: Timestamp;
  readonly lastLogin?: Timestamp;
  readonly totalLogins: number;
  readonly activeSessions: number;
  readonly authMethods: readonly string[];
  readonly emailVerified: boolean;
  /** An invitation was issued for the current email and password setup is incomplete. */
  readonly invitationPending: boolean;
}

/** go-common `AdminUserAccessMode`. */
export type AdminUserAccessMode = "oauth_only" | "invitation" | "password";

// --------------------------------------------------------------------------
// Paging
// --------------------------------------------------------------------------

/** go-common `PaginationRequest` (page-number form). */
export interface PageRequest {
  /** 1-based page number. */
  readonly page: number;
  /** Items per page. */
  readonly limit: number;
}

/** go-common `PaginationResponse`. Totals are present only when the server counted them. */
export interface PageInfo {
  readonly currentPage: number;
  readonly itemsPerPage: number;
  readonly hasNextPage: boolean;
  readonly hasPrevPage: boolean;
  readonly totalItems?: number;
  readonly totalPages?: number;
}

export interface Page<T> {
  readonly items: readonly T[];
  readonly page: PageInfo;
}

export const DEFAULT_PAGE_REQUEST: PageRequest = { page: 1, limit: 25 };

// --------------------------------------------------------------------------
// The signed-in principal
// --------------------------------------------------------------------------

/**
 * Who the browser is acting as. `role` is the effective role in
 * `activeOrganization` and is null when there is no active organization or a
 * site admin is acting in one it is not a member of; `capabilities` is
 * `capabilitiesFor(role, siteAdmin)`.
 */
export interface Principal {
  readonly user: IdentityUser;
  readonly siteAdmin: boolean;
  readonly activeOrganization: IdentityOrganization | null;
  readonly role: OrgRole | null;
  readonly capabilities: readonly Capability[];
  /** Every organization the user holds an effective role in. */
  readonly organizations: readonly OrganizationWithRole[];
}

export function principalHasCapability(principal: Principal | null, capability: Capability): boolean {
  return principal !== null && principal.capabilities.includes(capability);
}

// --------------------------------------------------------------------------
// Flow inputs and results
// --------------------------------------------------------------------------

export interface EmailCredentials {
  readonly email: string;
  readonly password: string;
}

/**
 * A pending second factor. go-common completes it by repeating the email
 * login with the code, so the client that issued the challenge keeps whatever
 * it needs behind `id`; screens hold only this handle.
 */
export interface OtpChallenge {
  readonly id: string;
  readonly email: string;
  readonly message?: string;
}

/** go-common `EmailAuthResponse` once the client has re-read the principal. */
export type LoginResult =
  | { readonly status: "signed_in"; readonly principal: Principal }
  | { readonly status: "otp_required"; readonly challenge: OtpChallenge };

export interface BeginOAuthResult {
  /** The complete provider authorization URL; the caller navigates to it. */
  readonly authorizationUrl: string;
}

/** The query parameters the OAuth callback page received. */
export interface OAuthCallbackParams {
  readonly code?: string | null;
  readonly state?: string | null;
  /** Set by the provider when the user denied or the provider failed. */
  readonly error?: string | null;
  readonly errorDescription?: string | null;
}

/** go-common `OAuthTokenResponse` once the client has re-read the principal. */
export interface OAuthCallbackResult {
  readonly principal: Principal;
  readonly accountCreated: boolean;
  /** The same-origin path recorded when the flow began. */
  readonly returnTo: string;
  readonly providerSlug: string;
}

export interface PasswordResetCompletion {
  /** The opaque token from the password-reset email link. */
  readonly verificationCode: string;
  readonly newPassword: string;
}

/**
 * The two go-common acceptance flows: an admin-created account's password
 * setup (`/auth/email/accept-invitation`, no session, then sign in), and a
 * signed-in user's organization invitation.
 */
export type InvitationAcceptance =
  | { readonly kind: "account"; readonly token: string; readonly password: string }
  | { readonly kind: "organization"; readonly invitationId: string };

export type InvitationAcceptanceResult =
  | { readonly kind: "account" }
  | { readonly kind: "organization"; readonly membership: OrganizationMembership };

/**
 * go-common `EmailCreateRequest`. Registration signs nobody in: go-common
 * emails a verification link, and following it (`completeEmailVerification`)
 * starts the session.
 */
export interface RegistrationInput {
  readonly email: string;
  readonly password: string;
  readonly username: string;
  readonly firstName: string;
  readonly lastName: string;
}

export interface MemberInvitationInput {
  readonly email: string;
  readonly role: OrgRole;
}

export interface AdminUserQuery {
  /** Matches email, username, name, or an exact numeric user ID. */
  readonly search?: string;
  readonly banned?: boolean;
  readonly siteAdmin?: boolean;
}

export interface AdminCreateUserInput {
  readonly email: string;
  readonly username: string;
  readonly firstName: string;
  readonly lastName: string;
  readonly siteAdmin: boolean;
  readonly accessMode: AdminUserAccessMode;
  /** Required for, and only accepted with, the password access mode. */
  readonly password?: string;
  readonly reason?: string;
}

export interface AdminOrganizationQuery {
  /** Matches name or slug. */
  readonly search?: string;
}

export interface AdminCreateOrganizationInput {
  readonly name: string;
  readonly slug: string;
  readonly description?: string;
  readonly parentOrganizationId?: OrganizationId;
}

// --------------------------------------------------------------------------
// Errors
// --------------------------------------------------------------------------

export const IDENTITY_ERROR_KINDS = [
  "unauthenticated",
  "forbidden",
  "otp_required",
  "rate_limited",
  "invalid",
  "not_found",
  "conflict",
  "network",
  "not_configured",
] as const;
export type IdentityErrorKind = (typeof IDENTITY_ERROR_KINDS)[number];

export interface IdentityErrorOptions {
  /** How long the caller should wait before retrying; set for `rate_limited`. */
  readonly retryAfterMs?: number;
  /** The HTTP status the error came from, when it came from one. */
  readonly status?: number;
  readonly cause?: unknown;
}

/** Every identity failure, typed by what a screen does about it. */
export class IdentityError extends Error {
  public readonly kind: IdentityErrorKind;
  public readonly retryAfterMs?: number;
  public readonly status?: number;

  public constructor(kind: IdentityErrorKind, message: string, options: IdentityErrorOptions = {}) {
    super(message, options.cause === undefined ? undefined : { cause: options.cause });
    this.name = "IdentityError";
    this.kind = kind;
    if (options.retryAfterMs !== undefined) this.retryAfterMs = options.retryAfterMs;
    if (options.status !== undefined) this.status = options.status;
  }
}

export function isIdentityError(error: unknown, kind?: IdentityErrorKind): error is IdentityError {
  return error instanceof IdentityError && (kind === undefined || error.kind === kind);
}

/**
 * Reads an HTTP `Retry-After` header (delta-seconds or an HTTP date) as a
 * non-negative millisecond delay; null when absent or unreadable.
 */
export function parseRetryAfterMs(header: string | null | undefined, now: number = Date.now()): number | null {
  const value = header?.trim();
  if (value === undefined || value.length === 0) return null;
  if (/^\d+$/u.test(value)) {
    const seconds = Number(value);
    return Number.isSafeInteger(seconds) ? seconds * 1000 : null;
  }
  // An HTTP date names its weekday and month; anything else (a signed or
  // fractional number, which Date.parse would read as a year) is unreadable.
  if (!/^[A-Za-z]{3}, /u.test(value)) return null;
  const at = Date.parse(value);
  return Number.isNaN(at) ? null : Math.max(0, at - now);
}

/**
 * Classifies a go-common HTTP failure by the status table in its API
 * reference: 401 unauthenticated (including banned), 403 forbidden (including
 * reauthentication required), 400/410/413 invalid, 404 not found, 409
 * conflict, 429 rate limited, and anything else a network-level failure.
 */
export function identityErrorFromHttpStatus(
  status: number,
  message: string,
  retryAfterMs: number | null = null,
): IdentityError {
  const kind: IdentityErrorKind = status === 401
    ? "unauthenticated"
    : status === 403
      ? "forbidden"
      : status === 400 || status === 410 || status === 413
        ? "invalid"
        : status === 404
          ? "not_found"
          : status === 409
            ? "conflict"
            : status === 429
              ? "rate_limited"
              : "network";
  return new IdentityError(kind, message, {
    status,
    ...(kind === "rate_limited" && retryAfterMs !== null ? { retryAfterMs } : {}),
  });
}

// --------------------------------------------------------------------------
// The client
// --------------------------------------------------------------------------

/**
 * Everything the browser asks of identity. Every method rejects with an
 * `IdentityError`; organization-scoped calls name their organization
 * explicitly rather than reading the active one.
 */
export interface IdentityClient {
  /** The signed-in principal, or null when there is no session. */
  getPrincipal(): Promise<Principal | null>;
  loginWithEmail(credentials: EmailCredentials): Promise<LoginResult>;
  /** Completes a login a second factor interrupted. A rejected code throws `otp_required` and leaves the challenge usable. */
  verifyLoginOtp(challenge: OtpChallenge, code: string): Promise<Principal>;
  logout(options?: { readonly everywhere?: boolean }): Promise<void>;

  listOAuthProviders(): Promise<readonly OAuthProvider[]>;
  /** `returnTo` must be a same-origin absolute path such as `/search/`. */
  beginOAuth(providerSlug: string, returnTo: string): Promise<BeginOAuthResult>;
  completeOAuthCallback(params: OAuthCallbackParams): Promise<OAuthCallbackResult>;

  /** Always resolves for a well-formed address, whether or not an account exists. */
  requestPasswordReset(email: string): Promise<void>;
  /** Sets the new password; it does not sign in. */
  completePasswordReset(completion: PasswordResetCompletion): Promise<void>;
  acceptInvitation(acceptance: InvitationAcceptance): Promise<InvitationAcceptanceResult>;
  /** Creates an unverified account and emails a verification link; it does not sign in. A taken address is `conflict`. */
  registerWithEmail(input: RegistrationInput): Promise<void>;
  /** Follows the emailed verification link; resolves with the principal go-common signed in, or null when it did not. */
  completeEmailVerification(token: string): Promise<Principal | null>;
  /** The signed-in user's own pending organization invitations (go-common `/user/invitations/list`). */
  listMyInvitations(page?: PageRequest): Promise<Page<OrganizationInvitation>>;
  /** Declines one of the signed-in user's own invitations (go-common `/user/invitations/decline`). */
  declineInvitation(invitationId: string): Promise<void>;

  /** Selects the organization later calls act in; a site admin may select any organization. */
  setActiveOrganization(organizationId: OrganizationId): Promise<Principal>;

  listMembers(organizationId: OrganizationId, page?: PageRequest): Promise<Page<OrganizationMember>>;
  changeMemberRole(organizationId: OrganizationId, userId: UserId, role: OrgRole): Promise<OrganizationMembership>;
  removeMember(organizationId: OrganizationId, userId: UserId): Promise<void>;
  inviteMember(organizationId: OrganizationId, invitation: MemberInvitationInput): Promise<OrganizationInvitation>;
  /** Pending invitations sent by the organization. */
  listInvitations(organizationId: OrganizationId, page?: PageRequest): Promise<Page<OrganizationInvitation>>;
  revokeInvitation(organizationId: OrganizationId, invitationId: string): Promise<void>;

  adminListUsers(query?: AdminUserQuery, page?: PageRequest): Promise<Page<AdminUser>>;
  adminCreateUser(input: AdminCreateUserInput): Promise<AdminUser>;
  adminSetSiteAdmin(userId: UserId, siteAdmin: boolean, reason?: string): Promise<AdminUser>;
  adminBanUser(userId: UserId, reason?: string): Promise<AdminUser>;
  adminUnbanUser(userId: UserId, reason?: string): Promise<AdminUser>;
  /** Returns how many sessions were revoked. */
  adminRevokeUserSessions(userId: UserId, reason?: string): Promise<number>;
  adminDeleteUser(userId: UserId, reason?: string): Promise<void>;
  adminListOrganizations(query?: AdminOrganizationQuery, page?: PageRequest): Promise<Page<IdentityOrganization>>;
  adminCreateOrganization(input: AdminCreateOrganizationInput): Promise<IdentityOrganization>;
}

/** The go-common routes are mounted here by the Phase 3 server. */
export const GO_COMMON_PATH_PREFIX = "/api/v1/common";

/**
 * The client bound to the server's go-common routes. The server does not
 * mount them yet (Phase 3 of the identity plan), so this refuses to build one
 * rather than call routes that answer 404; the demo export uses the in-memory
 * fake instead.
 */
export function createGoCommonIdentityClient(): IdentityClient {
  throw new IdentityError(
    "not_configured",
    "Server identity is not configured in this build: the go-common routes are not mounted yet.",
  );
}
