/**
 * A deterministic in-memory `IdentityClient`.
 *
 * Unit tests and the static demo export drive every identity flow against
 * it: email sign-in with and without a second factor, OAuth through a
 * simulated provider redirect, password reset and invitations through an
 * inspectable outbox, organization switching with inherited roles, member
 * management, and the site-admin user and organization consoles. Every error
 * kind is reachable through ordinary input, and `failNext` injects any error
 * (a network failure, say) into the next call of a chosen method.
 *
 * The seeded directory is fixed; only the clock (`now`) varies, and tests pass
 * their own. Only the session (who is signed in, and the active
 * organization) is persisted, through the optional `storage`; everything else
 * resets with the instance.
 */

import {
  DEFAULT_PAGE_REQUEST,
  IdentityError,
  capabilitiesFor,
  compareOrgRoles,
  isOrgRole,
  type AdminCreateOrganizationInput,
  type AdminCreateUserInput,
  type AdminOrganizationQuery,
  type AdminUser,
  type AdminUserQuery,
  type BeginOAuthResult,
  type Capability,
  type EmailCredentials,
  type IdentityClient,
  type IdentityErrorKind,
  type IdentityOrganization,
  type IdentityUser,
  type InvitationAcceptance,
  type InvitationAcceptanceResult,
  type LoginResult,
  type MemberInvitationInput,
  type MembershipRole,
  type OAuthCallbackParams,
  type OAuthCallbackResult,
  type OAuthProvider,
  type OrgRole,
  type OrganizationId,
  type OrganizationInvitation,
  type OrganizationMember,
  type OrganizationMembership,
  type OrganizationWithRole,
  type OtpChallenge,
  type Page,
  type PageRequest,
  type PasswordResetCompletion,
  type Principal,
  type UserId,
} from "./identity";

// --------------------------------------------------------------------------
// The seeded directory
// --------------------------------------------------------------------------

/** The password every seeded account with a password signs in with. */
export const FAKE_IDENTITY_PASSWORD = "open-splunk-demo";
/** The authenticator code the second-factor account accepts. */
export const FAKE_IDENTITY_OTP_CODE = "123456";
/** An OAuth code the fake answers with the identity-conflict (409) path. */
export const FAKE_OAUTH_CONFLICT_CODE = "fake-oauth-identity-conflict";
/** The account-invitation token seeded for the pending invited account. */
export const FAKE_ACCOUNT_INVITATION_TOKEN = "fake-account-invitation-iris";
/** The organization invitation seeded for the account without an organization. */
export const FAKE_PENDING_INVITATION_ID = "1";
/** An organization invitation whose link has expired. */
export const FAKE_EXPIRED_INVITATION_ID = "2";

/** Failed sign-ins per address inside the window before `rate_limited`. */
export const FAKE_LOGIN_ATTEMPT_LIMIT = 5;
export const FAKE_LOGIN_WINDOW_MS = 60_000;
/** Rejected codes per challenge before it is discarded with `rate_limited`. */
export const FAKE_OTP_ATTEMPT_LIMIT = 5;
export const FAKE_OTP_CHALLENGE_TTL_MS = 5 * 60_000;
/** One reset request per address inside the cooldown. */
export const FAKE_PASSWORD_RESET_COOLDOWN_MS = 60_000;
export const FAKE_PASSWORD_RESET_TTL_MS = 60 * 60_000;
/** go-common's defaults: 20 OAuth callbacks a minute, states live 10 minutes. */
export const FAKE_OAUTH_CALLBACK_LIMIT = 20;
export const FAKE_OAUTH_CALLBACK_WINDOW_MS = 60_000;
export const FAKE_OAUTH_STATE_TTL_MS = 10 * 60_000;
export const FAKE_INVITATION_TTL_MS = 7 * 24 * 60 * 60_000;
export const FAKE_MINIMUM_PASSWORD_LENGTH = 8;
export const FAKE_MAXIMUM_PAGE_LIMIT = 100;

export interface FakeIdentityAccount {
  readonly email: string;
  readonly description: string;
}

/** The seeded accounts a demo sign-in screen may offer, in directory order. */
export const FAKE_IDENTITY_ACCOUNTS: readonly FakeIdentityAccount[] = [
  { email: "ada@open-splunk.test", description: "Site admin; owner of Acme Operations" },
  { email: "owen@open-splunk.test", description: "Owner of Globex Security; member of Acme Operations" },
  { email: "alice@open-splunk.test", description: "Admin of Acme Operations (inherited in Acme Platform)" },
  { email: "mona@open-splunk.test", description: "Moderator of Acme Operations" },
  { email: "mark@open-splunk.test", description: "Member of Acme Operations and Globex Security; linked to GitHub" },
  { email: "tess@open-splunk.test", description: "Member of Acme Operations; authenticator code required" },
  { email: "nora@open-splunk.test", description: "No organization; one pending invitation" },
  { email: "bart@open-splunk.test", description: "Banned" },
];

/** The site admin the demo export starts signed in as. */
export const FAKE_SITE_ADMIN_EMAIL = "ada@open-splunk.test";

const SEED_CREATED_AT = "2026-01-05T09:00:00.000Z";
const SEED_UPDATED_AT = "2026-03-02T14:30:00.000Z";
const SEED_LAST_LOGIN = "2026-10-20T08:15:00.000Z";

type Mutable<T> = { -readonly [K in keyof T]: T[K] };

interface UserRecord {
  user: Mutable<IdentityUser>;
  password: string | null;
  otpCode: string | null;
  createdAt: string;
  updatedAt: string;
  lastLogin?: string;
  totalLogins: number;
  activeSessions: number;
  emailVerified: boolean;
  invitationPending: boolean;
  oauthProviders: string[];
}

interface MembershipRecord {
  organizationId: OrganizationId;
  userId: UserId;
  role: OrgRole;
  createdAt: string;
  updatedAt: string;
}

interface InvitationRecord {
  id: string;
  organizationId: OrganizationId;
  email: string;
  role: OrgRole;
  invitedByUserId: UserId;
  createdAt: string;
  expiresAt: string;
  acceptedAt?: string;
}

interface OAuthIdentity {
  readonly email: string;
  readonly firstName: string;
  readonly lastName: string;
  readonly username: string;
}

const FAKE_OAUTH_PROVIDERS: readonly OAuthProvider[] = [
  { slug: "github", displayName: "GitHub", iconKey: "github", formPost: false },
  { slug: "okta", displayName: "Okta", iconKey: "", formPost: false },
];

/** Who each simulated provider says signed in: GitHub links an existing account, Okta creates one. */
const FAKE_OAUTH_IDENTITIES: Readonly<Record<string, OAuthIdentity>> = {
  github: { email: "mark@open-splunk.test", firstName: "Mark", lastName: "Mendez", username: "mark" },
  okta: { email: "olivia@open-splunk.test", firstName: "Olivia", lastName: "Okafor", username: "olivia" },
};

function seedUser(
  id: string,
  username: string,
  firstName: string,
  lastName: string,
  options: {
    siteAdmin?: boolean;
    banned?: boolean;
    password?: string | null;
    otpCode?: string;
    activeSessions?: number;
    invitationPending?: boolean;
    oauthProviders?: string[];
  } = {},
): UserRecord {
  const password = options.password === undefined ? FAKE_IDENTITY_PASSWORD : options.password;
  return {
    user: {
      id,
      username,
      email: `${username}@open-splunk.test`,
      firstName,
      lastName,
      userType: options.siteAdmin === true ? "admin" : "regular",
      banned: options.banned === true,
      isBot: false,
    },
    password,
    otpCode: options.otpCode ?? null,
    createdAt: SEED_CREATED_AT,
    updatedAt: SEED_UPDATED_AT,
    ...(password === null ? {} : { lastLogin: SEED_LAST_LOGIN }),
    totalLogins: password === null ? 0 : 12,
    activeSessions: options.activeSessions ?? (password === null ? 0 : 1),
    emailVerified: password !== null,
    invitationPending: options.invitationPending === true,
    oauthProviders: options.oauthProviders ?? [],
  };
}

function seedOrganization(
  id: string,
  name: string,
  slug: string,
  description: string,
  parent?: IdentityOrganization,
): IdentityOrganization {
  return {
    id,
    name,
    slug,
    description,
    createdAt: SEED_CREATED_AT,
    updatedAt: SEED_UPDATED_AT,
    ...(parent === undefined ? {} : { parentOrganizationId: parent.id }),
    lineage: parent === undefined ? [] : [...parent.lineage, parent.id],
  };
}

function membership(organizationId: string, userId: string, role: OrgRole): MembershipRecord {
  return { organizationId, userId, role, createdAt: SEED_CREATED_AT, updatedAt: SEED_UPDATED_AT };
}

// --------------------------------------------------------------------------
// Options
// --------------------------------------------------------------------------

/** The subset of `Storage` the fake persists its session through. */
export type FakeIdentityStorage = Pick<Storage, "getItem" | "setItem" | "removeItem">;

export interface FakeIdentityOptions {
  /** Milliseconds since the epoch. Defaults to `Date.now`. */
  readonly now?: () => number;
  /** Where the session survives a reload; absent or null keeps it in memory. */
  readonly storage?: FakeIdentityStorage | null;
  readonly storageKey?: string;
  /** Who is signed in before any stored session applies; null (the default) starts signed out. */
  readonly initialSignedInEmail?: string | null;
  /** The same-origin page a simulated provider redirects back to. */
  readonly oauthCallbackPath?: string;
  /** Delay before every call settles, to exercise pending states. */
  readonly latencyMs?: number;
}

export const FAKE_IDENTITY_STORAGE_KEY = "open-splunk.fake-identity-session";
export const FAKE_OAUTH_CALLBACK_PATH = "/auth/oauth/callback/";

/** A message the fake would have emailed. */
export interface FakeOutboxMessage {
  readonly kind: "password_reset" | "account_invitation" | "organization_invitation";
  readonly email: string;
  /** The reset code, account-invitation token, or organization invitation ID the link carries. */
  readonly token: string;
}

interface StoredSession {
  readonly v: 1;
  readonly userId: UserId | null;
  readonly activeOrganizationId: OrganizationId | null;
}

interface Session {
  userId: UserId;
  activeOrganizationId: OrganizationId | null;
}

type MethodName = keyof IdentityClient;

const EMAIL_PATTERN = /^[^\s@]+@[^\s@]+\.[^\s@]+$/u;
const USERNAME_PATTERN = /^[A-Za-z0-9._-]{3,32}$/u;
const SLUG_PATTERN = /^[a-z0-9]+(?:-[a-z0-9]+)*$/u;
const OTP_CODE_PATTERN = /^\d{6}$/u;

function fail(kind: IdentityErrorKind, message: string, retryAfterMs?: number): never {
  throw new IdentityError(kind, message, retryAfterMs === undefined ? {} : { retryAfterMs });
}

function normalizeEmail(email: string): string {
  return email.trim().toLowerCase();
}

function requireEmail(email: unknown): string {
  if (typeof email !== "string") fail("invalid", "Enter an email address.");
  const normalized = normalizeEmail(email);
  if (!EMAIL_PATTERN.test(normalized)) fail("invalid", "Enter a valid email address.");
  return normalized;
}

function requireNewPassword(password: unknown): string {
  if (typeof password !== "string" || password.length < FAKE_MINIMUM_PASSWORD_LENGTH) {
    fail("invalid", `Passwords need at least ${FAKE_MINIMUM_PASSWORD_LENGTH} characters.`);
  }
  return password;
}

/** A same-origin absolute path: one leading slash, no scheme, host or backslash. */
export function isSafeReturnPath(value: string): boolean {
  return value.startsWith("/")
    && !value.startsWith("//")
    && !value.includes("\\")
    // Control characters would let a browser re-read the path as another origin.
    && ![...value].some((character) => {
      const code = character.codePointAt(0) ?? 0;
      return code < 0x20 || code === 0x7f;
    });
}

function comparableId(left: string, right: string): number {
  return left.length === right.length ? (left < right ? -1 : left > right ? 1 : 0) : left.length - right.length;
}

function displayName(user: IdentityUser): string {
  const name = `${user.firstName} ${user.lastName}`.trim();
  return name.length > 0 ? name : user.username;
}

function paginate<T>(items: readonly T[], request: PageRequest | undefined): Page<T> {
  const { page, limit } = request ?? DEFAULT_PAGE_REQUEST;
  if (!Number.isSafeInteger(page) || page < 1) fail("invalid", "Page numbers start at 1.");
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > FAKE_MAXIMUM_PAGE_LIMIT) {
    fail("invalid", `Page size must be between 1 and ${FAKE_MAXIMUM_PAGE_LIMIT}.`);
  }
  const start = (page - 1) * limit;
  const totalPages = Math.max(1, Math.ceil(items.length / limit));
  return {
    items: items.slice(start, start + limit),
    page: {
      currentPage: page,
      itemsPerPage: limit,
      hasNextPage: start + limit < items.length,
      hasPrevPage: page > 1,
      totalItems: items.length,
      totalPages,
    },
  };
}

function wait(milliseconds: number): Promise<void> {
  return new Promise((resolve) => {
    globalThis.setTimeout(resolve, milliseconds);
  });
}

// --------------------------------------------------------------------------
// The fake
// --------------------------------------------------------------------------

export class FakeIdentityClient implements IdentityClient {
  private readonly now: () => number;
  private readonly storage: FakeIdentityStorage | null;
  private readonly storageKey: string;
  private readonly oauthCallbackPath: string;
  private readonly latencyMs: number;

  private readonly users = new Map<UserId, UserRecord>();
  private readonly organizations = new Map<OrganizationId, IdentityOrganization>();
  private memberships: MembershipRecord[] = [];
  private readonly invitations = new Map<string, InvitationRecord>();
  private readonly accountInvitations = new Map<string, { userId: UserId; used: boolean }>();
  private readonly passwordResets = new Map<string, { userId: UserId; expiresAt: number; used: boolean }>();
  private readonly passwordResetRequests = new Map<string, number>();
  private readonly loginFailures = new Map<string, number[]>();
  private readonly challenges = new Map<string, { userId: UserId; createdAt: number; rejected: number }>();
  private readonly oauthStates = new Map<string, { slug: string; returnTo: string; createdAt: number }>();
  private oauthCallbacks: number[] = [];
  private readonly outbox: FakeOutboxMessage[] = [];
  private readonly faults = new Map<MethodName, IdentityError[]>();
  private session: Session | null = null;
  private sequence = 0;
  private nextUserId = 100;
  private nextOrganizationId = 100;
  private nextInvitationId = 10;

  public constructor(options: FakeIdentityOptions = {}) {
    this.now = options.now ?? Date.now;
    this.storage = options.storage ?? null;
    this.storageKey = options.storageKey ?? FAKE_IDENTITY_STORAGE_KEY;
    this.oauthCallbackPath = options.oauthCallbackPath ?? FAKE_OAUTH_CALLBACK_PATH;
    this.latencyMs = options.latencyMs ?? 0;
    this.seed();

    const initialEmail = options.initialSignedInEmail ?? null;
    const initialUser = initialEmail === null ? undefined : this.findUserByEmail(initialEmail);
    if (initialUser !== undefined && !initialUser.user.banned) {
      this.session = { userId: initialUser.user.id, activeOrganizationId: null };
    }
    this.restoreSession();
  }

  // ---- test and demo controls -------------------------------------------

  /** Makes the next call of `method` reject with `error`; queued faults apply in order. */
  public failNext(method: MethodName, error: IdentityError = new IdentityError("network", "The identity service could not be reached.")): void {
    const queue = this.faults.get(method) ?? [];
    queue.push(error);
    this.faults.set(method, queue);
  }

  /** Everything the fake would have emailed, oldest first. */
  public sentMessages(): readonly FakeOutboxMessage[] {
    return this.outbox.map((message) => ({ ...message }));
  }

  /** The most recent message of a kind sent to an address. */
  public lastMessage(kind: FakeOutboxMessage["kind"], email: string): FakeOutboxMessage | undefined {
    const address = normalizeEmail(email);
    return this.outbox.toReversed().find((message) => message.kind === kind && message.email === address);
  }

  // ---- session ------------------------------------------------------------

  public getPrincipal(): Promise<Principal | null> {
    return this.call("getPrincipal", () => this.currentPrincipal());
  }

  public loginWithEmail(credentials: EmailCredentials): Promise<LoginResult> {
    return this.call("loginWithEmail", () => {
      const email = requireEmail(credentials.email);
      if (typeof credentials.password !== "string" || credentials.password.length === 0) {
        fail("invalid", "Enter your password.");
      }
      this.assertLoginNotLimited(email);
      const record = this.findUserByEmail(email);
      if (record === undefined || record.password === null || record.password !== credentials.password || record.user.isBot) {
        this.recordLoginFailure(email);
        fail("unauthenticated", "Invalid credentials");
      }
      if (record.user.banned) fail("unauthenticated", "User account is banned");
      this.loginFailures.delete(email);
      if (record.otpCode !== null) {
        const id = `otp-challenge-${this.next()}`;
        this.challenges.set(id, { userId: record.user.id, createdAt: this.now(), rejected: 0 });
        return {
          status: "otp_required",
          challenge: { id, email, message: "Please provide your 6-digit authenticator code" },
        } satisfies LoginResult;
      }
      return { status: "signed_in", principal: this.establishSession(record, "password") } satisfies LoginResult;
    });
  }

  public verifyLoginOtp(challenge: OtpChallenge, code: string): Promise<Principal> {
    return this.call("verifyLoginOtp", () => {
      const pending = this.challenges.get(challenge.id);
      if (pending === undefined || this.now() - pending.createdAt > FAKE_OTP_CHALLENGE_TTL_MS) {
        this.challenges.delete(challenge.id);
        fail("invalid", "This sign-in attempt has expired. Sign in again.");
      }
      const trimmed = code.trim();
      if (!OTP_CODE_PATTERN.test(trimmed)) fail("invalid", "Enter the 6-digit code from your authenticator app.");
      const record = this.users.get(pending.userId);
      if (record === undefined || record.user.banned) {
        this.challenges.delete(challenge.id);
        fail("unauthenticated", "Invalid credentials");
      }
      if (record.otpCode !== trimmed) {
        pending.rejected += 1;
        if (pending.rejected >= FAKE_OTP_ATTEMPT_LIMIT) {
          this.challenges.delete(challenge.id);
          fail("rate_limited", "Too many requests. Please try again later.", FAKE_LOGIN_WINDOW_MS);
        }
        fail("otp_required", "That code was not accepted. Enter the current code from your authenticator app.");
      }
      this.challenges.delete(challenge.id);
      return this.establishSession(record, "password");
    });
  }

  public logout(options: { readonly everywhere?: boolean } = {}): Promise<void> {
    return this.call("logout", () => {
      if (this.session === null) return;
      const record = this.users.get(this.session.userId);
      if (record !== undefined) {
        record.activeSessions = options.everywhere === true ? 0 : Math.max(0, record.activeSessions - 1);
      }
      this.endSession();
    });
  }

  // ---- OAuth --------------------------------------------------------------

  public listOAuthProviders(): Promise<readonly OAuthProvider[]> {
    return this.call("listOAuthProviders", () => FAKE_OAUTH_PROVIDERS);
  }

  public beginOAuth(providerSlug: string, returnTo: string): Promise<BeginOAuthResult> {
    return this.call("beginOAuth", () => {
      if (!FAKE_OAUTH_PROVIDERS.some((provider) => provider.slug === providerSlug)) {
        fail("invalid", "That sign-in provider is not available.");
      }
      if (!isSafeReturnPath(returnTo)) fail("invalid", "The return destination must be a path on this site.");
      const state = `fake-oauth-state-${this.next()}`;
      this.oauthStates.set(state, { slug: providerSlug, returnTo, createdAt: this.now() });
      const query = new URLSearchParams({ code: this.oauthCode(providerSlug), state });
      return { authorizationUrl: `${this.oauthCallbackPath}?${query.toString()}` };
    });
  }

  public completeOAuthCallback(params: OAuthCallbackParams): Promise<OAuthCallbackResult> {
    return this.call("completeOAuthCallback", () => {
      const now = this.now();
      this.oauthCallbacks = this.oauthCallbacks.filter((at) => now - at < FAKE_OAUTH_CALLBACK_WINDOW_MS);
      if (this.oauthCallbacks.length >= FAKE_OAUTH_CALLBACK_LIMIT) {
        const oldest = this.oauthCallbacks[0] ?? now;
        fail("rate_limited", "Too many requests. Please try again later.", oldest + FAKE_OAUTH_CALLBACK_WINDOW_MS - now);
      }
      this.oauthCallbacks.push(now);

      const stateKey = params.state ?? "";
      const state = this.oauthStates.get(stateKey);
      // A state is single use whatever the outcome, as go-common clears its binding cookie.
      this.oauthStates.delete(stateKey);
      if (typeof params.error === "string" && params.error.length > 0) {
        fail("unauthenticated", params.errorDescription ?? "The provider did not complete the sign-in.");
      }
      if (typeof params.code !== "string" || params.code.length === 0 || stateKey.length === 0) {
        fail("invalid", "The sign-in response is incomplete. Start again.");
      }
      if (state === undefined || now - state.createdAt > FAKE_OAUTH_STATE_TTL_MS) {
        fail("invalid", "This sign-in link has expired or was already used. Start again.");
      }
      if (params.code === FAKE_OAUTH_CONFLICT_CODE) {
        fail("conflict", "This provider account is linked to a different user.");
      }
      if (params.code !== this.oauthCode(state.slug)) fail("unauthenticated", "OAuth authentication failed");

      const identity = FAKE_OAUTH_IDENTITIES[state.slug];
      if (identity === undefined) fail("invalid", "That sign-in provider is not available.");
      let record = this.findUserByEmail(identity.email);
      let accountCreated = false;
      if (record === undefined) {
        record = this.insertUser({
          email: identity.email,
          username: identity.username,
          firstName: identity.firstName,
          lastName: identity.lastName,
          siteAdmin: false,
          password: null,
        });
        record.emailVerified = true;
        accountCreated = true;
      }
      if (record.user.banned) fail("unauthenticated", "User account is banned");
      return {
        principal: this.establishSession(record, state.slug),
        accountCreated,
        returnTo: state.returnTo,
        providerSlug: state.slug,
      };
    });
  }

  // ---- password reset and invitations ------------------------------------

  public requestPasswordReset(email: string): Promise<void> {
    return this.call("requestPasswordReset", () => {
      const address = requireEmail(email);
      const now = this.now();
      const previous = this.passwordResetRequests.get(address);
      if (previous !== undefined && now - previous < FAKE_PASSWORD_RESET_COOLDOWN_MS) {
        fail("rate_limited", "Please wait before requesting another email", previous + FAKE_PASSWORD_RESET_COOLDOWN_MS - now);
      }
      this.passwordResetRequests.set(address, now);
      const record = this.findUserByEmail(address);
      if (record === undefined || record.user.isBot) return;
      const code = `fake-reset-${this.next()}`;
      this.passwordResets.set(code, { userId: record.user.id, expiresAt: now + FAKE_PASSWORD_RESET_TTL_MS, used: false });
      this.outbox.push({ kind: "password_reset", email: address, token: code });
    });
  }

  public completePasswordReset(completion: PasswordResetCompletion): Promise<void> {
    return this.call("completePasswordReset", () => {
      const reset = this.passwordResets.get(completion.verificationCode);
      if (reset === undefined) fail("invalid", "Invalid token");
      if (reset.used) fail("invalid", "Token has already been used");
      if (this.now() > reset.expiresAt) fail("invalid", "Token has expired");
      const password = requireNewPassword(completion.newPassword);
      const record = this.users.get(reset.userId);
      if (record === undefined) fail("invalid", "Invalid token");
      reset.used = true;
      record.password = password;
      record.updatedAt = this.isoNow();
      // Following the emailed link proves the address.
      record.emailVerified = true;
    });
  }

  public acceptInvitation(acceptance: InvitationAcceptance): Promise<InvitationAcceptanceResult> {
    return this.call("acceptInvitation", () => {
      if (acceptance.kind === "account") {
        const invitation = this.accountInvitations.get(acceptance.token);
        const record = invitation === undefined ? undefined : this.users.get(invitation.userId);
        if (invitation === undefined || invitation.used || record === undefined || record.user.banned || record.password !== null) {
          fail("invalid", "This invitation link is invalid or was already used.");
        }
        record.password = requireNewPassword(acceptance.password);
        record.emailVerified = true;
        record.invitationPending = false;
        record.updatedAt = this.isoNow();
        invitation.used = true;
        return { kind: "account" } satisfies InvitationAcceptanceResult;
      }

      const { record } = this.requireSession();
      const invitation = this.invitations.get(acceptance.invitationId);
      if (invitation === undefined || invitation.acceptedAt !== undefined) fail("not_found", "Invitation not found");
      if (invitation.email !== record.user.email) fail("forbidden", "This invitation was sent to a different email address.");
      if (this.now() > Date.parse(invitation.expiresAt)) fail("invalid", "This invitation has expired.");
      if (this.explicitMembership(invitation.organizationId, record.user.id) !== undefined) {
        fail("conflict", "You are already a member of this organization.");
      }
      const createdAt = this.isoNow();
      const added = { organizationId: invitation.organizationId, userId: record.user.id, role: invitation.role, createdAt, updatedAt: createdAt };
      this.memberships.push(added);
      invitation.acceptedAt = createdAt;
      return { kind: "organization", membership: this.membershipView(added.organizationId, added.userId) } satisfies InvitationAcceptanceResult;
    });
  }

  // ---- active organization -----------------------------------------------

  public setActiveOrganization(organizationId: OrganizationId): Promise<Principal> {
    return this.call("setActiveOrganization", () => {
      const { session, record } = this.requireSession();
      this.requireOrganization(organizationId);
      if (record.user.userType !== "admin" && this.effectiveRole(organizationId, record.user.id) === null) {
        fail("forbidden", "You are not a member of this organization.");
      }
      session.activeOrganizationId = organizationId;
      this.persistSession();
      return this.requirePrincipal();
    });
  }

  // ---- members -------------------------------------------------------------

  public listMembers(organizationId: OrganizationId, page?: PageRequest): Promise<Page<OrganizationMember>> {
    return this.call("listMembers", () => {
      this.requireOrganizationCapability(organizationId, "members.admin");
      const members = [...this.users.values()]
        .filter((record) => this.effectiveRole(organizationId, record.user.id) !== null)
        .map((record) => this.memberView(organizationId, record))
        .toSorted((left, right) => left.user.displayPrimary.localeCompare(right.user.displayPrimary) || comparableId(left.userId, right.userId));
      return paginate(members, page);
    });
  }

  public changeMemberRole(organizationId: OrganizationId, userId: UserId, role: OrgRole): Promise<OrganizationMembership> {
    return this.call("changeMemberRole", () => {
      if (!isOrgRole(role)) fail("invalid", "Choose a member, moderator, admin or owner role.");
      const actorCapabilities = this.requireOrganizationCapability(organizationId, "members.admin");
      const current = this.requireMember(organizationId, userId);
      if ((role === "owner" || current.effectiveRole === "owner") && !actorCapabilities.includes("org.admin")) {
        fail("forbidden", "Only an owner can grant or change the owner role.");
      }
      const explicit = this.explicitMembership(organizationId, userId);
      if (explicit?.role === "owner" && role !== "owner") this.assertNotLastOwner(organizationId);
      if (explicit === undefined) {
        const createdAt = this.isoNow();
        this.memberships.push({ organizationId, userId, role, createdAt, updatedAt: createdAt });
      } else {
        explicit.role = role;
        explicit.updatedAt = this.isoNow();
      }
      return this.membershipView(organizationId, userId);
    });
  }

  public removeMember(organizationId: OrganizationId, userId: UserId): Promise<void> {
    return this.call("removeMember", () => {
      const actorCapabilities = this.requireOrganizationCapability(organizationId, "members.admin");
      const current = this.requireMember(organizationId, userId);
      const explicit = this.explicitMembership(organizationId, userId);
      if (explicit === undefined) {
        fail("conflict", `This role is inherited from ${this.organizationName(current.inheritedFromOrganizationId)}; remove it there.`);
      }
      if (explicit.role === "owner") {
        if (!actorCapabilities.includes("org.admin")) fail("forbidden", "Only an owner can remove an owner.");
        this.assertNotLastOwner(organizationId);
      }
      this.memberships = this.memberships.filter((entry) => entry !== explicit);
    });
  }

  public inviteMember(organizationId: OrganizationId, invitation: MemberInvitationInput): Promise<OrganizationInvitation> {
    return this.call("inviteMember", () => {
      const actorCapabilities = this.requireOrganizationCapability(organizationId, "members.admin");
      const email = requireEmail(invitation.email);
      if (!isOrgRole(invitation.role)) fail("invalid", "Choose a member, moderator, admin or owner role.");
      if (invitation.role === "owner" && !actorCapabilities.includes("org.admin")) {
        fail("forbidden", "Only an owner can invite an owner.");
      }
      const existing = this.findUserByEmail(email);
      if (existing !== undefined && this.effectiveRole(organizationId, existing.user.id) !== null) {
        fail("conflict", "That person is already a member of this organization.");
      }
      if (this.pendingInvitations(organizationId).some((pending) => pending.email === email)) {
        fail("conflict", "That address already has a pending invitation.");
      }
      const { record: actor } = this.requireSession();
      const createdAt = this.now();
      const id = String(this.nextInvitationId++);
      const created: InvitationRecord = {
        id,
        organizationId,
        email,
        role: invitation.role,
        invitedByUserId: actor.user.id,
        createdAt: new Date(createdAt).toISOString(),
        expiresAt: new Date(createdAt + FAKE_INVITATION_TTL_MS).toISOString(),
      };
      this.invitations.set(id, created);
      this.outbox.push({ kind: "organization_invitation", email, token: id });
      return this.invitationView(created);
    });
  }

  public listInvitations(organizationId: OrganizationId, page?: PageRequest): Promise<Page<OrganizationInvitation>> {
    return this.call("listInvitations", () => {
      this.requireOrganizationCapability(organizationId, "members.admin");
      return paginate(this.pendingInvitations(organizationId).map((entry) => this.invitationView(entry)), page);
    });
  }

  public revokeInvitation(organizationId: OrganizationId, invitationId: string): Promise<void> {
    return this.call("revokeInvitation", () => {
      this.requireOrganizationCapability(organizationId, "members.admin");
      const invitation = this.invitations.get(invitationId);
      if (invitation === undefined || invitation.organizationId !== organizationId || invitation.acceptedAt !== undefined) {
        fail("not_found", "Invitation not found");
      }
      this.invitations.delete(invitationId);
    });
  }

  // ---- site administration ------------------------------------------------

  public adminListUsers(query: AdminUserQuery = {}, page?: PageRequest): Promise<Page<AdminUser>> {
    return this.call("adminListUsers", () => {
      this.requireSiteCapability("users.admin");
      const search = query.search?.trim().toLowerCase() ?? "";
      const users = [...this.users.values()]
        .filter((record) => {
          const { user } = record;
          if (query.banned !== undefined && user.banned !== query.banned) return false;
          if (query.siteAdmin !== undefined && (user.userType === "admin") !== query.siteAdmin) return false;
          if (search.length === 0) return true;
          if (/^\d+$/u.test(search)) return user.id === search;
          return [user.email, user.username, `${user.firstName} ${user.lastName}`]
            .some((value) => value.toLowerCase().includes(search));
        })
        .toSorted((left, right) => comparableId(left.user.id, right.user.id))
        .map((record) => this.adminUserView(record));
      return paginate(users, page);
    });
  }

  public adminCreateUser(input: AdminCreateUserInput): Promise<AdminUser> {
    return this.call("adminCreateUser", () => {
      this.requireSiteCapability("users.admin");
      const email = requireEmail(input.email);
      const username = input.username.trim();
      if (!USERNAME_PATTERN.test(username)) {
        fail("invalid", "Usernames are 3–32 letters, digits, dots, dashes or underscores.");
      }
      if (!["oauth_only", "invitation", "password"].includes(input.accessMode)) fail("invalid", "Choose how the account signs in.");
      if (input.accessMode === "password") requireNewPassword(input.password);
      else if (input.password !== undefined) fail("invalid", "A password is set only with the password access mode.");
      if (this.findUserByEmail(email) !== undefined) fail("conflict", "Email already exists");
      if ([...this.users.values()].some((record) => record.user.username.toLowerCase() === username.toLowerCase())) {
        fail("conflict", "User already exists");
      }
      const record = this.insertUser({
        email,
        username,
        firstName: input.firstName.trim(),
        lastName: input.lastName.trim(),
        siteAdmin: input.siteAdmin,
        password: input.accessMode === "password" ? input.password ?? null : null,
      });
      record.emailVerified = input.accessMode === "password";
      if (input.accessMode === "invitation") {
        const token = `fake-account-invitation-${this.next()}`;
        this.accountInvitations.set(token, { userId: record.user.id, used: false });
        record.invitationPending = true;
        this.outbox.push({ kind: "account_invitation", email, token });
      }
      return this.adminUserView(record);
    });
  }

  public adminSetSiteAdmin(userId: UserId, siteAdmin: boolean): Promise<AdminUser> {
    return this.call("adminSetSiteAdmin", () => {
      const target = this.requireOtherUser(userId, "change your own site-admin access");
      target.user.userType = siteAdmin ? "admin" : "regular";
      target.updatedAt = this.isoNow();
      return this.adminUserView(target);
    });
  }

  public adminBanUser(userId: UserId): Promise<AdminUser> {
    return this.call("adminBanUser", () => {
      const target = this.requireOtherUser(userId, "ban yourself");
      target.user.banned = true;
      target.activeSessions = 0;
      target.updatedAt = this.isoNow();
      return this.adminUserView(target);
    });
  }

  public adminUnbanUser(userId: UserId): Promise<AdminUser> {
    return this.call("adminUnbanUser", () => {
      const target = this.requireOtherUser(userId, "unban yourself");
      target.user.banned = false;
      target.updatedAt = this.isoNow();
      return this.adminUserView(target);
    });
  }

  public adminRevokeUserSessions(userId: UserId): Promise<number> {
    return this.call("adminRevokeUserSessions", () => {
      this.requireSiteCapability("users.admin");
      const target = this.users.get(userId);
      if (target === undefined) fail("not_found", "User not found");
      const revoked = target.activeSessions;
      target.activeSessions = 0;
      if (this.session?.userId === userId) this.endSession();
      return revoked;
    });
  }

  public adminDeleteUser(userId: UserId): Promise<void> {
    return this.call("adminDeleteUser", () => {
      this.requireOtherUser(userId, "delete your own account");
      this.users.delete(userId);
      this.memberships = this.memberships.filter((entry) => entry.userId !== userId);
    });
  }

  public adminListOrganizations(query: AdminOrganizationQuery = {}, page?: PageRequest): Promise<Page<IdentityOrganization>> {
    return this.call("adminListOrganizations", () => {
      this.requireSiteCapability("orgs.admin");
      const search = query.search?.trim().toLowerCase() ?? "";
      const organizations = [...this.organizations.values()]
        .filter((organization) => search.length === 0
          || organization.name.toLowerCase().includes(search)
          || organization.slug.includes(search))
        .toSorted((left, right) => left.name.localeCompare(right.name) || comparableId(left.id, right.id));
      return paginate(organizations, page);
    });
  }

  public adminCreateOrganization(input: AdminCreateOrganizationInput): Promise<IdentityOrganization> {
    return this.call("adminCreateOrganization", () => {
      this.requireSiteCapability("orgs.admin");
      const name = input.name.trim();
      if (name.length === 0 || name.length > 100) fail("invalid", "Organization names are 1–100 characters.");
      if (!SLUG_PATTERN.test(input.slug)) fail("invalid", "Slugs are lowercase letters and digits separated by single dashes.");
      if ([...this.organizations.values()].some((organization) => organization.slug === input.slug)) {
        fail("conflict", "That slug is already taken.");
      }
      const parent = input.parentOrganizationId === undefined ? undefined : this.requireOrganization(input.parentOrganizationId);
      const at = this.isoNow();
      const description = input.description?.trim();
      const organization: IdentityOrganization = {
        id: String(this.nextOrganizationId++),
        name,
        slug: input.slug,
        ...(description === undefined || description.length === 0 ? {} : { description }),
        createdAt: at,
        updatedAt: at,
        ...(parent === undefined ? {} : { parentOrganizationId: parent.id }),
        lineage: parent === undefined ? [] : [...parent.lineage, parent.id],
      };
      this.organizations.set(organization.id, organization);
      return organization;
    });
  }

  // ---- internals ------------------------------------------------------------

  private async call<T>(method: MethodName, operation: () => T): Promise<T> {
    if (this.latencyMs > 0) await wait(this.latencyMs);
    const fault = this.faults.get(method)?.shift();
    if (fault !== undefined) throw fault;
    // Callers get a copy: nothing they hold aliases the directory.
    return structuredClone(operation());
  }

  private seed(): void {
    const users = [
      seedUser("1", "ada", "Ada", "Lovelace", { siteAdmin: true, activeSessions: 2 }),
      seedUser("2", "owen", "Owen", "Ortiz"),
      seedUser("3", "alice", "Alice", "Abara"),
      seedUser("4", "mona", "Mona", "Moreau"),
      seedUser("5", "mark", "Mark", "Mendez", { oauthProviders: ["github"] }),
      seedUser("6", "tess", "Tess", "Takahashi", { otpCode: FAKE_IDENTITY_OTP_CODE }),
      seedUser("7", "nora", "Nora", "Novak"),
      seedUser("8", "bart", "Bart", "Becker", { banned: true, activeSessions: 0 }),
      seedUser("9", "iris", "Iris", "Ivanova", { password: null, invitationPending: true }),
    ];
    for (const record of users) this.users.set(record.user.id, record);
    this.accountInvitations.set(FAKE_ACCOUNT_INVITATION_TOKEN, { userId: "9", used: false });

    const acme = seedOrganization("1", "Acme Operations", "acme", "Production operations and on-call");
    const globex = seedOrganization("2", "Globex Security", "globex", "Security monitoring");
    const platform = seedOrganization("3", "Acme Platform", "acme-platform", "Platform engineering inside Acme", acme);
    for (const organization of [acme, globex, platform]) this.organizations.set(organization.id, organization);

    this.memberships = [
      membership("1", "1", "owner"),
      membership("2", "2", "owner"),
      membership("1", "2", "member"),
      membership("1", "3", "admin"),
      membership("1", "4", "moderator"),
      membership("3", "4", "admin"),
      membership("1", "5", "member"),
      membership("2", "5", "member"),
      membership("1", "6", "member"),
      membership("1", "8", "member"),
    ];

    this.invitations.set(FAKE_PENDING_INVITATION_ID, {
      id: FAKE_PENDING_INVITATION_ID,
      organizationId: "1",
      email: "nora@open-splunk.test",
      role: "member",
      invitedByUserId: "3",
      createdAt: SEED_UPDATED_AT,
      expiresAt: "2099-01-01T00:00:00.000Z",
    });
    this.invitations.set(FAKE_EXPIRED_INVITATION_ID, {
      id: FAKE_EXPIRED_INVITATION_ID,
      organizationId: "1",
      email: "late@open-splunk.test",
      role: "moderator",
      invitedByUserId: "1",
      createdAt: SEED_CREATED_AT,
      expiresAt: "2026-01-12T09:00:00.000Z",
    });
  }

  private next(): number {
    this.sequence += 1;
    return this.sequence;
  }

  private isoNow(): string {
    return new Date(this.now()).toISOString();
  }

  private oauthCode(slug: string): string {
    return `fake-oauth-code-${slug}`;
  }

  private insertUser(input: {
    email: string;
    username: string;
    firstName: string;
    lastName: string;
    siteAdmin: boolean;
    password: string | null;
  }): UserRecord {
    const at = this.isoNow();
    const record: UserRecord = {
      user: {
        id: String(this.nextUserId++),
        username: input.username,
        email: input.email,
        firstName: input.firstName,
        lastName: input.lastName,
        userType: input.siteAdmin ? "admin" : "regular",
        banned: false,
        isBot: false,
      },
      password: input.password,
      otpCode: null,
      createdAt: at,
      updatedAt: at,
      totalLogins: 0,
      activeSessions: 0,
      emailVerified: false,
      invitationPending: false,
      oauthProviders: [],
    };
    this.users.set(record.user.id, record);
    return record;
  }

  private findUserByEmail(email: string): UserRecord | undefined {
    const address = normalizeEmail(email);
    return [...this.users.values()].find((record) => record.user.email === address);
  }

  private assertLoginNotLimited(email: string): void {
    const now = this.now();
    const recent = (this.loginFailures.get(email) ?? []).filter((at) => now - at < FAKE_LOGIN_WINDOW_MS);
    this.loginFailures.set(email, recent);
    if (recent.length >= FAKE_LOGIN_ATTEMPT_LIMIT) {
      const oldest = recent[0] ?? now;
      fail("rate_limited", "Too many requests. Please try again later.", oldest + FAKE_LOGIN_WINDOW_MS - now);
    }
  }

  private recordLoginFailure(email: string): void {
    this.loginFailures.set(email, [...(this.loginFailures.get(email) ?? []), this.now()]);
  }

  private establishSession(record: UserRecord, method: string): Principal {
    record.activeSessions += 1;
    record.totalLogins += 1;
    record.lastLogin = this.isoNow();
    if (method !== "password" && !record.oauthProviders.includes(method)) record.oauthProviders.push(method);
    const previous = this.session?.userId === record.user.id ? this.session.activeOrganizationId : null;
    this.session = { userId: record.user.id, activeOrganizationId: previous };
    this.persistSession();
    return this.requirePrincipal();
  }

  private endSession(): void {
    this.session = null;
    this.persistSession();
  }

  private restoreSession(): void {
    if (this.storage === null) return;
    let raw: string | null = null;
    try {
      raw = this.storage.getItem(this.storageKey);
    } catch {
      return;
    }
    if (raw === null) return;
    try {
      const parsed = JSON.parse(raw) as Partial<StoredSession>;
      if (parsed.v !== 1) return;
      if (parsed.userId === null) {
        this.session = null;
        return;
      }
      const record = typeof parsed.userId === "string" ? this.users.get(parsed.userId) : undefined;
      if (record === undefined || record.user.banned) return;
      this.session = {
        userId: record.user.id,
        activeOrganizationId: typeof parsed.activeOrganizationId === "string" ? parsed.activeOrganizationId : null,
      };
    } catch {
      // A damaged entry is ignored; the next session change overwrites it.
    }
  }

  private persistSession(): void {
    if (this.storage === null) return;
    const stored: StoredSession = {
      v: 1,
      userId: this.session?.userId ?? null,
      activeOrganizationId: this.session?.activeOrganizationId ?? null,
    };
    try {
      this.storage.setItem(this.storageKey, JSON.stringify(stored));
    } catch {
      // Blocked storage only costs persistence across reloads.
    }
  }

  private currentPrincipal(): Principal | null {
    if (this.session === null) return null;
    const record = this.users.get(this.session.userId);
    if (record === undefined || record.user.banned || record.activeSessions === 0) {
      this.endSession();
      return null;
    }
    const siteAdmin = record.user.userType === "admin";
    const organizations = [...this.organizations.values()]
      .flatMap((organization): OrganizationWithRole[] => {
        const role = this.effectiveRole(organization.id, record.user.id);
        return role === null ? [] : [{ organization, ...role }];
      })
      .toSorted((left, right) => left.organization.name.localeCompare(right.organization.name));
    const requested = this.session.activeOrganizationId;
    const requestedOrganization = requested === null ? undefined : this.organizations.get(requested);
    const activeOrganization = requestedOrganization !== undefined
      && (siteAdmin || organizations.some((entry) => entry.organization.id === requestedOrganization.id))
      ? requestedOrganization
      : organizations[0]?.organization ?? null;
    const role = activeOrganization === null
      ? null
      : organizations.find((entry) => entry.organization.id === activeOrganization.id)?.effectiveRole ?? null;
    return {
      user: record.user,
      siteAdmin,
      activeOrganization,
      role,
      capabilities: capabilitiesFor(role, siteAdmin),
      organizations,
    };
  }

  private requirePrincipal(): Principal {
    const principal = this.currentPrincipal();
    if (principal === null) fail("unauthenticated", "Sign in to continue.");
    return principal;
  }

  private requireSession(): { session: Session; record: UserRecord } {
    const principal = this.currentPrincipal();
    const session = this.session;
    const record = session === null ? undefined : this.users.get(session.userId);
    if (principal === null || session === null || record === undefined) fail("unauthenticated", "Sign in to continue.");
    return { session, record };
  }

  private capabilitiesIn(organizationId: OrganizationId, record: UserRecord): readonly Capability[] {
    return capabilitiesFor(this.effectiveRole(organizationId, record.user.id)?.effectiveRole ?? null, record.user.userType === "admin");
  }

  private requireOrganizationCapability(organizationId: OrganizationId, capability: Capability): readonly Capability[] {
    const { record } = this.requireSession();
    this.requireOrganization(organizationId);
    const capabilities = this.capabilitiesIn(organizationId, record);
    if (!capabilities.includes(capability)) fail("forbidden", "You do not have permission to manage this organization's members.");
    return capabilities;
  }

  private requireSiteCapability(capability: Capability): UserRecord {
    const { record } = this.requireSession();
    if (!capabilitiesFor(null, record.user.userType === "admin").includes(capability)) {
      fail("forbidden", "Only a site admin can do that.");
    }
    return record;
  }

  private requireOtherUser(userId: UserId, action: string): UserRecord {
    const actor = this.requireSiteCapability("users.admin");
    const target = this.users.get(userId);
    if (target === undefined) fail("not_found", "User not found");
    if (target.user.id === actor.user.id) fail("conflict", `You cannot ${action}.`);
    return target;
  }

  private requireOrganization(organizationId: OrganizationId): IdentityOrganization {
    const organization = this.organizations.get(organizationId);
    if (organization === undefined) fail("not_found", "Organization not found");
    return organization;
  }

  private organizationName(organizationId: OrganizationId | undefined): string {
    return (organizationId === undefined ? undefined : this.organizations.get(organizationId)?.name) ?? "a parent organization";
  }

  private explicitMembership(organizationId: OrganizationId, userId: UserId): MembershipRecord | undefined {
    return this.memberships.find((entry) => entry.organizationId === organizationId && entry.userId === userId);
  }

  /** The role a user holds in an organization, explicitly or inherited from an ancestor. */
  private effectiveRole(organizationId: OrganizationId, userId: UserId): MembershipRole | null {
    const organization = this.organizations.get(organizationId);
    if (organization === undefined || !this.users.has(userId)) return null;
    const chain = [...organization.lineage, organization.id];
    let best: { role: OrgRole; organizationId: OrganizationId; depth: number } | null = null;
    for (const [depth, candidateId] of chain.entries()) {
      const entry = this.explicitMembership(candidateId, userId);
      // `>=` lets a deeper (nearer) organization supply an equal role.
      if (entry !== undefined && (best === null || compareOrgRoles(entry.role, best.role) >= 0)) {
        best = { role: entry.role, organizationId: candidateId, depth };
      }
    }
    const supplier = best;
    if (supplier === null) return null;
    const explicit = this.explicitMembership(organizationId, userId);
    return {
      explicitRole: explicit?.role ?? null,
      effectiveRole: supplier.role,
      ...(supplier.organizationId === organizationId ? {} : { inheritedFromOrganizationId: supplier.organizationId }),
      effectiveRoleDepth: supplier.depth,
    };
  }

  private requireMember(organizationId: OrganizationId, userId: UserId): MembershipRole {
    if (!this.users.has(userId)) fail("not_found", "User not found");
    const role = this.effectiveRole(organizationId, userId);
    if (role === null) fail("not_found", "That user is not a member of this organization.");
    return role;
  }

  private assertNotLastOwner(organizationId: OrganizationId): void {
    const owners = this.memberships.filter((entry) => entry.organizationId === organizationId && entry.role === "owner");
    if (owners.length <= 1) fail("conflict", "An organization needs at least one owner.");
  }

  private membershipView(organizationId: OrganizationId, userId: UserId): OrganizationMembership {
    const role = this.requireMember(organizationId, userId);
    const explicit = this.explicitMembership(organizationId, userId);
    return {
      organizationId,
      userId,
      createdAt: explicit?.createdAt ?? SEED_CREATED_AT,
      updatedAt: explicit?.updatedAt ?? SEED_UPDATED_AT,
      ...role,
    };
  }

  private memberView(organizationId: OrganizationId, record: UserRecord): OrganizationMember {
    const { user } = record;
    return {
      ...this.membershipView(organizationId, user.id),
      user: {
        id: user.id,
        displayPrimary: displayName(user),
        displaySecondary: user.email,
        userType: user.userType,
        banned: user.banned,
        isBot: user.isBot,
      },
      isActive: !user.banned,
      ...(record.lastLogin === undefined ? {} : { lastActivity: record.lastLogin }),
    };
  }

  private pendingInvitations(organizationId: OrganizationId): InvitationRecord[] {
    return [...this.invitations.values()]
      .filter((entry) => entry.organizationId === organizationId && entry.acceptedAt === undefined)
      .toSorted((left, right) => comparableId(left.id, right.id));
  }

  private invitationView(entry: InvitationRecord): OrganizationInvitation {
    return {
      ...entry,
      organization: this.requireOrganization(entry.organizationId),
      requiresAcceptance: true,
    };
  }

  private adminUserView(record: UserRecord): AdminUser {
    return {
      user: record.user,
      createdAt: record.createdAt,
      updatedAt: record.updatedAt,
      ...(record.lastLogin === undefined ? {} : { lastLogin: record.lastLogin }),
      totalLogins: record.totalLogins,
      activeSessions: record.activeSessions,
      authMethods: [...(record.password === null ? [] : ["password"]), ...record.oauthProviders],
      emailVerified: record.emailVerified,
      invitationPending: record.invitationPending,
    };
  }
}

export function createFakeIdentityClient(options: FakeIdentityOptions = {}): FakeIdentityClient {
  return new FakeIdentityClient(options);
}
