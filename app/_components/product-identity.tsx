"use client";

/**
 * The product shell's identity surfaces: the provider boundary every
 * `ProductShell` page sits in, the organization switcher, the signed-in
 * user's summary and sign-out, capability-gated navigation, and the states a
 * page shows instead of its content (checking the session, signed out,
 * identity unavailable, forbidden).
 *
 * A page whose identity is not configured -- the backend build until the
 * server mounts go-common's routes -- reports `legacy`, and every surface here
 * then renders exactly what the shell rendered before identity existed, so the
 * administrator bearer flow is untouched.
 */

import {
  Fragment,
  useCallback,
  useEffect,
  useId,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
  type MouseEvent as ReactMouseEvent,
  type ReactNode,
} from "react";
import Link from "next/link";

import {
  ORG_ROLE_CAPABILITIES,
  isIdentityError,
  type IdentityClient,
  type IdentityError,
  type Principal,
} from "@/lib/api/identity";
import {
  CAPABILITY_LABELS,
  PRODUCT_SECTION_ACCESS,
  PRODUCT_SECTION_LABELS,
  ORG_ROLE_LABELS,
  SIGN_IN_PATH,
  activeRoleLabel,
  identityUnavailableMessage,
  lowestRoleMeeting,
  meetsRequirement,
  missingCapabilities,
  organizationChoices,
  organizationSwitchErrorMessage,
  returnPathFor,
  signInHref,
  signOutErrorMessage,
  userDisplayName,
  userInitials,
  type AccessRequirement,
  type OrganizationChoice,
  type ProductSection,
} from "@/lib/product-access";
import type { SearchDataMode } from "@/lib/search/backend-data";

import { AppIcon } from "./app-icon";
import {
  IdentityProvider,
  resolveIdentityClient,
  useIdentity,
  useOptionalIdentity,
  type IdentityContextValue,
} from "./identity-provider";

// --------------------------------------------------------------------------
// The shell's view of identity
// --------------------------------------------------------------------------

/**
 * - `legacy`: no identity on this page (no provider, or identity is not
 *   configured); the shell behaves as it did before identity existed.
 * - `loading`: the principal is still being read.
 * - `signed_in` / `signed_out`: the last read succeeded.
 * - `error`: identity is configured but the principal could not be read.
 */
export type ShellIdentityMode = "legacy" | "loading" | "signed_in" | "signed_out" | "error";

export function shellIdentityMode(identity: IdentityContextValue | null): ShellIdentityMode {
  if (identity === null) return "legacy";
  if (identity.status === "unavailable") return identity.error?.kind === "not_configured" ? "legacy" : "error";
  return identity.status;
}

export interface ShellIdentity {
  readonly mode: ShellIdentityMode;
  readonly identity: IdentityContextValue | null;
  readonly principal: Principal | null;
  readonly error: IdentityError | null;
  /**
   * Whether navigation to a surface with this requirement is offered. While
   * the principal is still being read the answer is yes -- the page itself
   * waits, and hiding links the common principal holds would make every
   * first paint flicker -- and a legacy page offers everything it did before.
   */
  offers(requirement: AccessRequirement | null): boolean;
}

export function useShellIdentity(): ShellIdentity {
  const identity = useOptionalIdentity();
  const mode = shellIdentityMode(identity);
  const hasCapability = identity?.hasCapability;
  const offers = useCallback((requirement: AccessRequirement | null) => {
    if (requirement === null || mode === "legacy" || mode === "loading") return true;
    return mode === "signed_in" && hasCapability !== undefined && meetsRequirement(requirement, hasCapability);
  }, [hasCapability, mode]);
  return {
    mode,
    identity,
    principal: identity?.principal ?? null,
    error: identity?.error ?? null,
    offers,
  };
}

/**
 * Renders its children only where the shell offers a surface with this
 * requirement, and `fallback` (nothing, by default) where it does not: a link
 * into a surface the principal cannot open is left out, or replaced by its
 * plain text, rather than leading to a forbidden page.
 */
export function ShellOffer({ requirement, fallback = null, children }: {
  readonly requirement: AccessRequirement | null;
  readonly fallback?: ReactNode;
  readonly children?: ReactNode;
}) {
  const { offers } = useShellIdentity();
  return <>{offers(requirement) ? children : fallback}</>;
}

// --------------------------------------------------------------------------
// The provider boundary
// --------------------------------------------------------------------------

/**
 * The last principal each client resolved, for this page realm. A client-side
 * navigation remounts the shell, and starting the new page from the principal
 * the previous one already held -- then re-reading it -- keeps the page from
 * flashing its loading state on every link. Only a signed-in principal is
 * kept: a signed-out answer is always re-read, since a sign-in elsewhere in
 * the tab may have replaced it.
 */
const lastPrincipals = new WeakMap<IdentityClient, Principal>();

interface ShellIdentityBinding {
  readonly client: IdentityClient | null;
  readonly cached: Principal | undefined;
}

function bindShellIdentity(dataMode: SearchDataMode): ShellIdentityBinding {
  try {
    const client = resolveIdentityClient(dataMode);
    return { client, cached: lastPrincipals.get(client) };
  } catch {
    // The provider reports the same refusal as `unavailable` when given the mode.
    return { client: null, cached: undefined };
  }
}

function ShellPrincipalMemory({ client, revalidate }: { readonly client: IdentityClient; readonly revalidate: boolean }) {
  const { principal, refresh } = useIdentity();
  useEffect(() => {
    if (principal === null) lastPrincipals.delete(client);
    else lastPrincipals.set(client, principal);
  }, [client, principal]);
  useEffect(() => {
    if (!revalidate) return;
    // The store records the outcome; a failure keeps the remembered principal beside the error.
    refresh().catch(() => undefined);
  }, [refresh, revalidate]);
  return null;
}

/**
 * Mounts the page's identity provider for its data mode, unless a provider is
 * already above (a page, or a test, that supplied its own).
 */
export function ShellIdentityBoundary({ dataMode, children }: { readonly dataMode: SearchDataMode; readonly children?: ReactNode }) {
  const outer = useOptionalIdentity();
  const [binding] = useState(() => outer === null ? bindShellIdentity(dataMode) : null);
  if (outer !== null || binding === null) return <>{children}</>;
  if (binding.client === null) return <IdentityProvider dataMode={dataMode}>{children}</IdentityProvider>;
  return (
    <IdentityProvider client={binding.client} initialPrincipal={binding.cached}>
      <ShellPrincipalMemory client={binding.client} revalidate={binding.cached !== undefined} />
      {children}
    </IdentityProvider>
  );
}

// --------------------------------------------------------------------------
// Who is signed in
// --------------------------------------------------------------------------

export interface ShellSession {
  readonly initial: string;
  readonly label: string;
  readonly detail: string;
  /** The effective role and organization, when signed in with one. */
  readonly context: string | null;
  readonly siteAdmin: boolean;
}

/** The legacy persona a page without identity shows. */
export function legacySession(dataMode: SearchDataMode): ShellSession {
  return dataMode === "backend"
    ? { initial: "L", label: "Local session", detail: "Single-user backend mode", context: null, siteAdmin: false }
    : { initial: "A", label: "Administrator", detail: "admin@localhost", context: null, siteAdmin: false };
}

export function shellSessionFor(shell: Pick<ShellIdentity, "mode" | "principal" | "error">, dataMode: SearchDataMode): ShellSession {
  switch (shell.mode) {
    case "legacy":
      return legacySession(dataMode);
    case "loading":
      return { initial: "…", label: "Account", detail: "Checking your session…", context: null, siteAdmin: false };
    case "signed_out":
      return { initial: "?", label: "Signed out", detail: "Sign in to continue", context: null, siteAdmin: false };
    case "error":
      return { initial: "!", label: "Session unavailable", detail: identityUnavailableMessage(shell.error).title, context: null, siteAdmin: false };
    case "signed_in": {
      const principal = shell.principal;
      if (principal === null) return legacySession(dataMode);
      const role = activeRoleLabel(principal);
      const organization = principal.activeOrganization?.name;
      return {
        initial: userInitials(principal.user),
        label: userDisplayName(principal.user),
        detail: principal.user.email,
        context: role !== null && organization !== undefined ? `${role} · ${organization}` : null,
        siteAdmin: principal.siteAdmin,
      };
    }
  }
}

export function useShellSession(dataMode: SearchDataMode): ShellSession {
  const shell = useShellIdentity();
  return shellSessionFor(shell, dataMode);
}

/**
 * One piece of the session's text, for a page that renders its own user menu
 * outside the shell's hooks (the search workspace builds its utilities before
 * the shell's provider exists): `legacy` is what that page showed before.
 */
export function ShellSessionText({ part, legacy }: { readonly part: "initial" | "label" | "detail"; readonly legacy: string }) {
  const shell = useShellIdentity();
  if (shell.mode === "legacy") return <>{legacy}</>;
  return <>{shellSessionFor(shell, "demo")[part]}</>;
}

/**
 * Ends the session and leaves for the sign-in page. The client's logout is
 * called directly rather than through the provider, so the page does not
 * first fall into its own signed-out redirect (which would carry a return
 * path the user just chose to leave). A session that had already ended counts
 * as signed out.
 */
export function useShellSignOut(): {
  readonly available: boolean;
  readonly pending: boolean;
  readonly error: string | null;
  signOut(options?: { readonly everywhere?: boolean }): Promise<void>;
} {
  const shell = useShellIdentity();
  const client = shell.mode === "signed_in" ? shell.identity?.client ?? null : null;
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const signOut = useCallback(async (options?: { readonly everywhere?: boolean }) => {
    if (client === null) return;
    setPending(true);
    setError(null);
    try {
      await client.logout(options);
    } catch (caught) {
      if (!isIdentityError(caught, "unauthenticated")) {
        setError(signOutErrorMessage(caught));
        setPending(false);
        return;
      }
    }
    lastPrincipals.delete(client);
    window.location.assign(SIGN_IN_PATH);
  }, [client]);
  return { available: client !== null, pending, error, signOut };
}

/**
 * The sign-out entries of a user menu. A legacy page keeps the entry it had
 * (`legacy`); a signed-in page signs out here, or everywhere, and reports a
 * failure in place; a signed-out page offers sign-in instead.
 */
export function ShellSignOutItems({ legacy, onSelect }: { readonly legacy: ReactNode; readonly onSelect?: () => void }) {
  const shell = useShellIdentity();
  const { pending, error, signOut } = useShellSignOut();
  const errorId = useId();
  if (shell.mode === "legacy") return <>{legacy}</>;
  if (shell.mode !== "signed_in") {
    return <a role="menuitem" href={signInHref(currentReturnPath())} onClick={onSelect}>Sign in</a>;
  }
  return (
    <>
      <button role="menuitem" type="button" aria-describedby={error === null ? undefined : errorId} aria-disabled={pending || undefined} onClick={() => { if (!pending) void signOut(); }}>
        {pending ? "Signing out…" : "Sign out"}
      </button>
      <button role="menuitem" type="button" aria-disabled={pending || undefined} onClick={() => { if (!pending) void signOut({ everywhere: true }); }}>
        Sign out of all sessions
      </button>
      {error === null ? null : <p className="suite-identity-error" id={errorId} role="alert">{error}</p>}
    </>
  );
}

function currentReturnPath(): string | null {
  return typeof window === "undefined" ? null : returnPathFor(window.location);
}

// --------------------------------------------------------------------------
// Organization switching
// --------------------------------------------------------------------------

export interface OrganizationSwitch {
  readonly choices: readonly OrganizationChoice[];
  readonly active: OrganizationChoice | null;
  /** The organization being switched to. */
  readonly pending: OrganizationChoice | null;
  readonly error: string | null;
  /** The last outcome, for a polite live region. */
  readonly announcement: string;
  /** Resolves true when the active organization is (now) the chosen one. */
  select(choice: OrganizationChoice): Promise<boolean>;
}

export function useOrganizationSwitch(): OrganizationSwitch {
  const identity = useOptionalIdentity();
  const principal = identity?.status === "signed_in" ? identity.principal : null;
  const [pending, setPending] = useState<OrganizationChoice | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [announcement, setAnnouncement] = useState("");
  const choices = principal === null ? [] : organizationChoices(principal);
  const active = choices.find((choice) => choice.active) ?? null;

  const select = useCallback(async (choice: OrganizationChoice) => {
    if (identity === null || pending !== null) return false;
    if (choice.active) return true;
    setPending(choice);
    setError(null);
    setAnnouncement(`Switching to ${choice.name}…`);
    try {
      const next = await identity.setActiveOrganization(choice.id);
      setAnnouncement(`Switched to ${next.activeOrganization?.name ?? choice.name}.`);
      return true;
    } catch (caught) {
      const message = organizationSwitchErrorMessage(caught, choice.name);
      setError(message);
      setAnnouncement(message);
      // A refused or vanished organization, or an ended session, changes what
      // the principal is: re-read it so the list (or the sign-in redirect)
      // follows.
      if (isIdentityError(caught, "unauthenticated") || isIdentityError(caught, "forbidden") || isIdentityError(caught, "not_found")) {
        identity.refresh().catch(() => undefined);
      }
      return false;
    } finally {
      setPending(null);
    }
  }, [identity, pending]);

  return { choices, active, pending, error, announcement, select };
}

function OrganizationChoiceContents({ choice, pending }: { readonly choice: OrganizationChoice; readonly pending: boolean }) {
  return (
    <>
      <i className="suite-app-icon suite-org-icon" data-personal={choice.personal || undefined} aria-hidden="true">{choice.initial}</i>
      <span>
        <strong>{choice.name}</strong>
        <small>{pending ? "Switching…" : choice.detail}</small>
      </span>
      {choice.personal ? <span className="badge badge--info">Personal</span> : null}
      {choice.siteAdminAccess ? <span className="badge badge--warning">Site admin</span> : null}
      <AppIcon className="suite-org-check" name="check" size="xs" />
    </>
  );
}

export interface OrganizationSwitcherProps {
  readonly open: boolean;
  readonly popoverId: string;
  /** The `aria-controls` value: the popover's id only while it is mounted. */
  readonly controlsId: string | undefined;
  onTriggerClick(event: ReactMouseEvent<HTMLButtonElement>): void;
  onTriggerKeyDown(event: ReactKeyboardEvent<HTMLButtonElement>): void;
  /** Closes the menu after a choice, returning focus to the trigger. */
  onClose(): void;
}

/**
 * The product bar's organization menu: every organization the principal holds
 * a role in, its role (and where it was inherited from), personal workspaces
 * marked, and the active one checked. Nothing renders on a legacy page, or
 * once the principal is known to be signed out or unreadable -- the page's
 * own state says so.
 */
export function OrganizationSwitcher({ open, popoverId, controlsId, onTriggerClick, onTriggerKeyDown, onClose }: OrganizationSwitcherProps) {
  const shell = useShellIdentity();
  const organizations = useOrganizationSwitch();
  const errorId = useId();
  if (shell.mode === "loading") {
    return (
      <div className="suite-menu-anchor suite-org-anchor">
        <span className="suite-org-switcher" aria-busy="true">
          <span className="sr-only">Loading organizations</span>
          <span className="skeleton skeleton--line skeleton--line-short" aria-hidden="true" />
        </span>
      </div>
    );
  }
  if (shell.mode !== "signed_in") return null;
  const { active, choices, error, pending, select } = organizations;
  const activeName = active?.name ?? "No organization";
  return (
    <div className="suite-menu-anchor suite-org-anchor">
      <button
        className="suite-org-switcher"
        type="button"
        aria-label={`Organization: ${activeName}${active?.personal ? " (personal workspace)" : ""}. Switch organization`}
        aria-controls={controlsId}
        aria-haspopup="menu"
        aria-expanded={open}
        aria-busy={pending !== null || undefined}
        onClick={onTriggerClick}
        onKeyDown={onTriggerKeyDown}
      >
        <span className="suite-org-switcher-label">Org:</span>
        <strong>{activeName}</strong>
        {active?.personal ? <span className="badge badge--info">Personal</span> : null}
        <AppIcon name="chevron-down" size="xs" />
      </button>
      {open ? (
        <div
          className="suite-popover suite-org-popover"
          id={popoverId}
          role="menu"
          aria-label="Organizations"
          aria-busy={pending !== null || undefined}
          aria-describedby={error === null ? undefined : errorId}
          data-suite-menu="org"
        >
          <span className="suite-menu-label">Organizations</span>
          {choices.length === 0 ? (
            <output className="suite-app-catalog-state suite-org-empty">
              <i className="suite-app-icon suite-app-icon--muted" aria-hidden="true">—</i>
              <span><strong>No organizations</strong><small>Accept an invitation to join one.</small></span>
            </output>
          ) : choices.map((choice) => (
            <button
              key={choice.id}
              role="menuitemradio"
              type="button"
              aria-checked={choice.active}
              aria-disabled={pending !== null || undefined}
              data-personal={choice.personal || undefined}
              onClick={() => {
                void select(choice).then((switched) => { if (switched) onClose(); });
              }}
            >
              <OrganizationChoiceContents choice={choice} pending={pending?.id === choice.id} />
            </button>
          ))}
          {error === null ? null : <p className="suite-identity-error" id={errorId} role="alert">{error}</p>}
        </div>
      ) : null}
      <span className="sr-only" aria-live="polite">{organizations.announcement}</span>
    </div>
  );
}

/** The drawer's organization list, below the compact breakpoint where the bar's switcher folds away. */
export function OrganizationDrawerSection({ onSwitched }: { readonly onSwitched: () => void }) {
  const shell = useShellIdentity();
  const { choices, error, pending, select } = useOrganizationSwitch();
  if (shell.mode !== "signed_in") return null;
  return (
    <>
      <span className="drawer-label">ORGANIZATION</span>
      {choices.length === 0 ? <output className="drawer-app-state">No organizations yet</output> : choices.map((choice) => (
        <button
          className="drawer-org"
          key={`drawer-org-${choice.id}`}
          type="button"
          aria-pressed={choice.active}
          aria-disabled={pending !== null || undefined}
          onClick={() => {
            void select(choice).then((switched) => { if (switched) onSwitched(); });
          }}
        >
          <span aria-hidden="true">{choice.initial}</span>
          <span><strong>{choice.name}</strong><small>{pending?.id === choice.id ? "Switching…" : choice.detail}</small></span>
          {choice.personal ? <b className="badge badge--info">Personal</b> : null}
        </button>
      ))}
      {error === null ? null : <p className="suite-identity-error drawer-org-error" role="alert">{error}</p>}
    </>
  );
}

// --------------------------------------------------------------------------
// The page's own states
// --------------------------------------------------------------------------

type AccessStateKind = "signed-out" | "error" | "forbidden";

function AccessState({ kind, icon, title, children, actions, inline = false }: {
  readonly kind: AccessStateKind;
  /** Inside a page that already has its own frame (an administration section), rather than as the page. */
  readonly inline?: boolean;
  readonly icon: "logout" | "warning" | "circle-x";
  readonly title: string;
  readonly children?: ReactNode;
  readonly actions?: ReactNode;
}) {
  const Heading = inline ? "h2" : "h1";
  const state = (
    <section className="suite-access-state" data-state={kind} role={kind === "error" ? "alert" : undefined} aria-labelledby={`suite-access-${kind}-title`}>
      <span className="suite-access-icon" aria-hidden="true"><AppIcon name={icon} size="lg" /></span>
      <div className="suite-access-body">
        <Heading id={`suite-access-${kind}-title`} tabIndex={-1}>{title}</Heading>
        {children}
        {actions === undefined ? null : <div className="suite-access-actions">{actions}</div>}
      </div>
    </section>
  );
  return inline ? state : <div className="suite-page suite-access-page">{state}</div>;
}

function SignInRedirect() {
  const href = signInHref(currentReturnPath());
  useEffect(() => {
    window.location.replace(href);
  }, [href]);
  return (
    <AccessState kind="signed-out" icon="logout" title="Sign in to continue" actions={<a className="button button--primary" href={href}>Sign in</a>}>
      <p>You are signed out. Taking you to the sign-in page…</p>
    </AccessState>
  );
}

function IdentityUnavailable({ error }: { readonly error: IdentityError | null }) {
  const { refresh } = useIdentity();
  const [retrying, setRetrying] = useState(false);
  const { title, message } = identityUnavailableMessage(error);
  return (
    <AccessState
      kind="error"
      icon="warning"
      title={title}
      actions={(
        <>
          <button className="button button--primary" type="button" aria-disabled={retrying || undefined} onClick={() => {
            if (retrying) return;
            setRetrying(true);
            refresh().catch(() => undefined).finally(() => setRetrying(false));
          }}>{retrying ? "Retrying…" : "Try again"}</button>
          <a className="button" href={signInHref(currentReturnPath())}>Sign in again</a>
        </>
      )}
    >
      <p>{message}</p>
    </AccessState>
  );
}

export interface ProductForbiddenProps {
  readonly principal: Principal;
  readonly requirement: AccessRequirement | null;
  /** What the principal tried to open, as the heading names it. */
  readonly label: string;
  /** Offered when the principal holds another organization it could switch to. */
  readonly onSwitchOrganization?: () => void;
  /** Rendered inside a page's own frame rather than as the page. */
  readonly inline?: boolean;
}

/**
 * Shown in place of a surface the principal opened directly (a bookmark, a
 * pasted link) without the capability for it: what is missing, in plain
 * words, which role would grant it, and where the principal can go instead.
 */
export function ProductForbidden({ principal, requirement, label, onSwitchOrganization, inline = false }: ProductForbiddenProps) {
  const organization = principal.activeOrganization;
  const canSearch = principal.capabilities.includes("search");
  const otherOrganizations = principal.organizations.filter((entry) => entry.organization.id !== organization?.id).length;
  const switchAction = onSwitchOrganization !== undefined && otherOrganizations > 0
    ? <button className="button" type="button" onClick={onSwitchOrganization}>Switch organization</button>
    : null;
  if (organization === null) {
    return (
      <AccessState
        inline={inline}
        kind="forbidden"
        icon="circle-x"
        title="You are not in an organization yet"
        actions={<><Link className="button" href="/help/">Read the documentation</Link><ShellSignOutButton /></>}
      >
        <p>{`Signed in as ${principal.user.email}. ${label} needs an organization: ask an organization admin to invite this address, then accept the invitation from its email.`}</p>
      </AccessState>
    );
  }
  const held = new Set(principal.capabilities);
  const missing = missingCapabilities(requirement, (capability) => held.has(capability));
  const lowestRole = lowestRoleMeeting(requirement, (role) => ORG_ROLE_CAPABILITIES[role]);
  const role = activeRoleLabel(principal);
  const advice = lowestRole === null
    ? "Only a site administrator can open it."
    : `Ask an owner or admin of ${organization.name} for the ${ORG_ROLE_LABELS[lowestRole]} role${lowestRole === "owner" ? "" : " or higher"}.`;
  return (
    <AccessState
      inline={inline}
      kind="forbidden"
      icon="circle-x"
      title={`You do not have access to ${label}`}
      actions={(
        <>
          {canSearch ? <Link className="button button--primary" href="/search/events/">Go to Search</Link> : null}
          {switchAction}
          <Link className="button" href="/help/">Documentation</Link>
        </>
      )}
    >
      <p>{`Your role in ${organization.name} is ${role ?? "not set"}. ${advice}`}</p>
      {missing.length === 0 ? null : (
        <>
          <p className="suite-access-requirement-label">{requirement?.match === "any" && missing.length > 1 ? "Any one of these permissions is needed:" : "Permissions needed:"}</p>
          <ul className="suite-access-requirements">
            {missing.map((capability) => <li key={capability}><code>{capability}</code><span>{CAPABILITY_LABELS[capability]}</span></li>)}
          </ul>
        </>
      )}
    </AccessState>
  );
}

function ShellSignOutButton() {
  const { pending, error, signOut } = useShellSignOut();
  return (
    <>
      <button className="button" type="button" aria-disabled={pending || undefined} onClick={() => { if (!pending) void signOut(); }}>{pending ? "Signing out…" : "Sign out"}</button>
      {error === null ? null : <p className="suite-identity-error" role="alert">{error}</p>}
    </>
  );
}

export interface ProductAccessGateProps {
  readonly section: ProductSection;
  /** Overrides the section's requirement, for a page narrower than its section. */
  readonly requirement?: AccessRequirement | null;
  readonly onSwitchOrganization?: () => void;
  readonly children?: ReactNode;
}

/**
 * What a `ProductShell` page shows in its main region: its content for a
 * principal holding the section's capabilities (remounted when the active
 * organization changes, so nothing from the previous organization stays on
 * screen), or the state that explains why not. A legacy page, and a surface
 * without a requirement, always show their content.
 */
export function ProductAccessGate({ section, requirement: override, onSwitchOrganization, children }: ProductAccessGateProps) {
  const shell = useShellIdentity();
  const requirement = override === undefined ? PRODUCT_SECTION_ACCESS[section] : override;
  if (requirement === null || shell.mode === "legacy") return <>{children}</>;
  if (shell.mode === "loading") {
    return (
      <div className="suite-page suite-access-page" aria-busy="true">
        <div className="suite-access-state" data-state="loading" role="status">
          <span className="sr-only">Checking your access…</span>
          <span className="skeleton skeleton--line skeleton--line-medium" aria-hidden="true" />
          <span className="skeleton skeleton--line skeleton--line-long" aria-hidden="true" />
          <span className="skeleton skeleton--line skeleton--line-short" aria-hidden="true" />
        </div>
      </div>
    );
  }
  if (shell.mode === "signed_out") return <SignInRedirect />;
  if (shell.mode === "error" || shell.principal === null) return <IdentityUnavailable error={shell.error} />;
  const principal = shell.principal;
  const held = new Set(principal.capabilities);
  if (!meetsRequirement(requirement, (capability) => held.has(capability))) {
    return (
      <ProductForbidden
        principal={principal}
        requirement={requirement}
        label={PRODUCT_SECTION_LABELS[section]}
        onSwitchOrganization={onSwitchOrganization}
      />
    );
  }
  return <Fragment key={principal.activeOrganization?.id ?? "no-organization"}>{children}</Fragment>;
}
