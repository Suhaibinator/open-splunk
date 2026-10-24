"use client";

import { useCallback, useEffect, useEffectEvent, useId, useMemo, useRef, useState, type FormEvent, type ReactNode } from "react";
import Link from "next/link";

import {
  ORG_ROLES,
  isIdentityError,
  type IdentityClient,
  type IdentityOrganization,
  type OrgRole,
  type OrganizationId,
  type OrganizationInvitation,
  type OrganizationMember,
  type Page,
  type Principal,
} from "@/lib/api/identity";
import type { SearchDataMode } from "@/lib/search/backend-data";

import { AppIcon } from "../_components/app-icon";
import { BackendResourceState } from "../_components/backend-resource-state";
import { formatMediumDateTime } from "../_components/date-format";
import { FieldNote, fieldControlProps, type FieldError } from "../_components/field-validation";
import { IdentityProvider, useIdentity } from "../_components/identity-provider";
import { Modal } from "../_components/modal";
import { StatusLabel } from "../_components/status";
import {
  CAPABILITY_LABELS,
  MEMBERS_CAPABILITY,
  ROLE_LABELS,
  ROLE_SUMMARIES,
  capabilityChange,
  countExplicitOwners,
  describeMembersFailure,
  displayedRole,
  grantableRoles,
  invitationState,
  inviteEmailError,
  memberPolicy,
  roleChangeNotices,
  roleWithArticle,
  type FailureDescription,
  type MemberPolicy,
  type MembersActor,
  type RoleOption,
} from "./members-policy";

export const MEMBERS_PAGE_SIZE = 25;
export const INVITATIONS_PAGE_SIZE = 25;

/** What the panel is showing, on its root as `data-members-state`, for tests and styling. */
export type MembersPanelState = "loading" | "unavailable" | "signed_out" | "no_organization" | "forbidden" | "ready";

export interface MembersSectionProps {
  readonly dataMode: SearchDataMode;
  readonly now?: () => number;
}

/**
 * The administration section: its own identity provider, until the product
 * shell mounts one for every page.
 */
export function MembersSection({ dataMode, now }: MembersSectionProps) {
  return (
    <IdentityProvider dataMode={dataMode}>
      <MembersPanel now={now} />
    </IdentityProvider>
  );
}

export interface MembersPanelProps {
  /** Milliseconds since the epoch; decides which invitations have expired. */
  readonly now?: () => number;
}

function SectionHeader({ organization, action }: { organization: IdentityOrganization | null; action?: ReactNode }) {
  return (
    <header className="admin-section-header">
      <div>
        <h2>Members &amp; invitations</h2>
        <p>{organization === null ? "Who can use an organization, and with which role." : `Who can use ${organization.name}, and with which role.`}</p>
      </div>
      {action}
    </header>
  );
}

/** Reads the session, then shows the manager or the reason it cannot. */
export function MembersPanel({ now = Date.now }: MembersPanelProps) {
  const identity = useIdentity();
  const { principal, status, client, activeOrganization, refresh } = identity;
  const rereadSession = useCallback(() => {
    refresh().catch(() => undefined);
  }, [refresh]);

  let state: MembersPanelState;
  if (status === "loading") state = "loading";
  else if (status === "unavailable" || (status === "signed_in" && client === null)) state = "unavailable";
  else if (status === "signed_out" || principal === null) state = "signed_out";
  else if (activeOrganization === null) state = "no_organization";
  else if (!identity.hasCapability(MEMBERS_CAPABILITY)) state = "forbidden";
  else state = "ready";

  let body: ReactNode;
  switch (state) {
    case "loading":
      body = <BackendResourceState kind="loading" title="Checking your access" message="Reading who is signed in and which organization is active." />;
      break;
    case "unavailable": {
      const failure = describeMembersFailure(identity.error, "read your session");
      body = (
        <BackendResourceState
          kind="unavailable"
          title={identity.error?.kind === "not_configured" ? "Identity is not configured" : "Identity is unavailable"}
          message={failure.message}
          action={failure.retryable
            ? <button className="button button--secondary" type="button" onClick={rereadSession}>Try again</button>
            : undefined}
        />
      );
      break;
    }
    case "signed_out":
      body = (
        <BackendResourceState
          kind="unavailable"
          title="You are signed out"
          message="Your session ended or you signed out. Sign in to manage members."
          action={<Link className="button button--secondary" href="/signin/">Sign in</Link>}
        />
      );
      break;
    case "no_organization":
      body = (
        <BackendResourceState
          kind="empty"
          title="No organization selected"
          message="You do not belong to an organization yet. Accept an invitation, or ask an admin to add you."
        />
      );
      break;
    case "forbidden":
      body = (
        <BackendResourceState
          kind="unavailable"
          title="Members are managed by admins"
          message={`Managing the members of ${activeOrganization?.name ?? "this organization"} needs the Admin role. ${principal?.role === null || principal?.role === undefined ? "You hold no role here." : `You are ${roleWithArticle(principal.role)}.`}`}
        />
      );
      break;
    case "ready":
      // Narrowed above: a ready panel has a client, a principal and an organization.
      return (
        <MembersManager
          key={activeOrganization?.id}
          client={client as IdentityClient}
          now={now}
          onSessionChanged={rereadSession}
          organization={activeOrganization as IdentityOrganization}
          principal={principal as Principal}
        />
      );
  }

  return (
    <div className="admin-section-stack members-panel" data-members-state={state}>
      <SectionHeader organization={activeOrganization} />
      {body}
    </div>
  );
}

// --------------------------------------------------------------------------
// The manager
// --------------------------------------------------------------------------

type LoadResult<T> = { readonly ok: true; readonly value: T } | { readonly ok: false; readonly error: unknown };

interface Loaded<T> {
  /** The page this result answers; a different page is still loading. */
  readonly page: number;
  readonly generation: number;
  readonly result: LoadResult<T>;
}

type Dialog =
  | { readonly kind: "invite" }
  // The policy is read when the dialog opens, so a list refresh behind it does not move its controls.
  | { readonly kind: "role"; readonly member: OrganizationMember; readonly policy: MemberPolicy }
  | { readonly kind: "remove"; readonly member: OrganizationMember; readonly policy: MemberPolicy }
  | { readonly kind: "revoke"; readonly invitation: OrganizationInvitation };

interface MembersManagerProps {
  readonly client: IdentityClient;
  readonly organization: IdentityOrganization;
  readonly principal: Principal;
  readonly now: () => number;
  /** The actor's own access may have changed, or its session ended: re-read the principal. */
  readonly onSessionChanged: () => void;
}

function usePagedList<T>(
  load: (page: number) => Promise<Page<T>>,
  onSignedOut: () => void,
): {
  readonly loaded: Loaded<Page<T>> | null;
  readonly page: number;
  readonly setPage: (page: number) => void;
  readonly reload: () => void;
  readonly refreshing: boolean;
} {
  const [page, setPage] = useState(1);
  const [generation, setGeneration] = useState(0);
  const [loaded, setLoaded] = useState<Loaded<Page<T>> | null>(null);
  const signedOut = useEffectEvent(onSignedOut);

  useEffect(() => {
    let cancelled = false;
    load(page).then(
      (value) => {
        if (!cancelled) setLoaded({ page, generation, result: { ok: true, value } });
      },
      (error: unknown) => {
        if (cancelled) return;
        setLoaded({ page, generation, result: { ok: false, error } });
        if (isIdentityError(error, "unauthenticated")) signedOut();
      },
    );
    return () => {
      cancelled = true;
    };
  }, [generation, load, page]);

  const reload = useCallback(() => setGeneration((value) => value + 1), []);
  const current = loaded !== null && loaded.page === page ? loaded : null;
  return {
    loaded: current,
    page,
    setPage,
    reload,
    refreshing: current !== null && current.generation !== generation,
  };
}

function MembersManager({ client, organization, principal, now, onSessionChanged }: MembersManagerProps) {
  const actor = useMemo<MembersActor>(() => ({
    userId: principal.user.id,
    role: principal.role,
    siteAdmin: principal.siteAdmin,
  }), [principal]);
  const organizationId = organization.id;

  const loadMembers = useCallback(
    (page: number) => client.listMembers(organizationId, { page, limit: MEMBERS_PAGE_SIZE }),
    [client, organizationId],
  );
  const loadInvitations = useCallback(
    (page: number) => client.listInvitations(organizationId, { page, limit: INVITATIONS_PAGE_SIZE }),
    [client, organizationId],
  );
  const members = usePagedList(loadMembers, onSessionChanged);
  const invitations = usePagedList(loadInvitations, onSessionChanged);

  const [dialog, setDialog] = useState<Dialog | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [announcement, setAnnouncement] = useState("");
  const membersHeadingRef = useRef<HTMLHeadingElement>(null);
  const invitationsHeadingRef = useRef<HTMLHeadingElement>(null);
  const focusAfterReload = useRef<"members" | "invitations" | null>(null);

  const organizationNames = useMemo(() => {
    const names = new Map<OrganizationId, string>();
    for (const entry of principal.organizations) names.set(entry.organization.id, entry.organization.name);
    names.set(organization.id, organization.name);
    return names;
  }, [organization, principal.organizations]);
  const organizationName = useCallback(
    (id: OrganizationId | undefined) => (id === undefined ? undefined : organizationNames.get(id)) ?? "a parent organization",
    [organizationNames],
  );

  const memberPage = members.loaded?.result.ok === true ? members.loaded.result.value : null;
  const explicitOwners = memberPage === null ? null : countExplicitOwners(memberPage);

  // A removed row or invitation takes its trigger with it; focus its table's heading instead.
  useEffect(() => {
    if (focusAfterReload.current === "members" && !members.refreshing && members.loaded !== null) {
      focusAfterReload.current = null;
      membersHeadingRef.current?.focus();
    }
    if (focusAfterReload.current === "invitations" && !invitations.refreshing && invitations.loaded !== null) {
      focusAfterReload.current = null;
      invitationsHeadingRef.current?.focus();
    }
  }, [invitations.loaded, invitations.refreshing, members.loaded, members.refreshing]);

  function finish(message: string, options: { readonly reloadMembers?: boolean; readonly reloadInvitations?: boolean; readonly focus?: "members" | "invitations"; readonly selfChanged?: boolean }) {
    setDialog(null);
    setToast(message);
    setAnnouncement(message);
    if (options.focus !== undefined) focusAfterReload.current = options.focus;
    if (options.reloadMembers === true) members.reload();
    if (options.reloadInvitations === true) invitations.reload();
    if (options.selfChanged === true) onSessionChanged();
  }

  /** A mutation failed: refresh whatever may be stale, and re-read the session if it ended. */
  function failed(error: unknown) {
    if (isIdentityError(error, "unauthenticated") || isIdentityError(error, "otp_required") || isIdentityError(error, "forbidden")) onSessionChanged();
    if (isIdentityError(error, "not_found") || isIdentityError(error, "conflict")) {
      members.reload();
      invitations.reload();
    }
  }

  const roleDescription = principal.role === null
    ? principal.siteAdmin ? "You are acting as a site admin." : ""
    : `You are ${roleWithArticle(principal.role)}${principal.siteAdmin ? " and a site admin" : ""}.`;

  const memberNames = useMemo(() => {
    const names = new Map<string, string>();
    for (const member of memberPage?.items ?? []) names.set(member.userId, member.user.displayPrimary);
    names.set(principal.user.id, "You");
    return names;
  }, [memberPage, principal.user.id]);

  return (
    <div className="admin-section-stack members-panel" data-members-state="ready">
      <SectionHeader
        organization={organization}
        action={(
          <button className="button button--primary" type="button" onClick={() => setDialog({ kind: "invite" })}>
            <AppIcon name="plus" size="sm" /> Invite member
          </button>
        )}
      />
      <p className="members-context">
        <AppIcon name="users" size="sm" />
        <span><strong>{organization.name}</strong> {roleDescription}</span>
      </p>

      <section className="suite-card resource-table-card members-card" aria-labelledby="members-heading" aria-busy={members.refreshing || members.loaded === null}>
        <header className="suite-card-header">
          <div>
            <h3 id="members-heading" ref={membersHeadingRef} tabIndex={-1}>Members</h3>
            <p>Explicit roles are granted here; effective roles also count roles inherited from parent organizations.</p>
          </div>
          {memberPage?.page.totalItems === undefined ? null : <span>{memberPage.page.totalItems === 1 ? "1 member" : `${memberPage.page.totalItems} members`}</span>}
        </header>
        <MembersTable
          actor={actor}
          explicitOwners={explicitOwners}
          loaded={members.loaded}
          onChangeRole={(member) => setDialog({ kind: "role", member, policy: memberPolicy(actor, member, { explicitOwners, organizationName }) })}
          onInvite={() => setDialog({ kind: "invite" })}
          onRemove={(member) => setDialog({ kind: "remove", member, policy: memberPolicy(actor, member, { explicitOwners, organizationName }) })}
          onRetry={members.reload}
          organization={organization}
          organizationName={organizationName}
        />
        <Pager label="Members pages" loaded={members.loaded} onPage={members.setPage} />
      </section>

      <section className="suite-card resource-table-card members-card" aria-labelledby="members-invitations-heading" aria-busy={invitations.refreshing || invitations.loaded === null}>
        <header className="suite-card-header">
          <div>
            <h3 id="members-invitations-heading" ref={invitationsHeadingRef} tabIndex={-1}>Pending invitations</h3>
            <p>Invitations stay here until they are accepted or revoked; an expired link must be sent again.</p>
          </div>
        </header>
        <InvitationsTable
          loaded={invitations.loaded}
          memberNames={memberNames}
          now={now}
          onRetry={invitations.reload}
          onRevoke={(invitation) => setDialog({ kind: "revoke", invitation })}
        />
        <Pager label="Invitation pages" loaded={invitations.loaded} onPage={invitations.setPage} />
      </section>

      {dialog?.kind === "invite" ? (
        <InviteDialog
          actor={actor}
          client={client}
          onClose={() => setDialog(null)}
          onFailed={failed}
          onInvited={(invitation) => finish(`Invitation sent to ${invitation.email} as ${ROLE_LABELS[invitation.role].toLowerCase()}.`, { reloadInvitations: true })}
          organization={organization}
        />
      ) : null}
      {dialog?.kind === "role" ? (
        <RoleDialog
          actor={actor}
          client={client}
          member={dialog.member}
          onChanged={(role, self) => finish(`${self ? "Your" : `${dialog.member.user.displayPrimary}'s`} role is now ${ROLE_LABELS[role].toLowerCase()}.`, { reloadMembers: true, selfChanged: self })}
          onClose={() => setDialog(null)}
          onFailed={failed}
          organization={organization}
          organizationName={organizationName}
          policy={dialog.policy}
        />
      ) : null}
      {dialog?.kind === "remove" ? (
        <RemoveDialog
          client={client}
          member={dialog.member}
          onClose={() => setDialog(null)}
          onFailed={failed}
          onRemoved={(self) => finish(self ? `You left ${organization.name}.` : `${dialog.member.user.displayPrimary} was removed from ${organization.name}.`, { reloadMembers: true, focus: "members", selfChanged: self })}
          organization={organization}
          organizationName={organizationName}
          policy={dialog.policy}
        />
      ) : null}
      {dialog?.kind === "revoke" ? (
        <RevokeDialog
          client={client}
          invitation={dialog.invitation}
          onClose={() => setDialog(null)}
          onFailed={failed}
          onRevoked={() => finish(`The invitation for ${dialog.invitation.email} was revoked.`, { reloadInvitations: true, focus: "invitations" })}
          organization={organization}
        />
      ) : null}

      <div className="sr-only" aria-live="polite" data-members-announcement="">{announcement}</div>
      {toast === null ? null : (
        <div className="toast toast-success">
          <span><AppIcon name="check" size="sm" /></span>
          <strong>{toast}</strong>
          <button type="button" aria-label="Dismiss notification" onClick={() => setToast(null)}><AppIcon name="close" size="md" /></button>
        </div>
      )}
    </div>
  );
}

// --------------------------------------------------------------------------
// Tables
// --------------------------------------------------------------------------

function RoleBadge({ role }: { role: OrgRole }) {
  return <span className={`badge ${role === "admin" || role === "owner" ? "badge--info" : "badge--neutral"}`}>{ROLE_LABELS[role]}</span>;
}

function failureState(failure: FailureDescription, onRetry: () => void) {
  return (
    <BackendResourceState
      kind="error"
      title={failure.title}
      message={failure.message}
      action={failure.retryable ? <button type="button" onClick={onRetry}>Try again</button> : undefined}
    />
  );
}

interface MembersTableProps {
  readonly actor: MembersActor;
  readonly explicitOwners: number | null;
  readonly loaded: Loaded<Page<OrganizationMember>> | null;
  readonly organization: IdentityOrganization;
  readonly organizationName: (id: OrganizationId | undefined) => string;
  readonly onChangeRole: (member: OrganizationMember) => void;
  readonly onRemove: (member: OrganizationMember) => void;
  readonly onInvite: () => void;
  readonly onRetry: () => void;
}

function MembersTable({ actor, explicitOwners, loaded, organization, organizationName, onChangeRole, onRemove, onInvite, onRetry }: MembersTableProps) {
  if (loaded === null) {
    return <BackendResourceState kind="loading" title="Loading members" message={`Reading the members of ${organization.name}.`} />;
  }
  if (!loaded.result.ok) return failureState(describeMembersFailure(loaded.result.error, "load members"), onRetry);
  const { items } = loaded.result.value;
  if (items.length === 0) {
    return (
      <BackendResourceState
        kind="empty"
        title="No members on this page"
        message="Invite someone to give them access to this organization."
        action={<button type="button" onClick={onInvite}>Invite member</button>}
      />
    );
  }
  return (
    <div className="table-wrap">
      <table className="table table--cards members-table">
        <caption className="sr-only">Members of {organization.name}</caption>
        <thead>
          <tr>
            <th scope="col">Member</th>
            <th scope="col">Explicit role</th>
            <th scope="col">Effective role</th>
            <th scope="col">Status</th>
            <th scope="col">Last active</th>
            <th scope="col"><span className="sr-only">Actions</span></th>
          </tr>
        </thead>
        <tbody>
          {items.map((member) => (
            <MemberRow
              actor={actor}
              key={member.userId}
              member={member}
              onChangeRole={onChangeRole}
              onRemove={onRemove}
              organizationName={organizationName}
              policy={memberPolicy(actor, member, { explicitOwners, organizationName })}
            />
          ))}
        </tbody>
      </table>
    </div>
  );
}

interface MemberRowProps {
  readonly actor: MembersActor;
  readonly member: OrganizationMember;
  readonly policy: MemberPolicy;
  readonly organizationName: (id: OrganizationId | undefined) => string;
  readonly onChangeRole: (member: OrganizationMember) => void;
  readonly onRemove: (member: OrganizationMember) => void;
}

function MemberRow({ member, policy, organizationName, onChangeRole, onRemove }: MemberRowProps) {
  const noteId = useId();
  const name = member.user.displayPrimary;
  const reasons = [
    policy.roleChange.allowed ? null : policy.roleChange.reason,
    policy.removal.allowed ? null : policy.removal.reason,
  ].filter((reason, index, all): reason is string => reason !== null && all.indexOf(reason) === index);
  const describedBy = reasons.length === 0 ? undefined : noteId;
  return (
    <tr data-member-id={member.userId}>
      <td data-label="Member">
        <span className="members-person">
          <strong>{name}{policy.self ? <span className="members-self"> (you)</span> : null}</strong>
          {member.user.displaySecondary === undefined ? null : <small>{member.user.displaySecondary}</small>}
        </span>
      </td>
      <td data-label="Explicit role">
        {member.explicitRole === null
          ? <span className="members-muted">None</span>
          : <RoleBadge role={member.explicitRole} />}
        {policy.lastOwner ? <small className="members-note">Last owner</small> : null}
      </td>
      <td data-label="Effective role">
        <RoleBadge role={member.effectiveRole} />
        {member.inheritedFromOrganizationId === undefined
          ? null
          : <small className="members-note">Inherited from {organizationName(member.inheritedFromOrganizationId)}</small>}
      </td>
      <td data-label="Status">
        {member.isActive ? <StatusLabel tone="success">Active</StatusLabel> : <StatusLabel tone="error">Banned</StatusLabel>}
        {member.user.isBot ? <span className="badge badge--outline">Bot</span> : null}
      </td>
      <td data-label="Last active">{formatMediumDateTime(member.lastActivity === undefined ? null : new Date(member.lastActivity), "Never")}</td>
      <td data-label="Actions">
        <div className="members-actions">
          <button
            aria-describedby={policy.roleChange.allowed ? undefined : describedBy}
            aria-disabled={policy.roleChange.allowed ? undefined : true}
            aria-label={`Change role for ${name}`}
            className="button button--secondary button--compact"
            onClick={() => { if (policy.roleChange.allowed) onChangeRole(member); }}
            type="button"
          >
            Change role
          </button>
          <button
            aria-describedby={policy.removal.allowed ? undefined : describedBy}
            aria-disabled={policy.removal.allowed ? undefined : true}
            aria-label={policy.self ? "Leave organization" : `Remove ${name}`}
            className="button button--ghost button--compact"
            onClick={() => { if (policy.removal.allowed) onRemove(member); }}
            type="button"
          >
            {policy.self ? "Leave" : "Remove"}
          </button>
        </div>
        {reasons.length === 0 ? null : <small className="members-note" id={noteId}>{reasons.join(" ")}</small>}
      </td>
    </tr>
  );
}

interface InvitationsTableProps {
  readonly loaded: Loaded<Page<OrganizationInvitation>> | null;
  readonly memberNames: ReadonlyMap<string, string>;
  readonly now: () => number;
  readonly onRevoke: (invitation: OrganizationInvitation) => void;
  readonly onRetry: () => void;
}

function InvitationsTable({ loaded, memberNames, now, onRevoke, onRetry }: InvitationsTableProps) {
  if (loaded === null) return <BackendResourceState kind="loading" title="Loading invitations" message="Reading the invitations this organization has sent." />;
  if (!loaded.result.ok) return failureState(describeMembersFailure(loaded.result.error, "load invitations"), onRetry);
  const { items } = loaded.result.value;
  if (items.length === 0) {
    return <BackendResourceState kind="empty" title="No pending invitations" message="Invitations you send appear here until they are accepted or revoked." />;
  }
  const at = now();
  return (
    <div className="table-wrap">
      <table className="table table--cards members-table">
        <caption className="sr-only">Pending invitations</caption>
        <thead>
          <tr>
            <th scope="col">Email</th>
            <th scope="col">Role</th>
            <th scope="col">Invited by</th>
            <th scope="col">Sent</th>
            <th scope="col">Expires</th>
            <th scope="col"><span className="sr-only">Actions</span></th>
          </tr>
        </thead>
        <tbody>
          {items.map((invitation) => {
            const state = invitationState(invitation, at);
            return (
              <tr data-invitation-id={invitation.id} key={invitation.id}>
                <td data-label="Email"><strong>{invitation.email}</strong></td>
                <td data-label="Role"><RoleBadge role={invitation.role} /></td>
                <td data-label="Invited by">{memberNames.get(invitation.invitedByUserId) ?? `User ${invitation.invitedByUserId}`}</td>
                <td data-label="Sent">{formatMediumDateTime(new Date(invitation.createdAt), "Unknown")}</td>
                <td data-label="Expires">
                  {state === "expired" ? <StatusLabel tone="warning">Expired</StatusLabel> : <StatusLabel tone="info">Pending</StatusLabel>}
                  <small className="members-note">{formatMediumDateTime(new Date(invitation.expiresAt), "Unknown")}</small>
                </td>
                <td data-label="Actions">
                  <div className="members-actions">
                    <button
                      aria-label={`Revoke invitation for ${invitation.email}`}
                      className="button button--ghost button--compact"
                      onClick={() => onRevoke(invitation)}
                      type="button"
                    >
                      Revoke
                    </button>
                  </div>
                </td>
              </tr>
            );
          })}
        </tbody>
      </table>
    </div>
  );
}

function Pager<T>({ label, loaded, onPage }: { label: string; loaded: Loaded<Page<T>> | null; onPage: (page: number) => void }) {
  if (loaded === null || !loaded.result.ok) return null;
  const { currentPage, hasNextPage, hasPrevPage, totalPages } = loaded.result.value.page;
  if (!hasNextPage && !hasPrevPage) return null;
  return (
    <nav aria-label={label} className="members-pager">
      <button className="button button--secondary button--compact" disabled={!hasPrevPage} onClick={() => onPage(currentPage - 1)} type="button">Previous</button>
      <span>{totalPages === undefined ? `Page ${currentPage}` : `Page ${currentPage} of ${totalPages}`}</span>
      <button className="button button--secondary button--compact" disabled={!hasNextPage} onClick={() => onPage(currentPage + 1)} type="button">Next</button>
    </nav>
  );
}

// --------------------------------------------------------------------------
// Dialogs
// --------------------------------------------------------------------------

function DialogFailure({ failure }: { failure: FailureDescription | null }) {
  if (failure === null) return null;
  return (
    <div className="members-dialog-alert" role="alert">
      <AppIcon name="warning" size="sm" />
      <div><strong>{failure.title}</strong><p>{failure.message}</p></div>
    </div>
  );
}

function Notice({ title, children }: { title: string; children: ReactNode }) {
  return (
    <div className="access-mode-notice" role="note">
      <span aria-hidden="true"><AppIcon name="info" size="sm" /></span>
      <div><strong>{title}</strong><p>{children}</p></div>
    </div>
  );
}

interface RoleChoicesProps {
  readonly legend: string;
  readonly name: string;
  readonly options: readonly RoleOption[];
  readonly value: OrgRole | null;
  readonly disabled: boolean;
  readonly onChange: (role: OrgRole) => void;
}

function RoleChoices({ legend, name, options, value, disabled, onChange }: RoleChoicesProps) {
  const baseId = useId();
  return (
    <fieldset className="members-role-options" disabled={disabled}>
      <legend>{legend}</legend>
      {options.map((option) => {
        const id = `${baseId}-${option.role}`;
        const noteId = `${id}-note`;
        return (
          <label className="members-role-option" htmlFor={id} key={option.role}>
            <input
              aria-describedby={noteId}
              checked={value === option.role}
              disabled={option.disabled}
              id={id}
              name={name}
              onChange={() => onChange(option.role)}
              type="radio"
              value={option.role}
            />
            <strong>{option.current ? `${ROLE_LABELS[option.role]} (current)` : ROLE_LABELS[option.role]}</strong>
            <small id={noteId}>{option.disabled && option.reason !== undefined ? option.reason : ROLE_SUMMARIES[option.role]}</small>
          </label>
        );
      })}
    </fieldset>
  );
}

interface InviteDialogProps {
  readonly actor: MembersActor;
  readonly client: IdentityClient;
  readonly organization: IdentityOrganization;
  readonly onClose: () => void;
  readonly onFailed: (error: unknown) => void;
  readonly onInvited: (invitation: OrganizationInvitation) => void;
}

const INVITE_EMAIL_ID = "members-invite-email";

function InviteDialog({ actor, client, organization, onClose, onFailed, onInvited }: InviteDialogProps) {
  const grantable = grantableRoles(actor);
  const [email, setEmail] = useState("");
  const [role, setRole] = useState<OrgRole>("member");
  const [submitted, setSubmitted] = useState(false);
  const [pending, setPending] = useState(false);
  const [emailError, setEmailError] = useState<FieldError>(null);
  const [failure, setFailure] = useState<FailureDescription | null>(null);
  const emailRef = useRef<HTMLInputElement>(null);
  const options = ORG_ROLES.map((option): RoleOption => (grantable.includes(option)
    ? { role: option, current: false, disabled: false }
    : { role: option, current: false, disabled: true, reason: "Above what your role can grant." }));
  const localError = submitted ? inviteEmailError(email) : null;
  const shownEmailError = localError ?? emailError;

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSubmitted(true);
    setFailure(null);
    setEmailError(null);
    if (inviteEmailError(email) !== null) {
      emailRef.current?.focus();
      return;
    }
    setPending(true);
    try {
      const invitation = await client.inviteMember(organization.id, { email: email.trim(), role });
      onInvited(invitation);
    } catch (error) {
      setPending(false);
      if (isIdentityError(error, "invalid") || isIdentityError(error, "conflict")) {
        setEmailError(error.message);
        emailRef.current?.focus();
      } else {
        setFailure(describeMembersFailure(error, "send the invitation"));
      }
      onFailed(error);
    }
  }

  return (
    <Modal
      footer={(
        <>
          <button className="button button--secondary" onClick={onClose} type="button">Cancel</button>
          <button className="button button--primary" disabled={pending} form="members-invite-form" type="submit">{pending ? "Sending…" : "Send invitation"}</button>
        </>
      )}
      initialFocus={`#${INVITE_EMAIL_ID}`}
      onClose={onClose}
      subtitle={`They join ${organization.name} with this role once they accept.`}
      title="Invite member"
    >
      <form className="admin-form" id="members-invite-form" noValidate onSubmit={(event) => { void submit(event); }}>
        <DialogFailure failure={failure} />
        <label htmlFor={INVITE_EMAIL_ID}>
          <span>Email address</span>
          <input
            {...fieldControlProps(INVITE_EMAIL_ID, shownEmailError)}
            autoComplete="email"
            disabled={pending}
            id={INVITE_EMAIL_ID}
            inputMode="email"
            onChange={(event) => { setEmail(event.target.value); setEmailError(null); }}
            placeholder="name@example.com"
            ref={emailRef}
            type="email"
            value={email}
          />
          <FieldNote error={shownEmailError} fieldId={INVITE_EMAIL_ID}>The invitation link is sent to this address and expires after seven days.</FieldNote>
        </label>
        <RoleChoices disabled={pending} legend="Role" name="members-invite-role" onChange={setRole} options={options} value={role} />
      </form>
    </Modal>
  );
}

interface RoleDialogProps {
  readonly actor: MembersActor;
  readonly client: IdentityClient;
  readonly member: OrganizationMember;
  readonly organization: IdentityOrganization;
  readonly organizationName: (id: OrganizationId | undefined) => string;
  readonly policy: MemberPolicy;
  readonly onClose: () => void;
  readonly onFailed: (error: unknown) => void;
  readonly onChanged: (role: OrgRole, self: boolean) => void;
}

function RoleDialog({ actor, client, member, organization, organizationName, policy, onClose, onFailed, onChanged }: RoleDialogProps) {
  const current = displayedRole(member);
  const [selected, setSelected] = useState<OrgRole>(current);
  const [pending, setPending] = useState(false);
  const [failure, setFailure] = useState<FailureDescription | null>(null);
  const options = policy.roleChange.allowed ? policy.roleChange.options : [];
  const choice = options.find((option) => option.role === selected);
  const canSave = !pending && choice !== undefined && !choice.disabled && selected !== current;
  const notices = roleChangeNotices(actor, member, selected);
  const change = capabilityChange(current, selected);
  const selfDemotion = notices.some((notice) => notice.kind === "self_demotion");
  const name = member.user.displayPrimary;

  async function submit(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (!canSave) return;
    setPending(true);
    setFailure(null);
    try {
      const membership = await client.changeMemberRole(organization.id, member.userId, selected);
      onChanged(membership.explicitRole ?? selected, policy.self);
    } catch (error) {
      setPending(false);
      setFailure(describeMembersFailure(error, "change the role"));
      onFailed(error);
    }
  }

  return (
    <Modal
      footer={(
        <>
          <button className="button button--secondary" onClick={onClose} type="button">Cancel</button>
          <button className={`button ${selfDemotion ? "button--danger" : "button--primary"}`} disabled={!canSave} form="members-role-form" type="submit">
            {pending ? "Saving…" : selfDemotion ? "Lower my role" : "Save role"}
          </button>
        </>
      )}
      initialFocus={'input[name="members-role"]:checked'}
      onClose={onClose}
      subtitle={member.explicitRole === null
        ? `${name} has no explicit role here; they are ${ROLE_LABELS[member.effectiveRole].toLowerCase()} through ${organizationName(member.inheritedFromOrganizationId)}.`
        : `Explicit role ${ROLE_LABELS[member.explicitRole].toLowerCase()}; effective role ${ROLE_LABELS[member.effectiveRole].toLowerCase()}.`}
      title={policy.self ? "Change your role" : `Change role for ${name}`}
    >
      <form className="admin-form" id="members-role-form" onSubmit={(event) => { void submit(event); }}>
        <DialogFailure failure={failure} />
        <RoleChoices disabled={pending} legend={`Role in ${organization.name}`} name="members-role" onChange={setSelected} options={options} value={selected} />
        {notices.map((notice) => {
          switch (notice.kind) {
            case "owner_grant":
              return (
                <Notice key={notice.kind} title="Owners can manage everything">
                  An owner holds every capability, including changing and removing other owners. To hand over ownership, make {policy.self ? "the new owner" : name} an owner first, then change your own role.
                </Notice>
              );
            case "self_demotion":
              return (
                <Notice key={notice.kind} title="You are lowering your own role">
                  {notice.siteAdmin
                    ? "You keep full access as a site admin, but your organization role changes."
                    : "You lose the capabilities listed below as soon as you save, and may not be able to undo it."}
                </Notice>
              );
            case "inherited_floor":
              return (
                <Notice key={notice.kind} title="Inherited role still applies">
                  They hold {ROLE_LABELS[notice.role].toLowerCase()} through {organizationName(notice.organizationId)}, which outranks this role.
                </Notice>
              );
          }
        })}
        {selected === current ? null : (
          <div className="members-capability-change">
            {change.gained.length === 0 ? null : (
              <div><strong>Gains</strong><ul>{change.gained.map((capability) => <li key={capability}>{CAPABILITY_LABELS[capability]}</li>)}</ul></div>
            )}
            {change.lost.length === 0 ? null : (
              <div><strong>Loses</strong><ul>{change.lost.map((capability) => <li key={capability}>{CAPABILITY_LABELS[capability]}</li>)}</ul></div>
            )}
          </div>
        )}
      </form>
    </Modal>
  );
}

interface RemoveDialogProps {
  readonly client: IdentityClient;
  readonly member: OrganizationMember;
  readonly organization: IdentityOrganization;
  readonly organizationName: (id: OrganizationId | undefined) => string;
  readonly policy: MemberPolicy;
  readonly onClose: () => void;
  readonly onFailed: (error: unknown) => void;
  readonly onRemoved: (self: boolean) => void;
}

function RemoveDialog({ client, member, organization, organizationName, policy, onClose, onFailed, onRemoved }: RemoveDialogProps) {
  const [pending, setPending] = useState(false);
  const [failure, setFailure] = useState<FailureDescription | null>(null);
  const name = member.user.displayPrimary;
  const removal = policy.removal;

  async function confirm() {
    if (!removal.allowed || pending) return;
    setPending(true);
    setFailure(null);
    try {
      await client.removeMember(organization.id, member.userId);
      onRemoved(policy.self);
    } catch (error) {
      setPending(false);
      setFailure(describeMembersFailure(error, policy.self ? "leave the organization" : "remove the member"));
      onFailed(error);
    }
  }

  return (
    <Modal
      footer={(
        <>
          <button className="button button--secondary" onClick={onClose} type="button">Cancel</button>
          <button className="button button--danger" disabled={pending || !removal.allowed} onClick={() => { void confirm(); }} type="button">
            {pending ? "Removing…" : policy.self ? "Leave organization" : "Remove member"}
          </button>
        </>
      )}
      onClose={onClose}
      title={policy.self ? `Leave ${organization.name}?` : `Remove ${name}?`}
    >
      <div className="admin-form">
        <DialogFailure failure={failure} />
        <p className="members-dialog-copy">
          {policy.self
            ? `You lose your ${ROLE_LABELS[member.explicitRole ?? member.effectiveRole].toLowerCase()} role in ${organization.name} immediately.`
            : `${name}${member.user.displaySecondary === undefined ? "" : ` (${member.user.displaySecondary})`} loses their ${ROLE_LABELS[member.explicitRole ?? member.effectiveRole].toLowerCase()} role in ${organization.name} immediately. Their saved searches and dashboards stay where they are.`}
        </p>
        {removal.allowed && removal.keepsInheritedRole !== undefined ? (
          <Notice title="An inherited role remains">
            {policy.self ? "You" : "They"} still hold {ROLE_LABELS[removal.keepsInheritedRole].toLowerCase()} through {organizationName(removal.inheritedFromOrganizationId)}, so access continues until that role is removed there.
          </Notice>
        ) : null}
        {removal.allowed ? null : <Notice title="This member cannot be removed here">{removal.reason}</Notice>}
      </div>
    </Modal>
  );
}

interface RevokeDialogProps {
  readonly client: IdentityClient;
  readonly invitation: OrganizationInvitation;
  readonly organization: IdentityOrganization;
  readonly onClose: () => void;
  readonly onFailed: (error: unknown) => void;
  readonly onRevoked: () => void;
}

function RevokeDialog({ client, invitation, organization, onClose, onFailed, onRevoked }: RevokeDialogProps) {
  const [pending, setPending] = useState(false);
  const [failure, setFailure] = useState<FailureDescription | null>(null);

  async function confirm() {
    if (pending) return;
    setPending(true);
    setFailure(null);
    try {
      await client.revokeInvitation(organization.id, invitation.id);
      onRevoked();
    } catch (error) {
      setPending(false);
      setFailure(describeMembersFailure(error, "revoke the invitation"));
      onFailed(error);
    }
  }

  return (
    <Modal
      footer={(
        <>
          <button className="button button--secondary" onClick={onClose} type="button">Cancel</button>
          <button className="button button--danger" disabled={pending} onClick={() => { void confirm(); }} type="button">{pending ? "Revoking…" : "Revoke invitation"}</button>
        </>
      )}
      onClose={onClose}
      title="Revoke invitation?"
    >
      <div className="admin-form">
        <DialogFailure failure={failure} />
        <p className="members-dialog-copy">
          The {ROLE_LABELS[invitation.role].toLowerCase()} invitation for <strong>{invitation.email}</strong> stops working immediately. You can invite them to {organization.name} again later.
        </p>
      </div>
    </Modal>
  );
}
