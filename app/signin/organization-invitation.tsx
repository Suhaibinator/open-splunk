"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useState } from "react";

import type { OrganizationInvitation, Principal } from "@/lib/api/identity";

import { useIdentity } from "../_components/identity-provider";
import { AuthAlert, AuthCardHeader, AuthLoading, AuthNotice, SubmitButton } from "./auth-frame";
import { describeAuthError, type AuthErrorCopy } from "./auth-error-copy";
import { useResource, type AuthNavigate } from "./auth-hooks";
import { demoOutboxLink } from "./demo-outbox";
import { RegistrationForm } from "./registration-form";
import {
  DEFAULT_RETURN_TO,
  organizationInvitationHref,
  rememberPendingReturn,
  signInHref,
} from "./return-to";
import { PrincipalSummary } from "./sign-in-completion";

export interface OrganizationInvitationFlowProps {
  readonly invitationId: string | null;
  /** The address the link says it was sent to, already passed through `prefillEmail`; a prefill, never trusted. Empty or null when unknown. */
  readonly invitedEmail: string | null;
  readonly navigate: AuthNavigate;
  readonly demo?: boolean;
  /** Milliseconds since the epoch; tests pass their own. */
  readonly now?: () => number;
}

/**
 * `/auth/invitation/`: one organization invitation, whoever opens it.
 *
 * Signed out, it offers to sign in (and come back) or, for an address with no
 * account yet, to register; go-common binds the invitation to the address, so
 * it appears among the new account's invitations once the emailed
 * verification signs them in. Signed in, it shows the invitation if it is one
 * of the user's own, and accepts or declines it.
 */
export function OrganizationInvitationFlow({ invitationId, invitedEmail: linkEmail, navigate, demo = false, now = Date.now }: OrganizationInvitationFlowProps) {
  const identity = useIdentity();
  const invitedEmail = linkEmail === null || linkEmail.length === 0 ? null : linkEmail;
  const [sessionEnded, setSessionEnded] = useState(false);
  const { adoptPrincipal } = identity;
  // The server refused the session mid-flow: the page drops to its signed-out
  // card, whose sign-in comes back here, instead of offering buttons that can
  // only be refused again.
  const endSession = useCallback(() => {
    setSessionEnded(true);
    adoptPrincipal(null);
  }, [adoptPrincipal]);

  if (invitationId === null || invitationId.length === 0) {
    return (
      <>
        <AuthCardHeader icon="warning" title="This invitation link is incomplete" intro="Open the link from your invitation email again." focusOnMount />
        <Link className="button button--secondary button--block signin-action" href={signInHref()}>Go to sign in</Link>
      </>
    );
  }
  const href = organizationInvitationHref(invitationId, invitedEmail);

  if (identity.status === "loading") {
    return (
      <>
        <AuthCardHeader title="Your invitation" intro="Checking who is signed in…" />
        <AuthLoading label="Loading your invitation" />
      </>
    );
  }
  if (identity.status === "unavailable") {
    return (
      <>
        <AuthCardHeader icon="warning" title="Invitations are unavailable" intro="Open Splunk could not check your session." focusOnMount />
        <AuthAlert
          copy={describeAuthError(identity.error, "session")}
          action={(
            <button className="button button--secondary button--compact" type="button" onClick={() => { void identity.refresh().catch(() => undefined); }}>
              Try again
            </button>
          )}
        />
      </>
    );
  }
  if (identity.status === "signed_out" || identity.principal === null) {
    return <SignedOutInvitation href={href} invitedEmail={invitedEmail} navigate={navigate} demo={demo} sessionEnded={sessionEnded} />;
  }
  return <SignedInInvitation invitationId={invitationId} principal={identity.principal} navigate={navigate} now={now} onSessionEnded={endSession} />;
}

type SignedOutMode = { readonly kind: "choose" } | { readonly kind: "register" } | { readonly kind: "registered"; readonly email: string };

function SignedOutInvitation({ href, invitedEmail, navigate, demo, sessionEnded }: {
  readonly href: string;
  readonly invitedEmail: string | null;
  readonly navigate: AuthNavigate;
  readonly demo: boolean;
  /** The session ended while this page had it signed in. */
  readonly sessionEnded: boolean;
}) {
  const { client } = useIdentity();
  const [mode, setMode] = useState<SignedOutMode>({ kind: "choose" });

  if (mode.kind === "registered") {
    const link = demo ? demoOutboxLink(client, "email_verification", mode.email) : null;
    return (
      <>
        <AuthCardHeader icon="check" title="Verify your email" intro={<>We sent a verification link to <b>{mode.email}</b>.</>} focusOnMount />
        <AuthNotice tone="success" title="Account created">
          Open the link to confirm your address. You will come back here to accept the invitation.
        </AuthNotice>
        {link === null ? null : (
          <AuthNotice title="Demo inbox">The preview has no email. <Link href={link}>Open the verification link</Link>.</AuthNotice>
        )}
      </>
    );
  }

  if (mode.kind === "register") {
    return (
      <>
        <AuthCardHeader title="Create your account" intro="Then confirm your email address to accept the invitation." focusOnMount />
        <RegistrationForm
          initialEmail={invitedEmail ?? ""}
          onCancel={() => setMode({ kind: "choose" })}
          onRegistered={(email) => {
            rememberPendingReturn(href);
            setMode({ kind: "registered", email });
          }}
          returnTo={href}
        />
      </>
    );
  }

  return (
    <>
      <AuthCardHeader
        icon="users"
        title="You're invited to Open Splunk"
        intro={invitedEmail === null
          ? "Sign in with the address the invitation was sent to, or create an account for it."
          : <>This invitation was sent to <b>{invitedEmail}</b>. Sign in with that address, or create an account for it.</>}
      />
      {sessionEnded ? <AuthNotice title="Your session has ended">Sign in again to answer this invitation.</AuthNotice> : null}
      <div className="signin-actions">
        <SubmitButton pending={false} pendingLabel="Opening sign-in…" type="button" onClick={() => navigate(signInHref({ returnTo: href, email: invitedEmail }))}>
          Sign in to accept
        </SubmitButton>
        <SubmitButton pending={false} pendingLabel="Opening…" type="button" variant="secondary" onClick={() => setMode({ kind: "register" })}>
          I&apos;m new: create an account
        </SubmitButton>
      </div>
    </>
  );
}

type Lookup =
  | { readonly status: "loading" }
  | { readonly status: "error"; readonly copy: AuthErrorCopy }
  | { readonly status: "ready"; readonly invitation: OrganizationInvitation | null };

type Outcome =
  | { readonly kind: "accepted" }
  | { readonly kind: "declined" }
  | { readonly kind: "already_member" }
  | { readonly kind: "expired" }
  | { readonly kind: "unavailable" };

function SignedInInvitation({ invitationId, principal, navigate, now, onSessionEnded }: {
  readonly invitationId: string;
  readonly principal: Principal;
  readonly navigate: AuthNavigate;
  readonly now: () => number;
  /** A request was refused as unauthenticated: this session is over. */
  readonly onSessionEnded: () => void;
}) {
  const identity = useIdentity();
  const { client } = identity;
  // go-common lists a user's invitations a page at a time; one link names one, so the first page of 100 is searched.
  const load = useMemo(
    () => (client === null ? null : () => client.listMyInvitations({ page: 1, limit: 100 })),
    [client],
  );
  const { resource, retry } = useResource(load);
  const lookup: Lookup = resource.status === "ready"
    ? { status: "ready", invitation: resource.value.items.find((item) => item.id === invitationId) ?? null }
    : resource.status === "error"
      ? { status: "error", copy: describeAuthError(resource.error, "invitations") }
      : { status: "loading" };
  const [outcome, setOutcome] = useState<Outcome | null>(null);
  const [action, setAction] = useState<"accept" | "decline" | "open" | "sign_out" | null>(null);
  const [failure, setFailure] = useState<AuthErrorCopy | null>(null);
  const lookupSessionEnded = lookup.status === "error" && lookup.copy.kind === "unauthenticated";

  useEffect(() => {
    if (lookupSessionEnded) onSessionEnded();
  }, [lookupSessionEnded, onSessionEnded]);

  async function signOut(): Promise<void> {
    setAction("sign_out");
    setFailure(null);
    try {
      await identity.signOut();
    } catch (error) {
      setFailure(describeAuthError(error, "session"));
      setAction(null);
    }
  }

  async function respond(kind: "accept" | "decline"): Promise<void> {
    if (action !== null || client === null) return;
    setAction(kind);
    setFailure(null);
    try {
      if (kind === "accept") {
        await client.acceptInvitation({ kind: "organization", invitationId });
        setOutcome({ kind: "accepted" });
      } else {
        await client.declineInvitation(invitationId);
        setOutcome({ kind: "declined" });
      }
    } catch (error) {
      const copy = describeAuthError(error, kind === "accept" ? "accept_invitation" : "decline_invitation");
      if (copy.kind === "unauthenticated") onSessionEnded();
      else if (copy.kind === "conflict") setOutcome({ kind: "already_member" });
      else if (copy.deadLink) setOutcome({ kind: "expired" });
      else if (copy.kind === "not_found" || copy.kind === "forbidden") setOutcome({ kind: "unavailable" });
      else setFailure(copy);
    } finally {
      setAction(null);
    }
  }

  async function openOrganization(organizationId: string): Promise<void> {
    setAction("open");
    setFailure(null);
    try {
      await identity.setActiveOrganization(organizationId);
      navigate(DEFAULT_RETURN_TO);
    } catch (error) {
      const copy = describeAuthError(error, "session");
      setAction(null);
      if (copy.kind === "unauthenticated") onSessionEnded();
      else setFailure(copy);
    }
  }

  const switchAccount = (
    <SubmitButton pending={action === "sign_out"} pendingLabel="Signing out…" type="button" variant="secondary" onClick={() => { void signOut(); }}>
      Sign in with a different account
    </SubmitButton>
  );

  if (lookup.status === "loading") {
    return (
      <>
        <AuthCardHeader title="Your invitation" intro="Looking up the invitation…" />
        <AuthLoading label="Loading your invitation" />
      </>
    );
  }
  if (lookup.status === "error") {
    return (
      <>
        <AuthCardHeader icon="warning" title="Your invitation" focusOnMount />
        <AuthAlert
          copy={lookup.copy}
          action={(
            <button className="button button--secondary button--compact" type="button" onClick={retry}>
              Try again
            </button>
          )}
        />
      </>
    );
  }

  const { invitation } = lookup;
  const expired = invitation !== null && Date.parse(invitation.expiresAt) < now();
  const state: Outcome["kind"] | "pending" = outcome?.kind ?? (invitation === null ? "unavailable" : expired ? "expired" : "pending");
  const organization = invitation?.organization ?? null;

  if (state === "unavailable" || invitation === null || organization === null) {
    return (
      <>
        <AuthCardHeader
          icon="warning"
          title="This invitation isn't available"
          intro={<>No pending invitation with this link is addressed to <b>{principal.user.email}</b>. It may have been sent to another address, answered already, or revoked.</>}
          focusOnMount
        />
        <PrincipalSummary principal={principal} />
        {failure === null ? null : <AuthAlert copy={failure} />}
        <div className="signin-actions">
          {switchAccount}
          <Link className="button button--ghost button--block" href={DEFAULT_RETURN_TO}>Continue to Open Splunk</Link>
        </div>
      </>
    );
  }

  if (state === "expired") {
    return (
      <>
        <AuthCardHeader icon="clock" title="This invitation has expired" intro={<>The invitation to <b>{organization.name}</b> is no longer valid. Ask one of its administrators to invite you again.</>} focusOnMount />
        <Link className="button button--secondary button--block signin-action" href={DEFAULT_RETURN_TO}>Continue to Open Splunk</Link>
      </>
    );
  }

  if (state === "declined") {
    return (
      <>
        <AuthCardHeader icon="check" title="Invitation declined" intro={<>You will not join <b>{organization.name}</b>. Its administrators can invite you again later.</>} focusOnMount />
        <Link className="button button--secondary button--block signin-action" href={DEFAULT_RETURN_TO}>Continue to Open Splunk</Link>
      </>
    );
  }

  if (state === "accepted" || state === "already_member") {
    return (
      <>
        <AuthCardHeader
          icon="check"
          title={state === "accepted" ? `You joined ${organization.name}` : `You're already in ${organization.name}`}
          intro={state === "accepted" ? `You are a ${invitation.role} of the organization.` : "There was nothing to accept."}
          focusOnMount
        />
        {failure === null ? null : <AuthAlert copy={failure} />}
        <SubmitButton pending={action === "open"} pendingLabel={`Opening ${organization.name}…`} type="button" onClick={() => { void openOrganization(organization.id); }}>
          Open {organization.name}
        </SubmitButton>
      </>
    );
  }

  return (
    <>
      <AuthCardHeader icon="users" title={`Join ${organization.name}`} intro="You've been invited to an organization." focusOnMount />
      <dl className="signin-invitation">
        <div><dt>Organization</dt><dd>{organization.name}{organization.description === undefined ? null : <small>{organization.description}</small>}</dd></div>
        <div><dt>Role</dt><dd><span className="badge badge--info">{invitation.role}</span></dd></div>
        <div><dt>Sent to</dt><dd>{invitation.email}</dd></div>
        <div><dt>Expires</dt><dd><time dateTime={invitation.expiresAt}>{new Date(invitation.expiresAt).toLocaleDateString(undefined, { dateStyle: "medium" })}</time></dd></div>
      </dl>
      {failure === null ? null : <AuthAlert copy={failure} />}
      <div className="signin-actions">
        <SubmitButton pending={action === "accept"} pendingLabel="Accepting…" type="button" unavailable={action !== null} onClick={() => { void respond("accept"); }}>
          Accept invitation
        </SubmitButton>
        <SubmitButton pending={action === "decline"} pendingLabel="Declining…" type="button" unavailable={action !== null} variant="secondary" onClick={() => { void respond("decline"); }}>
          Decline
        </SubmitButton>
      </div>
      <p className="signin-step-meta">Signed in as {principal.user.email}.</p>
    </>
  );
}
