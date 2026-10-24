"use client";

import Link from "next/link";
import { useMemo } from "react";

import type { OrganizationInvitation, Principal } from "@/lib/api/identity";

import { AppIcon } from "../_components/app-icon";
import { AuthAlert, AuthCardHeader, AuthLoading, AuthNotice } from "./auth-frame";
import { describeAuthError, type AuthErrorCopy } from "./auth-error-copy";
import { useResource, type AuthNavigate } from "./auth-hooks";
import { ORGANIZATION_INVITATION_PATH, organizationInvitationHref } from "./return-to";
import { useIdentity } from "../_components/identity-provider";

/**
 * Where a completed sign-in goes. A principal with somewhere to work --
 * an organization, or the site-admin flag -- goes straight to `returnTo`; one
 * with neither would land on pages that refuse it, so it stops here and sees
 * its pending invitations instead (or is told to ask for one).
 */
export function principalCanEnter(principal: Principal, returnTo: string): boolean {
  return principal.siteAdmin
    || principal.organizations.length > 0
    || returnTo.startsWith(ORGANIZATION_INVITATION_PATH);
}

export function displayNameOf(principal: Principal): string {
  const { firstName, lastName, username, email } = principal.user;
  const name = `${firstName} ${lastName}`.trim();
  return name.length > 0 ? name : username.length > 0 ? username : email;
}

function initialsOf(principal: Principal): string {
  const words = displayNameOf(principal).split(/\s+/u).filter(Boolean);
  return words.slice(0, 2).map((word) => word[0]?.toUpperCase() ?? "").join("") || "?";
}

/** Who is signed in, as the card shows it. */
export function PrincipalSummary({ principal }: { readonly principal: Principal }) {
  return (
    <div className="signin-identity">
      <span className="signin-identity-mark" aria-hidden="true">{initialsOf(principal)}</span>
      <span>
        <b>{displayNameOf(principal)}</b>
        <small>{principal.user.email}</small>
        {principal.activeOrganization === null ? null : (
          <small>{principal.activeOrganization.name}{principal.siteAdmin ? " · site administrator" : ""}</small>
        )}
      </span>
    </div>
  );
}

/** The card after a sign-in succeeded and the browser is on its way. */
export function RedirectingCard({ principal, target }: { readonly principal: Principal; readonly target: string }) {
  return (
    <>
      <AuthCardHeader icon="check" title={`Welcome, ${displayNameOf(principal)}`} intro="You are signed in." focusOnMount />
      <AuthNotice tone="success" title="Opening Open Splunk…">
        If nothing happens, <Link href={target}>continue</Link>.
      </AuthNotice>
    </>
  );
}

type InvitationsState =
  | { readonly status: "loading" }
  | { readonly status: "ready"; readonly invitations: readonly OrganizationInvitation[] }
  | { readonly status: "error"; readonly copy: AuthErrorCopy };

/**
 * The card for a signed-in user with no organization: their pending
 * invitations, each a link to accept it, or the advice to ask for one.
 */
export function NoOrganizationCard({ principal, navigate, onSignOut }: {
  readonly principal: Principal;
  readonly navigate: AuthNavigate;
  readonly onSignOut: () => void;
}) {
  const { client } = useIdentity();
  const load = useMemo(() => (client === null ? null : () => client.listMyInvitations()), [client]);
  const { resource, retry } = useResource(load);
  const state: InvitationsState = resource.status === "ready"
    ? { status: "ready", invitations: resource.value.items }
    : resource.status === "error"
      ? { status: "error", copy: describeAuthError(resource.error, "invitations") }
      : { status: "loading" };

  return (
    <>
      <AuthCardHeader
        icon="users"
        title="You're not in an organization yet"
        intro="Searches, dashboards and data belong to an organization. Join one to start."
        focusOnMount
      />
      <PrincipalSummary principal={principal} />
      {state.status === "loading" ? <AuthLoading label="Loading your invitations" /> : null}
      {state.status === "error" ? (
        <AuthAlert
          copy={state.copy}
          action={state.copy.kind === "unauthenticated" ? (
            // The session ended after sign-in; reading again would only fail again.
            <button className="button button--secondary button--compact" type="button" onClick={onSignOut}>
              Sign in again
            </button>
          ) : (
            <button className="button button--secondary button--compact" type="button" onClick={retry}>
              Try again
            </button>
          )}
        />
      ) : null}
      {state.status === "ready" && state.invitations.length === 0 ? (
        <AuthNotice title="No pending invitations">
          Ask an administrator of your organization to invite {principal.user.email}.
        </AuthNotice>
      ) : null}
      {state.status === "ready" && state.invitations.length > 0 ? (
        <ul className="signin-invitation-list" aria-label="Pending invitations">
          {state.invitations.map((invitation) => (
            <li key={invitation.id}>
              <span>
                <b>{invitation.organization.name}</b>
                <small>Invited as {invitation.role}</small>
              </span>
              <button
                className="button button--primary button--compact"
                type="button"
                onClick={() => navigate(organizationInvitationHref(invitation.id, principal.user.email))}
              >
                Review <AppIcon name="chevron-right" size="xs" />
              </button>
            </li>
          ))}
        </ul>
      ) : null}
      <div className="signin-actions">
        <button className="button button--ghost button--block" type="button" onClick={onSignOut}>Sign in with a different account</button>
      </div>
    </>
  );
}
