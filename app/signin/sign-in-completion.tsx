"use client";

import Link from "next/link";
import { useMemo } from "react";

import type { OrganizationInvitation, Principal } from "@/lib/api/identity";
import { userDisplayName, userInitials } from "@/lib/product-access";

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

/**
 * The name the card greets a principal by. The shell's own helper, so the
 * card and the product bar never disagree about who is signed in.
 */
export function displayNameOf(principal: Principal): string {
  return userDisplayName(principal.user);
}

/**
 * Who is signed in, as the card shows it. Each name and address is
 * bidi-isolated: an Arabic or Hebrew value otherwise takes its neighbours'
 * punctuation into its own run and reads back to front.
 */
export function PrincipalSummary({ principal }: { readonly principal: Principal }) {
  return (
    <div className="signin-identity">
      <span className="signin-identity-mark" aria-hidden="true">{userInitials(principal.user)}</span>
      <span>
        <b><bdi>{displayNameOf(principal)}</bdi></b>
        <small><bdi>{principal.user.email}</bdi></small>
        {principal.activeOrganization === null ? null : (
          <small><bdi>{principal.activeOrganization.name}</bdi>{principal.siteAdmin ? " · site administrator" : ""}</small>
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
                <b><bdi>{invitation.organization.name}</bdi></b>
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
