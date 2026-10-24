"use client";

import Link from "next/link";
import type { ReactNode } from "react";

import { IdentityError, type IdentityClient } from "@/lib/api/identity";
import type { SearchDataMode } from "@/lib/search/backend-data";

import { IdentityProvider, useIdentity } from "../_components/identity-provider";
import { AuthAlert, AuthCardHeader, AuthFrame, AuthLoading } from "./auth-frame";
import { describeAuthError } from "./auth-error-copy";
import { useLocationQuery, type AuthNavigate } from "./auth-hooks";
import { prefillEmail } from "./email-address";
import { EmailVerification } from "./email-verification";
import { OAuthCallback, oauthCallbackParams } from "./oauth-callback";
import { OrganizationInvitationFlow } from "./organization-invitation";
import { PasswordResetRequest } from "./password-reset-request";
import { passwordResetRequestHref, signInHref } from "./return-to";
import { TokenPasswordFlow } from "./token-password-flow";
import { useAuthNavigate } from "./use-auth-navigate";

/**
 * The route components behind every sign-in page but `/signin/` itself. Each
 * is a client page in the static export: it mounts identity for its data
 * mode, waits for the query string (a static page is rendered without one),
 * then hands the flow what it needs.
 */

export interface AuthRouteProps {
  readonly dataMode: SearchDataMode;
}

/** `AuthRoute` renders a flow only once it has a client; this states that for the type checker. */
function requireClient(client: IdentityClient | null): IdentityClient {
  if (client === null) throw new IdentityError("not_configured", "Account sign-in is not configured on this server.");
  return client;
}

interface RouteContext {
  readonly query: URLSearchParams;
  readonly navigate: AuthNavigate;
  readonly demo: boolean;
}

function AuthRoute({ dataMode, loadingTitle, needsClient = true, children }: AuthRouteProps & {
  readonly loadingTitle: string;
  /** False for a flow that renders its own unavailable state. */
  readonly needsClient?: boolean;
  readonly children: (context: RouteContext) => ReactNode;
}) {
  return (
    <IdentityProvider dataMode={dataMode}>
      <AuthFrame footerNote={dataMode === "backend" ? "Signed-in session" : "Preview directory · sample accounts"}>
        <AuthRouteBody dataMode={dataMode} loadingTitle={loadingTitle} needsClient={needsClient}>{children}</AuthRouteBody>
      </AuthFrame>
    </IdentityProvider>
  );
}

function AuthRouteBody({ dataMode, loadingTitle, needsClient, children }: AuthRouteProps & {
  readonly loadingTitle: string;
  readonly needsClient: boolean;
  readonly children: (context: RouteContext) => ReactNode;
}) {
  const identity = useIdentity();
  const query = useLocationQuery();
  const navigate = useAuthNavigate();
  if (needsClient && identity.client === null) {
    return (
      <>
        <AuthCardHeader icon="warning" title="Sign-in is not available here" intro="This server does not have account sign-in configured." focusOnMount />
        <AuthAlert copy={describeAuthError(identity.error, "session")} />
        <Link className="button button--secondary button--block signin-action" href={signInHref()}>Go to sign in</Link>
      </>
    );
  }
  if (query === null) {
    return (
      <>
        <AuthCardHeader title={loadingTitle} />
        <AuthLoading label={`Loading ${loadingTitle.toLowerCase()}`} />
      </>
    );
  }
  return <>{children({ query, navigate, demo: dataMode === "demo" })}</>;
}

/** `/signin/reset/` */
export function PasswordResetRequestRoute({ dataMode }: AuthRouteProps) {
  return (
    <AuthRoute dataMode={dataMode} loadingTitle="Reset your password">
      {({ query, demo }) => <PasswordResetRequest demo={demo} initialEmail={prefillEmail(query.get("email"))} />}
    </AuthRoute>
  );
}

/** `/auth/email/reset/?token=` -- go-common's `PasswordResetPath`. */
export function PasswordResetCompleteRoute({ dataMode }: AuthRouteProps) {
  return (
    <AuthRoute dataMode={dataMode} loadingTitle="Choose a new password">
      {({ query }) => <PasswordResetCompletion token={query.get("token")} />}
    </AuthRoute>
  );
}

function PasswordResetCompletion({ token }: { readonly token: string | null }) {
  const { client } = useIdentity();
  return (
    <TokenPasswordFlow
      flow="reset_complete"
      intro="Choose a password you have not used here before. Other signed-in sessions are signed out."
      pendingLabel="Saving…"
      recovery={{ href: passwordResetRequestHref(), label: "Request a new link" }}
      run={(verificationCode, newPassword) => requireClient(client).completePasswordReset({ verificationCode, newPassword })}
      submitLabel="Save new password"
      successDetail="Your password was changed."
      successTitle="Password updated"
      title="Choose a new password"
      token={token}
    />
  );
}

/** `/auth/email/accept-invitation/?token=` -- go-common's `AccountInvitationPath`. */
export function AccountInvitationRoute({ dataMode }: AuthRouteProps) {
  return (
    <AuthRoute dataMode={dataMode} loadingTitle="Set up your account">
      {({ query }) => <AccountInvitation token={query.get("token")} />}
    </AuthRoute>
  );
}

function AccountInvitation({ token }: { readonly token: string | null }) {
  const { client } = useIdentity();
  return (
    <TokenPasswordFlow
      flow="account_invitation"
      intro="An administrator created an Open Splunk account for you. Choose a password to finish setting it up."
      pendingLabel="Setting up…"
      run={async (invitationToken, password) => {
        await requireClient(client).acceptInvitation({ kind: "account", token: invitationToken, password });
      }}
      submitLabel="Set password"
      successDetail="Your account is ready."
      successTitle="Account ready"
      title="Set up your account"
      token={token}
    />
  );
}

/** `/auth/email/verify/?token=` -- go-common's `EmailVerificationPath`. */
export function EmailVerificationRoute({ dataMode }: AuthRouteProps) {
  return (
    <AuthRoute dataMode={dataMode} loadingTitle="Verifying your email" needsClient={false}>
      {({ query, navigate }) => <EmailVerification navigate={navigate} token={query.get("token")} />}
    </AuthRoute>
  );
}

/** `/auth/oauth/callback/` -- the redirect URI registered with each provider. */
export function OAuthCallbackRoute({ dataMode }: AuthRouteProps) {
  return (
    <AuthRoute dataMode={dataMode} loadingTitle="Finishing sign-in" needsClient={false}>
      {({ query, navigate }) => <OAuthCallback navigate={navigate} params={oauthCallbackParams(query)} />}
    </AuthRoute>
  );
}

/** `/auth/invitation/?id=` -- an organization invitation. */
export function OrganizationInvitationRoute({ dataMode }: AuthRouteProps) {
  return (
    <AuthRoute dataMode={dataMode} loadingTitle="Your invitation" needsClient={false}>
      {({ query, navigate, demo }) => (
        <OrganizationInvitationFlow
          demo={demo}
          invitationId={query.get("id")}
          invitedEmail={prefillEmail(query.get("email"))}
          navigate={navigate}
        />
      )}
    </AuthRoute>
  );
}
