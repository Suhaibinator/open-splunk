"use client";

import Link from "next/link";
import { useEffect, useRef } from "react";

import type { Principal } from "@/lib/api/identity";

import { useIdentity } from "../_components/identity-provider";
import { AuthAlert, AuthCardHeader, AuthLoading, AuthNotice } from "./auth-frame";
import { describeAuthError } from "./auth-error-copy";
import { useOnce, type AuthNavigate } from "./auth-hooks";
import { DEFAULT_RETURN_TO, signInHref, takePendingReturn } from "./return-to";
import { NoOrganizationCard, RedirectingCard, principalCanEnter } from "./sign-in-completion";

export interface EmailVerificationProps {
  readonly token: string | null;
  readonly navigate: AuthNavigate;
}

interface Verified {
  readonly principal: Principal | null;
  readonly target: string;
}

/**
 * `/auth/email/verify/`: the link go-common emails after registration. It
 * verifies once, which normally signs the user in, then resumes the page that
 * started the registration (an invitation, usually).
 */
export function EmailVerification({ token, navigate }: EmailVerificationProps) {
  const identity = useIdentity();
  const { client, adoptPrincipal } = identity;
  const key = client === null || token === null || token.length === 0 ? null : token;
  const verification = useOnce<Verified>(key, async () => {
    if (client === null || token === null) throw new Error("No identity client");
    const principal = await client.completeEmailVerification(token);
    return { principal, target: takePendingReturn() ?? DEFAULT_RETURN_TO };
  });
  const result = verification.result;
  const principal = result?.principal ?? null;
  const target = result?.target ?? DEFAULT_RETURN_TO;
  const canEnter = principal !== null && principalCanEnter(principal, target);
  const handled = useRef<Verified | null>(null);

  useEffect(() => {
    if (result === null || handled.current === result) return;
    handled.current = result;
    if (result.principal === null) return;
    adoptPrincipal(result.principal);
    // The verification token is spent: Back must not land on it and replay it.
    if (canEnter) navigate(target, { replace: true });
  }, [adoptPrincipal, canEnter, navigate, result, target]);

  if (token === null || token.length === 0) {
    return (
      <>
        <AuthCardHeader icon="warning" title="This link is incomplete" intro="Open the verification link from your email again." focusOnMount />
        <Link className="button button--secondary button--block signin-action" href={signInHref()}>Back to sign in</Link>
      </>
    );
  }

  if (client === null) {
    return (
      <>
        <AuthCardHeader icon="warning" title="Verification is unavailable" focusOnMount />
        <AuthAlert copy={describeAuthError(identity.error, "session")} />
      </>
    );
  }

  if (!verification.settled) {
    return (
      <>
        <AuthCardHeader title="Verifying your email" intro="This takes a moment." />
        <AuthLoading label="Verifying your email address" />
      </>
    );
  }

  if (result === null) {
    const copy = describeAuthError(verification.error, "verify_email");
    return (
      <>
        <AuthCardHeader icon="warning" title={copy.title} intro={copy.detail} focusOnMount />
        {copy.deadLink ? null : <AuthAlert copy={copy} />}
        <Link className="button button--primary button--block signin-action" href={signInHref()}>Go to sign in</Link>
      </>
    );
  }

  if (principal === null) {
    return (
      <>
        <AuthCardHeader icon="check" title="Email verified" intro="Your address is confirmed. Sign in to continue." focusOnMount />
        <Link className="button button--primary button--block signin-action" href={signInHref({ returnTo: target })}>Sign in</Link>
      </>
    );
  }

  if (!canEnter) {
    return (
      <NoOrganizationCard
        principal={principal}
        navigate={navigate}
        onSignOut={() => { void identity.signOut().then(() => navigate(signInHref(), { replace: true })).catch(() => undefined); }}
      />
    );
  }

  return (
    <>
      <RedirectingCard principal={principal} target={target} />
      <AuthNotice tone="success" title="Email verified">Your address is confirmed.</AuthNotice>
    </>
  );
}
