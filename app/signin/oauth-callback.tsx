"use client";

import Link from "next/link";
import { useEffect, useRef } from "react";

import type { OAuthCallbackParams, OAuthCallbackResult } from "@/lib/api/identity";

import { useIdentity } from "../_components/identity-provider";
import { AuthAlert, AuthCardHeader, AuthLoading, AuthNotice } from "./auth-frame";
import { describeAuthError } from "./auth-error-copy";
import { useOnce, type AuthNavigate } from "./auth-hooks";
import { safeReturnTo, signInHref } from "./return-to";
import { NoOrganizationCard, RedirectingCard, principalCanEnter } from "./sign-in-completion";

export interface OAuthCallbackProps {
  readonly params: OAuthCallbackParams;
  readonly navigate: AuthNavigate;
}

/** The query a provider redirect carries, read from the callback page's URL. */
export function oauthCallbackParams(query: URLSearchParams): OAuthCallbackParams {
  return {
    code: query.get("code"),
    state: query.get("state"),
    error: query.get("error"),
    errorDescription: query.get("error_description"),
  };
}

/**
 * `/auth/oauth/callback/`: exchanges the provider's code exactly once, then
 * continues to the path recorded when the flow began -- re-validated here,
 * since nothing about a redirect is trusted.
 */
export function OAuthCallback({ params, navigate }: OAuthCallbackProps) {
  const identity = useIdentity();
  const { client } = identity;
  const key = client === null ? null : JSON.stringify([params.state ?? "", params.code ?? "", params.error ?? ""]);
  const exchange = useOnce<OAuthCallbackResult>(key, () => {
    if (client === null) return Promise.reject(new Error("No identity client"));
    return client.completeOAuthCallback(params);
  });
  const result = exchange.result;
  const target = result === null ? "/" : safeReturnTo(result.returnTo);
  const canEnter = result !== null && principalCanEnter(result.principal, target);
  const handled = useRef<OAuthCallbackResult | null>(null);
  const { adoptPrincipal } = identity;

  useEffect(() => {
    if (result === null || handled.current === result) return;
    handled.current = result;
    adoptPrincipal(result.principal);
    if (canEnter) navigate(target);
  }, [adoptPrincipal, canEnter, navigate, result, target]);

  if (client === null) {
    return (
      <>
        <AuthCardHeader icon="warning" title="Sign-in is unavailable" focusOnMount />
        <AuthAlert copy={describeAuthError(identity.error, "session")} />
        <Link className="button button--secondary button--block signin-action" href={signInHref()}>Back to sign in</Link>
      </>
    );
  }

  if (!exchange.settled) {
    return (
      <>
        <AuthCardHeader title="Finishing sign-in" intro="Confirming your identity with the provider…" />
        <AuthLoading label="Finishing sign-in with your provider" />
      </>
    );
  }

  if (result === null) {
    const copy = describeAuthError(exchange.error, "oauth_callback");
    return (
      <>
        <AuthCardHeader icon="warning" title="Sign-in did not finish" intro="Nothing was changed. You can start again." focusOnMount />
        <AuthAlert copy={copy} />
        <Link className="button button--primary button--block signin-action" href={signInHref()}>Back to sign in</Link>
      </>
    );
  }

  if (!canEnter) {
    return (
      <NoOrganizationCard
        principal={result.principal}
        navigate={navigate}
        onSignOut={() => { void identity.signOut().then(() => navigate(signInHref())).catch(() => undefined); }}
      />
    );
  }

  return (
    <>
      <RedirectingCard principal={result.principal} target={target} />
      {result.accountCreated ? <AuthNotice title="Account created">Your Open Splunk account was created from your provider profile.</AuthNotice> : null}
    </>
  );
}
