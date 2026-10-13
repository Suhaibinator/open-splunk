"use client";

import Link from "next/link";
import { useState } from "react";
import { createIdentityClient, startGoogleSignIn } from "@/lib/api/identity-client";
import { useIdentity, IdentityControls } from "../_components/identity-provider";
import { Wordmark } from "../_components/wordmark";

export function SignInScreen({ dataMode, apiBaseUrl = "" }: { dataMode: "backend" | "demo"; apiBaseUrl?: string }) {
  const identity = useIdentity();
  const [pending, setPending] = useState(false);
  const [error, setError] = useState<string | null>(null);
  async function signIn() {
    setPending(true); setError(null);
    try {
      const returnUrl = new URLSearchParams(window.location.search).get("returnUrl");
      await startGoogleSignIn(createIdentityClient(apiBaseUrl), returnUrl ?? "/");
    } catch (reason) {
      setError(reason instanceof Error ? reason.message : "Google sign-in failed."); setPending(false);
    }
  }
  return <main className="signin-page">
    <section className="signin-story" aria-label="Open Splunk product introduction"><Wordmark href="/" size="hero" /><div className="signin-story-copy"><span className="signin-kicker">SEARCH AND EXPLORE</span><h1>Open your Splunk workspace.</h1><p>Search your organization’s events and manage access with your account.</p><div className="signin-capabilities"><span><b>Search</b><small>Explore your events</small></span><span><b>Organize</b><small>Work across indexes</small></span><span><b>Collaborate</b><small>Control access by organization</small></span></div></div></section>
    <section className="signin-panel"><div className="signin-card"><Wordmark className="signin-mobile-brand" href="/" size="hero" /><header><div className="signin-lock" aria-hidden="true">↗</div><h1>Sign in</h1><p>Use your Google account to continue.</p></header>
      {dataMode === "demo" ? <><p>This frontend preview does not authenticate accounts.</p><Link className="signin-submit" href="/">Continue to preview</Link></> : identity.user ? <><p>Signed in as {identity.user.email}</p><Link className="signin-submit" href="/">Open workspace</Link><IdentityControls apiBaseUrl={apiBaseUrl} /></> : <><button className="signin-submit" type="button" disabled={pending} aria-busy={pending} onClick={() => { void signIn(); }}>{pending ? "Opening Google…" : "Continue with Google"}</button><p className="signin-help-notice">Signing in creates an account. Access begins when an organization administrator adds you.</p></>}
      {error ? <p className="signin-error" role="alert">{error}</p> : null}<div className="signin-divider"><span>Need help?</span></div><Link className="signin-preview-link" href="/help/">Read the documentation <span aria-hidden="true">↗</span></Link>
    </div></section>
  </main>;
}
