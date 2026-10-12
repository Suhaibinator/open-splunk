"use client";

import Link from "next/link";
import { useSearchParams } from "next/navigation";
import { useEffect, useState } from "react";
import { createIdentityClient, finishGoogleSignIn } from "@/lib/api/identity-client";

export function GoogleCallback({ apiBaseUrl }: { apiBaseUrl: string }) {
  const parameters = useSearchParams();
  const [error, setError] = useState<string | null>(null);
  const code = parameters.get("code") ?? "";
  const state = parameters.get("state") ?? "";
  const providerError = parameters.get("error");
  useEffect(() => {
    if (providerError) return;
    let current = true;
    void finishGoogleSignIn(createIdentityClient(apiBaseUrl), code, state).then((destination) => { if (current) window.location.replace(destination); }).catch((reason: unknown) => { if (current) setError(reason instanceof Error ? reason.message : "Google sign-in failed."); });
    return () => { current = false; };
  }, [code, state, providerError, apiBaseUrl]);
  const displayedError = providerError ? "Google sign-in was not completed." : error;
  return <main className="signin-page"><section className="signin-card"><h1>Signing in with Google</h1>{displayedError ? <><p role="alert">{displayedError}</p><Link className="button" href="/signin/">Return to sign in</Link></> : <p role="status">Completing your sign-in…</p>}</section></main>;
}
