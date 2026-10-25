"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useState, useSyncExternalStore } from "react";

import { OPEN_SPLUNK_BUILD_LABEL } from "@/lib/build-identity";
import {
  clearAdministratorBearerToken,
  hasAdministratorBearerToken,
  isValidAdministratorBearerToken,
  setAdministratorBearerToken,
} from "@/lib/api";

import { AppIcon } from "../_components/app-icon";
import { Wordmark } from "../_components/wordmark";
import { SignInStory } from "./auth-frame";

function subscribeToAdministratorSession(): () => void {
  return () => undefined;
}

export interface AdministratorSessionScreenProps {
  /** Offered when the server also has account sign-in. */
  readonly onUseAccount?: () => void;
}

/**
 * The connected server's memory-only administrator bearer session. It is the
 * whole sign-in page while account identity is not configured, and an
 * alternative to it once it is.
 */
export function AdministratorSessionScreen({ onUseAccount }: AdministratorSessionScreenProps) {
  const router = useRouter();
  const [administratorToken, setAdministratorToken] = useState("");
  const currentAdministratorSessionActive = useSyncExternalStore(
    subscribeToAdministratorSession,
    hasAdministratorBearerToken,
    () => false,
  );
  const [administratorSessionOverride, setAdministratorSessionOverride] = useState<boolean | null>(null);
  const administratorSessionActive = administratorSessionOverride ?? currentAdministratorSessionActive;
  const [showAdministratorToken, setShowAdministratorToken] = useState(false);
  const [tokenError, setTokenError] = useState<string | null>(null);

  function openAdministratorSession(event: React.FormEvent<HTMLFormElement>): void {
    event.preventDefault();
    const token = administratorToken.trim();
    if (!isValidAdministratorBearerToken(token)) {
      setTokenError("Enter the 32–512 character administrator bearer token configured on this server.");
      return;
    }
    setAdministratorBearerToken(token);
    setAdministratorToken("");
    setTokenError(null);
    setAdministratorSessionOverride(true);
    router.push("/admin/");
  }

  function clearAdministratorSession(): void {
    clearAdministratorBearerToken();
    setAdministratorToken("");
    setTokenError(null);
    setAdministratorSessionOverride(false);
  }

  return (
    <main className="signin-page">
      <SignInStory
        kicker="LOCAL SERVER"
        headline="Open your local Splunk workspace."
        intro={onUseAccount === undefined
          ? "This installation uses a trusted local single-user session. Browser authentication is not configured."
          : "Administrators can open a memory-only session with the server's bearer token."}
        footer="Local access · check backend health in Administration"
      />

      <section className="signin-panel">
        <div className="signin-card">
          <Wordmark className="signin-mobile-brand" href="/" size="hero" />
          <header><span className="signin-lock" aria-hidden="true">↳</span><h1>Administrator session</h1><p>Use the bearer token configured for this local server.</p></header>
          {administratorSessionActive ? (
            <>
              <div className="signin-help-notice" role="note"><span aria-hidden="true"><AppIcon name="check" size="sm" /></span>The administrator credential is available to protected API calls in this tab.</div>
              <button className="signin-submit" type="button" onClick={() => router.push("/admin/")}>Open Administration</button>
              <button className="signin-preview-link" type="button" onClick={clearAdministratorSession}>Clear administrator session</button>
            </>
          ) : (
            <form onSubmit={openAdministratorSession}>
              <label htmlFor="administrator-token">Administrator bearer token</label>
              <div className="signin-password-field" data-invalid={tokenError !== null}>
                <input
                  id="administrator-token"
                  name="administrator-token"
                  type={showAdministratorToken ? "text" : "password"}
                  value={administratorToken}
                  onChange={(event) => { setAdministratorToken(event.target.value); setTokenError(null); }}
                  autoComplete="off"
                  autoCapitalize="none"
                  spellCheck={false}
                  aria-invalid={tokenError !== null}
                  aria-describedby={tokenError === null ? "administrator-token-lifetime" : "administrator-token-error administrator-token-lifetime"}
                />
                <button type="button" onClick={() => setShowAdministratorToken((current) => !current)}>{showAdministratorToken ? "Hide" : "Show"}</button>
              </div>
              {tokenError === null ? null : <div id="administrator-token-error" className="signin-error" role="alert"><span aria-hidden="true"><AppIcon name="warning" size="sm" /></span>{tokenError}</div>}
              <div id="administrator-token-lifetime" className="signin-help-notice" role="note"><span aria-hidden="true"><AppIcon name="info" size="sm" /></span>The token stays in memory only. Reloading, closing this tab, or opening a new tab clears it; the server verifies it on the next protected request.</div>
              <button className="signin-submit" type="submit">Open administrator session</button>
            </form>
          )}
          <div className="signin-divider"><span>or</span></div>
          {onUseAccount === undefined
            ? <Link className="signin-preview-link" href="/search/events/">Open Search &amp; Reporting <AppIcon name="chevron-right" size="xs" /></Link>
            : <button className="signin-preview-link" type="button" onClick={onUseAccount}>Sign in with your account instead</button>}
          <footer><span>Open Splunk {OPEN_SPLUNK_BUILD_LABEL} · local server</span><span>Memory-only bearer session</span></footer>
        </div>
      </section>
    </main>
  );
}
