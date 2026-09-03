"use client";

import Link from "next/link";
import { useRouter } from "next/navigation";
import { useEffect, useState } from "react";

import { OPEN_SPLUNK_BUILD_LABEL } from "@/lib/build-identity";
import {
  clearAdministratorBearerToken,
  hasAdministratorBearerToken,
  isValidAdministratorBearerToken,
  setAdministratorBearerToken,
} from "@/lib/api";

import { AppIcon } from "../_components/app-icon";
import { Wordmark } from "../_components/wordmark";

interface SignInScreenProps {
  dataMode: "backend" | "demo";
}

export function SignInScreen({ dataMode }: SignInScreenProps) {
  const localSession = dataMode === "backend";
  const router = useRouter();
  const [administratorToken, setAdministratorToken] = useState("");
  const [administratorSessionActive, setAdministratorSessionActive] = useState(false);
  const [showAdministratorToken, setShowAdministratorToken] = useState(false);
  const [tokenError, setTokenError] = useState<string | null>(null);

  useEffect(() => {
    const frame = window.requestAnimationFrame(() => {
      setAdministratorSessionActive(hasAdministratorBearerToken());
    });
    return () => window.cancelAnimationFrame(frame);
  }, []);

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
    setAdministratorSessionActive(true);
    router.push("/admin/");
  }

  function clearAdministratorSession(): void {
    clearAdministratorBearerToken();
    setAdministratorToken("");
    setTokenError(null);
    setAdministratorSessionActive(false);
  }

  return (
    <main className="signin-page">
      <section className="signin-story" aria-label="Open Splunk product introduction">
        <Wordmark href="/" size="hero" />
        <div className="signin-story-copy">
          <span className="signin-kicker">{localSession ? "LOCAL SERVER" : "FRONTEND PREVIEW"}</span>
          <h1>{localSession ? "Open your local Splunk workspace." : "Explore the Open Splunk workspace."}</h1>
          <p>{localSession
            ? "This installation uses a trusted local single-user session. Browser authentication is not configured."
            : "Review the SPL workflow, operational pages, and responsive interface with deterministic sample data."}</p>
          <div className="signin-capabilities">
            <span><i aria-hidden="true">⌕</i><b>Event-first search</b><small>Investigate raw events and pivot instantly.</small></span>
            <span><i aria-hidden="true">⌁</i><b>Search analytics</b><small>Transform searches into tables and charts.</small></span>
            <span><i aria-hidden="true">▣</i><b>Self-hosted control</b><small>Keep collection and retention in your environment.</small></span>
          </div>
        </div>
        <div className="signin-signal" aria-hidden="true">
          <span style={{ "--signal-height": "38%" } as React.CSSProperties} />
          <span style={{ "--signal-height": "58%" } as React.CSSProperties} />
          <span style={{ "--signal-height": "45%" } as React.CSSProperties} />
          <span style={{ "--signal-height": "76%" } as React.CSSProperties} />
          <span style={{ "--signal-height": "52%" } as React.CSSProperties} />
          <span style={{ "--signal-height": "88%" } as React.CSSProperties} />
          <span style={{ "--signal-height": "63%" } as React.CSSProperties} />
          <span style={{ "--signal-height": "70%" } as React.CSSProperties} />
          <span style={{ "--signal-height": "48%" } as React.CSSProperties} />
          <span style={{ "--signal-height": "82%" } as React.CSSProperties} />
        </div>
        <footer>{localSession ? "Local access · check backend health in Administration" : "Preview workspace · backend health is not checked here"}</footer>
      </section>

      <section className="signin-panel">
        <div className="signin-card">
          <Wordmark className="signin-mobile-brand" href="/" size="hero" />
          <header><span className="signin-lock" aria-hidden="true">↳</span><h1>{localSession ? "Administrator session" : "Frontend preview"}</h1><p>{localSession ? "Use the bearer token configured for this local server." : "Authentication is not connected in this build."}</p></header>
          {localSession ? (
            administratorSessionActive ? (
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
            )
          ) : (
            <>
              <div className="signin-help-notice" role="note"><span aria-hidden="true"><AppIcon name="info" size="sm" /></span>Do not enter credentials. This preview does not check or store passwords.</div>
              <Link className="signin-submit" href="/">Continue to preview</Link>
            </>
          )}
          <div className="signin-divider"><span>or</span></div>
          <Link className="signin-preview-link" href="/search/events/">Open Search &amp; Reporting <AppIcon name="chevron-right" size="xs" /></Link>
          <footer><span>Open Splunk {OPEN_SPLUNK_BUILD_LABEL} · {localSession ? "local server" : "preview"}</span><span>{localSession ? "Memory-only bearer session" : "Authentication unavailable"}</span></footer>
        </div>
      </section>
    </main>
  );
}
