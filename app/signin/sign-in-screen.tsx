import Link from "next/link";

interface SignInScreenProps {
  dataMode: "backend" | "demo";
}

export function SignInScreen({ dataMode }: SignInScreenProps) {
  const localSession = dataMode === "backend";
  return (
    <main className="signin-page">
      <section className="signin-story" aria-label="Open Splunk product introduction">
        <Link className="signin-wordmark" href="/" aria-label="Open Splunk home"><span>open</span><b>&gt;</b><span>splunk</span></Link>
        <div className="signin-story-copy">
          <span className="signin-kicker">{localSession ? "LOCAL SERVER" : "FRONTEND PREVIEW"}</span>
          <h1>{localSession ? "Open your local Splunk workspace." : "Explore the Open Splunk workspace."}</h1>
          <p>{localSession
            ? "This installation uses a trusted local single-user session. Browser authentication is not configured."
            : "Review the SPL workflow, operational pages, and responsive interface with deterministic sample data."}</p>
          <div className="signin-capabilities">
            <span><i>⌕</i><b>Event-first search</b><small>Investigate raw events and pivot instantly.</small></span>
            <span><i>⌁</i><b>Search analytics</b><small>Transform searches into tables and charts.</small></span>
            <span><i>▣</i><b>Self-hosted control</b><small>Keep collection and retention in your environment.</small></span>
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
        <footer><span className="signin-status-dot" /> {localSession ? "Local single-user mode · check backend health in Administration" : "Preview fixture · backend health is not checked here"}</footer>
      </section>

      <section className="signin-panel">
        <div className="signin-card">
          <div className="signin-mobile-brand"><span>open</span><b>&gt;</b><span>splunk</span></div>
          <header><span className="signin-lock">↳</span><h1>{localSession ? "Local session" : "Frontend preview"}</h1><p>{localSession ? "Access is provided by the trusted local server configuration." : "Authentication is not connected in this build."}</p></header>
          <div className="signin-help-notice" role="note"><span>i</span>{localSession ? "No credentials are requested or stored. User accounts and sign-out are unavailable in single-user mode." : "Do not enter credentials. This preview does not check or store passwords."}</div>
          <Link className="signin-submit" href="/" style={{ textDecoration: "none" }}>{localSession ? "Continue to local workspace" : "Continue to preview"}</Link>
          <div className="signin-divider"><span>or</span></div>
          <Link className="signin-preview-link" href="/search/">Open Search &amp; Reporting <span aria-hidden="true">›</span></Link>
          <footer><span>Open Splunk v0.1.0 {localSession ? "local server" : "preview"}</span><span>{localSession ? "Single-user access" : "Authentication unavailable"}</span></footer>
        </div>
      </section>
    </main>
  );
}
