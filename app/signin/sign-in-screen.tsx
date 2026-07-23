import Link from "next/link";

export function SignInScreen() {
  return (
    <main className="signin-page">
      <section className="signin-story" aria-label="Open Splunk product introduction">
        <Link className="signin-wordmark" href="/" aria-label="Open Splunk home"><span>open</span><b>&gt;</b><span>splunk</span></Link>
        <div className="signin-story-copy">
          <span className="signin-kicker">FRONTEND PREVIEW</span>
          <h1>Explore the Open Splunk workspace.</h1>
          <p>Review the SPL workflow, operational pages, and responsive interface with deterministic sample data.</p>
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
        <footer><span className="signin-status-dot" /> Preview fixture · backend health is not checked here</footer>
      </section>

      <section className="signin-panel">
        <div className="signin-card">
          <div className="signin-mobile-brand"><span>open</span><b>&gt;</b><span>splunk</span></div>
          <header><span className="signin-lock">↳</span><h1>Frontend preview</h1><p>Authentication is not connected in this build.</p></header>
          <div className="signin-help-notice" role="note"><span>i</span>Do not enter credentials. This preview does not check or store passwords.</div>
          <Link className="signin-submit" href="/" style={{ textDecoration: "none" }}>Continue to preview</Link>
          <div className="signin-divider"><span>or</span></div>
          <Link className="signin-preview-link" href="/search/">Open Search &amp; Reporting <span aria-hidden="true">›</span></Link>
          <footer><span>Open Splunk v0.1.0 preview</span><span>Authentication unavailable</span></footer>
        </div>
      </section>
    </main>
  );
}
