export default function HomePage() {
  return (
    <main className="shell">
      <header className="topbar">
        <span className="brand">Open Splunk</span>
        <span className="status">Repository scaffold</span>
      </header>
      <section className="workspace">
        <p className="eyebrow">Search &amp; Reporting</p>
        <h1>The application foundation is ready.</h1>
        <p>
          Next comes the protobuf contracts, SRouter API, collector gRPC stream, and
          ClickHouse-backed SPL engine.
        </p>
      </section>
    </main>
  );
}

