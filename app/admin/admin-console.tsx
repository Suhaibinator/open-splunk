"use client";

import type { FormEvent } from "react";
import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";

import { searchLaunchHref } from "@/lib/search/launch-url";

import { PageHeading } from "../_components/product-shell";
import { Modal } from "../search-workspace/modal";

type AdminSection = "overview" | "indexes" | "collectors" | "access" | "server";

interface IndexRow {
  name: string;
  status: "Active" | "Paused";
  events: string;
  storage: string;
  retention: string;
  lastEvent: string;
}

const INITIAL_INDEXES: IndexRow[] = [
  { name: "gradethis", status: "Active", events: "18.6M", storage: "284 GB", retention: "30 days", lastEvent: "1 sec ago" },
  { name: "platform", status: "Active", events: "4.2M", storage: "91 GB", retention: "14 days", lastEvent: "3 sec ago" },
  { name: "internal", status: "Paused", events: "643K", storage: "12 GB", retention: "7 days", lastEvent: "2 hr ago" },
];

const NAV_ITEMS: Array<{ key: AdminSection; label: string; detail: string; icon: string }> = [
  { key: "overview", label: "System overview", detail: "Health and capacity", icon: "▥" },
  { key: "indexes", label: "Indexes", detail: "Storage and retention", icon: "▦" },
  { key: "collectors", label: "Data inputs", detail: "Collectors and tokens", icon: "⇣" },
  { key: "access", label: "Users & access", detail: "Roles and authentication", icon: "♙" },
  { key: "server", label: "Server settings", detail: "Limits and preferences", icon: "⚙" },
];

export function AdminConsole() {
  const [section, setSection] = useState<AdminSection>("overview");
  const [indexes, setIndexes] = useState(INITIAL_INDEXES);
  const [filter, setFilter] = useState("");
  const [modal, setModal] = useState<"index" | "token" | null>(null);
  const [indexName, setIndexName] = useState("");
  const [retention, setRetention] = useState("30");
  const [tokenName, setTokenName] = useState("");
  const [tokenSecret, setTokenSecret] = useState<string | null>(null);
  const [toast, setToast] = useState<string | null>(null);
  const [saving, setSaving] = useState(false);

  const filteredIndexes = useMemo(() => indexes.filter((item) => item.name.includes(filter.trim().toLowerCase())), [filter, indexes]);

  function openIndexDialog() {
    setIndexName("");
    setRetention("30");
    setModal("index");
  }

  function createIndex(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    const normalized = indexName.trim().toLowerCase().replace(/[^a-z0-9_-]+/g, "-");
    if (normalized.length === 0) {
      setToast("Use at least one letter or number for the index name.");
      return;
    }
    if (indexes.some((item) => item.name === normalized)) {
      setToast(`Index “${normalized}” already exists.`);
      return;
    }
    setIndexes((current) => [...current, { name: normalized, status: "Active", events: "0", storage: "0 B", retention: `${retention} days`, lastEvent: "No events" }]);
    setModal(null);
    setToast(`Preview only: simulated index “${normalized}” in this browser session.`);
  }

  function createToken(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    if (tokenName.trim().length === 0) return;
    const suffix = crypto.randomUUID().replaceAll("-", "").slice(0, 20);
    setTokenSecret(`ospl_demo_${suffix}`);
  }

  function closeTokenDialog() {
    setModal(null);
    setTokenSecret(null);
    setTokenName("");
  }

  function saveServerSettings(event: FormEvent<HTMLFormElement>) {
    event.preventDefault();
    setSaving(true);
    window.setTimeout(() => {
      setSaving(false);
      setToast("Preview only: settings were simulated in this browser session.");
    }, 500);
  }

  const primaryAction = section === "indexes"
    ? <button className="suite-button suite-button--primary" type="button" onClick={openIndexDialog}>＋ Simulate index</button>
    : section === "collectors"
      ? <button className="suite-button suite-button--primary" type="button" onClick={() => setModal("token")}>Generate demo token</button>
      : undefined;

  return (
    <div className="suite-page admin-page">
      <PageHeading eyebrow="SYSTEM" title="Administration" description="Preview data, access, and deployment management. Changes remain in this browser session." actions={primaryAction} />

      <div className="admin-mobile-section-picker">
        <label htmlFor="admin-section">Administration section</label>
        <select id="admin-section" value={section} onChange={(event) => setSection(event.target.value as AdminSection)}>
          {NAV_ITEMS.map((item) => <option value={item.key} key={item.key}>{item.label}</option>)}
        </select>
      </div>

      <div className="admin-layout">
        <aside className="admin-sidebar" aria-label="Administration navigation">
          <span className="admin-sidebar-label">SETTINGS</span>
          {NAV_ITEMS.map((item) => (
            <button className={section === item.key ? "active" : undefined} type="button" onClick={() => setSection(item.key)} key={item.key}>
              <i aria-hidden="true">{item.icon}</i><span><strong>{item.label}</strong><small>{item.detail}</small></span><b aria-hidden="true">›</b>
            </button>
          ))}
          <div className="admin-sidebar-meta"><span className="status-dot status-dot--healthy" /><div><strong>open-splunk.local</strong><small>v0.1.0 · Healthy</small></div></div>
        </aside>

        <section className="admin-content" aria-live="polite">
          {section === "overview" ? <OverviewSection onNavigate={setSection} /> : null}
          {section === "indexes" ? (
            <IndexesSection
              filter={filter}
              indexes={filteredIndexes}
              onFilterChange={setFilter}
              onToggle={(name) => {
                const selected = indexes.find((item) => item.name === name);
                if (selected === undefined) return;
                const nextStatus = selected.status === "Active" ? "Paused" : "Active";
                setIndexes((current) => current.map((item) => item.name === name ? { ...item, status: nextStatus } : item));
                setToast(`Preview only: index “${name}” ${nextStatus === "Active" ? "resumed" : "paused"} in this browser session.`);
              }}
            />
          ) : null}
          {section === "collectors" ? <CollectorsSection onCreateToken={() => setModal("token")} /> : null}
          {section === "access" ? <AccessSection /> : null}
          {section === "server" ? <ServerSection saving={saving} onSave={saveServerSettings} /> : null}
        </section>
      </div>

      {modal === "index" ? (
        <Modal
          title="Simulate index creation"
          subtitle="This preview adds an index to the local interface only; it does not change the server."
          onClose={() => setModal(null)}
          footer={<><button className="button secondary" type="button" onClick={() => setModal(null)}>Cancel</button><button className="button primary" type="submit" form="create-index-form" disabled={indexName.trim().length === 0}>Add preview index</button></>}
        >
          <form className="admin-form" id="create-index-form" onSubmit={createIndex}>
            <label htmlFor="new-index-name"><span>Index name</span><input id="new-index-name" value={indexName} onChange={(event) => setIndexName(event.target.value)} placeholder="application-logs" /><small>Lowercase letters, numbers, hyphens, and underscores.</small></label>
            <label htmlFor="new-index-retention"><span>Retention</span><select id="new-index-retention" value={retention} onChange={(event) => setRetention(event.target.value)}><option value="7">7 days</option><option value="14">14 days</option><option value="30">30 days</option><option value="90">90 days</option></select><small>Events older than this window are removed automatically.</small></label>
            <div className="access-mode-notice" role="note"><span>i</span><div><strong>Preview-only change</strong><p>The simulated index is searchable only as interface state and disappears when this page reloads.</p></div></div>
          </form>
        </Modal>
      ) : null}

      {modal === "token" ? (
        <Modal
          title={tokenSecret === null ? "Generate demo token" : "Demo token generated"}
          subtitle={tokenSecret === null ? "Preview the token workflow without creating a server credential." : "This value is illustrative and cannot authorize a collector."}
          onClose={closeTokenDialog}
          footer={tokenSecret === null ? <><button className="button secondary" type="button" onClick={closeTokenDialog}>Cancel</button><button className="button primary" type="submit" form="create-token-form" disabled={tokenName.trim().length === 0}>Generate demo value</button></> : <button className="button primary" type="button" onClick={closeTokenDialog}>Close</button>}
        >
          {tokenSecret === null ? (
            <form className="admin-form" id="create-token-form" onSubmit={createToken}>
              <label htmlFor="new-token-name"><span>Token name</span><input id="new-token-name" value={tokenName} onChange={(event) => setTokenName(event.target.value)} placeholder="prod-api-collector" /></label>
              <div className="access-mode-notice" role="note"><span>i</span><div><strong>Not a real credential</strong><p>The generated value is not stored, scoped to indexes, or accepted by collectors.</p></div></div>
            </form>
          ) : (
            <div className="token-reveal"><span className="token-warning-icon">i</span><p>Demo value only. Do not place it in a collector configuration; the server will reject it.</p><div><code>{tokenSecret}</code><button type="button" onClick={() => void navigator.clipboard.writeText(tokenSecret)}>Copy demo value</button></div></div>
          )}
        </Modal>
      ) : null}

      {toast === null ? null : <output className="toast toast-success"><span>✓</span><strong>{toast}</strong><button type="button" aria-label="Dismiss notification" onClick={() => setToast(null)}>×</button></output>}
    </div>
  );
}

function OverviewSection({ onNavigate }: { onNavigate: (section: AdminSection) => void }) {
  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>System overview</h2><p>Current health, storage, and administrative attention.</p></div><span className="status-label status-label--complete"><i />Operational</span></header>
      <div className="admin-summary-grid">
        <article><span className="summary-icon summary-icon--green">▦</span><div><small>Indexes</small><strong>3</strong><p>2 active · 1 paused</p></div><button type="button" onClick={() => onNavigate("indexes")}>Manage</button></article>
        <article><span className="summary-icon summary-icon--blue">⇣</span><div><small>Collectors</small><strong>2</strong><p>Both reporting normally</p></div><button type="button" onClick={() => onNavigate("collectors")}>Inspect</button></article>
        <article><span className="summary-icon summary-icon--violet">♙</span><div><small>Administrators</small><strong>1</strong><p>Preview persona</p></div><button type="button" onClick={() => onNavigate("access")}>Review</button></article>
        <article><span className="summary-icon summary-icon--orange">▰</span><div><small>Storage used</small><strong>387 GB</strong><p>38% of 1 TB</p></div><button type="button" onClick={() => onNavigate("server")}>Limits</button></article>
      </div>
      <div className="admin-overview-grid">
        <section className="suite-card"><header className="suite-card-header"><div><h3>Service health</h3><p>Core components and current latency.</p></div><button type="button">Refresh</button></header><ul className="health-service-list"><li><span className="status-dot status-dot--healthy" /><div><strong>Search API</strong><small>SRouter protobuf · p95 118 ms</small></div><b>Healthy</b></li><li><span className="status-dot status-dot--healthy" /><div><strong>ClickHouse</strong><small>1 node · 11 ms query ping</small></div><b>Healthy</b></li><li><span className="status-dot status-dot--healthy" /><div><strong>Control plane</strong><small>SQLite WAL · last checkpoint 2 min ago</small></div><b>Healthy</b></li><li><span className="status-dot status-dot--warning" /><div><strong>Export worker</strong><small>1 artifact awaiting cleanup</small></div><b className="warning-copy">Attention</b></li></ul></section>
        <section className="suite-card"><header className="suite-card-header"><div><h3>Administrative activity</h3><p>Recent configuration changes.</p></div><Link href="/activity/">Audit log</Link></header><ol className="admin-activity-list"><li><span>SU</span><div><strong>Search limits updated</strong><small>Administrator · 18 minutes ago</small></div></li><li><span>TK</span><div><strong>Ingestion token used</strong><small>prod-gradethis · 31 minutes ago</small></div></li><li><span>IX</span><div><strong>Index retention confirmed</strong><small>gradethis · Yesterday</small></div></li></ol></section>
      </div>
    </div>
  );
}

function IndexesSection({ filter, indexes, onFilterChange, onToggle }: { filter: string; indexes: IndexRow[]; onFilterChange: (value: string) => void; onToggle: (name: string) => void }) {
  const [openActions, setOpenActions] = useState<string | null>(null);
  const openActionWrapRef = useRef<HTMLDivElement>(null);
  const actionTriggerRefs = useRef(new Map<string, HTMLButtonElement>());

  useEffect(() => {
    if (openActions === null) return;

    function closeAndRestoreFocus() {
      const trigger = actionTriggerRefs.current.get(openActions as string);
      setOpenActions(null);
      window.requestAnimationFrame(() => trigger?.focus());
    }

    function handlePointerDown(event: PointerEvent) {
      if (!openActionWrapRef.current?.contains(event.target as Node)) setOpenActions(null);
    }

    function handleMenuKeys(event: KeyboardEvent) {
      if (event.key === "Escape") {
        event.preventDefault();
        closeAndRestoreFocus();
        return;
      }
      const items = Array.from(openActionWrapRef.current?.querySelectorAll<HTMLElement>('[role="menuitem"]') ?? []);
      if (items.length === 0) return;
      const currentIndex = items.indexOf(document.activeElement as HTMLElement);
      let nextIndex = currentIndex;
      if (event.key === "ArrowDown") nextIndex = currentIndex < 0 ? 0 : (currentIndex + 1) % items.length;
      else if (event.key === "ArrowUp") nextIndex = currentIndex < 0 ? items.length - 1 : (currentIndex - 1 + items.length) % items.length;
      else if (event.key === "Home") nextIndex = 0;
      else if (event.key === "End") nextIndex = items.length - 1;
      else return;
      event.preventDefault();
      items[nextIndex]?.focus();
    }

    document.addEventListener("pointerdown", handlePointerDown);
    document.addEventListener("keydown", handleMenuKeys);
    return () => {
      document.removeEventListener("pointerdown", handlePointerDown);
      document.removeEventListener("keydown", handleMenuKeys);
    };
  }, [openActions]);

  function toggleActions(name: string, trigger: HTMLButtonElement) {
    actionTriggerRefs.current.set(name, trigger);
    if (openActions === name) {
      setOpenActions(null);
      return;
    }
    setOpenActions(name);
    window.requestAnimationFrame(() => openActionWrapRef.current?.querySelector<HTMLElement>('[role="menuitem"]')?.focus());
  }

  function runToggleAction(name: string) {
    onToggle(name);
    setOpenActions(null);
    window.requestAnimationFrame(() => actionTriggerRefs.current.get(name)?.focus());
  }

  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>Indexes</h2><p>Logical data boundaries, retention, and search availability.</p></div><span>{indexes.length} indexes</span></header>
      <div className="resource-toolbar"><label><span className="sr-only">Filter indexes</span><i aria-hidden="true">⌕</i><input value={filter} onChange={(event) => onFilterChange(event.target.value)} placeholder="Filter indexes" /></label><button type="button" disabled title="Status filtering requires the management API">All statuses ▾</button><button type="button" disabled title="Column customization is not available in this preview">Columns ▾</button></div>
      <div className="suite-card resource-table-card"><div className="responsive-table-wrap"><table className="product-table admin-resource-table"><thead><tr><th scope="col">Name</th><th scope="col">Status</th><th scope="col">Events today</th><th scope="col">Storage</th><th scope="col">Retention</th><th scope="col">Last event</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead><tbody>{indexes.map((item) => <tr key={item.name}><td><Link className="resource-name" href={searchLaunchHref(`index=${item.name} | sort -_time`)} aria-label={`Search index ${item.name}`}><span aria-hidden="true">▦</span><div><strong>{item.name}</strong><small>index={item.name}</small></div></Link></td><td><span className={`status-label status-label--${item.status === "Active" ? "complete" : "neutral"}`}><i />{item.status}</span></td><td className="numeric-data">{item.events}</td><td>{item.storage}</td><td>{item.retention}</td><td>{item.lastEvent}</td><td><div className="resource-action-wrap" ref={openActions === item.name ? openActionWrapRef : undefined}><button ref={(element) => { if (element === null) actionTriggerRefs.current.delete(item.name); else actionTriggerRefs.current.set(item.name, element); }} className="row-overflow" type="button" aria-label={`Actions for ${item.name}`} aria-haspopup="menu" aria-expanded={openActions === item.name} onClick={(event) => toggleActions(item.name, event.currentTarget)}>•••</button>{openActions === item.name ? <div className="resource-action-menu" role="menu" aria-label={`Actions for ${item.name}`}><Link role="menuitem" href={searchLaunchHref(`index=${item.name} | sort -_time`)}>Search this index</Link><button role="menuitem" type="button" onClick={() => runToggleAction(item.name)}>{item.status === "Active" ? "Simulate pausing ingestion" : "Simulate resuming ingestion"}</button></div> : null}</div></td></tr>)}</tbody></table></div></div>
      <p className="resource-footnote">This preview shows logical index boundaries; server storage and access configuration are not changed here.</p>
    </div>
  );
}

function CollectorsSection({ onCreateToken }: { onCreateToken: () => void }) {
  return (
    <div className="admin-section-stack">
      <header className="admin-section-header"><div><h2>Data inputs</h2><p>Preview collector health, queue depth, and token workflows.</p></div><span className="status-label status-label--complete"><i />2 connected</span></header>
      <div className="collector-grid"><article className="collector-card"><header><span className="collector-host-icon">▣</span><div><strong>api-prod-03</strong><small>Linux · collector v0.1.0</small></div><span className="status-label status-label--complete"><i />Live</span></header><dl><div><dt>Last seen</dt><dd>1 sec ago</dd></div><div><dt>Queue depth</dt><dd>0 events</dd></div><div><dt>Throughput</dt><dd>214 evt/s</dd></div><div><dt>Destination</dt><dd>gradethis</dd></div></dl><footer><span><i style={{ width: "72%" }} /></span><small>72% of configured peak</small><button type="button">Details</button></footer></article><article className="collector-card"><header><span className="collector-host-icon">▣</span><div><strong>worker-prod-02</strong><small>Linux · collector v0.1.0</small></div><span className="status-label status-label--complete"><i />Live</span></header><dl><div><dt>Last seen</dt><dd>3 sec ago</dd></div><div><dt>Queue depth</dt><dd>42 events</dd></div><div><dt>Throughput</dt><dd>87 evt/s</dd></div><div><dt>Destination</dt><dd>platform</dd></div></dl><footer><span><i style={{ width: "36%" }} /></span><small>36% of configured peak</small><button type="button">Details</button></footer></article></div>
      <section className="suite-card token-section"><header className="suite-card-header"><div><h3>Ingestion tokens</h3><p>Sample records illustrating index-scoped credentials.</p></div><button type="button" onClick={onCreateToken}>Generate demo token</button></header><div className="responsive-table-wrap"><table className="product-table"><thead><tr><th scope="col">Name</th><th scope="col">Prefix</th><th scope="col">Allowed indexes</th><th scope="col">Last used</th><th scope="col">Status</th><th scope="col"><span className="sr-only">Actions</span></th></tr></thead><tbody><tr><td><strong>prod-gradethis</strong></td><td><code>ospl_ing_a73…</code></td><td>gradethis</td><td>1 sec ago</td><td><span className="status-label status-label--complete"><i />Active</span></td><td><button className="row-overflow" type="button" aria-label="Actions for prod-gradethis">•••</button></td></tr><tr><td><strong>platform-workers</strong></td><td><code>ospl_ing_2f1…</code></td><td>platform</td><td>3 sec ago</td><td><span className="status-label status-label--complete"><i />Active</span></td><td><button className="row-overflow" type="button" aria-label="Actions for platform-workers">•••</button></td></tr></tbody></table></div></section>
    </div>
  );
}

function AccessSection() {
  return (
    <div className="admin-section-stack"><header className="admin-section-header"><div><h2>Users &amp; access</h2><p>Preview accounts, roles, and index permissions.</p></div><button className="suite-button suite-button--primary" type="button" disabled>Invite user unavailable</button></header><div className="access-mode-notice"><span>i</span><div><strong>Authentication is not connected</strong><p>Administrator is a preview persona. The role model is illustrative and does not grant or enforce backend access.</p></div></div><section className="suite-card"><header className="suite-card-header"><div><h3>Administrators</h3><p>Sample users with full deployment access.</p></div><span>1 preview user</span></header><div className="user-row"><span className="user-avatar-large">A</span><div><strong>Administrator</strong><small>admin@localhost · Preview persona</small></div><span className="role-pill">admin</span><b>Fixture user</b><button className="row-overflow" type="button">•••</button></div></section><section className="suite-card"><header className="suite-card-header"><div><h3>Role permissions</h3><p>Illustrative permissions for the preview administrator.</p></div></header><div className="permission-grid"><span><i>✓</i><strong>Search all indexes</strong><small>Read and export indexed events</small></span><span><i>✓</i><strong>Manage data</strong><small>Indexes, collectors, and tokens</small></span><span><i>✓</i><strong>Manage knowledge</strong><small>Saved searches and dashboards</small></span><span><i>✓</i><strong>Administer system</strong><small>Limits, users, and diagnostics</small></span></div></section></div>
  );
}

function ServerSection({ saving, onSave }: { saving: boolean; onSave: (event: FormEvent<HTMLFormElement>) => void }) {
  return (
    <form className="admin-section-stack server-settings" onSubmit={onSave}><header className="admin-section-header"><div><h2>Server settings</h2><p>Preview search limits, locale, and result retention controls.</p></div><button className="suite-button suite-button--primary" type="submit" disabled={saving}>{saving ? "Applying…" : "Apply preview settings"}</button></header><section className="suite-card settings-group"><header><h3>Search behavior</h3><p>Preview defaults shown when a user creates a search job.</p></header><div className="settings-form-grid"><label><span>Default time range</span><select defaultValue="24h"><option value="15m">Last 15 minutes</option><option value="4h">Last 4 hours</option><option value="24h">Last 24 hours</option><option value="7d">Last 7 days</option></select><small>Users can override this for each search.</small></label><label><span>Maximum runtime</span><div className="input-with-unit"><input type="number" defaultValue="300" min="10" /><b>seconds</b></div><small>Long-running searches are canceled automatically.</small></label><label><span>Maximum result rows</span><input type="number" defaultValue="50000" min="1000" step="1000" /><small>Exports have a separate server-side limit.</small></label><label><span>Concurrent searches</span><input type="number" defaultValue="4" min="1" max="32" /><small>Maximum active jobs for this node.</small></label></div></section><section className="suite-card settings-group"><header><h3>Regional settings</h3><p>How dates and times are displayed in the browser.</p></header><div className="settings-form-grid"><label><span>Time zone</span><select defaultValue="America/Los_Angeles"><option>America/Los_Angeles</option><option>UTC</option><option>America/New_York</option></select></label><label><span>Week starts on</span><select defaultValue="sunday"><option value="sunday">Sunday</option><option value="monday">Monday</option></select></label></div></section><section className="suite-card danger-zone"><header><div><h3>Diagnostic bundle</h3><p>Diagnostic bundle generation is not connected in this preview.</p></div><button className="suite-button" type="button">Generate bundle</button></header></section></form>
  );
}
