"use client";

import type { FormEvent, KeyboardEvent as ReactKeyboardEvent, ReactNode } from "react";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";

import type { SearchDataMode } from "@/lib/search/backend-data";
import { searchLaunchHref, splFromFindInput } from "@/lib/search/launch-url";

import { installModalSurface } from "./modal-surface";

type ProductSection = "home" | "search" | "analytics" | "datasets" | "reports" | "dashboards" | "activity" | "admin";

interface ProductShellProps {
  activeSection: ProductSection;
  appName: string;
  children: ReactNode;
  dataMode: SearchDataMode;
}

const PRIMARY_NAV: Array<{ key: ProductSection; label: string; href: string }> = [
  { key: "search", label: "Search", href: "/search/" },
  { key: "analytics", label: "Analytics", href: "/analytics/" },
  { key: "datasets", label: "Datasets", href: "/datasets/" },
  { key: "reports", label: "Reports", href: "/reports/" },
  { key: "activity", label: "Activity", href: "/activity/" },
  { key: "dashboards", label: "Dashboards", href: "/dashboards/" },
];

function submitProductFind(event: FormEvent<HTMLFormElement>, dataMode: "backend" | "demo") {
  event.preventDefault();
  const value = String(new FormData(event.currentTarget).get("find") ?? "").trim();
  if (value.length === 0) return;
  window.location.assign(searchLaunchHref(splFromFindInput(value, dataMode === "backend" ? "" : "gradethis")));
}

export function ProductShell({ activeSection, appName, children, dataMode }: ProductShellProps) {
  const [menu, setMenu] = useState<"apps" | "help" | "user" | null>(null);
  const [mobileOpen, setMobileOpen] = useState(false);
  const findRef = useRef<HTMLInputElement>(null);
  const mobileTriggerRef = useRef<HTMLButtonElement>(null);
  const mobileDrawerRef = useRef<HTMLDialogElement>(null);
  const menuTriggerRef = useRef<HTMLButtonElement | null>(null);
  const backendDisclosure = activeSection === "search"
    ? "Searches and supported search objects use the configured backend."
    : activeSection === "admin"
      ? "Indexes and ingestion tokens use registered backend routes; unavailable administration surfaces are labeled."
      : activeSection === "datasets"
        ? "The index catalog comes from backend bootstrap; unavailable statistics are omitted."
        : activeSection === "activity"
          ? "Activity shows retained transient jobs and separately persisted search history; audit events are unavailable."
          : activeSection === "reports"
            ? "This page shows persisted backend saved searches; scheduling is not inferred."
            : "This page remains sample preview content; use Search, Datasets, Reports, Activity, or Administration for connected data.";

  function toggleMenu(nextMenu: "apps" | "help" | "user", trigger: HTMLButtonElement) {
    menuTriggerRef.current = trigger;
    setMenu((current) => current === nextMenu ? null : nextMenu);
  }

  function closeMenu(returnFocus = false) {
    setMenu(null);
    if (returnFocus) window.requestAnimationFrame(() => menuTriggerRef.current?.focus());
  }

  function openMenuFromKeyboard(event: ReactKeyboardEvent<HTMLButtonElement>, nextMenu: "apps" | "help" | "user") {
    if (event.key !== "ArrowDown" && event.key !== "ArrowUp") return;
    event.preventDefault();
    menuTriggerRef.current = event.currentTarget;
    setMenu(nextMenu);
    window.requestAnimationFrame(() => {
      const items = document.querySelectorAll<HTMLElement>(`[data-suite-menu="${nextMenu}"] [role="menuitem"]`);
      (event.key === "ArrowUp" ? items.item(items.length - 1) : items.item(0))?.focus();
    });
  }

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if ((event.metaKey || event.ctrlKey) && event.key.toLowerCase() === "k") {
        event.preventDefault();
        findRef.current?.focus();
      }
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  useEffect(() => {
    if (menu === null) return;
    function navigateMenu(event: KeyboardEvent) {
      const popover = document.querySelector<HTMLElement>(`[data-suite-menu="${menu}"]`);
      if (popover === null) return;
      const items = Array.from(popover.querySelectorAll<HTMLElement>('[role="menuitem"]'));
      const current = items.indexOf(document.activeElement as HTMLElement);
      let next = current;
      if (event.key === "ArrowDown") next = current < 0 ? 0 : (current + 1) % items.length;
      else if (event.key === "ArrowUp") next = current < 0 ? items.length - 1 : (current - 1 + items.length) % items.length;
      else if (event.key === "Home") next = 0;
      else if (event.key === "End") next = items.length - 1;
      else if (event.key === "Escape") {
        event.preventDefault();
        closeMenu(true);
        return;
      } else if (event.key === "Tab") {
        event.preventDefault();
        const trigger = menuTriggerRef.current;
        const activeElement = document.activeElement;
        let target: HTMLElement | null = null;
        if (event.shiftKey && activeElement instanceof HTMLElement && popover.contains(activeElement)) {
          target = trigger;
        } else if (trigger !== null) {
          const controls = Array.from(document.querySelectorAll<HTMLElement>(
            'a[href], button:not(:disabled), input:not(:disabled), select:not(:disabled), textarea:not(:disabled), [tabindex]:not([tabindex="-1"])',
          )).filter((control) =>
            control.getClientRects().length > 0
            && !popover.contains(control)
            && !control.classList.contains("suite-dismiss")
          );
          const triggerIndex = controls.indexOf(trigger);
          target = controls[triggerIndex + (event.shiftKey ? -1 : 1)] ?? trigger;
        }
        setMenu(null);
        if (target?.isConnected) window.requestAnimationFrame(() => target?.focus());
        return;
      } else return;
      if (items.length === 0) return;
      event.preventDefault();
      items[next]?.focus();
    }
    document.addEventListener("keydown", navigateMenu);
    return () => document.removeEventListener("keydown", navigateMenu);
  }, [menu]);

  useEffect(() => {
    if (!mobileOpen) return;
    const drawer = mobileDrawerRef.current;
    if (drawer === null) return;
    return installModalSurface({
      container: drawer,
      excludedSiblingClassNames: ["suite-mobile-backdrop"],
      onEscape: () => setMobileOpen(false),
      returnFocus: mobileTriggerRef.current,
    });
  }, [mobileOpen]);

  return (
    <div className="suite-shell">
      <a className="skip-link" href="#suite-main-content">Skip to main content</a>
      <header className="suite-product-bar">
        <button
          ref={mobileTriggerRef}
          className="suite-mobile-trigger"
          type="button"
          aria-label="Open product navigation"
          aria-expanded={mobileOpen}
          onClick={() => setMobileOpen((current) => !current)}
        >
          <span /><span /><span />
        </button>
        <Link className="wordmark suite-wordmark" href="/" aria-label="Open Splunk home">
          <span>open</span><b>&gt;</b><span>splunk</span>
        </Link>
        <div className="suite-menu-anchor">
          <button
            className="suite-app-switcher"
            type="button"
            aria-controls="suite-app-popover"
            aria-haspopup="menu"
            aria-expanded={menu === "apps"}
            onClick={(event) => toggleMenu("apps", event.currentTarget)}
            onKeyDown={(event) => openMenuFromKeyboard(event, "apps")}
          >
            App: <strong>{appName}</strong> <span aria-hidden="true">▾</span>
          </button>
          {menu === "apps" ? (
            <div className="suite-popover suite-app-popover" id="suite-app-popover" role="menu" data-suite-menu="apps">
              <span className="suite-menu-label">Your apps</span>
              <Link role="menuitem" href="/search/"><i className="suite-app-icon">⌕</i><span><strong>Search &amp; Reporting</strong><small>{dataMode === "backend" ? "Search backend-authorized indexes" : "Explore deterministic sample data"}</small></span></Link>
              <Link role="menuitem" href="/dashboards/"><i className="suite-app-icon suite-app-icon--grade">G</i><span><strong>GradeThis Operations</strong><small>{dataMode === "backend" ? "Illustrative operations preview" : "Preview service-health workspace"}</small></span></Link>
              <span className="suite-menu-rule" />
              <Link role="menuitem" href="/admin/"><i className="suite-app-icon suite-app-icon--muted">⚙</i><span><strong>Administration</strong><small>{dataMode === "backend" ? "Indexes and ingestion tokens" : "Preview system settings"}</small></span></Link>
            </div>
          ) : null}
        </div>

        <nav className="suite-utilities" aria-label="Product utilities">
          <Link className={`suite-health${dataMode === "backend" ? " suite-health--backend" : ""}`} href="/admin/"><span /> {dataMode === "backend" ? "Backend mode" : "Healthy"}</Link>
          <Link href="/activity/">Messages <span aria-hidden="true">▾</span></Link>
          <Link href="/admin/">Settings <span aria-hidden="true">▾</span></Link>
          <Link href="/activity/">Activity {dataMode === "demo" ? <span className="activity-count">1</span> : null} <span aria-hidden="true">▾</span></Link>
          <div className="suite-menu-anchor">
            <button type="button" aria-controls="suite-help-popover" aria-haspopup="menu" aria-expanded={menu === "help"} onClick={(event) => toggleMenu("help", event.currentTarget)} onKeyDown={(event) => openMenuFromKeyboard(event, "help")}>Help <span aria-hidden="true">▾</span></button>
            {menu === "help" ? (
              <div className="suite-popover suite-utility-popover" id="suite-help-popover" role="menu" data-suite-menu="help">
                <span className="suite-menu-label">Documentation is not bundled in this frontend preview.</span>
                <span className="suite-menu-rule" />
                <button role="menuitem" type="button" onClick={() => closeMenu(true)}>Close · Open Splunk preview v0.1.0</button>
              </div>
            ) : null}
          </div>
          <form className="suite-find" onSubmit={(event) => submitProductFind(event, dataMode)}>
            <label className="sr-only" htmlFor="suite-find-input">Find</label>
            <input id="suite-find-input" ref={findRef} name="find" placeholder="Find" autoComplete="off" />
            <kbd>⌘K</kbd>
            <button type="submit" aria-label="Search">⌕</button>
          </form>
          <div className="suite-menu-anchor">
            <button className="suite-user-button" type="button" aria-label="Administrator account menu" aria-controls="suite-user-popover" aria-haspopup="menu" aria-expanded={menu === "user"} onClick={(event) => toggleMenu("user", event.currentTarget)} onKeyDown={(event) => openMenuFromKeyboard(event, "user")}>
              <span>A</span><b>Administrator</b><i aria-hidden="true">▾</i>
            </button>
            {menu === "user" ? (
              <div className="suite-popover suite-utility-popover suite-user-popover" id="suite-user-popover" role="menu" data-suite-menu="user">
                <div className="suite-user-summary"><span>A</span><div><strong>Administrator</strong><small>admin@localhost</small></div></div>
                <Link role="menuitem" href="/admin/">Account settings</Link>
                <Link role="menuitem" href="/signin/">Sign out</Link>
              </div>
            ) : null}
          </div>
        </nav>
      </header>

      <nav className="suite-app-bar" aria-label={`${appName} navigation`}>
        <div className="suite-primary-nav">
          {PRIMARY_NAV.map((item) => (
            <Link
              className={activeSection === item.key ? "active" : undefined}
              href={item.href}
              key={`${item.label}-${item.href}`}
              aria-current={activeSection === item.key ? "page" : undefined}
            >
              {item.label}
            </Link>
          ))}
        </div>
        <div className="suite-app-identity"><span aria-hidden="true">{activeSection === "admin" ? "⚙" : "⌕"}</span><strong>{appName}</strong></div>
      </nav>

      {menu !== null ? <button className="suite-dismiss" type="button" aria-label="Close menu" onClick={() => closeMenu(true)} /> : null}

      {mobileOpen ? (
        <dialog ref={mobileDrawerRef} className="suite-mobile-drawer is-open" open aria-modal="true" aria-label="Mobile product navigation">
          <header><div><span className="suite-user-avatar">A</span><span><strong>Administrator</strong><small>admin@localhost</small></span></div><button type="button" aria-label="Close navigation" onClick={() => setMobileOpen(false)}>×</button></header>
          <span className="suite-mobile-label">APPLICATION</span>
          <Link className={activeSection === "home" ? "active" : undefined} aria-current={activeSection === "home" ? "page" : undefined} href="/"><span>⌂</span>Home</Link>
          <Link className={activeSection === "search" ? "active" : undefined} aria-current={activeSection === "search" ? "page" : undefined} href="/search/"><span>⌕</span>Search &amp; Reporting</Link>
          <Link className={activeSection === "analytics" ? "active" : undefined} aria-current={activeSection === "analytics" ? "page" : undefined} href="/analytics/"><span>⌁</span>Analytics</Link>
          <Link className={activeSection === "datasets" ? "active" : undefined} aria-current={activeSection === "datasets" ? "page" : undefined} href="/datasets/"><span>▦</span>Datasets</Link>
          <Link className={activeSection === "reports" ? "active" : undefined} aria-current={activeSection === "reports" ? "page" : undefined} href="/reports/"><span>▤</span>Reports</Link>
          <Link className={activeSection === "dashboards" ? "active" : undefined} aria-current={activeSection === "dashboards" ? "page" : undefined} href="/dashboards/"><span>▥</span>Dashboards</Link>
          <span className="suite-mobile-label">SYSTEM</span>
          <Link className={activeSection === "activity" ? "active" : undefined} aria-current={activeSection === "activity" ? "page" : undefined} href="/activity/"><span>↻</span>Activity {dataMode === "demo" ? <b className="activity-count">1</b> : null}</Link>
          <Link className={activeSection === "admin" ? "active" : undefined} aria-current={activeSection === "admin" ? "page" : undefined} href="/admin/"><span>⚙</span>Administration</Link>
          <span className="suite-mobile-label">HELP DOCUMENTATION IS NOT INCLUDED IN THIS PREVIEW</span>
          <span className="suite-mobile-rule" />
          <Link href="/signin/"><span>⇥</span>Sign out</Link>
        </dialog>
      ) : null}
      {mobileOpen ? <button className="suite-mobile-backdrop" type="button" aria-label="Close navigation" onClick={() => setMobileOpen(false)} /> : null}

      <main className="suite-main" id="suite-main-content" tabIndex={-1}>
        <output className={`suite-data-disclosure suite-data-disclosure--${dataMode}`}>
          <strong>{dataMode === "backend" ? "Backend mode" : "Demo workspace"}</strong>
          <span>{dataMode === "backend" ? backendDisclosure : "Metrics, records, and management actions on this page are sample preview data."}</span>
        </output>
        {children}
      </main>
    </div>
  );
}

interface PageHeadingProps {
  eyebrow?: string;
  title: string;
  description: string;
  actions?: ReactNode;
}

export function PageHeading({ eyebrow, title, description, actions }: PageHeadingProps) {
  return (
    <header className="suite-page-heading">
      <div>
        {eyebrow === undefined ? null : <span className="suite-eyebrow">{eyebrow}</span>}
        <h1>{title}</h1>
        <p>{description}</p>
      </div>
      {actions === undefined ? null : <div className="suite-page-actions">{actions}</div>}
    </header>
  );
}
