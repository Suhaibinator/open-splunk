"use client";

import type { FormEvent, KeyboardEvent as ReactKeyboardEvent, ReactNode } from "react";
import { useEffect, useRef, useState } from "react";
import Link from "next/link";

import type { SearchDataMode } from "@/lib/search/backend-data";
import { searchLaunchHref, splFromFindInput } from "@/lib/search/launch-url";

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

function submitProductFind(event: FormEvent<HTMLFormElement>) {
  event.preventDefault();
  const value = String(new FormData(event.currentTarget).get("find") ?? "").trim();
  if (value.length === 0) return;
  window.location.assign(searchLaunchHref(splFromFindInput(value)));
}

export function ProductShell({ activeSection, appName, children, dataMode }: ProductShellProps) {
  const [menu, setMenu] = useState<"apps" | "help" | "user" | null>(null);
  const [mobileOpen, setMobileOpen] = useState(false);
  const findRef = useRef<HTMLInputElement>(null);
  const mobileTriggerRef = useRef<HTMLButtonElement>(null);
  const mobileDrawerRef = useRef<HTMLDialogElement>(null);
  const menuTriggerRef = useRef<HTMLButtonElement | null>(null);

  function toggleMenu(nextMenu: "apps" | "help" | "user", trigger: HTMLButtonElement) {
    menuTriggerRef.current = trigger;
    setMenu((current) => current === nextMenu ? null : nextMenu);
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
      if (event.key === "Escape") {
        setMenu(null);
        setMobileOpen(false);
      }
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
        setMenu(null);
        menuTriggerRef.current?.focus();
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
    const trigger = mobileTriggerRef.current;
    const previousOverflow = document.body.style.overflow;
    const inertedElements: HTMLElement[] = [];
    document.body.style.overflow = "hidden";
    let current: HTMLElement | null = drawer;
    while (current !== null && current.parentElement !== null && current !== document.body) {
      const parent = current.parentElement;
      for (const sibling of parent.children) {
        if (sibling !== current
          && sibling instanceof HTMLElement
          && !sibling.classList.contains("suite-mobile-backdrop")
          && !sibling.inert) {
          sibling.inert = true;
          inertedElements.push(sibling);
        }
      }
      current = parent;
    }
    window.requestAnimationFrame(() => drawer?.querySelector<HTMLElement>("button, a")?.focus());

    function trapFocus(event: KeyboardEvent) {
      if (event.key !== "Tab" || drawer === null) return;
      const controls = Array.from(drawer.querySelectorAll<HTMLElement>('button, a[href], input, select, [tabindex]:not([tabindex="-1"])'));
      const first = controls[0];
      const last = controls.at(-1);
      if (first === undefined || last === undefined) return;
      if (event.shiftKey && document.activeElement === first) {
        event.preventDefault();
        last.focus();
      } else if (!event.shiftKey && document.activeElement === last) {
        event.preventDefault();
        first.focus();
      }
    }

    document.addEventListener("keydown", trapFocus);
    return () => {
      document.body.style.overflow = previousOverflow;
      for (const element of inertedElements) element.inert = false;
      document.removeEventListener("keydown", trapFocus);
      window.requestAnimationFrame(() => trigger?.focus());
    };
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
            aria-haspopup="menu"
            aria-expanded={menu === "apps"}
            onClick={(event) => toggleMenu("apps", event.currentTarget)}
            onKeyDown={(event) => openMenuFromKeyboard(event, "apps")}
          >
            App: <strong>{appName}</strong> <span aria-hidden="true">▾</span>
          </button>
          {menu === "apps" ? (
            <div className="suite-popover suite-app-popover" role="menu" data-suite-menu="apps">
              <span className="suite-menu-label">Your apps</span>
              <Link role="menuitem" href="/search/"><i className="suite-app-icon">⌕</i><span><strong>Search &amp; Reporting</strong><small>Search all authorized indexes</small></span></Link>
              <Link role="menuitem" href="/dashboards/"><i className="suite-app-icon suite-app-icon--grade">G</i><span><strong>GradeThis Operations</strong><small>Index health and investigation tools</small></span></Link>
              <span className="suite-menu-rule" />
              <Link role="menuitem" href="/admin/"><i className="suite-app-icon suite-app-icon--muted">＋</i><span><strong>Manage apps</strong><small>Configure workspaces and access</small></span></Link>
            </div>
          ) : null}
        </div>

        <nav className="suite-utilities" aria-label="Product utilities">
          <Link className="suite-health" href="/admin/"><span /> Healthy</Link>
          <Link href="/activity/">Messages <span aria-hidden="true">▾</span></Link>
          <Link href="/admin/">Settings <span aria-hidden="true">▾</span></Link>
          <Link href="/activity/">Activity <span className="activity-count">1</span> <span aria-hidden="true">▾</span></Link>
          <div className="suite-menu-anchor">
            <button type="button" aria-controls="suite-help-popover" aria-haspopup="menu" aria-expanded={menu === "help"} onClick={(event) => toggleMenu("help", event.currentTarget)} onKeyDown={(event) => openMenuFromKeyboard(event, "help")}>Help <span aria-hidden="true">▾</span></button>
            {menu === "help" ? (
              <div className="suite-popover suite-utility-popover" id="suite-help-popover" role="menu" data-suite-menu="help">
                <span className="suite-menu-label">Documentation is not bundled in this frontend preview.</span>
                <span className="suite-menu-rule" />
                <button role="menuitem" type="button" onClick={() => setMenu(null)}>Close · Open Splunk preview v0.1.0</button>
              </div>
            ) : null}
          </div>
          <form className="suite-find" onSubmit={submitProductFind}>
            <label className="sr-only" htmlFor="suite-find-input">Find</label>
            <input id="suite-find-input" ref={findRef} name="find" placeholder="Find" autoComplete="off" />
            <kbd>⌘K</kbd>
            <button type="submit" aria-label="Search">⌕</button>
          </form>
          <div className="suite-menu-anchor">
            <button className="suite-user-button" type="button" aria-label="Administrator account menu" aria-haspopup="menu" aria-expanded={menu === "user"} onClick={(event) => toggleMenu("user", event.currentTarget)} onKeyDown={(event) => openMenuFromKeyboard(event, "user")}>
              <span>A</span><b>Administrator</b><i aria-hidden="true">▾</i>
            </button>
            {menu === "user" ? (
              <div className="suite-popover suite-utility-popover suite-user-popover" role="menu" data-suite-menu="user">
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
            >
              {item.label}
            </Link>
          ))}
        </div>
        <div className="suite-app-identity"><span aria-hidden="true">{activeSection === "admin" ? "⚙" : "⌕"}</span><strong>{appName}</strong></div>
      </nav>

      {menu !== null ? <button className="suite-dismiss" type="button" aria-label="Close menu" onClick={() => setMenu(null)} /> : null}

      {mobileOpen ? (
        <dialog ref={mobileDrawerRef} className="suite-mobile-drawer is-open" open aria-modal="true" aria-label="Mobile product navigation">
          <header><div><span className="suite-user-avatar">A</span><span><strong>Administrator</strong><small>admin@localhost</small></span></div><button type="button" aria-label="Close navigation" onClick={() => setMobileOpen(false)}>×</button></header>
          <span className="suite-mobile-label">APPLICATION</span>
          <Link className={activeSection === "home" ? "active" : undefined} href="/"><span>⌂</span>Home</Link>
          <Link className={activeSection === "search" ? "active" : undefined} href="/search/"><span>⌕</span>Search &amp; Reporting</Link>
          <Link className={activeSection === "analytics" ? "active" : undefined} href="/analytics/"><span>⌁</span>Analytics</Link>
          <Link className={activeSection === "datasets" ? "active" : undefined} href="/datasets/"><span>▦</span>Datasets</Link>
          <Link className={activeSection === "reports" ? "active" : undefined} href="/reports/"><span>▤</span>Reports</Link>
          <Link className={activeSection === "dashboards" ? "active" : undefined} href="/dashboards/"><span>▥</span>Dashboards</Link>
          <span className="suite-mobile-label">SYSTEM</span>
          <Link className={activeSection === "activity" ? "active" : undefined} href="/activity/"><span>↻</span>Activity <b className="activity-count">1</b></Link>
          <Link className={activeSection === "admin" ? "active" : undefined} href="/admin/"><span>⚙</span>Administration</Link>
          <span className="suite-mobile-label">HELP DOCUMENTATION IS NOT INCLUDED IN THIS PREVIEW</span>
          <span className="suite-mobile-rule" />
          <Link href="/signin/"><span>⇥</span>Sign out</Link>
        </dialog>
      ) : null}
      {mobileOpen ? <button className="suite-mobile-backdrop" type="button" aria-label="Close navigation" onClick={() => setMobileOpen(false)} /> : null}

      <main className="suite-main" id="suite-main-content" tabIndex={-1}>
        <output className={`suite-data-disclosure suite-data-disclosure--${dataMode}`}>
          <strong>{dataMode === "backend" ? "Backend search mode" : "Demo workspace"}</strong>
          <span>{dataMode === "backend" ? "Only the Search workspace uses configured backend data. Content and actions on this page remain sample preview data." : "Metrics, records, and management actions on this page are sample preview data."}</span>
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
