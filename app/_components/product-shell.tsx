"use client";

import type { FormEvent, KeyboardEvent as ReactKeyboardEvent, ReactNode } from "react";
import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";
import Link from "next/link";

import type { AppSummary } from "@/gen/ts/open_splunk/app";
import { createOpenSplunkApiClient, getSystemBootstrap } from "@/lib/api";
import type { SearchDataMode } from "@/lib/search/backend-data";
import {
  backendAppHref,
  backendAppSearchHref,
  canonicalBackendAppId,
  currentBackendAppId,
  replaceBackendAppId,
  subscribeToBackendAppId,
} from "@/lib/search/app-navigation";
import { searchLaunchHref, splFromFindInput } from "@/lib/search/launch-url";
import { OPEN_SPLUNK_BUILD_LABEL } from "@/lib/build-identity";
import { createErrorMessage } from "@/lib/error-message";
import {
  currentThemePreference,
  setThemePreference,
  subscribeToThemePreference,
  THEME_PREFERENCES,
  type ThemePreference,
} from "@/lib/theme-preference";

import { AppIcon } from "./app-icon";
import { installModalSurface } from "./modal-surface";
import { Wordmark } from "./wordmark";

type ProductSection = "home" | "search" | "analytics" | "datasets" | "reports" | "dashboards" | "activity" | "admin";

interface ProductShellProps {
  activeSection: ProductSection;
  appName: string;
  apiBaseUrl?: string;
  children: ReactNode;
  dataMode: SearchDataMode;
  /**
   * Replaces the built-in app switcher, for a page that switches apps in place
   * rather than by navigating. Supplying it also means the caller owns the app
   * catalog, so the shell makes no bootstrap request of its own and the drawer
   * offers the single app named by `appName` -- which is why a supplied
   * switcher, unlike the built-in one, stays on the bar below 760px: the drawer
   * has no app list to switch with in its place.
   */
  appSwitcher?: ReactNode;
  /** Replaces the built-in utilities nav, for a page with its own menus. */
  utilities?: ReactNode;
  /** The demo/backend band above the page content. */
  disclosure?: boolean;
  /** Class and id of the `<main>` element the skip link targets. */
  mainClassName?: string;
  mainId?: string;
  /** Overlays -- modals, toasts, scrims -- rendered after `</main>`. */
  overlays?: ReactNode;
  /** Runs before the drawer's sign-out link is followed. */
  onSignOut?: () => void;
  /** Extra classes and test hooks for the shell element. */
  shellClassName?: string;
  shellId?: string;
  shellTestId?: string;
}

type BackendAppCatalogState = "idle" | "loading" | "available" | "error";
type ProductMenu = "apps" | "help" | "user";

const PRODUCT_MENU_POPOVER_IDS: Record<ProductMenu, string> = {
  apps: "suite-app-popover",
  help: "suite-help-popover",
  user: "suite-user-popover",
};

export function productMenuControlId(activeMenu: ProductMenu | null, triggerMenu: ProductMenu): string | undefined {
  return activeMenu === triggerMenu ? PRODUCT_MENU_POPOVER_IDS[triggerMenu] : undefined;
}

const PRIMARY_NAV: Array<{ key: ProductSection; label: string; href: string }> = [
  { key: "search", label: "Search", href: "/search/events/" },
  { key: "analytics", label: "Analytics", href: "/analytics/" },
  { key: "datasets", label: "Datasets", href: "/datasets/" },
  { key: "reports", label: "Reports", href: "/reports/saved-searches/" },
  { key: "activity", label: "Activity", href: "/activity/jobs/" },
  { key: "dashboards", label: "Dashboards", href: "/dashboards/" },
];

function submitProductFind(
  event: FormEvent<HTMLFormElement>,
  dataMode: "backend" | "demo",
  backendAppId: string | undefined,
) {
  event.preventDefault();
  const value = String(new FormData(event.currentTarget).get("find") ?? "").trim();
  if (value.length === 0) return;
  const href = searchLaunchHref(splFromFindInput(value, dataMode === "backend" ? "" : "gradethis"));
  window.location.assign(dataMode === "backend" && backendAppId !== undefined
    ? backendAppHref(href, backendAppId)
    : href);
}

/**
 * Every focusable entry of a popover: plain items, and the theme radios, which
 * the arrow keys walk through as one list even though the radios sit inside
 * their own group.
 */
const MENU_ITEM_SELECTOR = '[role="menuitem"], [role="menuitemradio"]';

function focusFirstMenuItem(nextMenu: "apps" | "help" | "user") {
  window.requestAnimationFrame(() => {
    document.querySelector<HTMLElement>(`[data-suite-menu="${nextMenu}"] :is(${MENU_ITEM_SELECTOR})`)?.focus();
  });
}

const THEME_LABELS: Record<ThemePreference, string> = {
  dark: "Dark",
  light: "Light",
  system: "System",
};

interface ThemeMenuProps {
  onSelect: (preference: ThemePreference) => void;
  preference: ThemePreference;
}

/**
 * The System / Light / Dark radio group of the user popover. Exactly one entry
 * is checked: `system` is the absence of a stored choice, so it is what a
 * first visit and a cleared storage both show.
 */
export function ThemeMenu({ onSelect, preference }: ThemeMenuProps) {
  return (
    <>
      <span className="suite-menu-rule" />
      <fieldset className="suite-theme-menu">
        <legend className="suite-menu-label">Theme</legend>
        {THEME_PREFERENCES.map((option) => (
          <button key={option} role="menuitemradio" type="button" aria-checked={preference === option} onClick={() => onSelect(option)}>
            <AppIcon name="check" size="xs" />{THEME_LABELS[option]}
          </button>
        ))}
      </fieldset>
    </>
  );
}

const appCatalogErrorMessage = createErrorMessage("The backend app catalog could not be loaded.");

function appLabel(app: AppSummary): string {
  return app.displayName.trim() || app.slug.trim() || app.appId;
}

function appDetail(app: AppSummary, selected: boolean): string {
  if (selected) return "Selected backend app";
  if (app.defaultIndexNames.length === 0) return "No default indexes advertised";
  const count = app.defaultIndexNames.length;
  return `${count.toLocaleString()} default ${count === 1 ? "index" : "indexes"}`;
}

export function ProductShell({
  activeSection,
  apiBaseUrl = "",
  appName,
  appSwitcher,
  children,
  dataMode,
  disclosure = true,
  mainClassName = "",
  mainId = "suite-main-content",
  onSignOut,
  overlays,
  shellClassName = "",
  shellId,
  shellTestId,
  utilities,
}: ProductShellProps) {
  const ownsCatalog = appSwitcher === undefined;
  const [menu, setMenu] = useState<ProductMenu | null>(null);
  const [mobileOpen, setMobileOpen] = useState(false);
  const [backendApps, setBackendApps] = useState<AppSummary[]>([]);
  const [selectedBackendAppId, setSelectedBackendAppId] = useState<string | null>(null);
  const [backendAppCatalogState, setBackendAppCatalogState] = useState<BackendAppCatalogState>(
    dataMode === "backend" ? "loading" : "idle",
  );
  const [backendAppCatalogError, setBackendAppCatalogError] = useState<string | null>(null);
  const [backendAppCatalogGeneration, setBackendAppCatalogGeneration] = useState(0);
  const activeCatalogGenerationRef = useRef(0);
  const apiClient = useMemo(
    () => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }),
    [apiBaseUrl],
  );
  const preferredAppId = useSyncExternalStore(
    subscribeToBackendAppId,
    currentBackendAppId,
    () => undefined,
  );
  const themePreference = useSyncExternalStore(
    subscribeToThemePreference,
    currentThemePreference,
    () => "system" as const,
  );
  const findRef = useRef<HTMLInputElement>(null);
  const mobileTriggerRef = useRef<HTMLButtonElement>(null);
  const mobileDrawerRef = useRef<HTMLDialogElement>(null);
  const menuTriggerRef = useRef<HTMLButtonElement | null>(null);
  const localSession = dataMode === "backend";
  const sessionInitial = localSession ? "L" : "A";
  const sessionLabel = localSession ? "Local session" : "Administrator";
  const sessionDetail = localSession ? "Single-user backend mode" : "admin@localhost";
  const selectedBackendApp = backendApps.find((app) => app.appId === selectedBackendAppId);
  const navigationBackendAppId = selectedBackendAppId ?? preferredAppId;
  const switcherAppName = dataMode === "backend" && selectedBackendApp !== undefined
    ? appLabel(selectedBackendApp)
    : appName;
  const backendDisclosure = activeSection === "search"
    ? "Searches and supported search objects use the configured backend."
    : activeSection === "admin"
      ? "Indexes and ingestion tokens use registered backend routes; unavailable administration surfaces are labeled."
      : activeSection === "datasets"
        ? "Search authorization comes from backend bootstrap; the registered index route adds available retention and source defaults."
        : activeSection === "activity"
          ? "Activity shows retained jobs and search history, plus capability-gated mutation and search-attempt audit journals for administrator sessions."
          : activeSection === "reports"
            ? "This page shows persisted backend saved searches; scheduling is not inferred."
            : activeSection === "home"
              ? "Recent searches come from persisted backend search history when the server advertises it."
              : activeSection === "analytics"
                ? "Search-performance summaries use retained backend history when the server advertises it."
                : activeSection === "dashboards"
                  ? "Dashboard definitions and panel searches use registered backend routes when available."
                  : "This page uses the configured backend where the server advertises support.";

  function toggleMenu(nextMenu: ProductMenu, trigger: HTMLButtonElement) {
    menuTriggerRef.current = trigger;
    setMenu((current) => current === nextMenu ? null : nextMenu);
  }

  const closeMenu = useCallback((returnFocus = false) => {
    setMenu(null);
    if (returnFocus) window.requestAnimationFrame(() => menuTriggerRef.current?.focus());
  }, []);

  function openMenuFromKeyboard(event: ReactKeyboardEvent<HTMLButtonElement>, nextMenu: ProductMenu) {
    if (event.key !== "ArrowDown" && event.key !== "ArrowUp") return;
    event.preventDefault();
    menuTriggerRef.current = event.currentTarget;
    setMenu(nextMenu);
    if (event.key === "ArrowDown") focusFirstMenuItem(nextMenu);
    else window.requestAnimationFrame(() => {
      const items = document.querySelectorAll<HTMLElement>(`[data-suite-menu="${nextMenu}"] :is(${MENU_ITEM_SELECTOR})`);
      items.item(items.length - 1)?.focus();
    });
  }

  function productHref(href: string): string {
    return dataMode === "backend" && navigationBackendAppId !== undefined
      ? backendAppHref(href, navigationBackendAppId)
      : href;
  }

  useEffect(() => {
    function onKeyDown(event: KeyboardEvent) {
      if (!(event.metaKey || event.ctrlKey) || event.key.toLowerCase() !== "k") return;
      // A page that supplies its own `utilities` brings its own find input, so
      // the field is resolved from the bar when the built-in slot's ref is
      // empty. Without that, the shell would swallow the browser's own Ctrl/⌘K
      // on exactly the pages whose input it cannot reach and focus nothing.
      const find = findRef.current
        ?? document.querySelector<HTMLInputElement>(".suite-product-bar .suite-find input");
      if (find === null) return;
      event.preventDefault();
      find.focus();
    }
    document.addEventListener("keydown", onKeyDown);
    return () => document.removeEventListener("keydown", onKeyDown);
  }, []);

  useEffect(() => {
    activeCatalogGenerationRef.current = backendAppCatalogGeneration;
    if (!ownsCatalog) return;
    let current = true;
    if (dataMode !== "backend") {
      queueMicrotask(() => {
        if (!current || activeCatalogGenerationRef.current !== backendAppCatalogGeneration) return;
        setBackendApps([]);
        setSelectedBackendAppId(null);
        setBackendAppCatalogError(null);
        setBackendAppCatalogState("idle");
      });
      return () => {
        current = false;
      };
    }
    const controller = new AbortController();
    queueMicrotask(() => {
      if (!current) return;
      setSelectedBackendAppId(null);
      setBackendAppCatalogState("loading");
      setBackendAppCatalogError(null);
    });
    void getSystemBootstrap(apiClient, preferredAppId, { signal: controller.signal })
      .then((bootstrap) => {
        if (!current) return;
        const canonicalAppId = canonicalBackendAppId(preferredAppId, bootstrap.selectedAppId);
        if (canonicalAppId !== undefined) replaceBackendAppId(canonicalAppId);
        setBackendApps(bootstrap.apps);
        setSelectedBackendAppId(bootstrap.selectedAppId);
        setBackendAppCatalogState("available");
      })
      .catch((error: unknown) => {
        if (!current || controller.signal.aborted) return;
        setBackendApps([]);
        setSelectedBackendAppId(null);
        setBackendAppCatalogError(appCatalogErrorMessage(error));
        setBackendAppCatalogState("error");
      });
    return () => {
      current = false;
      controller.abort();
    };
  }, [apiClient, backendAppCatalogGeneration, dataMode, ownsCatalog, preferredAppId]);

  useEffect(() => {
    if (menu === null) return;
    function navigateMenu(event: KeyboardEvent) {
      const popover = document.querySelector<HTMLElement>(`[data-suite-menu="${menu}"]`);
      if (popover === null) return;
      const items = Array.from(popover.querySelectorAll<HTMLElement>(MENU_ITEM_SELECTOR));
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
  }, [closeMenu, menu]);

  useEffect(() => {
    if (!mobileOpen) return;
    const drawer = mobileDrawerRef.current;
    if (drawer === null) return;
    return installModalSurface({
      container: drawer,
      excludedSiblingClassNames: ["drawer-backdrop"],
      onEscape: () => setMobileOpen(false),
      returnFocus: mobileTriggerRef.current,
    });
  }, [mobileDrawerRef, mobileOpen, mobileTriggerRef]);

  return (
    <div className={`suite-shell ${shellClassName}`.trim()} data-testid={shellTestId} id={shellId}>
      <a className="skip-link" href={`#${mainId}`}>Skip to main content</a>
      <header className="suite-product-bar">
        <button
          ref={mobileTriggerRef}
          className="drawer-trigger"
          type="button"
          aria-label="Open product navigation"
          aria-expanded={mobileOpen}
          onClick={() => setMobileOpen((current) => !current)}
        >
          <span /><span /><span />
        </button>
        <Wordmark href={productHref("/")} />
        {appSwitcher ?? <div className="suite-menu-anchor suite-catalog-anchor">
          <button
            className="suite-app-switcher"
            type="button"
            aria-controls={productMenuControlId(menu, "apps")}
            aria-haspopup="menu"
            aria-expanded={menu === "apps"}
            onClick={(event) => {
              const opening = menu !== "apps";
              toggleMenu("apps", event.currentTarget);
              if (opening && event.detail === 0) focusFirstMenuItem("apps");
            }}
            onKeyDown={(event) => openMenuFromKeyboard(event, "apps")}
          >
            App: <strong>{switcherAppName}</strong> <AppIcon name="chevron-down" size="xs" />
          </button>
          {menu === "apps" ? (
            <div className="suite-popover suite-app-popover" id={PRODUCT_MENU_POPOVER_IDS.apps} role="menu" data-suite-menu="apps">
              <span className="suite-menu-label">{dataMode === "backend" ? "Server apps" : "Your apps"}</span>
              {dataMode === "backend" ? (
                backendAppCatalogState === "loading" ? (
                  <output className="suite-app-catalog-state"><i className="suite-app-icon suite-app-icon--muted" aria-hidden="true">…</i><span><strong>Loading apps</strong><small>Reading system bootstrap</small></span></output>
                ) : backendAppCatalogState === "error" ? (
                  <button role="menuitem" type="button" onClick={() => setBackendAppCatalogGeneration((current) => current + 1)}><i className="suite-app-icon suite-app-icon--muted" aria-hidden="true">!</i><span><strong>Retry app catalog</strong><small>{backendAppCatalogError}</small></span></button>
                ) : backendApps.length === 0 ? (
                  <output className="suite-app-catalog-state"><i className="suite-app-icon suite-app-icon--muted" aria-hidden="true">—</i><span><strong>No authorized apps</strong><small>The backend returned an empty app catalog</small></span></output>
                ) : backendApps.map((app) => {
                  const selected = app.appId === selectedBackendAppId;
                  const label = appLabel(app);
                  return (
                    <Link className={selected ? "selected" : undefined} role="menuitem" href={backendAppSearchHref(app.appId)} key={app.appId} onClick={() => closeMenu()} aria-label={`Open ${label} in Search`}>
                      <i className="suite-app-icon" aria-hidden="true">{label.charAt(0).toUpperCase() || "⌕"}</i>
                      <span><strong>{label}</strong><small>{appDetail(app, selected)}</small></span>
                    </Link>
                  );
                })
              ) : (
                <>
                  <Link role="menuitem" href="/search/events/"><i className="suite-app-icon" aria-hidden="true"><AppIcon name="search" size="md" /></i><span><strong>Search &amp; Reporting</strong><small>Explore deterministic sample data</small></span></Link>
                  <Link role="menuitem" href="/dashboards/"><i className="suite-app-icon suite-app-icon--grade" aria-hidden="true">G</i><span><strong>GradeThis Operations</strong><small>Preview service-health workspace</small></span></Link>
                </>
              )}
              <span className="suite-menu-rule" />
              <Link role="menuitem" href={productHref("/admin/")}><i className="suite-app-icon suite-app-icon--muted" aria-hidden="true"><AppIcon name="settings" size="md" /></i><span><strong>Administration</strong><small>{dataMode === "backend" ? "Indexes and ingestion tokens" : "Preview system settings"}</small></span></Link>
            </div>
          ) : null}
        </div>}

        {utilities ?? <nav className="suite-utilities" aria-label="Product utilities">
          <span className="suite-context">{dataMode === "backend" ? "Backend workspace" : "Demo workspace"}</span>
          <Link href={productHref("/admin/")}>Settings</Link>
          <Link href={productHref("/activity/jobs/")}>Activity {dataMode === "demo" ? <span className="activity-count">1</span> : null}</Link>
          <div className="suite-menu-anchor">
            <button type="button" aria-controls={productMenuControlId(menu, "help")} aria-haspopup="menu" aria-expanded={menu === "help"} onClick={(event) => { const opening = menu !== "help"; toggleMenu("help", event.currentTarget); if (opening && event.detail === 0) focusFirstMenuItem("help"); }} onKeyDown={(event) => openMenuFromKeyboard(event, "help")}>Help <AppIcon name="chevron-down" size="xs" /></button>
            {menu === "help" ? (
              <div className="suite-popover suite-utility-popover" id={PRODUCT_MENU_POPOVER_IDS.help} role="menu" data-suite-menu="help">
                <span className="suite-menu-label">Documentation is not bundled in this frontend preview.</span>
                <span className="suite-menu-rule" />
                <button role="menuitem" type="button" onClick={() => closeMenu(true)}>Close · Open Splunk {OPEN_SPLUNK_BUILD_LABEL}</button>
              </div>
            ) : null}
          </div>
          <form className="suite-find" onSubmit={(event) => submitProductFind(event, dataMode, navigationBackendAppId)}>
            <label className="sr-only" htmlFor="suite-find-input">Find</label>
            <input id="suite-find-input" ref={findRef} name="find" placeholder="Find" autoComplete="off" />
            <kbd aria-label="Control or Command K">Ctrl/⌘K</kbd>
            <button type="submit" aria-label="Search"><AppIcon name="search" size="sm" /></button>
          </form>
          <div className="suite-menu-anchor">
            <button className="suite-user-button" type="button" aria-label={`${sessionLabel} menu`} aria-controls={productMenuControlId(menu, "user")} aria-haspopup="menu" aria-expanded={menu === "user"} onClick={(event) => { const opening = menu !== "user"; toggleMenu("user", event.currentTarget); if (opening && event.detail === 0) focusFirstMenuItem("user"); }} onKeyDown={(event) => openMenuFromKeyboard(event, "user")}>
              <span>{sessionInitial}</span><b>{sessionLabel}</b><AppIcon name="chevron-down" size="xs" />
            </button>
            {menu === "user" ? (
              <div className="suite-popover suite-utility-popover suite-user-popover" id={PRODUCT_MENU_POPOVER_IDS.user} role="menu" data-suite-menu="user">
                <div className="suite-user-summary"><span aria-hidden="true">{sessionInitial}</span><div><strong>{sessionLabel}</strong><small>{sessionDetail}</small></div></div>
                <Link role="menuitem" href={productHref("/admin/")}>{localSession ? "Server administration" : "Account settings"}</Link>
                <Link role="menuitem" href="/signin/">{localSession ? "About local access" : "Sign out"}</Link>
                <ThemeMenu preference={themePreference} onSelect={setThemePreference} />
              </div>
            ) : null}
          </div>
        </nav>}
      </header>

      <nav className="suite-app-bar" aria-label={`${appName} navigation`}>
        <div className="suite-primary-nav">
          {PRIMARY_NAV.map((item) => (
            <Link
              className={activeSection === item.key ? "active" : undefined}
              href={productHref(item.href)}
              key={`${item.label}-${item.href}`}
              aria-current={activeSection === item.key ? "page" : undefined}
            >
              {item.label}
            </Link>
          ))}
        </div>
        <div className="suite-app-identity"><span aria-hidden="true"><AppIcon name={activeSection === "admin" ? "settings" : "search"} size="md" /></span><strong>{appName}</strong></div>
      </nav>

      {menu !== null ? <button className="suite-dismiss" type="button" aria-label="Close menu" onClick={() => closeMenu(true)} /> : null}

      {mobileOpen ? (
        <dialog ref={mobileDrawerRef} className="drawer" open aria-modal="true" aria-label="Mobile product navigation">
          <header><div><span className="suite-user-avatar" aria-hidden="true">{sessionInitial}</span><span><strong>{sessionLabel}</strong><small>{sessionDetail}</small></span></div><button type="button" aria-label="Close navigation" onClick={() => setMobileOpen(false)}><AppIcon name="close" size="lg" /></button></header>
          <span className="drawer-label">APPLICATION</span>
          <Link className={activeSection === "home" ? "active" : undefined} aria-current={activeSection === "home" ? "page" : undefined} href={productHref("/")}><span aria-hidden="true"><AppIcon name="home" size="md" /></span>Home</Link>
          {!ownsCatalog ? (
            <Link className={activeSection === "search" ? "active" : undefined} aria-current={activeSection === "search" ? "page" : undefined} href={productHref("/search/events/")}><span aria-hidden="true"><AppIcon name="search" size="md" /></span>{appName}</Link>
          ) : dataMode === "backend" ? (
            backendAppCatalogState === "loading" ? (
              <output className="drawer-app-state">Loading server apps…</output>
            ) : backendAppCatalogState === "error" ? (
              <button className="drawer-app-retry" type="button" onClick={() => setBackendAppCatalogGeneration((current) => current + 1)}>Retry server apps</button>
            ) : backendApps.length === 0 ? (
              <output className="drawer-app-state">No authorized server apps</output>
            ) : backendApps.map((app) => {
              const label = appLabel(app);
              const selected = app.appId === selectedBackendAppId;
              return <Link className={selected ? "selected-app" : undefined} href={backendAppSearchHref(app.appId)} key={`mobile-${app.appId}`}><span aria-hidden="true">{label.charAt(0).toUpperCase() || "⌕"}</span>{label}{selected ? <b>Selected</b> : null}</Link>;
            })
          ) : (
            <Link className={activeSection === "search" ? "active" : undefined} aria-current={activeSection === "search" ? "page" : undefined} href={productHref("/search/events/")}><span aria-hidden="true"><AppIcon name="search" size="md" /></span>Search &amp; Reporting</Link>
          )}
          <Link className={activeSection === "analytics" ? "active" : undefined} aria-current={activeSection === "analytics" ? "page" : undefined} href={productHref("/analytics/")}><span aria-hidden="true"><AppIcon name="analytics" size="md" /></span>Analytics</Link>
          <Link className={activeSection === "datasets" ? "active" : undefined} aria-current={activeSection === "datasets" ? "page" : undefined} href={productHref("/datasets/")}><span aria-hidden="true"><AppIcon name="database" size="md" /></span>Datasets</Link>
          <Link className={activeSection === "reports" ? "active" : undefined} aria-current={activeSection === "reports" ? "page" : undefined} href={productHref("/reports/saved-searches/")}><span aria-hidden="true"><AppIcon name="file" size="md" /></span>Reports</Link>
          <Link className={activeSection === "dashboards" ? "active" : undefined} aria-current={activeSection === "dashboards" ? "page" : undefined} href={productHref("/dashboards/")}><span aria-hidden="true"><AppIcon name="dashboard" size="md" /></span>Dashboards</Link>
          <span className="drawer-label">SYSTEM</span>
          <Link className={activeSection === "activity" ? "active" : undefined} aria-current={activeSection === "activity" ? "page" : undefined} href={productHref("/activity/jobs/")}><span aria-hidden="true"><AppIcon name="activity" size="md" /></span>Activity {dataMode === "demo" ? <b className="activity-count">1</b> : null}</Link>
          <Link className={activeSection === "admin" ? "active" : undefined} aria-current={activeSection === "admin" ? "page" : undefined} href={productHref("/admin/")}><span aria-hidden="true"><AppIcon name="settings" size="md" /></span>Administration</Link>
          <span className="drawer-label">HELP DOCUMENTATION IS NOT INCLUDED IN THIS PREVIEW</span>
          <span className="drawer-rule" />
          <Link href="/signin/" onClick={onSignOut}><span aria-hidden="true"><AppIcon name={localSession ? "info" : "logout"} size="md" /></span>{localSession ? "About local access" : "Sign out"}</Link>
        </dialog>
      ) : null}
      {mobileOpen ? <button className="drawer-backdrop" type="button" aria-label="Close navigation" onClick={() => setMobileOpen(false)} /> : null}

      <main className={`suite-main ${mainClassName}`.trim()} id={mainId} tabIndex={-1}>
        {disclosure ? (
          <output className={`suite-data-disclosure suite-data-disclosure--${dataMode}`}>
            <strong>{dataMode === "backend" ? "Backend mode" : "Demo workspace"}</strong>
            <span>{dataMode === "backend" ? backendDisclosure : "Metrics, records, and management actions on this page are sample preview data."}</span>
          </output>
        ) : null}
        {children}
      </main>
      {overlays}
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
