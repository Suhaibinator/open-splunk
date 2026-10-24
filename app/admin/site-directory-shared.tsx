"use client";

import Link from "next/link";
import {
  useCallback,
  useEffect,
  useId,
  useRef,
  useState,
  type KeyboardEvent as ReactKeyboardEvent,
  type ReactNode,
  type RefObject,
} from "react";

import type { Capability, IdentityClient, IdentityError, Page, UserId } from "@/lib/api/identity";

import { AppIcon } from "../_components/app-icon";
import { BackendResourceState } from "../_components/backend-resource-state";
import { useIdentity } from "../_components/identity-provider";
import {
  asIdentityError,
  pageForEmptyResult,
  pagePositionLabel,
  pageRangeLabel,
  presentDirectoryError,
  retryAfterSeconds,
  type DirectorySubject,
} from "./site-directory-data";

/** Where a signed-out administrator goes to sign back in. */
export const SIGN_IN_PATH = "/signin/";

/** Focuses an element after the current commit, once a closing dialog has handed focus back. */
export function focusAfterCommit(element: HTMLElement | null): void {
  globalThis.setTimeout(() => element?.focus(), 0);
}

// --------------------------------------------------------------------------
// The capability gate
// --------------------------------------------------------------------------

const GATE_COPY: Readonly<Record<DirectorySubject, { readonly noun: string; readonly capability: Capability }>> = {
  organizations: { capability: "orgs.admin", noun: "organizations" },
  users: { capability: "users.admin", noun: "users" },
};

export type SiteDirectoryAccess =
  | { readonly allowed: true; readonly client: IdentityClient; readonly selfId: UserId }
  | { readonly allowed: false; readonly reason: string };

/** Whether the signed-in principal may open this directory, and the client to open it with. */
export function useSiteDirectoryAccess(subject: DirectorySubject): SiteDirectoryAccess {
  const identity = useIdentity();
  const { capability, noun } = GATE_COPY[subject];
  if (identity.status === "signed_in" && identity.client !== null && identity.principal !== null && identity.hasCapability(capability)) {
    return { allowed: true, client: identity.client, selfId: identity.principal.user.id };
  }
  return {
    allowed: false,
    reason: identity.status === "loading"
      ? "Checking your access…"
      : `Only a signed-in site administrator can manage ${noun}.`,
  };
}

/**
 * What a panel shows in place of its directory when the principal may not
 * open it: still reading the session, identity unavailable, signed out, or
 * signed in without site-admin access.
 */
export function SiteDirectoryAccessState({ subject }: { readonly subject: DirectorySubject }) {
  const identity = useIdentity();
  const { noun } = GATE_COPY[subject];
  if (identity.status === "loading") {
    return <BackendResourceState kind="loading" title="Checking your access" message={`Reading your session before loading ${noun}.`} />;
  }
  if (identity.status === "unavailable") {
    const presentation = identity.error === null
      ? { message: "The identity service did not answer.", title: "Identity is unavailable" }
      : presentDirectoryError(identity.error, subject);
    const retryable = identity.client !== null && identity.error?.kind !== "not_configured";
    return (
      <BackendResourceState
        kind="unavailable"
        title={presentation.title}
        message={presentation.message}
        action={retryable ? <button type="button" onClick={() => void identity.refresh().catch(() => undefined)}>Try again</button> : undefined}
      />
    );
  }
  if (identity.status === "signed_out") {
    return (
      <BackendResourceState
        kind="unavailable"
        title={`Sign in to manage ${noun}`}
        message={`Managing ${noun} needs a signed-in site administrator.`}
        action={<Link href={SIGN_IN_PATH}>Sign in</Link>}
      />
    );
  }
  const who = identity.principal?.user.email ?? "your account";
  return (
    <BackendResourceState
      kind="unavailable"
      title="Site administrator access required"
      message={`Managing ${noun} is limited to site administrators. You are signed in as ${who}, which does not hold that access. Ask a site administrator if you need it.`}
    />
  );
}

export interface DirectoryHeaderProps {
  readonly headingId: string;
  readonly title: string;
  readonly description: string;
  readonly actionLabel: string;
  readonly onAction: () => void;
  /** Disables the action and says why, for a principal without the capability. */
  readonly disabledReason: string | null;
}

/** The section header every directory shares: its heading (a focus target after a removal) and its one primary action. */
export function DirectoryHeader({ headingId, title, description, actionLabel, onAction, disabledReason }: DirectoryHeaderProps) {
  const reasonId = `${headingId}-action-reason`;
  return (
    <header className="admin-section-header">
      <div>
        <h2 id={headingId} tabIndex={-1}>{title}</h2>
        <p>{description}</p>
      </div>
      <div className="site-directory-header-action">
        <button
          className="button button--primary"
          type="button"
          disabled={disabledReason !== null}
          aria-describedby={disabledReason === null ? undefined : reasonId}
          onClick={onAction}
        >
          <AppIcon name="plus" size="sm" /> {actionLabel}
        </button>
        {disabledReason === null ? null : <small id={reasonId}>{disabledReason}</small>}
      </div>
    </header>
  );
}

export interface DirectorySearchFormProps {
  /** The search landmark's name, and the search box's label. */
  readonly label: string;
  readonly placeholder: string;
  readonly value: string;
  readonly inputRef: RefObject<HTMLInputElement | null>;
  readonly onChange: (value: string) => void;
  readonly onSubmit: () => void;
  /** Further controls (a filter) between the box and the submit button. */
  readonly children?: ReactNode;
}

/** The directory's search, on the admin console's shared toolbar: the box, any filter, then Search. */
export function DirectorySearchForm({ label, placeholder, value, inputRef, onChange, onSubmit, children }: DirectorySearchFormProps) {
  return (
    <form
      className="admin-toolbar"
      role="search"
      aria-label={label}
      onSubmit={(event) => {
        event.preventDefault();
        onSubmit();
      }}
    >
      <label>
        <span className="sr-only">{label}</span>
        <input ref={inputRef} type="search" value={value} placeholder={placeholder} onChange={(event) => onChange(event.target.value)} />
      </label>
      {children}
      <button type="submit">Search</button>
    </form>
  );
}

// --------------------------------------------------------------------------
// Loading one page
// --------------------------------------------------------------------------

type Outcome<T> =
  | { readonly ok: true; readonly page: Page<T> }
  | { readonly ok: false; readonly error: IdentityError; readonly at: number };

interface Settled<T> {
  readonly key: string;
  readonly outcome: Outcome<T>;
  /** The last page that loaded, kept on screen while a newer request is in flight. */
  readonly lastPage: Page<T> | null;
}

export interface DirectoryPageState<T> {
  readonly status: "loading" | "ready" | "error";
  /** The current page, or the previous one while a newer one loads. */
  readonly page: Page<T> | null;
  readonly error: IdentityError | null;
  /** When the current error arrived, for a rate-limit countdown. */
  readonly errorAt: number | null;
  reload(): void;
  /** Rewrites the loaded rows in place, after a mutation returned the new record. */
  updateItems(update: (items: readonly T[]) => readonly T[]): void;
}

/**
 * Loads the page `load` names whenever `key` changes, keeps the previous
 * page visible while the next one is in flight, and drops an answer that
 * arrives after a newer request. `load` must change exactly when `key` does.
 */
export function useDirectoryPage<T>(key: string, load: () => Promise<Page<T>>): DirectoryPageState<T> {
  const [generation, setGeneration] = useState(0);
  const [settled, setSettled] = useState<Settled<T> | null>(null);
  const requestKey = `${key}#${generation}`;

  useEffect(() => {
    let live = true;
    load().then(
      (page) => {
        if (live) setSettled({ key: requestKey, lastPage: page, outcome: { ok: true, page } });
      },
      (caught: unknown) => {
        if (!live) return;
        const error = asIdentityError(caught);
        setSettled((previous) => ({
          key: requestKey,
          lastPage: previous?.lastPage ?? null,
          outcome: { at: Date.now(), error, ok: false },
        }));
      },
    );
    return () => {
      live = false;
    };
  }, [load, requestKey]);

  const reload = useCallback(() => setGeneration((value) => value + 1), []);
  const updateItems = useCallback((update: (items: readonly T[]) => readonly T[]) => {
    setSettled((previous) => {
      if (previous === null || !previous.outcome.ok) return previous;
      const page = { ...previous.outcome.page, items: update(previous.outcome.page.items) };
      return { ...previous, lastPage: page, outcome: { ok: true, page } };
    });
  }, []);

  const current = settled?.key === requestKey ? settled : null;
  if (current === null) {
    return { error: null, errorAt: null, page: settled?.lastPage ?? null, reload, status: "loading", updateItems };
  }
  if (current.outcome.ok) {
    return { error: null, errorAt: null, page: current.outcome.page, reload, status: "ready", updateItems };
  }
  return { error: current.outcome.error, errorAt: current.outcome.at, page: current.lastPage, reload, status: "error", updateItems };
}

/**
 * Steps a directory back from an empty page past the first (see
 * `pageForEmptyResult`). True while it does, so the panel shows the page as
 * loading rather than as an empty directory.
 */
export function useEmptyPageFallback<T>(directory: DirectoryPageState<T>, setPage: (page: number) => void): boolean {
  const target = directory.status === "ready" && directory.page !== null
    ? pageForEmptyResult(directory.page.page, directory.page.items.length)
    : null;
  useEffect(() => {
    if (target !== null) setPage(target);
  }, [setPage, target]);
  return target !== null;
}

/** Seconds left until `until`, ticking once a second; 0 once it has passed or when there is nothing to wait for. */
export function useSecondsUntil(until: number | null): number {
  const [now, setNow] = useState(() => Date.now());
  useEffect(() => {
    if (until === null) return;
    const timer = globalThis.setInterval(() => {
      const at = Date.now();
      setNow(at);
      if (at >= until) globalThis.clearInterval(timer);
    }, 1000);
    return () => globalThis.clearInterval(timer);
  }, [until]);
  if (until === null || now >= until) return 0;
  return retryAfterSeconds(until - now);
}

// --------------------------------------------------------------------------
// States
// --------------------------------------------------------------------------

export interface DirectoryErrorStateProps {
  readonly error: IdentityError;
  readonly errorAt: number | null;
  readonly subject: DirectorySubject;
  readonly onRetry: () => void;
  readonly onClearSearch: () => void;
}

/** A failed list load, with the one next step its error kind allows. */
export function DirectoryErrorState({ error, errorAt, subject, onRetry, onClearSearch }: DirectoryErrorStateProps) {
  const presentation = presentDirectoryError(error, subject);
  const until = presentation.retryAfterMs === undefined || errorAt === null ? null : errorAt + presentation.retryAfterMs;
  const wait = useSecondsUntil(until);
  let action: ReactNode;
  if (presentation.action === "retry") {
    action = (
      <button type="button" onClick={onRetry} disabled={wait > 0}>
        {wait > 0 ? `Retry in ${wait} s` : "Retry"}
      </button>
    );
  } else if (presentation.action === "sign_in") {
    action = <Link href={SIGN_IN_PATH}>Sign in</Link>;
  } else if (presentation.action === "clear_search") {
    action = <button type="button" onClick={onClearSearch}>Clear search</button>;
  }
  return <BackendResourceState kind={presentation.kind} title={presentation.title} message={presentation.message} action={action} />;
}

/** Placeholder rows while the first page loads; the caption carries the spoken state. */
export function DirectorySkeletonRows({ columns, rows = 4 }: { readonly columns: number; readonly rows?: number }) {
  return (
    <>
      {Array.from({ length: rows }, (_row, row) => (
        <tr key={row} aria-hidden="true">
          {Array.from({ length: columns }, (_cell, column) => (
            <td key={column}>
              <span className={`skeleton skeleton--line ${column === 0 ? "skeleton--line-long" : "skeleton--line-medium"}`} />
              <span className="sr-only">Loading</span>
            </td>
          ))}
        </tr>
      ))}
    </>
  );
}

export interface DirectoryPagerProps {
  readonly label: string;
  readonly noun: string;
  readonly page: Page<unknown>;
  readonly busy: boolean;
  readonly onPage: (page: number) => void;
}

/**
 * The range, the position, and Previous / Next; the range line is the live
 * region that announces a new page.
 *
 * Keyboard focus stays on the pager while pages turn. A page in flight holds
 * the buttons with `aria-disabled` rather than `disabled`, which would drop
 * the focused button's focus to the document on every press; and when the
 * page that arrives is the last (or first), the button that led there is
 * disabled for real, so focus moves to the other one.
 */
export function DirectoryPager({ label, noun, page, busy, onPage }: DirectoryPagerProps) {
  const info = page.page;
  const previousRef = useRef<HTMLButtonElement>(null);
  const nextRef = useRef<HTMLButtonElement>(null);
  const pressed = useRef<"previous" | "next" | null>(null);

  useEffect(() => {
    if (busy || pressed.current === null) return;
    const [from, to] = pressed.current === "next" ? [nextRef, previousRef] : [previousRef, nextRef];
    const stranded = pressed.current === "next" ? !info.hasNextPage : !info.hasPrevPage;
    const active = document.activeElement;
    if (stranded && (active === null || active === document.body || active === from.current)) to.current?.focus();
    pressed.current = null;
  }, [busy, info.hasNextPage, info.hasPrevPage]);

  function turn(direction: "previous" | "next") {
    if (busy) return;
    pressed.current = direction;
    onPage(info.currentPage + (direction === "next" ? 1 : -1));
  }

  return (
    <nav className="admin-pagination-footer admin-pagination-footer--inset" aria-label={label}>
      <div>
        <strong role="status">{busy ? `Loading ${noun}…` : pageRangeLabel(info, page.items.length, noun)}</strong>
        <small>{pagePositionLabel(info)}</small>
      </div>
      <div className="site-directory-pager-buttons">
        <button
          ref={previousRef}
          className="button button--secondary button--compact"
          type="button"
          disabled={!info.hasPrevPage}
          aria-disabled={busy || undefined}
          onClick={() => turn("previous")}
        >
          <AppIcon name="chevron-left" size="xs" /> Previous
        </button>
        <button
          ref={nextRef}
          className="button button--secondary button--compact"
          type="button"
          disabled={!info.hasNextPage}
          aria-disabled={busy || undefined}
          onClick={() => turn("next")}
        >
          Next <AppIcon name="chevron-right" size="xs" />
        </button>
      </div>
    </nav>
  );
}

/** The fixed confirmation after a change; an `<output>` is a polite live region. */
export function DirectoryToast({ message, onDismiss }: { readonly message: string | null; readonly onDismiss: () => void }) {
  if (message === null) return null;
  return (
    <output className="toast toast-success">
      <span><AppIcon name="check" size="sm" /></span>
      <strong>{message}</strong>
      <button type="button" aria-label="Dismiss notification" onClick={onDismiss}><AppIcon name="close" size="md" /></button>
    </output>
  );
}

/** A dialog's failed submission; `role="alert"` announces it the moment it appears. */
export function DirectoryFormError({ message }: { readonly message: string | null }) {
  if (message === null) return null;
  return (
    <div className="backend-inline-error" role="alert">
      <span>{message}</span>
    </div>
  );
}

// --------------------------------------------------------------------------
// The row menu
// --------------------------------------------------------------------------

export interface DirectoryMenuItem {
  readonly key: string;
  readonly label: string;
  readonly danger?: boolean;
  /** Shown under the label, and the reason the item is disabled. */
  readonly disabledReason?: string | null;
  /** Receives the trigger, which a dialog opened from here should return focus to. */
  readonly onSelect: (trigger: HTMLButtonElement | null) => void;
}

/** Every item, disabled ones included: a disabled item stays focusable so its reason can be read. */
function menuItemsIn(container: HTMLElement | null): HTMLButtonElement[] {
  return Array.from(container?.querySelectorAll<HTMLButtonElement>('[role="menuitem"]') ?? []);
}

/**
 * A row's overflow menu: the trigger, then a `role="menu"` list that takes
 * focus on open, moves with the arrow keys, Home and End, and closes on
 * Escape, Tab or a click outside, returning focus to the trigger. A disabled
 * item is `aria-disabled` rather than `disabled`, so arrowing onto it still
 * reads the reason it describes; activating it does nothing.
 */
export function DirectoryRowMenu({ label, items }: { readonly label: string; readonly items: readonly DirectoryMenuItem[] }) {
  const [open, setOpen] = useState(false);
  const wrapRef = useRef<HTMLDivElement>(null);
  const triggerRef = useRef<HTMLButtonElement>(null);
  const menuId = useId();

  useEffect(() => {
    if (!open) return;
    menuItemsIn(wrapRef.current)[0]?.focus();
    function closeOnOutsidePointer(event: PointerEvent) {
      if (!wrapRef.current?.contains(event.target as Node)) setOpen(false);
    }
    document.addEventListener("pointerdown", closeOnOutsidePointer);
    return () => document.removeEventListener("pointerdown", closeOnOutsidePointer);
  }, [open]);

  function closeToTrigger() {
    setOpen(false);
    triggerRef.current?.focus();
  }

  function handleMenuKey(event: ReactKeyboardEvent<HTMLDivElement>) {
    if (event.key === "Escape") {
      event.preventDefault();
      closeToTrigger();
      return;
    }
    if (event.key === "Tab") {
      setOpen(false);
      return;
    }
    const entries = menuItemsIn(wrapRef.current);
    if (entries.length === 0) return;
    const index = entries.findIndex((item) => item === document.activeElement);
    let next: number;
    if (event.key === "ArrowDown") next = index < 0 ? 0 : (index + 1) % entries.length;
    else if (event.key === "ArrowUp") next = index < 0 ? entries.length - 1 : (index - 1 + entries.length) % entries.length;
    else if (event.key === "Home") next = 0;
    else if (event.key === "End") next = entries.length - 1;
    else return;
    event.preventDefault();
    entries[next]?.focus();
  }

  return (
    <div className="resource-action-wrap" ref={wrapRef}>
      <button
        ref={triggerRef}
        className="row-overflow"
        type="button"
        aria-label={label}
        aria-haspopup="menu"
        aria-expanded={open}
        aria-controls={open ? menuId : undefined}
        onClick={() => setOpen((value) => !value)}
      >
        <AppIcon name="more" size="md" />
      </button>
      {open ? (
        <div className="resource-action-menu site-directory-menu" id={menuId} role="menu" tabIndex={-1} aria-label={label} onKeyDown={handleMenuKey}>
          {items.map((item) => {
            const labelId = `${menuId}-${item.key}-label`;
            const reasonId = `${menuId}-${item.key}-reason`;
            const disabled = item.disabledReason !== undefined && item.disabledReason !== null;
            return (
              <button
                key={item.key}
                role="menuitem"
                type="button"
                className={item.danger === true ? "site-directory-menu-item is-danger" : "site-directory-menu-item"}
                aria-disabled={disabled ? true : undefined}
                // Named by the label alone, so the reason is read once, as the description.
                aria-labelledby={labelId}
                aria-describedby={disabled ? reasonId : undefined}
                onClick={() => {
                  if (disabled) return;
                  setOpen(false);
                  item.onSelect(triggerRef.current);
                }}
              >
                <strong id={labelId}>{item.label}</strong>
                {disabled ? <small id={reasonId}>{item.disabledReason}</small> : null}
              </button>
            );
          })}
        </div>
      ) : null}
    </div>
  );
}
