"use client";

import { useCallback, useEffect, useMemo, useRef, useState, useSyncExternalStore } from "react";

/**
 * Moves the browser. A flow component receives it as a prop so a test can
 * watch where it goes; the route wrappers bind it to the router.
 */
export type AuthNavigate = (url: string) => void;

function subscribeToNothing(): () => void {
  return () => undefined;
}

function readSearch(): string {
  return window.location.search;
}

function readNoSearch(): null {
  return null;
}

/**
 * The page's query string, or null before hydration. A static export renders
 * every page without one, so a flow shows its loading state until the browser
 * says which link it was opened from.
 */
export function useLocationQuery(): URLSearchParams | null {
  const search = useSyncExternalStore<string | null>(subscribeToNothing, readSearch, readNoSearch);
  return useMemo(() => (search === null ? null : new URLSearchParams(search)), [search]);
}

export interface Cooldown {
  /** Milliseconds left; 0 when the action may run. */
  readonly remainingMs: number;
  start(milliseconds: number): void;
  clear(): void;
}

/** A countdown that holds a rate-limited action disabled until the server's `Retry-After` passes. */
export function useCooldown(now: () => number = Date.now): Cooldown {
  const [until, setUntil] = useState(0);
  const [at, setAt] = useState(0);

  useEffect(() => {
    if (until === 0) return undefined;
    const tick = () => {
      const current = now();
      if (current >= until) setUntil(0);
      setAt(current);
    };
    const timer = globalThis.setInterval(tick, 1000);
    return () => globalThis.clearInterval(timer);
  }, [now, until]);

  const start = useCallback((milliseconds: number) => {
    const current = now();
    setAt(current);
    setUntil(milliseconds > 0 ? current + milliseconds : 0);
  }, [now]);
  const clear = useCallback(() => setUntil(0), []);
  return { remainingMs: until === 0 ? 0 : Math.max(0, until - at), start, clear };
}

export type Resource<T> =
  | { readonly status: "loading" }
  | { readonly status: "ready"; readonly value: T }
  | { readonly status: "error"; readonly error: unknown };

/**
 * Reads `load` when it changes and on `retry`. A read that a newer one
 * superseded, or that settles after unmount, is dropped. Pass a memoised
 * loader, or null while there is nothing to load from.
 */
export function useResource<T>(load: (() => Promise<T>) | null): { readonly resource: Resource<T>; retry(): void } {
  const [resource, setResource] = useState<Resource<T>>({ status: "loading" });
  const generation = useRef(0);

  const run = useCallback((loader: () => Promise<T>) => {
    generation.current += 1;
    const id = generation.current;
    loader().then(
      (value) => { if (generation.current === id) setResource({ status: "ready", value }); },
      (error: unknown) => { if (generation.current === id) setResource({ status: "error", error }); },
    );
  }, []);

  useEffect(() => {
    if (load === null) return undefined;
    run(load);
    return () => { generation.current += 1; };
  }, [load, run]);

  const retry = useCallback(() => {
    if (load === null) return;
    setResource({ status: "loading" });
    run(load);
  }, [load, run]);

  return { resource, retry };
}

/**
 * Runs `operation` once per `key` for the life of the component, however many
 * times React replays the effect (StrictMode mounts twice in development). A
 * one-time token -- an OAuth code, a verification link -- must reach the server
 * exactly once, so the promise is kept and re-awaited instead of re-sent.
 */
export function useOnce<T>(key: string | null, operation: () => Promise<T>): { readonly result: T | null; readonly error: unknown; readonly settled: boolean } {
  const started = useRef<{ key: string; promise: Promise<T> } | null>(null);
  const [state, setState] = useState<{ key: string | null; result: T | null; error: unknown; settled: boolean }>({ key: null, result: null, error: null, settled: false });
  const latest = useRef(operation);
  useEffect(() => {
    latest.current = operation;
  });

  useEffect(() => {
    if (key === null) return undefined;
    if (started.current?.key !== key) started.current = { key, promise: latest.current() };
    let live = true;
    started.current.promise.then(
      (result) => { if (live) setState({ key, result, error: null, settled: true }); },
      (error: unknown) => { if (live) setState({ key, result: null, error, settled: true }); },
    );
    return () => { live = false; };
  }, [key]);

  return state.key === key ? state : { result: null, error: null, settled: false };
}
