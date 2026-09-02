import { parseSearchLaunch, replaceSearchLaunchSource, type SearchLaunchSource } from "./launch-url";

export interface SearchLaunchRange {
  earliest: string;
  label: string;
  latest: string;
  timezone?: string;
}

/** The launch a search URL describes, range parameters included. */
export interface SearchLaunchLocation {
  earliest: string | null;
  label: string | null;
  latest: string | null;
  run: boolean;
  source: SearchLaunchSource | null;
  timezone: string | null;
  value: string | null;
}

/**
 * What each browser-history entry remembers so Back and Forward can restore
 * the workspace from memory: the draft, its range and, once the server has
 * created it, the job whose results the entry showed. Stored beside whatever
 * the framework keeps on `history.state`, never instead of it.
 */
export interface SearchLaunchHistoryState {
  earliest: string;
  label: string;
  latest: string;
  q: string;
  searchJobId?: string;
  timezone?: string;
}

/**
 * `navigate` is a launch the user would expect Back to undo (running a query,
 * opening a saved search); `rewrite` relabels the current entry (the draft
 * was just saved, or its saved search deleted) and never adds history.
 */
export type SearchLaunchMode = "navigate" | "rewrite";
export type SearchLaunchTransition = "none" | "push" | "replace";

export type HistoryNavigationDecision =
  | { kind: "invalid"; message: string }
  | { kind: "launch" }
  | { kind: "open-job"; searchJobId: string }
  | { kind: "restore"; query: string | null; range: SearchLaunchRange | null; run: boolean };

/** Reads a URL as a launch location; an invalid URL counts as a bare page. */
export function searchLaunchLocation(parameters: URLSearchParams): SearchLaunchLocation {
  let source: SearchLaunchSource | null = null;
  let value: string | null = null;
  let run = false;
  try {
    ({ source, value, run } = parseSearchLaunch(parameters));
  } catch {
    // A URL the workspace could not launch from has nothing worth preserving.
  }
  return {
    earliest: parameters.get("earliest"),
    label: parameters.get("label"),
    latest: parameters.get("latest"),
    run,
    source,
    timezone: parameters.get("timezone")?.trim() || null,
    value,
  };
}

function sameRange(current: SearchLaunchLocation, next: SearchLaunchLocation): boolean {
  return current.earliest === next.earliest
    && current.latest === next.latest
    && current.label === next.label
    && current.timezone === next.timezone;
}

function sameLaunch(current: SearchLaunchLocation, next: SearchLaunchLocation): boolean {
  return current.source === next.source
    && current.value === next.value
    && current.run === next.run
    && sameRange(current, next);
}

/**
 * Whether moving from `current` to `next` deserves its own history entry.
 * A bare page adopts its first launch in place, and running the same launch
 * again (or giving a range to one that had none) refines the entry rather
 * than stacking a duplicate behind it.
 */
export function launchTransition(
  current: SearchLaunchLocation,
  next: SearchLaunchLocation,
  mode: SearchLaunchMode,
): SearchLaunchTransition {
  if (sameLaunch(current, next)) return "none";
  if (mode === "rewrite" || current.source === null) return "replace";
  if (
    current.source === next.source
    && current.value === next.value
    && (current.earliest === null || sameRange(current, next))
  ) {
    return "replace";
  }
  return "push";
}

/** Builds the URL for a launch while keeping unrelated parameters such as `appId`. */
export function searchLaunchUrl(
  current: URL,
  source: SearchLaunchSource,
  value: string,
  run: boolean,
  range: SearchLaunchRange | null,
): URL {
  const url = new URL(current.href);
  url.search = replaceSearchLaunchSource(url.searchParams, source, value, run).toString();
  if (range !== null) {
    url.searchParams.set("earliest", range.earliest);
    url.searchParams.set("latest", range.latest);
    url.searchParams.set("label", range.label);
    if (range.timezone) url.searchParams.set("timezone", range.timezone);
    else url.searchParams.delete("timezone");
  }
  return url;
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

function optionalString(value: unknown): string | undefined {
  return typeof value === "string" && value.length > 0 ? value : undefined;
}

/** Recovers the workspace's part of a history entry's state, if it has one. */
export function readSearchLaunchState(state: unknown): SearchLaunchHistoryState | null {
  if (!isRecord(state)) return null;
  const { q, earliest, latest, label } = state;
  if (typeof q !== "string" || typeof earliest !== "string" || typeof latest !== "string" || typeof label !== "string") {
    return null;
  }
  return {
    earliest,
    label,
    latest,
    q,
    searchJobId: optionalString(state.searchJobId),
    timezone: optionalString(state.timezone),
  };
}

/**
 * Decides what a Back or Forward landing on this URL should do. A backend
 * entry that remembers its job reopens the retained results without a new
 * run; a `q` entry restores its draft from the URL and the remembered state,
 * running only when the URL says so; persisted launches go back through the
 * URL launch path so the saved search or history entry is opened properly.
 */
export function historyNavigationDecision(
  parameters: URLSearchParams,
  state: SearchLaunchHistoryState | null,
  backend: boolean,
): HistoryNavigationDecision {
  let launch: ReturnType<typeof parseSearchLaunch>;
  try {
    launch = parseSearchLaunch(parameters);
  } catch (error) {
    return { kind: "invalid", message: error instanceof Error ? error.message : "The search launch URL is invalid." };
  }
  if (backend && state?.searchJobId !== undefined) return { kind: "open-job", searchJobId: state.searchJobId };
  if (launch.source === "q") {
    return { kind: "restore", query: launch.value, range: launchRange(parameters, state), run: launch.run };
  }
  if (launch.source === null) {
    return state === null
      ? { kind: "launch" }
      : { kind: "restore", query: state.q, range: launchRange(parameters, state), run: false };
  }
  return { kind: "launch" };
}

function launchRange(parameters: URLSearchParams, state: SearchLaunchHistoryState | null): SearchLaunchRange | null {
  const earliest = parameters.get("earliest");
  const latest = parameters.get("latest");
  if (earliest !== null && latest !== null) {
    return {
      earliest,
      label: parameters.get("label") || `${earliest} to ${latest}`,
      latest,
      timezone: parameters.get("timezone")?.trim() || undefined,
    };
  }
  if (state === null) return null;
  return { earliest: state.earliest, label: state.label, latest: state.latest, timezone: state.timezone };
}

interface CommitSearchLaunchOptions {
  mode: SearchLaunchMode;
  run?: boolean;
  state: SearchLaunchHistoryState;
}

/**
 * Writes a launch to the address bar and remembers `state` on the resulting
 * history entry. The framework's own history state is carried along so its
 * router keeps handling Back and Forward for the entry.
 */
export function commitSearchLaunch(
  source: SearchLaunchSource,
  value: string,
  range: SearchLaunchRange | null,
  options: CommitSearchLaunchOptions,
): SearchLaunchTransition {
  const current = new URL(window.location.href);
  const next = searchLaunchUrl(current, source, value, options.run ?? source !== "searchJobId", range);
  const transition = launchTransition(
    searchLaunchLocation(current.searchParams),
    searchLaunchLocation(next.searchParams),
    options.mode,
  );
  const historyState = {
    ...window.history.state,
    ...options.state,
    searchJobId: options.state.searchJobId,
    timezone: options.state.timezone,
  };
  if (transition === "push") window.history.pushState(historyState, "", next);
  else window.history.replaceState(historyState, "", next);
  return transition;
}

/** Updates the current entry's remembered state without touching its URL. */
export function stampSearchLaunchState(patch: Partial<SearchLaunchHistoryState>): void {
  window.history.replaceState({ ...window.history.state, ...patch }, "", window.location.href);
}
