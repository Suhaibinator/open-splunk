import { formatSplValue } from "@/lib/search/spl-syntax";

interface SearchLaunchOptions {
  earliest?: string;
  latest?: string;
  label?: string;
  run?: boolean;
  timezone?: string;
}

export type SearchLaunchSource = "q" | "savedSearchId" | "historySearchId" | "searchJobId";

export interface ParsedSearchLaunch {
  source: SearchLaunchSource | null;
  value: string | null;
  run: boolean;
}

export const DEFAULT_SEARCH_HEAD_LIMIT = 10;

export function boundedIndexSearchQuery(indexName: string): string {
  const normalizedIndex = indexName.trim();
  if (normalizedIndex.length === 0) throw new TypeError("An index name is required.");
  const indexSelector = /^[A-Za-z0-9_.-]+$/u.test(normalizedIndex)
    ? normalizedIndex
    : formatSplValue(normalizedIndex);
  return `index=${indexSelector}\n| head ${DEFAULT_SEARCH_HEAD_LIMIT}`;
}

export function searchLaunchHref(query: string, options: SearchLaunchOptions = {}): string {
  const parameters = new URLSearchParams({
    q: query,
    earliest: options.earliest ?? "-24h",
    latest: options.latest ?? "now",
    run: options.run === false ? "0" : "1",
  });
  if (options.label !== undefined) parameters.set("label", options.label);
  if (options.timezone !== undefined) parameters.set("timezone", options.timezone);
  return `/search/?${parameters.toString()}`;
}

function objectLaunchHref(parameter: Exclude<SearchLaunchSource, "q">, id: string, run = true): string {
  const normalizedId = id.trim();
  if (normalizedId.length === 0) throw new TypeError("A persisted search ID is required.");
  const parameters = new URLSearchParams({
    [parameter]: normalizedId,
    run: run ? "1" : "0",
  });
  return `/search/?${parameters.toString()}`;
}

export function savedSearchLaunchHref(savedSearchId: string, run = true): string {
  return objectLaunchHref("savedSearchId", savedSearchId, run);
}

export function historySearchLaunchHref(searchJobId: string, run = true): string {
  return objectLaunchHref("historySearchId", searchJobId, run);
}

export function searchJobLaunchHref(searchJobId: string): string {
  return objectLaunchHref("searchJobId", searchJobId, false);
}

/** Reads one canonical launch source and rejects ambiguous deep links. */
export function parseSearchLaunch(parameters: URLSearchParams): ParsedSearchLaunch {
  const candidates = (["q", "savedSearchId", "historySearchId", "searchJobId"] as const)
    .flatMap((source) => parameters.getAll(source).map((rawValue) => ({
      source,
      value: source === "q" ? rawValue : rawValue.trim(),
    })));
  if (candidates.length > 1) {
    throw new TypeError("A search launch URL must contain exactly one source.");
  }
  const candidate = candidates[0];
  if (candidate !== undefined && candidate.value.trim().length === 0) {
    throw new TypeError("A search launch source cannot be empty.");
  }
  return {
    source: candidate?.source ?? null,
    value: candidate?.value ?? null,
    run: parameters.get("run") !== "0" && candidate?.source !== "searchJobId",
  };
}

/** Replaces every launch-source parameter before installing the selected one. */
export function replaceSearchLaunchSource(
  parameters: URLSearchParams,
  source: SearchLaunchSource,
  value: string,
  run = source !== "searchJobId",
): URLSearchParams {
  if (value.trim().length === 0) throw new TypeError("A search launch value is required.");
  const normalized = source === "q" ? value : value.trim();
  const next = new URLSearchParams(parameters);
  for (const key of ["q", "savedSearchId", "historySearchId", "searchJobId"] as const) next.delete(key);
  for (const key of ["earliest", "latest", "label", "timezone"] as const) next.delete(key);
  next.set(source, normalized);
  next.set("run", run ? "1" : "0");
  return next;
}

export function splFromFindInput(value: string, defaultIndex = "gradethis"): string {
  const trimmed = value.trim();
  if (/\bindex\s*=|\|/i.test(trimmed)) return trimmed;
  const normalizedIndex = defaultIndex.trim();
  const indexPrefix = normalizedIndex.length === 0
    ? ""
    : `index=${formatSplValue(normalizedIndex)} `;
  if (/^(?:NOT\s+)?[A-Za-z_][A-Za-z0-9_.-]*\s*(?:=|!=|>=|<=|>|<)/i.test(trimmed)) {
    return `${indexPrefix}${trimmed}`;
  }
  return `${indexPrefix}${formatSplValue(trimmed)}`;
}
