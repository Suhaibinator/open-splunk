import { formatSplValue } from "@/lib/search/spl-syntax";

interface SearchLaunchOptions {
  earliest?: string;
  latest?: string;
  label?: string;
  run?: boolean;
  timezone?: string;
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

function objectLaunchHref(parameter: "savedSearchId" | "historySearchId", id: string, run = true): string {
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
