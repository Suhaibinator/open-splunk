import type { DemoEvent } from "@/lib/demo/search-data";

export function expandPageEvents(
  expandedEvents: ReadonlySet<string>,
  pageEventIds: readonly string[],
): Set<string> {
  return new Set([...expandedEvents, ...pageEventIds]);
}

export function collapsePageEvents(
  expandedEvents: ReadonlySet<string>,
  pageEventIds: readonly string[],
): Set<string> {
  const pageIds = new Set(pageEventIds);
  return new Set([...expandedEvents].filter((eventId) => !pageIds.has(eventId)));
}

export function serializeRawPageForClipboard(
  events: ReadonlyArray<Pick<DemoEvent, "raw">>,
): string {
  return events.map((event) => event.raw).join("\n");
}

export function maximumReachableResultPage(
  pageKeys: Iterable<string>,
  pageSize: number,
): number {
  const prefix = `${pageSize}:`;
  let maximumPage = 1;
  for (const key of pageKeys) {
    if (!key.startsWith(prefix)) continue;
    const page = Number(key.slice(prefix.length));
    if (Number.isSafeInteger(page) && page > maximumPage) maximumPage = page;
  }
  return maximumPage;
}

// The page count shown as "of N". The reported total is authoritative when the server sent one;
// the reachable floor keeps the count honest when the total is absent or was truncated, because
// pages already reached by following cursors exist regardless of what the total claims.
export function resultPageCount(
  totalRows: number | null,
  pageSize: number,
  reachableFloor: number,
): number {
  const floor = Math.max(1, Number.isSafeInteger(reachableFloor) ? reachableFloor : 1);
  if (totalRows === null || !Number.isSafeInteger(totalRows) || totalRows < 0) return floor;
  if (!Number.isSafeInteger(pageSize) || pageSize < 1) return floor;
  return Math.max(floor, Math.ceil(totalRows / pageSize));
}

export const BASE_EVENT_PAGE_SIZES = [10, 20, 50, 100, 500] as const;

export function eventPageSizeOptions(
  maximumPageSize: number | null,
  currentPageSize: number,
): number[] {
  return [...new Set([
    ...BASE_EVENT_PAGE_SIZES,
    ...(maximumPageSize === null ? [] : [maximumPageSize]),
    currentPageSize,
  ])].filter((size) => size > 0).toSorted((left, right) => left - right);
}
