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
