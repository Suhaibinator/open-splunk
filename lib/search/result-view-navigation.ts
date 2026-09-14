import { resolveRoutedView, routedViewPath } from "@/lib/view-navigation";
import { SearchResultTab } from "@/gen/ts/open_splunk/search";

import { splitSplPipeline } from "./spl-syntax";

export const SEARCH_BASE_PATH = "/search/";
export const SEARCH_RESULT_VIEWS = ["events", "patterns", "statistics", "visualization"] as const;
export type SearchResultView = typeof SEARCH_RESULT_VIEWS[number];

export function isSearchResultView(value: string): value is SearchResultView {
  return SEARCH_RESULT_VIEWS.includes(value as SearchResultView);
}

export function searchResultViewPath(view: SearchResultView): string {
  return routedViewPath(SEARCH_BASE_PATH, view);
}

export function searchResultViewFromPathname(pathname: string): SearchResultView | null {
  return resolveRoutedView(pathname, SEARCH_BASE_PATH, SEARCH_RESULT_VIEWS);
}

function hasPipelineCommand(query: string, commands: readonly string[]): boolean {
  const allowed = new Set(commands);
  return splitSplPipeline(query).slice(1).some((stage) => {
    const command = /^\s*([A-Za-z]+)/u.exec(stage)?.[1]?.toLowerCase();
    return command !== undefined && allowed.has(command);
  });
}

export function searchResultViewForQuery(query: string): SearchResultView {
  if (hasPipelineCommand(query, ["timechart"])) return "visualization";
  if (hasPipelineCommand(query, ["table", "stats", "top", "rare"])) return "statistics";
  return "events";
}

export function searchResultViewForDefinition(
  query: string,
  preferredView: SearchResultTab,
): SearchResultView {
  if (preferredView === SearchResultTab.SEARCH_RESULT_TAB_STATISTICS) return "statistics";
  if (preferredView === SearchResultTab.SEARCH_RESULT_TAB_VISUALIZATION) return "visualization";
  if (preferredView === SearchResultTab.SEARCH_RESULT_TAB_EVENTS) return "events";
  return searchResultViewForQuery(query);
}
