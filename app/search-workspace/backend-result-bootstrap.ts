import type { TimelinePoint } from "@/lib/demo/search-data";
import {
  adaptSearchResults,
  type AdaptedSearchResults,
} from "@/lib/search/backend-data";

import type { BackendResultPage } from "./backend-result-pages";

type BackendResultAdapter = typeof adaptSearchResults;

/** Adapt one displayed page once, then expose its timeline to another consumer. */
export function adaptAndApplyBackendResultPage(
  page: BackendResultPage,
  apply: (page: BackendResultPage, adapted: AdaptedSearchResults) => void,
  adapt: BackendResultAdapter = adaptSearchResults,
): readonly TimelinePoint[] {
  const adapted = adapt(page.schema, page.rows);
  apply(page, adapted);
  return adapted.timeline;
}

/** Keep chart pagination append-only without cloning immutable timeline points. */
export function seedBackendChartPoints(firstPagePoints: readonly TimelinePoint[]): TimelinePoint[] {
  return [...firstPagePoints];
}
