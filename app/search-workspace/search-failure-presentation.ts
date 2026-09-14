import {
  SearchFailureCode,
  type SearchFailure,
} from "@/gen/ts/open_splunk/search";
import type { EditorProblem } from "@/lib/search/spl-diagnostic-markers";
import type { ParsedSearchLaunch } from "@/lib/search/launch-url";
import type { TimeRange } from "./model";

export interface SearchFailurePresentation {
  actions: SearchFailureActionId[];
  detail: string;
  guidance: string[];
  title: string;
}

export type SearchFailureActionId = "retry" | "server-settings";

export interface WorkspaceSearchFailure {
  failure: SearchFailure;
  problems: EditorProblem[];
  source: string;
}

export interface ActiveSearchFailure extends WorkspaceSearchFailure {
  retryLaunch?: ParsedSearchLaunch;
  timeRange: TimeRange;
}

function defaultTitle(code: SearchFailureCode): string {
  switch (code) {
    case SearchFailureCode.SEARCH_FAILURE_CODE_INVALID_SPL:
      return "Search syntax needs attention";
    case SearchFailureCode.SEARCH_FAILURE_CODE_UNSUPPORTED_SPL:
      return "This SPL search is not supported";
    case SearchFailureCode.SEARCH_FAILURE_CODE_INVALID_TIME_RANGE:
      return "The search time range is invalid";
    case SearchFailureCode.SEARCH_FAILURE_CODE_INDEX_NOT_FOUND:
      return "A searched index was not found";
    case SearchFailureCode.SEARCH_FAILURE_CODE_INDEX_FORBIDDEN:
      return "A searched index is not available to you";
    case SearchFailureCode.SEARCH_FAILURE_CODE_RESOURCE_LIMIT:
      return "Search exceeded a resource limit";
    case SearchFailureCode.SEARCH_FAILURE_CODE_TIMEOUT:
      return "Search timed out";
    case SearchFailureCode.SEARCH_FAILURE_CODE_STORAGE_UNAVAILABLE:
      return "Search storage is unavailable";
    case SearchFailureCode.SEARCH_FAILURE_CODE_RESULT_EXPIRED:
      return "Search results expired";
    case SearchFailureCode.SEARCH_FAILURE_CODE_EXECUTION:
      return "Search execution failed";
    case SearchFailureCode.SEARCH_FAILURE_CODE_INTERNAL:
    case SearchFailureCode.SEARCH_FAILURE_CODE_UNSPECIFIED:
    case SearchFailureCode.UNRECOGNIZED:
    default:
      return "Search failed";
  }
}

export function presentSearchFailure(
  failure: SearchFailure,
  problems: readonly EditorProblem[],
): SearchFailurePresentation {
  const tooComplex = problems.some(
    (problem) => problem.diagnostic.code === "SPL_QUERY_TOO_COMPLEX",
  );
  if (tooComplex) {
    return {
      actions: [
        ...(failure.retryable ? ["retry" as const] : []),
        "server-settings",
      ],
      detail: failure.message || "The SPL source or planned search crossed a structural limit.",
      guidance: [
        "Narrow the search time range or add an index constraint.",
        "Reduce BY-field cardinality and intermediate result size.",
        "Add head before expensive transforming commands when a bounded sample is sufficient.",
      ],
      title: "Search is too complex",
    };
  }
  return {
    actions: failure.retryable ? ["retry"] : [],
    detail: failure.message || "The search stopped before producing a result snapshot.",
    guidance: [],
    title: defaultTitle(failure.code),
  };
}

export function transportSearchFailure(message: string): SearchFailure {
  return {
    code: SearchFailureCode.SEARCH_FAILURE_CODE_INTERNAL,
    diagnostics: [],
    message,
    retryable: true,
  };
}

export function activeTransportSearchFailure(
  message: string,
  source: string,
  timeRange: TimeRange,
  retryLaunch?: ParsedSearchLaunch,
): ActiveSearchFailure {
  return {
    failure: transportSearchFailure(message),
    problems: [],
    retryLaunch,
    source,
    timeRange: { ...timeRange },
  };
}

export function invalidSplSearchFailure(
  message: string,
  diagnostics: SearchFailure["diagnostics"] = [],
): SearchFailure {
  return {
    code: SearchFailureCode.SEARCH_FAILURE_CODE_INVALID_SPL,
    diagnostics,
    message,
    retryable: false,
  };
}
