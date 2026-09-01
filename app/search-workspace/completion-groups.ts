import { SearchSuggestionKind } from "@/gen/ts/open_splunk/search_api";

import type { AppIconName } from "../_components/app-icon";

/**
 * The kinds a completion can carry, in the order the menu lists them.
 *
 * Mirrors the server's `SearchSuggestionKind` (`internal/spl/suggestions.go`)
 * plus `value`, which only the workspace produces: the server never suggests
 * field values, so those come from the field summary the workspace already
 * holds.
 */
export const COMPLETION_KINDS = ["command", "function", "field", "value", "keyword", "index"] as const;

export type CompletionKind = (typeof COMPLETION_KINDS)[number];

export interface CompletionKindPresentation {
  /** Group heading. */
  heading: string;
  /** Right-aligned hint beside the heading. */
  hint: string;
  icon: AppIconName;
}

export const COMPLETION_KIND_PRESENTATION: Record<CompletionKind, CompletionKindPresentation> = {
  command: { heading: "Commands", hint: "Enter a pipeline stage", icon: "terminal" },
  function: { heading: "Functions", hint: "eval and stats functions", icon: "function" },
  field: { heading: "Fields", hint: "Fields seen in results", icon: "tag" },
  value: { heading: "Values", hint: "Values seen in results", icon: "quote" },
  keyword: { heading: "Keywords", hint: "Operators and clauses", icon: "braces" },
  index: { heading: "Indexes", hint: "Indexes you can search", icon: "database" },
};

/** The server's relevance ladder, reused for suggestions built locally. */
export const COMPLETION_RELEVANCE = { any: 0.5, prefix: 0.75, exact: 1 } as const;

export interface RankedCompletion {
  kind: CompletionKind;
  label: string;
  relevance: number;
}

export interface CompletionGroup<Item extends RankedCompletion> {
  kind: CompletionKind;
  /** Members with their positions in the flat, ordered list the keyboard walks. */
  items: Array<{ index: number; item: Item }>;
}

/**
 * Maps a server suggestion kind onto the menu's vocabulary. Unknown kinds
 * land with the keywords: the group is the catch-all for "some SPL word".
 */
export function completionKindFromSuggestion(kind: SearchSuggestionKind): CompletionKind {
  switch (kind) {
    case SearchSuggestionKind.SEARCH_SUGGESTION_KIND_COMMAND:
      return "command";
    case SearchSuggestionKind.SEARCH_SUGGESTION_KIND_FUNCTION:
      return "function";
    case SearchSuggestionKind.SEARCH_SUGGESTION_KIND_FIELD:
      return "field";
    case SearchSuggestionKind.SEARCH_SUGGESTION_KIND_VALUE:
      return "value";
    case SearchSuggestionKind.SEARCH_SUGGESTION_KIND_INDEX:
      return "index";
    default:
      return "keyword";
  }
}

/** Relevance of a local candidate for the prefix typed so far. */
export function localCompletionRelevance(label: string, prefix: string): number {
  const typed = prefix.toLowerCase();
  if (typed.length === 0) return COMPLETION_RELEVANCE.any;
  const candidate = label.toLowerCase();
  if (candidate === typed) return COMPLETION_RELEVANCE.exact;
  return candidate.startsWith(typed) ? COMPLETION_RELEVANCE.prefix : COMPLETION_RELEVANCE.any;
}

/**
 * Orders completions the way the menu shows them: by kind, then by relevance
 * (highest first), then by label. The result is the flat list the keyboard
 * walks, so `completionIndex` addresses the same item the menu highlights.
 */
export function orderCompletions<Item extends RankedCompletion>(items: readonly Item[]): Item[] {
  return items
    .map((item, position) => ({ item, position }))
    .toSorted((left, right) => {
      const byKind = COMPLETION_KINDS.indexOf(left.item.kind) - COMPLETION_KINDS.indexOf(right.item.kind);
      if (byKind !== 0) return byKind;
      const byRelevance = right.item.relevance - left.item.relevance;
      if (byRelevance !== 0) return byRelevance;
      const byLabel = left.item.label.localeCompare(right.item.label, "en");
      return byLabel === 0 ? left.position - right.position : byLabel;
    })
    .map(({ item }) => item);
}

/**
 * Splits an ordered list into its kind groups without disturbing the flat
 * indexes. Only kinds that have members appear.
 */
export function groupCompletions<Item extends RankedCompletion>(
  ordered: readonly Item[],
): Array<CompletionGroup<Item>> {
  const groups: Array<CompletionGroup<Item>> = [];
  ordered.forEach((item, index) => {
    const current = groups.at(-1);
    if (current !== undefined && current.kind === item.kind) {
      current.items.push({ index, item });
      return;
    }
    groups.push({ kind: item.kind, items: [{ index, item }] });
  });
  return groups;
}
