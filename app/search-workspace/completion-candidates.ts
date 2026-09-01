import type { DemoField, DemoFieldValue } from "@/lib/demo/search-data";
import type { CompletionContext } from "@/lib/search/spl-editor";
import { formatSplValue } from "@/lib/search/spl-syntax";

import { COMPLETION_RELEVANCE, localCompletionRelevance } from "./completion-groups";
import type { CompletionItem } from "./components/search-editor";
import { COMPLETIONS, NUMBER_FORMAT } from "./constants";

export interface LocalCompletionOptions {
  /**
   * Fall back to the pipeline commands when nothing completes the fragment
   * under the caret -- an explicit Ctrl+Space always answers, and a command
   * there starts a new stage. While typing, the popup instead stays closed.
   */
  commands: boolean;
  /**
   * Offer index names for `index=` from the field summary. Off when a server
   * is connected: it knows the indexes the user may search, the summary only
   * the ones the last result touched.
   */
  indexes: boolean;
}

function fieldValueDetail(value: DemoFieldValue): string {
  const count = value.exactCount ?? NUMBER_FORMAT.format(value.count);
  return `${value.countIsApproximate ? "≈" : ""}${count} events`;
}

function fieldDetail(field: DemoField): string {
  const distinct = field.distinctCountExact
    ?? (field.distinctCount === null ? null : NUMBER_FORMAT.format(field.distinctCount));
  if (distinct === null) return field.type;
  return `${field.type} · ${field.distinctCountIsApproximate ? "≈" : ""}${distinct} distinct values`;
}

function fieldCompletions(fields: readonly DemoField[], prefix: string): CompletionItem[] {
  const typed = prefix.toLowerCase();
  const items: CompletionItem[] = [];
  for (const field of fields) {
    if (!field.name.toLowerCase().startsWith(typed)) continue;
    items.push({
      kind: "field",
      label: field.name,
      insertion: field.name,
      detail: fieldDetail(field),
      relevance: localCompletionRelevance(field.name, prefix),
    });
  }
  return items;
}

function valueCompletions(
  fields: readonly DemoField[],
  fieldName: string,
  prefix: string,
  indexes: boolean,
): CompletionItem[] {
  const field = fields.find((candidate) => candidate.name === fieldName);
  if (field === undefined) return [];
  const isIndex = fieldName === "index";
  if (isIndex && !indexes) return [];
  const typed = prefix.toLowerCase();
  const items: CompletionItem[] = [];
  for (const entry of field.values) {
    if (entry.value === null || entry.pivotable === false) continue;
    const raw = String(entry.value);
    if (!raw.toLowerCase().startsWith(typed)) continue;
    const spelled = isIndex ? raw : formatSplValue(entry.value);
    items.push({
      kind: isIndex ? "index" : "value",
      label: spelled,
      insertion: spelled,
      detail: fieldValueDetail(entry),
      relevance: localCompletionRelevance(raw, prefix),
    });
  }
  return items;
}

function commandCompletions(prefix: string): CompletionItem[] {
  const typed = prefix.toLowerCase();
  if (typed.length === 0) return COMPLETIONS;
  const items: CompletionItem[] = [];
  for (const completion of COMPLETIONS) {
    if (!completion.label.startsWith(typed)) continue;
    items.push({ ...completion, relevance: localCompletionRelevance(completion.label, prefix) });
  }
  return items;
}

/**
 * The completions the workspace can offer without a server, for the fragment
 * under the caret: commands in command position, field names for a bare
 * term (and for the implicit `search` head), and the values the field
 * summary has seen after `field=`. Unordered; the caller sorts.
 */
export function localCompletions(
  context: CompletionContext | null,
  fields: readonly DemoField[],
  options: LocalCompletionOptions,
): CompletionItem[] {
  if (context === null) return options.commands ? COMPLETIONS : [];
  const { prefix } = context;
  if (context.stage === "command" && context.followsPipeline) return commandCompletions(prefix);
  const items = context.stage === "value"
    ? valueCompletions(fields, context.fieldName ?? "", prefix, options.indexes)
    : fieldCompletions(fields, prefix);
  return items.length === 0 && options.commands ? COMPLETIONS : items;
}

/**
 * Whether a list still has something to offer: a candidate the user has not
 * already spelled out in full. A popup that only repeats the typed word
 * closes, which hands the arrow keys back to history recall.
 */
export function extendsFragment(items: readonly CompletionItem[]): boolean {
  return items.some((item) => item.relevance !== COMPLETION_RELEVANCE.exact);
}

/**
 * Whether typing should open the popup for this fragment. A command position
 * after a pipe opens (the stage wants a command); a bare term needs at least
 * one character so a space after a word stays quiet; a `field=` opens as
 * soon as the operator is typed. All of them also need something to show: a
 * local candidate that extends the fragment, or a server that may have one.
 */
export function typeaheadOpens(
  context: CompletionContext | null,
  fields: readonly DemoField[],
  options: { server: boolean },
): boolean {
  if (context === null) return false;
  if (context.stage === "command" && !context.followsPipeline) return false;
  if (context.stage === "term" && context.prefix.length === 0) return false;
  if (options.server) return true;
  return extendsFragment(localCompletions(context, fields, { commands: false, indexes: true }));
}
