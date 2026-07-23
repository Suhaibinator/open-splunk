import {
  ServerFeature,
  type GetSystemBootstrapResponse,
} from "@/gen/ts/open_splunk/v1/system_api";
import {
  IndexAccessState,
  IndexState,
  type IndexSummary,
} from "@/gen/ts/open_splunk/v1/index";

import type { OpenSplunkApiClient } from "./open-splunk-client";
import type { ProtobufRequestOptions } from "./protobuf-transport";

export interface BrowserApiLimitsModel {
  maximumPageSize: number;
  maximumPreviewRows: number;
  maximumWebsocketSubscriptions: number;
  maximumWebsocketFrameBytes: bigint;
  maximumExportRows: bigint;
  maximumExportBytes: bigint;
  defaultSearchTimeoutMs: number;
  searchResultRetentionMs: number;
  maximumTimelineBuckets: number;
  maximumFieldSummaryValues: number;
}

export interface BrowserIndexModel {
  id: string;
  name: string;
  displayName: string;
  state: IndexState;
  ingestionAccess: IndexAccessState;
  searchAccess: IndexAccessState;
  searchable: boolean;
  ingestible: boolean;
}

export interface SystemBootstrapModel {
  serverVersion: string;
  apiVersion: string;
  splCompatibilityVersion: string;
  searchWebsocketPath: string | null;
  features: ReadonlySet<ServerFeature>;
  limits: BrowserApiLimitsModel;
  apps: GetSystemBootstrapResponse["apps"];
  indexes: BrowserIndexModel[];
  selectedAppId: string | null;
  serverTime: Date;
}

function durationToMilliseconds(duration: { seconds: bigint; nanos: number } | undefined): number {
  if (duration === undefined) return 0;
  const milliseconds = Number(duration.seconds) * 1_000 + duration.nanos / 1_000_000;
  return Number.isFinite(milliseconds) && milliseconds >= 0 ? milliseconds : 0;
}

function adaptIndex(index: IndexSummary): BrowserIndexModel {
  const searchable = index.state === IndexState.INDEX_STATE_ACTIVE
    && index.searchAccess === IndexAccessState.INDEX_ACCESS_STATE_ENABLED;
  const ingestible = index.state === IndexState.INDEX_STATE_ACTIVE
    && index.ingestionAccess === IndexAccessState.INDEX_ACCESS_STATE_ENABLED;
  return {
    id: index.indexId,
    name: index.name,
    displayName: index.displayName || index.name,
    state: index.state,
    ingestionAccess: index.ingestionAccess,
    searchAccess: index.searchAccess,
    searchable,
    ingestible,
  };
}

function sameOriginPath(path: string): string | null {
  if (!path.startsWith("/") || path.startsWith("//") || path.includes("\\")) return null;
  try {
    const base = new URL("https://open-splunk.invalid/");
    const resolved = new URL(path, base);
    if (
      resolved.origin !== base.origin
      || resolved.username
      || resolved.password
      || resolved.search
      || resolved.hash
    ) {
      return null;
    }
    return resolved.pathname;
  } catch {
    return null;
  }
}

export function adaptSystemBootstrap(response: GetSystemBootstrapResponse): SystemBootstrapModel {
  const limits = response.limits;
  if (response.serverTime === undefined || Number.isNaN(response.serverTime.valueOf())) {
    throw new TypeError("System bootstrap did not include a valid server clock.");
  }
  const serverTime = new Date(response.serverTime);
  return {
    serverVersion: response.serverVersion,
    apiVersion: response.apiVersion,
    splCompatibilityVersion: response.splCompatibilityVersion,
    searchWebsocketPath: sameOriginPath(response.searchWebsocketPath),
    features: new Set(response.features.filter((feature) =>
      feature !== ServerFeature.SERVER_FEATURE_UNSPECIFIED && feature !== ServerFeature.UNRECOGNIZED
    )),
    limits: {
      maximumPageSize: limits?.maximumPageSize ?? 0,
      maximumPreviewRows: limits?.maximumPreviewRows ?? 0,
      maximumWebsocketSubscriptions: limits?.maximumWebsocketSubscriptions ?? 0,
      maximumWebsocketFrameBytes: limits?.maximumWebsocketFrameBytes ?? 0n,
      maximumExportRows: limits?.maximumExportRows ?? 0n,
      maximumExportBytes: limits?.maximumExportBytes ?? 0n,
      defaultSearchTimeoutMs: durationToMilliseconds(limits?.defaultSearchTimeout),
      searchResultRetentionMs: durationToMilliseconds(limits?.searchResultRetention),
      maximumTimelineBuckets: limits?.maximumTimelineBuckets ?? 0,
      maximumFieldSummaryValues: limits?.maximumFieldSummaryValues ?? 0,
    },
    apps: response.apps,
    indexes: response.indexes.map(adaptIndex),
    selectedAppId: response.selectedAppId?.trim() || null,
    serverTime,
  };
}

export async function getSystemBootstrap(
  client: OpenSplunkApiClient,
  preferredAppId?: string,
  options?: ProtobufRequestOptions,
): Promise<SystemBootstrapModel> {
  const response = await client.system.bootstrap({
    preferredAppId: preferredAppId?.trim() || undefined,
  }, options);
  return adaptSystemBootstrap(response);
}

export function supportsServerFeature(
  bootstrap: SystemBootstrapModel,
  feature: ServerFeature,
): boolean {
  return bootstrap.features.has(feature);
}

export class IndexScopeResolutionError extends Error {
  public constructor(message: string) {
    super(message);
    this.name = "IndexScopeResolutionError";
  }
}

function searchStages(spl: string): string[] {
  const stages: string[] = [];
  let stageStart = 0;
  let quote: "\"" | null = null;
  let escaped = false;
  for (let index = 0; index < spl.length; index += 1) {
    const character = spl[index];
    if (quote !== null) {
      if (escaped) {
        escaped = false;
      } else if (character === "\\") {
        escaped = true;
      } else if (character === quote) {
        quote = null;
      }
    } else if (character === "\"") {
      quote = character;
    } else if (character === "|") {
      stages.push(spl.slice(stageStart, index));
      stageStart = index + 1;
    }
  }
  stages.push(spl.slice(stageStart));
  return stages;
}

function readQuotedSelector(
  source: string,
  start: number,
): { selector: string; nextIndex: number } | null {
  const quote = source[start];
  if (quote !== "\"") return null;
  let selector = "";
  let escaped = false;
  for (let index = start + 1; index < source.length; index += 1) {
    const character = source[index];
    if (escaped) {
      selector += character;
      escaped = false;
    } else if (character === "\\") {
      escaped = true;
    } else if (character === quote) {
      return { selector, nextIndex: index + 1 };
    } else {
      selector += character;
    }
  }
  return null;
}

interface SearchExpressionToken {
  kind: "word" | "string" | "equals" | "not-equals" | "left-paren" | "right-paren";
  text: string;
}

function tokenizeSearchExpression(source: string): SearchExpressionToken[] {
  const tokens: SearchExpressionToken[] = [];
  for (let index = 0; index < source.length;) {
    const character = source[index];
    if (/\s|,/.test(character)) {
      index += 1;
      continue;
    }
    if (character === "(" || character === ")") {
      tokens.push({ kind: character === "(" ? "left-paren" : "right-paren", text: character });
      index += 1;
      continue;
    }
    if (character === "!" && source[index + 1] === "=") {
      tokens.push({ kind: "not-equals", text: "!=" });
      index += 2;
      continue;
    }
    if (character === "=") {
      tokens.push({ kind: "equals", text: character });
      index += 1;
      continue;
    }
    if (character === "\"") {
      const quoted = readQuotedSelector(source, index);
      if (quoted === null) break;
      tokens.push({ kind: "string", text: quoted.selector });
      index = quoted.nextIndex;
      continue;
    }
    const start = index;
    while (
      index < source.length
      && !/[\s,()=]/.test(source[index])
      && !(source[index] === "!" && source[index + 1] === "=")
    ) {
      index += 1;
    }
    if (index === start) {
      index += 1;
      continue;
    }
    tokens.push({ kind: "word", text: source.slice(start, index) });
  }
  return tokens;
}

interface IndexExpressionAnalysis {
  selectors: string[];
  exhaustivelyConstrained: boolean;
}

function mergeIndexExpressionAnalysis(
  left: IndexExpressionAnalysis,
  right: IndexExpressionAnalysis,
  operator: "and" | "or",
  negated: boolean,
): IndexExpressionAnalysis {
  // De Morgan flips the guarantee rule beneath a negated group.
  const effectiveOperator = negated
    ? operator === "and" ? "or" : "and"
    : operator;
  return {
    selectors: [...left.selectors, ...right.selectors],
    exhaustivelyConstrained: effectiveOperator === "and"
      ? left.exhaustivelyConstrained || right.exhaustivelyConstrained
      : left.exhaustivelyConstrained && right.exhaustivelyConstrained,
  };
}

function analyzeIndexExpression(source: string): IndexExpressionAnalysis {
  const tokens = tokenizeSearchExpression(source);
  let cursor = 0;

  function canStartOperand(): boolean {
    const token = tokens[cursor];
    return token !== undefined
      && token.kind !== "right-paren"
      && !(token.kind === "word" && ["and", "or"].includes(token.text.toLowerCase()));
  }

  function parseAnd(negated: boolean): IndexExpressionAnalysis {
    let result = parseOr(negated);
    while (cursor < tokens.length && tokens[cursor]?.kind !== "right-paren") {
      const token = tokens[cursor];
      const explicitAnd = token?.kind === "word" && token.text.toLowerCase() === "and";
      if (explicitAnd) cursor += 1;
      if (!canStartOperand()) break;
      result = mergeIndexExpressionAnalysis(result, parseOr(negated), "and", negated);
    }
    return result;
  }

  function parseOr(negated: boolean): IndexExpressionAnalysis {
    let result = parseUnary(negated);
    while (
      tokens[cursor]?.kind === "word"
      && tokens[cursor]?.text.toLowerCase() === "or"
    ) {
      cursor += 1;
      result = mergeIndexExpressionAnalysis(result, parseUnary(negated), "or", negated);
    }
    return result;
  }

  function parseUnary(negated: boolean): IndexExpressionAnalysis {
    const token = tokens[cursor];
    if (token === undefined) return { selectors: [], exhaustivelyConstrained: false };
    if (token.kind === "word" && token.text.toLowerCase() === "not") {
      cursor += 1;
      return parseUnary(!negated);
    }
    if (token.kind === "left-paren") {
      cursor += 1;
      const result = parseAnd(negated);
      if (tokens[cursor]?.kind === "right-paren") cursor += 1;
      return result;
    }
    const operator = tokens[cursor + 1];
    const value = tokens[cursor + 2];
    const isPositiveIndexEquality = !negated
      && token.kind === "word"
      && token.text === "index"
      && operator?.kind === "equals"
      && (value?.kind === "word" || value?.kind === "string");
    const selectors = isPositiveIndexEquality && value.text.trim().length > 0
      ? [value.text.trim()]
      : [];
    cursor += operator?.kind === "equals" || operator?.kind === "not-equals" ? 3 : 1;
    return {
      selectors,
      exhaustivelyConstrained: isPositiveIndexEquality,
    };
  }

  if (tokens.length === 0) return { selectors: [], exhaustivelyConstrained: false };
  const result = parseAnd(false);
  return {
    selectors: [...new Set(result.selectors)],
    exhaustivelyConstrained: result.exhaustivelyConstrained,
  };
}

function positiveIndexSelectorsFromExpression(source: string): string[] {
  return analyzeIndexExpression(source).selectors;
}

function stageRedefinesOrClosesIndex(command: { name: string; expression: string }): boolean {
  if (
    command.name === "stats"
    || command.name === "top"
    || command.name === "rare"
    || command.name === "timechart"
  ) {
    return true;
  }
  if (command.name === "eval" && /(?:^|,)\s*index\s*=/.test(command.expression)) return true;
  return command.name === "rename" && /(?:^|\s|,)index(?:\s|,|$)/.test(command.expression);
}

/**
 * Reports whether the eligible SPL filters guarantee that every matching row
 * is constrained by at least one positive exact `index=` predicate.
 */
export function splIndexScopeIsExhaustive(spl: string): boolean {
  const stages = searchStages(spl);
  let exhaustive = analyzeIndexExpression(stages[0] ?? "").exhaustivelyConstrained;
  for (const stage of stages.slice(1)) {
    const command = stageCommand(stage);
    if (command === null) continue;
    if (stageRedefinesOrClosesIndex(command)) break;
    if (command.name === "search") {
      // Pipeline search stages compose with the prior filter using AND.
      exhaustive ||= analyzeIndexExpression(command.expression).exhaustivelyConstrained;
    }
  }
  return exhaustive;
}

function stageCommand(stage: string): { name: string; expression: string } | null {
  const match = /^\s*([A-Za-z]+)\b/.exec(stage);
  if (match === null) return null;
  return {
    name: match[1].toLowerCase(),
    expression: stage.slice(match[0].length),
  };
}

/**
 * Mirrors the v0.1 planner's positive index-reference traversal: the base
 * expression and downstream `search` commands are inspected until a command
 * can redefine `index` or closes the event schema.
 */
export function indexSelectorsFromSPL(spl: string): string[] {
  const stages = searchStages(spl);
  const selectors = positiveIndexSelectorsFromExpression(stages[0] ?? "");
  for (const stage of stages.slice(1)) {
    const command = stageCommand(stage);
    if (command === null) continue;
    if (stageRedefinesOrClosesIndex(command)) break;
    if (command.name === "search") {
      selectors.push(...positiveIndexSelectorsFromExpression(command.expression));
    }
  }
  return [...new Set(selectors)];
}

function uniqueIndexNames(indexes: BrowserIndexModel[]): string[] {
  const seen = new Set<string>();
  const result: string[] = [];
  for (const index of indexes) {
    if (!index.searchable || seen.has(index.name)) continue;
    seen.add(index.name);
    result.push(index.name);
  }
  return result;
}

function selectedAppDefaultIndexes(
  bootstrap: SystemBootstrapModel,
  searchable: readonly string[],
): string[] {
  const selectedApp = bootstrap.selectedAppId === null
    ? undefined
    : bootstrap.apps.find((app) => app.appId === bootstrap.selectedAppId);
  if (selectedApp === undefined || selectedApp.defaultIndexNames.length === 0) return [];
  const byName = new Map(searchable.map((name) => [name, name]));
  return [...new Set(selectedApp.defaultIndexNames.flatMap((candidate) => {
    const match = byName.get(candidate.trim());
    return match === undefined ? [] : [match];
  }))];
}

export interface ResolveIndexScopeOptions {
  spl: string;
  bootstrap: SystemBootstrapModel;
  /**
   * An explicit caller selection takes precedence over SPL extraction. Passing
   * an empty array is equivalent to no explicit selection.
   */
  requestedIndexes?: readonly string[];
  maximumIndexes?: number;
}

/**
 * Resolves exact, currently authorized index names. The backend deliberately
 * rejects wildcard, case-folded, and empty scopes.
 */
export function resolveExactIndexScope({
  spl,
  bootstrap,
  requestedIndexes,
  maximumIndexes = 128,
}: ResolveIndexScopeOptions): string[] {
  const searchable = uniqueIndexNames(bootstrap.indexes);
  if (searchable.length === 0) {
    throw new IndexScopeResolutionError("No searchable indexes are available.");
  }
  const exactNames = new Set(searchable);
  const selectors = requestedIndexes && requestedIndexes.length > 0
    ? [...requestedIndexes]
    : indexSelectorsFromSPL(spl);
  const defaults = selectedAppDefaultIndexes(bootstrap, searchable);
  const effectiveSelectors = selectors.length > 0
    ? selectors
    : defaults.length > 0
      ? defaults
      : searchable;
  const resolved: string[] = [];
  const seen = new Set<string>();

  for (const rawSelector of effectiveSelectors) {
    const selector = rawSelector.trim();
    if (selector.length === 0) continue;
    if (selector.includes("*") || selector.includes("?")) {
      throw new IndexScopeResolutionError(
        `Wildcard index selector "${selector}" is not supported by SPL compatibility ${bootstrap.splCompatibilityVersion || "v0.1"}. Choose an exact index name.`,
      );
    }
    if (!exactNames.has(selector)) {
      const caseMatch = searchable.find((name) => name.toLowerCase() === selector.toLowerCase());
      throw new IndexScopeResolutionError(
        caseMatch === undefined
          ? `Index "${selector}" is not an authorized searchable index.`
          : `Index names are case-sensitive. Use "${caseMatch}" instead of "${selector}".`,
      );
    }
    if (seen.has(selector)) continue;
    seen.add(selector);
    resolved.push(selector);
  }

  if (resolved.length === 0) {
    throw new IndexScopeResolutionError("The search does not resolve to an authorized index.");
  }
  if (resolved.length > maximumIndexes) {
    throw new IndexScopeResolutionError(
      `The search resolves to ${resolved.length} indexes; the server maximum is ${maximumIndexes}.`,
    );
  }
  return resolved;
}
