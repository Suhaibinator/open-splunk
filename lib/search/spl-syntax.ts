import completionCatalog from "../../internal/spl/completion_catalog.json";

export interface SplPipelineCommandDefinition {
  readonly name: string;
  readonly insertion: string;
  readonly detail: string;
}

export interface SplFunctionDefinition {
  readonly name: string;
  readonly insertion: string;
  readonly detail: string;
  readonly class: "scalar" | "aggregate";
  readonly highlight_requires_call: boolean;
}

export interface SplKeywordDefinition {
  readonly name: string;
  readonly insertion: string;
  readonly detail: string;
  readonly highlight: boolean;
}

interface SplCompletionCatalogDefinition {
  commands: readonly SplPipelineCommandDefinition[];
  functions: readonly SplFunctionDefinition[];
  keywords: readonly SplKeywordDefinition[];
}

const SHARED_SPL_COMPLETION_CATALOG =
  completionCatalog as SplCompletionCatalogDefinition;

export const SPL_PIPELINE_COMMANDS = SHARED_SPL_COMPLETION_CATALOG.commands;
export const SPL_FUNCTIONS = SHARED_SPL_COMPLETION_CATALOG.functions;
export const SPL_KEYWORDS = SHARED_SPL_COMPLETION_CATALOG.keywords;

export const UNSUPPORTED_SPL_PIPELINE_COMMANDS = [
  "eventstats",
  "streamstats",
  "transaction",
  "join",
  "map",
  "subsearch",
] as const;

const SUPPORTED_PIPELINE_COMMAND_SET = new Set<string>(
  SPL_PIPELINE_COMMANDS.map((command) => command.name),
);

export function isSupportedSplPipelineCommand(command: string): boolean {
  return SUPPORTED_PIPELINE_COMMAND_SET.has(command.toLowerCase());
}

export interface SplStructure {
  pipes: number[];
  unclosedQuote: { offset: number } | null;
}

/**
 * Locates pipeline boundaries and an unclosed double quote using the same
 * escape behavior as the backend lexer.
 */
export function scanSplStructure(spl: string): SplStructure {
  const pipes: number[] = [];
  let inDoubleQuotedString = false;
  let quoteOffset = -1;

  for (let offset = 0; offset < spl.length; offset += 1) {
    const character = spl[offset];
    if (inDoubleQuotedString) {
      if (character === "\\") {
        // A backslash consumes the next character only while scanning a
        // double-quoted value. Outside a quote it remains part of the token.
        offset += 1;
        continue;
      }
      if (character === '"') inDoubleQuotedString = false;
      continue;
    }
    if (character === '"') {
      inDoubleQuotedString = true;
      quoteOffset = offset;
      continue;
    }
    if (character === "|") pipes.push(offset);
  }

  return {
    pipes,
    unclosedQuote: inDoubleQuotedString ? { offset: quoteOffset } : null,
  };
}

export function splitSplPipeline(spl: string): string[] {
  const stages: string[] = [];
  let stageStart = 0;
  for (const pipe of scanSplStructure(spl).pipes) {
    stages.push(spl.slice(stageStart, pipe));
    stageStart = pipe + 1;
  }
  stages.push(spl.slice(stageStart));
  return stages;
}

export function firstSplPipelineBoundary(spl: string): number {
  return scanSplStructure(spl).pipes[0] ?? -1;
}

export function isSplOffsetInDoubleQuotedValue(spl: string, offset: number): boolean {
  const safeOffset = Math.max(0, Math.min(offset, spl.length));
  return scanSplStructure(spl.slice(0, safeOffset)).unclosedQuote !== null;
}

export type SplValue = string | number | boolean | null;

function escapeDoubleQuotedSplValue(value: string): string {
  return value
    .replaceAll("\\", "\\\\")
    .replaceAll('"', '\\"')
    .replaceAll("\n", "\\n")
    .replaceAll("\r", "\\r")
    .replaceAll("\t", "\\t");
}

export function formatSplValue(value: SplValue): string {
  if (value === null) return "null";
  if (typeof value === "number" || typeof value === "boolean") return String(value);
  return `"${escapeDoubleQuotedSplValue(value)}"`;
}
