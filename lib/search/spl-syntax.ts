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
  "transaction",
  "join",
  "map",
  "subsearch",
] as const;

const SUPPORTED_PIPELINE_COMMAND_SET = new Set<string>(
  SPL_PIPELINE_COMMANDS.map((command) => command.name),
);

const SINGLE_QUOTED_SCALAR_COMMAND_SET = new Set([
  "eval",
  "where",
]);

const UNICODE_WHITESPACE = /\p{White_Space}/u;

export function isSupportedSplPipelineCommand(command: string): boolean {
  return SUPPORTED_PIPELINE_COMMAND_SET.has(command.toLowerCase());
}

export function isScalarExpressionPipelineCommand(command: string): boolean {
  return SINGLE_QUOTED_SCALAR_COMMAND_SET.has(command.toLowerCase());
}

export interface SplOffsetRange {
  startOffset: number;
  endOffset: number;
}

export interface SplStructure {
  pipes: number[];
  quotes: Array<{ offset: number; endOffset: number; quote: '"' | "'" }>;
  scalarStageRanges: SplOffsetRange[];
  unclosedQuote: { offset: number; quote: '"' | "'" } | null;
}

function pipelineCommandAt(spl: string, stageStart: number): string | null {
  let offset = stageStart;
  while (offset < spl.length) {
    const codePoint = spl.codePointAt(offset)!;
    const character = String.fromCodePoint(codePoint);
    if (!UNICODE_WHITESPACE.test(character)) break;
    offset += codePoint > 0xffff ? 2 : 1;
  }
  const commandStart = offset;
  if (offset >= spl.length || !/[A-Za-z]/.test(spl[offset]!)) return null;
  offset += 1;
  while (offset < spl.length && /[A-Za-z0-9_-]/.test(spl[offset]!)) offset += 1;
  return spl.slice(commandStart, offset).toLowerCase();
}

function singleQuoteStartsScalarField(spl: string, offset: number): boolean {
  const previous = spl[offset - 1];
  return previous === undefined || /[\p{White_Space}(,=+\-*/%!<>]/u.test(previous);
}

/**
 * Locates pipeline boundaries and an unclosed value or field quote using the
 * same escape behavior as the backend lexer. Offsets are browser UTF-16 code
 * unit indexes; server byte ranges are converted before reaching this API.
 */
export function scanSplStructure(spl: string): SplStructure {
  const pipes: number[] = [];
  const quotes: SplStructure["quotes"] = [];
  const scalarStageRanges: SplOffsetRange[] = [];
  let activeQuote: '"' | "'" | null = null;
  let quoteOffset = -1;
  let scalarStageStart: number | null = null;

  for (let offset = 0; offset < spl.length; offset += 1) {
    const character = spl[offset];
    if (activeQuote !== null) {
      if (character === "\\") {
        // A backslash consumes the next code unit only while scanning a
        // quoted token. The backend remains authoritative for whether the
        // particular escape is legal in a value or field identifier.
        offset += 1;
        continue;
      }
      if (character === activeQuote) {
        quotes.push({ offset: quoteOffset, endOffset: offset + 1, quote: activeQuote });
        activeQuote = null;
      }
      continue;
    }
    if (
      character === '"'
      || (
        character === "'"
        && scalarStageStart !== null
        && singleQuoteStartsScalarField(spl, offset)
      )
    ) {
      activeQuote = character;
      quoteOffset = offset;
      continue;
    }
    if (character === "|") {
      if (scalarStageStart !== null) {
        scalarStageRanges.push({ startOffset: scalarStageStart, endOffset: offset });
      }
      pipes.push(offset);
      const nextStageStart = offset + 1;
      const command = pipelineCommandAt(spl, nextStageStart);
      scalarStageStart = command !== null && isScalarExpressionPipelineCommand(command)
        ? nextStageStart
        : null;
    }
  }

  if (activeQuote !== null) {
    quotes.push({ offset: quoteOffset, endOffset: spl.length, quote: activeQuote });
  }
  if (scalarStageStart !== null) {
    scalarStageRanges.push({ startOffset: scalarStageStart, endOffset: spl.length });
  }
  return {
    pipes,
    quotes,
    scalarStageRanges,
    unclosedQuote: activeQuote === null
      ? null
      : { offset: quoteOffset, quote: activeQuote },
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
  return scanSplStructure(spl.slice(0, safeOffset)).unclosedQuote?.quote === '"';
}

export function isSplOffsetInQuotedValue(spl: string, offset: number): boolean {
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
