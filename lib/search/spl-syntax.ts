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

const COUNT_EVAL_SCALAR_COMMAND_SET = new Set([
  "stats",
  "eventstats",
  "streamstats",
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

type CountEvalScanState =
  | {
      kind: "aggregate";
      parenthesisDepth: number;
      pendingPredicateOpening: number | null;
    }
  | {
      kind: "predicate";
      aggregateParenthesisDepth: number;
      parenthesisDepth: number;
      startOffset: number;
    };

function pipelineCommandAt(spl: string, stageStart: number): string | null {
  let offset = skipUnicodeWhitespace(spl, stageStart);
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

function skipUnicodeWhitespace(spl: string, startOffset: number): number {
  let offset = startOffset;
  while (offset < spl.length) {
    const codePoint = spl.codePointAt(offset)!;
    const character = String.fromCodePoint(codePoint);
    if (!UNICODE_WHITESPACE.test(character)) break;
    offset += codePoint > 0xffff ? 2 : 1;
  }
  return offset;
}

function isAsciiWordCodeUnit(codeUnit: number): boolean {
  return (
    (codeUnit >= 0x30 && codeUnit <= 0x39)
    || (codeUnit >= 0x41 && codeUnit <= 0x5a)
    || codeUnit === 0x5f
    || (codeUnit >= 0x61 && codeUnit <= 0x7a)
  );
}

function asciiCaseInsensitiveWordEnd(
  spl: string,
  offset: number,
  lowercaseWord: string,
): number | null {
  for (let index = 0; index < lowercaseWord.length; index += 1) {
    const codeUnit = spl.charCodeAt(offset + index);
    const lowercaseCodeUnit = codeUnit >= 0x41 && codeUnit <= 0x5a
      ? codeUnit + 0x20
      : codeUnit;
    if (lowercaseCodeUnit !== lowercaseWord.charCodeAt(index)) return null;
  }

  const endOffset = offset + lowercaseWord.length;
  return isAsciiWordCodeUnit(spl.charCodeAt(endOffset)) ? null : endOffset;
}

function countEvalPredicateOpeningAt(spl: string, offset: number): number | null {
  const firstCodeUnit = spl.charCodeAt(offset);
  if (firstCodeUnit !== 0x43 && firstCodeUnit !== 0x63) return null;
  if (isAsciiWordCodeUnit(spl.charCodeAt(offset - 1))) return null;

  const countEnd = asciiCaseInsensitiveWordEnd(spl, offset, "count");
  if (countEnd === null) return null;
  let cursor = skipUnicodeWhitespace(spl, countEnd);
  if (spl[cursor] !== "(") return null;
  cursor = skipUnicodeWhitespace(spl, cursor + 1);
  const evalEnd = asciiCaseInsensitiveWordEnd(spl, cursor, "eval");
  if (evalEnd === null) return null;
  cursor = skipUnicodeWhitespace(spl, evalEnd);
  return spl[cursor] === "(" ? cursor : null;
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
  const countEvalScan: { state: CountEvalScanState | null } = { state: null };

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
        && (scalarStageStart !== null || countEvalScan.state?.kind === "predicate")
        && singleQuoteStartsScalarField(spl, offset)
      )
    ) {
      activeQuote = character;
      quoteOffset = offset;
      continue;
    }
    const currentCountEvalState = countEvalScan.state;
    if (currentCountEvalState?.kind === "predicate") {
      if (character === "(") {
        currentCountEvalState.parenthesisDepth += 1;
        continue;
      }
      if (character === ")") {
        currentCountEvalState.parenthesisDepth -= 1;
        if (currentCountEvalState.parenthesisDepth === 0) {
          scalarStageRanges.push({
            startOffset: currentCountEvalState.startOffset,
            endOffset: offset,
          });
          countEvalScan.state = {
            kind: "aggregate",
            parenthesisDepth: currentCountEvalState.aggregateParenthesisDepth,
            pendingPredicateOpening: null,
          };
        }
        continue;
      }
    } else if (currentCountEvalState?.kind === "aggregate") {
      if (currentCountEvalState.pendingPredicateOpening === offset) {
        countEvalScan.state = {
          kind: "predicate",
          aggregateParenthesisDepth: currentCountEvalState.parenthesisDepth,
          parenthesisDepth: 1,
          startOffset: offset + 1,
        };
        continue;
      }
      if (
        currentCountEvalState.pendingPredicateOpening === null
        && currentCountEvalState.parenthesisDepth === 0
      ) {
        currentCountEvalState.pendingPredicateOpening = countEvalPredicateOpeningAt(spl, offset);
      }
      if (character === "(") currentCountEvalState.parenthesisDepth += 1;
      else if (character === ")" && currentCountEvalState.parenthesisDepth > 0) {
        currentCountEvalState.parenthesisDepth -= 1;
      }
    }
    if (character === "|") {
      if (scalarStageStart !== null) {
        scalarStageRanges.push({ startOffset: scalarStageStart, endOffset: offset });
      }
      if (countEvalScan.state?.kind === "predicate") {
        scalarStageRanges.push({ startOffset: countEvalScan.state.startOffset, endOffset: offset });
      }
      pipes.push(offset);
      const nextStageStart = offset + 1;
      const command = pipelineCommandAt(spl, nextStageStart);
      scalarStageStart = command !== null && isScalarExpressionPipelineCommand(command)
        ? nextStageStart
        : null;
      countEvalScan.state = command !== null && COUNT_EVAL_SCALAR_COMMAND_SET.has(command)
        ? { kind: "aggregate", parenthesisDepth: 0, pendingPredicateOpening: null }
        : null;
    }
  }

  if (activeQuote !== null) {
    quotes.push({ offset: quoteOffset, endOffset: spl.length, quote: activeQuote });
  }
  if (scalarStageStart !== null) {
    scalarStageRanges.push({ startOffset: scalarStageStart, endOffset: spl.length });
  }
  if (countEvalScan.state?.kind === "predicate") {
    scalarStageRanges.push({ startOffset: countEvalScan.state.startOffset, endOffset: spl.length });
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
