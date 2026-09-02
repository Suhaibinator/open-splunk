import {
  isSplOffsetInQuotedValue,
  isSupportedSplPipelineCommand,
  scanSplStructure,
} from "./spl-syntax";

export type DiagnosticKind = "empty" | "unsupported" | "unclosed-quote";

export interface SplDiagnostic {
  kind: DiagnosticKind;
  token: string;
  message: string;
  line: number;
  column: number;
  suggestion: string;
  actionLabel?: string;
  quote?: '"' | "'";
  removeStart?: number;
  removeEnd?: number;
  /** UTF-16 span of the offending source, when the diagnostic has one to mark. */
  start?: number;
  end?: number;
}

/**
 * The kinds a completion can carry, in the order the menu lists them.
 *
 * Mirrors the server's `SearchSuggestionKind` (`internal/spl/suggestions.go`)
 * plus `value`, which only the workspace produces: the server never suggests
 * field values, so those come from the field summary the workspace holds.
 */
export const COMPLETION_KINDS = ["command", "function", "field", "value", "keyword", "index"] as const;

export type CompletionKind = (typeof COMPLETION_KINDS)[number];

/**
 * What the fragment under the caret is. `command` is the first word of a
 * stage (or of the implicit `search` head); `term` is a bare word further
 * along the stage, where a field, function or keyword goes; `value` is the
 * right-hand side of a `field=` / `field!=` / `field<` … comparison.
 */
export type CompletionStage = "command" | "term" | "value";

export interface CompletionContext {
  fragmentStart: number;
  fragmentEnd: number;
  prefix: string;
  followsPipeline: boolean;
  stage: CompletionStage;
  /** The field a `value` fragment compares against. */
  fieldName?: string;
}

const COMMAND_FRAGMENT = /^(\s*)([A-Za-z_]*)$/u;
// `field`, a comparison operator, then the unquoted value typed so far.
const VALUE_FRAGMENT = /([A-Za-z_][\w.]*)\s*(?:!=|<=|>=|=|<|>)\s*([\w.-]*)$/u;
// A bare word after whitespace, an opening bracket, a comma or a sort sign.
const TERM_FRAGMENT = /(?:^|[\s(,+-])([A-Za-z_][\w.]*)$/u;

// Backend diagnostics and completion replacements use UTF-8 byte offsets,
// while JavaScript string slicing uses UTF-16 code-unit offsets. If a forged
// or stale backend offset lands inside a code point, stop before that code
// point so the editor never splits a surrogate pair.
export function utf16OffsetsForUtf8ByteOffsets(
  value: string,
  byteOffsets: readonly bigint[],
): number[] {
  const converted = Array.from<number>({ length: byteOffsets.length });
  const pending = byteOffsets.map((byteOffset, index) => ({
    byteOffset: byteOffset < 0n ? 0n : byteOffset,
    index,
  })).toSorted((left, right) => left.byteOffset < right.byteOffset
    ? -1
    : left.byteOffset > right.byteOffset
      ? 1
      : left.index - right.index);

  let pendingIndex = 0;
  let consumedBytes = 0n;
  let utf16Offset = 0;
  for (const character of value) {
    while (
      pendingIndex < pending.length
      && pending[pendingIndex]!.byteOffset <= consumedBytes
    ) {
      converted[pending[pendingIndex]!.index] = utf16Offset;
      pendingIndex += 1;
    }
    const codePoint = character.codePointAt(0)!;
    const width = BigInt(
      codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4,
    );
    const nextBytes = consumedBytes + width;
    while (
      pendingIndex < pending.length
      && pending[pendingIndex]!.byteOffset < nextBytes
    ) {
      converted[pending[pendingIndex]!.index] = utf16Offset;
      pendingIndex += 1;
    }
    consumedBytes += width;
    utf16Offset += character.length;
  }
  while (pendingIndex < pending.length) {
    converted[pending[pendingIndex]!.index] = utf16Offset;
    pendingIndex += 1;
  }
  return converted;
}

/** One-based line and code-point column of a UTF-16 offset, as the editor reports positions. */
export function sourceLocation(spl: string, offset: number): { line: number; column: number } {
  const before = spl.slice(0, Math.max(0, offset));
  const lines = before.split("\n");
  return {
    line: lines.length,
    column: Array.from(lines.at(-1) ?? "").length + 1,
  };
}

function pipelineCommandToken(stage: string): { token: string; offset: number } | null {
  const match = /^(\p{White_Space}*)([^\p{White_Space}|(),=!<>"']+)/u.exec(stage);
  const token = match?.[2];
  return token === undefined
    ? null
    : { token: token.toLowerCase(), offset: match?.[1].length ?? 0 };
}

export function getQueryDiagnostic(spl: string): SplDiagnostic | null {
  if (spl.trim().length === 0) {
    return {
      kind: "empty",
      token: "",
      message: "Enter an SPL search before running.",
      line: 1,
      column: 1,
      suggestion: "Start with an index, source, sourcetype, or search term.",
    };
  }

  const structure = scanSplStructure(spl);
  if (structure.unclosedQuote !== null) {
    const { offset, quote } = structure.unclosedQuote;
    const location = sourceLocation(spl, offset);
    const quoteName = quote === '"' ? "double" : "single";
    const tokenKind = quote === '"' ? "value" : "field identifier";
    return {
      kind: "unclosed-quote",
      token: quote,
      message: `Expected a closing ${quoteName} quotation mark.`,
      line: location.line,
      column: location.column,
      suggestion: `Close the quoted ${tokenKind} before running the search.`,
      actionLabel: `Add closing ${quote}`,
      quote,
      start: offset,
      end: spl.length,
    };
  }

  const boundaries = [-1, ...structure.pipes, spl.length];
  // The source before the first pipe is a search expression, not a command.
  // Validate every actual pipeline stage against the backend parser's command
  // switch so the editor never advertises or locally accepts a stale command.
  for (let stageIndex = 1; stageIndex < boundaries.length - 1; stageIndex += 1) {
    const pipeBefore = boundaries[stageIndex];
    const stageEnd = boundaries[stageIndex + 1];
    const contentStart = pipeBefore + 1;
    const stage = spl.slice(contentStart, stageEnd);
    const command = pipelineCommandToken(stage);
    if (command === null || isSupportedSplPipelineCommand(command.token)) continue;

    const tokenOffset = contentStart + command.offset;
    const location = sourceLocation(spl, tokenOffset);
    return {
      kind: "unsupported",
      token: command.token,
      message: `Unsupported command “${command.token}” at pipeline stage ${stageIndex}.`,
      line: location.line,
      column: location.column,
      suggestion: unsupportedCommandSuggestion(command.token),
      actionLabel: "Remove stage",
      removeStart: pipeBefore < 0 ? 0 : pipeBefore,
      removeEnd: stageEnd,
      start: tokenOffset,
      end: tokenOffset + command.token.length,
    };
  }

  return null;
}

function unsupportedCommandSuggestion(command: string): string {
  if (command === "transaction") {
    return "Use stats count by a correlation field, then inspect matching events.";
  }
  if (command === "chart") {
    return "Use stats for aggregate tables or timechart for count-over-time series.";
  }
  return "Remove this stage or use a supported command such as search, stats, top, rare, or timechart.";
}

export function applyDiagnosticFix(spl: string, diagnostic: SplDiagnostic): string {
  if (diagnostic.kind === "unclosed-quote" && diagnostic.quote !== undefined) {
    return `${spl}${diagnostic.quote}`;
  }
  if (
    diagnostic.kind === "unsupported"
    && diagnostic.removeStart !== undefined
    && diagnostic.removeEnd !== undefined
  ) {
    const before = spl.slice(0, diagnostic.removeStart).trimEnd();
    const after = spl.slice(diagnostic.removeEnd).trimStart();
    return [before, after].filter((part) => part.length > 0).join("\n");
  }
  return spl;
}

export function completionContextAt(spl: string, cursor: number): CompletionContext | null {
  const safeCursor = Math.max(0, Math.min(cursor, spl.length));
  const prefix = spl.slice(0, safeCursor);
  const structure = scanSplStructure(prefix);
  if (structure.unclosedQuote !== null) return null;

  const lastPipe = structure.pipes.at(-1) ?? -1;
  const stageStart = lastPipe + 1;
  const stagePrefix = prefix.slice(stageStart);
  const followsPipeline = lastPipe >= 0;
  const command = COMMAND_FRAGMENT.exec(stagePrefix);
  if (command !== null) {
    return {
      fragmentStart: stageStart + command[1].length,
      fragmentEnd: safeCursor,
      prefix: command[2],
      followsPipeline,
      stage: "command",
    };
  }
  const value = VALUE_FRAGMENT.exec(stagePrefix);
  if (value !== null) {
    return {
      fragmentStart: safeCursor - value[2].length,
      fragmentEnd: safeCursor,
      prefix: value[2],
      followsPipeline,
      stage: "value",
      fieldName: value[1],
    };
  }
  const term = TERM_FRAGMENT.exec(stagePrefix);
  if (term === null) return null;
  return {
    fragmentStart: safeCursor - term[1].length,
    fragmentEnd: safeCursor,
    prefix: term[1],
    followsPipeline,
    stage: "term",
  };
}

export function isCursorInQuotedValue(spl: string, cursor: number): boolean {
  return isSplOffsetInQuotedValue(spl, cursor);
}
