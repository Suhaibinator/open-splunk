import {
  isSplOffsetInDoubleQuotedValue,
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
}

export interface CompletionContext {
  fragmentStart: number;
  fragmentEnd: number;
  prefix: string;
  followsPipeline: boolean;
}

function sourceLocation(spl: string, offset: number): { line: number; column: number } {
  const before = spl.slice(0, Math.max(0, offset));
  const lines = before.split("\n");
  return {
    line: lines.length,
    column: Array.from(lines.at(-1) ?? "").length + 1,
  };
}

function pipelineCommandToken(stage: string): { token: string; offset: number } | null {
  const match = /^(\p{White_Space}*)([^\p{White_Space}|(),=!<>"]+)/u.exec(stage);
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
    const { offset } = structure.unclosedQuote;
    const location = sourceLocation(spl, offset);
    return {
      kind: "unclosed-quote",
      token: '"',
      message: "Expected a closing double quotation mark.",
      line: location.line,
      column: location.column,
      suggestion: "Close the quoted value before running the search.",
      actionLabel: 'Add closing "',
      quote: '"',
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
  const match = /^(\s*)([A-Za-z_]*)$/.exec(stagePrefix);
  if (match === null) return null;

  return {
    fragmentStart: stageStart + match[1].length,
    fragmentEnd: safeCursor,
    prefix: match[2],
    followsPipeline: lastPipe >= 0,
  };
}

export function isCursorInQuotedValue(spl: string, cursor: number): boolean {
  return isSplOffsetInDoubleQuotedValue(spl, cursor);
}
