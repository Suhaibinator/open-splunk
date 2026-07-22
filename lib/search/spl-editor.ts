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

interface SplStructure {
  pipes: number[];
  unclosedQuote: { character: '"' | "'"; offset: number } | null;
}

const UNSUPPORTED_COMMANDS = new Set(["join", "map", "subsearch", "transaction"]);

function scanStructure(spl: string): SplStructure {
  const pipes: number[] = [];
  let quote: '"' | "'" | null = null;
  let quoteOffset = -1;
  let escaped = false;

  for (let offset = 0; offset < spl.length; offset += 1) {
    const character = spl[offset];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (character === "\\") {
      escaped = true;
      continue;
    }
    if (quote !== null) {
      if (character === quote) quote = null;
      continue;
    }
    if (character === '"' || character === "'") {
      quote = character;
      quoteOffset = offset;
      continue;
    }
    if (character === "|") pipes.push(offset);
  }

  return {
    pipes,
    unclosedQuote: quote === null ? null : { character: quote, offset: quoteOffset },
  };
}

function sourceLocation(spl: string, offset: number): { line: number; column: number } {
  const before = spl.slice(0, Math.max(0, offset));
  const lines = before.split("\n");
  return {
    line: lines.length,
    column: Array.from(lines.at(-1) ?? "").length + 1,
  };
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

  const structure = scanStructure(spl);
  if (structure.unclosedQuote !== null) {
    const { character, offset } = structure.unclosedQuote;
    const location = sourceLocation(spl, offset);
    return {
      kind: "unclosed-quote",
      token: character,
      message: `Expected a closing ${character === '"' ? "double" : "single"} quotation mark.`,
      line: location.line,
      column: location.column,
      suggestion: "Close the quoted value before running the search.",
      actionLabel: `Add closing ${character}`,
      quote: character,
    };
  }

  const boundaries = [-1, ...structure.pipes, spl.length];
  for (let stageIndex = 0; stageIndex < boundaries.length - 1; stageIndex += 1) {
    const pipeBefore = boundaries[stageIndex];
    const stageEnd = boundaries[stageIndex + 1];
    const contentStart = pipeBefore + 1;
    const stage = spl.slice(contentStart, stageEnd);
    const commandMatch = /^\s*([A-Za-z][A-Za-z0-9_-]*)(?=\s|$)/.exec(stage);
    const token = commandMatch?.[1]?.toLowerCase();
    if (token === undefined || !UNSUPPORTED_COMMANDS.has(token)) continue;

    const tokenOffset = contentStart + (commandMatch?.[0].lastIndexOf(commandMatch[1]) ?? 0);
    const location = sourceLocation(spl, tokenOffset);
    return {
      kind: "unsupported",
      token,
      message: `Unsupported command “${token}” at pipeline stage ${stageIndex + 1}.`,
      line: location.line,
      column: location.column,
      suggestion: token === "transaction"
        ? "Use stats with values() grouped by a correlation field."
        : "Remove this stage or use a supported transforming command.",
      actionLabel: "Remove stage",
      removeStart: pipeBefore < 0 ? 0 : pipeBefore,
      removeEnd: stageEnd,
    };
  }

  return null;
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
  const structure = scanStructure(prefix);
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
  const safeCursor = Math.max(0, Math.min(cursor, spl.length));
  return scanStructure(spl.slice(0, safeCursor)).unclosedQuote !== null;
}
