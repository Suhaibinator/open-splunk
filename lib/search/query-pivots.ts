export type PivotMode = "include" | "exclude" | "new";

type SplValue = string | number | boolean | null;

const SAFE_FIELD = /^[A-Za-z_][A-Za-z0-9_.]*$/;

function escapeDoubleQuoted(value: string): string {
  return value.replaceAll("\\", "\\\\").replaceAll('"', '\\"');
}

function formatField(field: string): string {
  if (SAFE_FIELD.test(field)) {
    return field;
  }

  return `'${field.replaceAll("'", "\\'")}'`;
}

export function formatSplValue(value: SplValue): string {
  if (value === null) {
    return "null";
  }
  if (typeof value === "number" || typeof value === "boolean") {
    return String(value);
  }

  return `"${escapeDoubleQuoted(value)}"`;
}

/**
 * Returns the first pipeline boundary while respecting quoted strings and
 * escaped characters. This keeps field pivots out of later transforming
 * commands without corrupting pipes inside regexes or string literals.
 */
function firstPipelineBoundary(spl: string): number {
  let quote: '"' | "'" | null = null;
  let escaped = false;

  for (let index = 0; index < spl.length; index += 1) {
    const character = spl[index];
    if (escaped) {
      escaped = false;
      continue;
    }
    if (character === "\\") {
      escaped = true;
      continue;
    }
    if (quote !== null) {
      if (character === quote) {
        quote = null;
      }
      continue;
    }
    if (character === '"' || character === "'") {
      quote = character;
      continue;
    }
    if (character === "|") {
      return index;
    }
  }

  return -1;
}

function indexScopeFromBase(base: string): string | null {
  const match = base.match(/(?:^|\s)index\s*=\s*("(?:\\.|[^"])*"|'(?:\\.|[^'])*'|[^\s()]+)/i);
  return match?.[0]?.trim() ?? null;
}

export function applyFieldPivot(
  spl: string,
  field: string,
  value: SplValue,
  mode: PivotMode,
): string {
  const comparison = `${formatField(field)}=${formatSplValue(value)}`;
  const clause = mode === "exclude" ? `NOT ${comparison}` : comparison;
  const pipelineBoundary = firstPipelineBoundary(spl);
  const base = (pipelineBoundary === -1 ? spl : spl.slice(0, pipelineBoundary)).trim();
  const pipeline = pipelineBoundary === -1 ? "" : spl.slice(pipelineBoundary).trimStart();

  if (mode === "new") {
    const scope = indexScopeFromBase(base);
    return scope === null ? comparison : `${scope} ${comparison}`;
  }

  const nextBase = base.length === 0 ? clause : `${base} ${clause}`;
  return pipeline.length === 0 ? nextBase : `${nextBase}\n${pipeline}`;
}
