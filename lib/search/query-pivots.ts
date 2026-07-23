import { indexSelectorsFromSPL } from "@/lib/api/system-bootstrap";
import {
  firstSplPipelineBoundary,
  formatSplValue,
  type SplValue,
} from "@/lib/search/spl-syntax";

export type PivotMode = "include" | "exclude" | "new";

const MAX_FIELD_NAME_BYTES = 8_720;
const MAX_FIELD_PATH_SEGMENTS = 17;
const MAX_FIELD_PATH_SEGMENT_BYTES = 256;
const UNREPRESENTABLE_FIELD_CHARACTER = /[\p{White_Space}|(),=!<>"]/u;
const UNREPRESENTABLE_FIELD_KEYWORD = /^(?:AND|OR|NOT)$/i;

function utf8Length(value: string): number {
  return new TextEncoder().encode(value).length;
}

function hasWellFormedUnicode(value: string): boolean {
  for (let index = 0; index < value.length; index += 1) {
    const codeUnit = value.charCodeAt(index);
    if (codeUnit >= 0xd800 && codeUnit <= 0xdbff) {
      const next = value.charCodeAt(index + 1);
      if (next < 0xdc00 || next > 0xdfff) return false;
      index += 1;
    } else if (codeUnit >= 0xdc00 && codeUnit <= 0xdfff) {
      return false;
    }
  }
  return true;
}

function splitFieldPath(field: string): string[] | null {
  const segments: string[] = [];
  let segment = "";
  let escaped = false;

  for (const character of field) {
    if (escaped) {
      if (character !== "." && character !== "\\") return null;
      segment += character;
      escaped = false;
      continue;
    }
    if (character === "\\") {
      escaped = true;
      continue;
    }
    if (character === ".") {
      if (segment.length === 0) return null;
      segments.push(segment);
      segment = "";
      continue;
    }
    segment += character;
  }
  if (escaped || segment.length === 0) return null;
  segments.push(segment);
  return segments;
}

/**
 * Reports whether a field can be emitted as an unquoted identifier accepted
 * by the backend's v0.1 lexer and deterministic dotted-path resolver.
 *
 * Quoted field identifiers are intentionally not synthesized: the server only
 * supports quotes for values, and treating `'field name'` as an identifier
 * would produce two ordinary word tokens rather than one field.
 */
export function isSplFieldRepresentable(field: string): boolean {
  if (
    field.length === 0
    || !hasWellFormedUnicode(field)
    || utf8Length(field) > MAX_FIELD_NAME_BYTES
    || UNREPRESENTABLE_FIELD_CHARACTER.test(field)
    || UNREPRESENTABLE_FIELD_KEYWORD.test(field)
    || field.includes("*")
    || field.toLowerCase().startsWith("__os_")
  ) {
    return false;
  }

  const segments = splitFieldPath(field);
  return segments !== null
    && segments.length <= MAX_FIELD_PATH_SEGMENTS
    && segments.every((segment) => utf8Length(segment) <= MAX_FIELD_PATH_SEGMENT_BYTES);
}

export { formatSplValue } from "@/lib/search/spl-syntax";

function exactIndexScopeClause(indexes: readonly string[]): string | null {
  const unique = [...new Set(indexes.map((index) => index.trim()).filter(Boolean))];
  if (unique.length === 0) return null;
  const comparisons = unique.map((index) => `index=${formatSplValue(index)}`);
  return comparisons.length === 1 ? comparisons[0] : `(${comparisons.join(" OR ")})`;
}

export function applyFieldPivot(
  spl: string,
  field: string,
  value: SplValue,
  mode: PivotMode,
  newSearchIndexScope?: readonly string[],
): string {
  // Some ingested JSON keys cannot be spelled in the backend's current SPL
  // field grammar. Preserve the draft rather than inserting a knowingly
  // invalid pseudo-quoted identifier.
  if (!isSplFieldRepresentable(field)) return spl;

  const comparison = `${field}=${formatSplValue(value)}`;
  const clause = mode === "exclude" ? `NOT ${comparison}` : comparison;
  const pipelineBoundary = firstSplPipelineBoundary(spl);
  const base = (pipelineBoundary === -1 ? spl : spl.slice(0, pipelineBoundary)).trim();
  const pipeline = pipelineBoundary === -1 ? "" : spl.slice(pipelineBoundary).trimStart();

  if (mode === "new") {
    // A completed backend job exposes its exact effective scope. Prefer that
    // over reconstructing authorization intent from boolean SPL. Demo mode
    // falls back to every positive index reference, never a negated one.
    const scope = exactIndexScopeClause(
      newSearchIndexScope ?? indexSelectorsFromSPL(base),
    );
    return scope === null ? comparison : `${scope} ${comparison}`;
  }

  const nextBase = base.length === 0 ? clause : `${base} ${clause}`;
  return pipeline.length === 0 ? nextBase : `${nextBase}\n${pipeline}`;
}
