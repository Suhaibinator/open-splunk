import type { PrepareNearbyContextResponse } from "@/gen/ts/open_splunk/nearby_api";
import type { TypedValue } from "@/gen/ts/open_splunk/value";
import { resolveAbsoluteTimeRange, timeBucketBoundaryNanoseconds } from "./backend-data";
import { formatSplValue } from "./spl-syntax";

export type NearbyScalarKind = "string" | "number" | "boolean";
export interface NearbyScalar { kind: NearbyScalarKind; value: string }
export type NearbyOperator = "=" | "!=" | "<" | "<=" | ">" | ">=";
export interface NearbyComparison {
  id: string;
  field: string;
  operator: NearbyOperator;
  scalar: NearbyScalar;
  enabled: boolean;
}
export interface NearbyContext {
  anchorTime: string;
  earliest: string;
  latest: string;
  index: string;
  host: string;
  source: string;
  clipped: boolean;
  fields: Array<{ field: string; scalar: NearbyScalar }>;
}
export interface NearbyDraft {
  anchorTime: string;
  earliest: string;
  latest: string;
  clipped: boolean;
  comparisons: NearbyComparison[];
}
export const NEARBY_OPERATORS: readonly NearbyOperator[] = ["=", "!=", "<", "<=", ">", ">="];
const SUGGESTIONS = new Set(["trace_id", "span_id", "request_id"]);
const NUMERIC = /^-?(?:0|[1-9]\d*)(?:\.\d+)?(?:[eE][+-]?\d+)?$/u;

/** Scalar text is never routed through Number, including retained uint64 values. */
export function nearbyScalar(value: TypedValue | undefined): NearbyScalar | null {
  switch (value?.kind?.$case) {
    case "stringValue": return { kind: "string", value: value.kind.value };
    case "boolValue": return { kind: "boolean", value: String(value.kind.value) };
    case "sint64Value":
    case "uint64Value": return { kind: "number", value: value.kind.value.toString() };
    case "decimalValue": return NUMERIC.test(value.kind.value.value) ? { kind: "number", value: value.kind.value.value } : null;
    case "doubleValue": return Number.isFinite(value.kind.value) ? { kind: "number", value: String(value.kind.value) } : null;
    default: return null;
  }
}

function invalidNearbyField(): never {
  throw new Error("Enter a supported field name.");
}

export function nearbyFieldReference(field: string): string {
  const encoder = new TextEncoder();
  if (!field || !field.isWellFormed() || field.trim() !== field || /[\p{Cc}*?]/u.test(field)
    || field.toLowerCase().startsWith("__os_") || encoder.encode(field).length > 8_720) invalidNearbyField();
  let segment = "";
  let escaped = false;
  const segments: string[] = [];
  for (const character of field) {
    if (escaped) {
      if (character !== "." && character !== "\\") invalidNearbyField();
      segment += character;
      escaped = false;
    } else if (character === "\\") {
      escaped = true;
    } else if (character === ".") {
      segments.push(segment);
      segment = "";
    } else {
      segment += character;
    }
  }
  segments.push(segment);
  if (escaped || segments.length > 17 || segments.some((value) => !value || encoder.encode(value).length > 256)) invalidNearbyField();
  return `'${field.replaceAll("\\", "\\\\").replaceAll("'", "\\'")}'`;
}

export function nearbyScalarLiteral(scalar: NearbyScalar): string {
  if (scalar.kind === "string") {
    if (!scalar.value.isWellFormed()) throw new Error("Enter valid text for the comparison.");
    return formatSplValue(scalar.value);
  }
  if (scalar.kind === "boolean") {
    if (scalar.value !== "true" && scalar.value !== "false") throw new Error("Boolean values must be true or false.");
    return scalar.value;
  }
  if (!NUMERIC.test(scalar.value) || !Number.isFinite(Number(scalar.value))) throw new Error("Enter a supported finite number for the comparison.");
  // Authored integer literals stop at int64/uint64. A decimal suffix retains
  // the exact source digits while using the compiler's exact decimal comparison.
  if (/^-?\d+$/u.test(scalar.value)) {
    const integer = BigInt(scalar.value);
    if (integer < -9_223_372_036_854_775_808n || integer > 18_446_744_073_709_551_615n) return `${scalar.value}.0`;
  }
  return scalar.value;
}

/** Bind the server response to the selected retained row before any new admission. */
function invalidNearbyContext(): never {
  throw new Error("Nearby context is unavailable for this result. Rerun the search and try again.");
}

export function adaptNearbyContext(response: PrepareNearbyContextResponse, searchJobId: string, rowId: string): NearbyContext {
  if (response.searchJobId !== searchJobId || response.rowId !== rowId || !searchJobId || !rowId || !response.index) invalidNearbyContext();
  const anchor = timeBucketBoundaryNanoseconds(response.anchorTime);
  const earliest = timeBucketBoundaryNanoseconds(response.earliest);
  const latest = timeBucketBoundaryNanoseconds(response.latest);
  const minimum = timeBucketBoundaryNanoseconds("1900-01-01T00:00:00Z")!;
  const maximum = timeBucketBoundaryNanoseconds("2262-01-01T00:00:00Z")!;
  if (anchor === null || earliest === null || latest === null) return invalidNearbyContext();
  if (anchor < minimum || anchor > maximum) invalidNearbyContext();
  const lower = anchor - 300_000_000_000n;
  const upper = anchor + 300_000_000_000n;
  if (earliest !== (lower < minimum ? minimum : lower) || latest !== (upper > maximum ? maximum : upper)
    || response.clipped !== (lower < minimum || upper > maximum)) invalidNearbyContext();
  const fields: NearbyContext["fields"] = [];
  const seen = new Set<string>();
  for (const field of response.fields) {
    if (seen.has(field.fieldName)) invalidNearbyContext();
    seen.add(field.fieldName);
    const scalar = nearbyScalar(field.value);
    if (scalar === null) continue;
    try { nearbyFieldReference(field.fieldName); nearbyScalarLiteral(scalar); } catch { continue; }
    fields.push({ field: field.fieldName, scalar });
  }
  return { ...response, fields };
}

export function createNearbyDraft(context: NearbyContext): NearbyDraft {
  if (!context.index) throw new Error("The original index is unavailable. Rerun the search to find nearby events.");
  resolveAbsoluteTimeRange(context.earliest, context.latest);
  const required = ["index", "host", "source"] as const;
  return {
    anchorTime: context.anchorTime,
    earliest: context.earliest,
    latest: context.latest,
    clipped: context.clipped,
    comparisons: [
      ...required.map((field): NearbyComparison => ({ id: `context:${field}`, field, operator: "=", scalar: { kind: "string", value: context[field] }, enabled: true })),
      ...context.fields.filter(({ field }) => SUGGESTIONS.has(field)).map(({ field, scalar }): NearbyComparison => ({ id: `suggestion:${field}`, field, operator: "=", scalar, enabled: false })),
    ],
  };
}

/** Base search keeps index admission explicit; where comparisons treat stars literally. */
export function nearbySearch(draft: NearbyDraft): { query: string; timeRange: { earliest: string; latest: string; label: string; timezone: string } } {
  const active = draft.comparisons.filter((comparison) => comparison.enabled);
  const index = active.find((comparison) => comparison.field === "index" && comparison.operator === "=" && comparison.scalar.kind === "string");
  if (index === undefined || !index.scalar.value || /[*?]/u.test(index.scalar.value)) throw new Error("Keep an exact index comparison enabled.");
  const clauses = active.map(({ field, operator, scalar }) => {
    if (!NEARBY_OPERATORS.includes(operator)) throw new Error("Choose a supported comparison operator.");
    return `${nearbyFieldReference(field)} ${operator} ${nearbyScalarLiteral(scalar)}`;
  });
  const range = resolveAbsoluteTimeRange(draft.earliest, draft.latest);
  return {
    query: `index=${formatSplValue(index.scalar.value)} | where ${clauses.join(" AND ")}`,
    timeRange: { ...range, label: "Nearby events", timezone: "UTC" },
  };
}

/** Only the exact generated query can remain attached; free SPL edits detach. */
export function nearbyBuilderAttached(draft: NearbyDraft, query: string): boolean {
  try { return nearbySearch(draft).query === query; } catch { return false; }
}

/** Navigation or another click invalidates even prepare APIs that ignore abort. */
export function createNearbyPreparationGate() {
  let revision = 0;
  let controller: AbortController | null = null;
  const invalidate = () => { revision += 1; controller?.abort(); controller = null; };
  return {
    invalidate,
    async prepare<T>(load: (signal: AbortSignal) => Promise<T>, apply: (value: T) => void): Promise<boolean> {
      invalidate();
      const ownRevision = revision;
      const ownController = new AbortController();
      controller = ownController;
      try {
        const value = await load(ownController.signal);
        if (revision !== ownRevision || ownController.signal.aborted) return false;
        apply(value);
        return true;
      } catch (error) {
        if (revision !== ownRevision || ownController.signal.aborted) return false;
        throw error;
      } finally {
        if (revision === ownRevision) controller = null;
      }
    },
  };
}
