import type { SearchLimits } from "@/gen/ts/open_splunk/server_settings_api";
import { describeByteQuantity, formatByteQuantity, parseByteQuantity } from "@/lib/byte-quantity";

/**
 * The server-settings form, its units, and the reason a value is not acceptable.
 *
 * Three things used to be spread across this module and the component that
 * renders it, and each was wrong in a way the other could not see.
 *
 * The units were the first. Four fields were labelled "…bytes" -- "Retained
 * bytes per job", "Total retained result bytes", "Bytes read" -- and held
 * mebibytes, because `searchLimitsToForm` divided the protobuf value by a
 * mebibyte on the way in and `searchLimitsFromForm` multiplied it back on the
 * way out. The only statement of the real unit was the grey hint underneath, and
 * the index and token policy fields on the same console took raw bytes under
 * labels spelled the same way. Those fields now hold a quantity that carries its
 * own unit (`lib/byte-quantity.ts`), so the label names the thing being limited
 * and the value names its scale.
 *
 * The second was a whole mechanism that existed only to undo the first. Dividing
 * by a mebibyte is lossy, so a limit the server reported as 67108865 came back
 * as 64 and would have been written back as 67108864 -- silently editing a value
 * the administrator never touched. `searchLimitsFromForm` therefore took an
 * `exactBase` copy of the server's limits and restored, field by field, whatever
 * the display had rounded. `formatByteQuantity` round-trips exactly, so all four
 * byte fields dropped out of that mechanism. What remains is the two duration
 * fields, whose display genuinely is lossy: a runtime carries nanoseconds the
 * "seconds" field cannot show, and a 901-second result retention displays as 15
 * minutes. Those two are the whole of `exactBase` now, and each is commented.
 *
 * The third was that the component decided what counted as invalid. It parsed
 * the minimum and maximum back out of their own display strings to compare
 * against, which is only exact while the display is -- precisely the property
 * the mebibyte division did not have -- and it meant the rule that a per-job
 * result ceiling cannot exceed the total was written inside a render. Validation
 * lives here now, compares against the protobuf limits themselves, and is
 * covered by search-limits-form.test.ts.
 */
export interface SearchLimitForm {
  bytesRead: string;
  concurrency: string;
  groupedRows: string;
  memory: string;
  resultBytes: string;
  resultRows: string;
  retentionMinutes: string;
  rowsRead: string;
  runtimeSeconds: string;
  threads: string;
  totalResultBytes: string;
}

export type SearchLimitKey = keyof SearchLimitForm;

export const SEARCH_LIMIT_GROUPS = ["Execution", "Results and retention", "Scheduling"] as const;

export type SearchLimitGroup = typeof SEARCH_LIMIT_GROUPS[number];

interface SearchLimitField {
  group: SearchLimitGroup;
  /**
   * `bytes` reads and writes a unit-bearing quantity; `count` is a plain whole
   * number of the thing named by `unit`. The kind decides how the value is
   * parsed, how a range is stated back, and what an unparseable entry is told.
   */
  kind: "bytes" | "count";
  label: string;
  /** The noun a count is measured in, and the empty string for a quantity. */
  unit: string;
}

/**
 * Every field, in the order each group renders.
 *
 * No label says "bytes" any more. A field whose kind is `bytes` shows its scale
 * in the value -- `512 MiB` -- so a label that also named a unit would be the
 * second place a scale is stated, which is how the original defect started.
 */
export const SEARCH_LIMIT_FIELDS = {
  runtimeSeconds: { group: "Execution", kind: "count", label: "Maximum runtime", unit: "seconds" },
  memory: { group: "Execution", kind: "bytes", label: "Per-query memory", unit: "" },
  rowsRead: { group: "Execution", kind: "count", label: "Rows read", unit: "rows" },
  bytesRead: { group: "Execution", kind: "bytes", label: "Data read per query", unit: "" },
  groupedRows: { group: "Execution", kind: "count", label: "Grouped rows", unit: "rows" },
  threads: { group: "Execution", kind: "count", label: "ClickHouse threads", unit: "threads" },
  resultRows: { group: "Results and retention", kind: "count", label: "Retained rows per job", unit: "rows" },
  resultBytes: { group: "Results and retention", kind: "bytes", label: "Retained results per job", unit: "" },
  totalResultBytes: { group: "Results and retention", kind: "bytes", label: "Total retained results", unit: "" },
  retentionMinutes: { group: "Results and retention", kind: "count", label: "Result retention", unit: "minutes" },
  concurrency: { group: "Scheduling", kind: "count", label: "Concurrent searches", unit: "searches" },
} as const satisfies Record<SearchLimitKey, SearchLimitField>;

export const SEARCH_LIMIT_KEYS = Object.keys(SEARCH_LIMIT_FIELDS) as SearchLimitKey[];

/** `maximum_concurrent_searches` is a uint32 on the wire; the rest are uint64. */
const MAXIMUM_CONCURRENCY = 0xffff_ffffn;

function durationSeconds(value: SearchLimits["maximumRuntime"]): bigint {
  return value?.seconds ?? 0n;
}

/**
 * One field's value from the protobuf, in the unit its form field states.
 *
 * Reading the limits themselves is what lets a range be checked exactly. The
 * component used to parse the minimum and the maximum back out of their own
 * display strings, so the comparison inherited whatever the display had rounded.
 */
export function searchLimitValue(limits: SearchLimits, key: SearchLimitKey): bigint {
  switch (key) {
    case "bytesRead": return limits.maximumBytesToRead;
    case "concurrency": return BigInt(limits.maximumConcurrentSearches);
    case "groupedRows": return limits.maximumGroupedRows;
    case "memory": return limits.maximumMemoryBytes;
    case "resultBytes": return limits.maximumResultBytes;
    case "resultRows": return limits.maximumResultRows;
    case "retentionMinutes": return durationSeconds(limits.resultRetention) / 60n;
    case "rowsRead": return limits.maximumRowsToRead;
    case "runtimeSeconds": return durationSeconds(limits.maximumRuntime);
    case "threads": return limits.maximumThreads;
    case "totalResultBytes": return limits.maximumTotalResultBytes;
  }
}

/** One field's value as the text its input holds. */
export function searchLimitText(limits: SearchLimits, key: SearchLimitKey): string {
  const value = searchLimitValue(limits, key);
  return SEARCH_LIMIT_FIELDS[key].kind === "bytes" ? formatByteQuantity(value) : value.toString();
}

export function searchLimitsToForm(value: SearchLimits): SearchLimitForm {
  return {
    bytesRead: searchLimitText(value, "bytesRead"),
    concurrency: searchLimitText(value, "concurrency"),
    groupedRows: searchLimitText(value, "groupedRows"),
    memory: searchLimitText(value, "memory"),
    resultBytes: searchLimitText(value, "resultBytes"),
    resultRows: searchLimitText(value, "resultRows"),
    retentionMinutes: searchLimitText(value, "retentionMinutes"),
    rowsRead: searchLimitText(value, "rowsRead"),
    runtimeSeconds: searchLimitText(value, "runtimeSeconds"),
    threads: searchLimitText(value, "threads"),
    totalResultBytes: searchLimitText(value, "totalResultBytes"),
  };
}

export function parsePositiveInteger(value: string): bigint | null {
  if (!/^[1-9][0-9]*$/.test(value)) return null;
  try {
    return BigInt(value);
  } catch {
    return null;
  }
}

/** Reads one field the way its kind says to, without judging the result's range. */
export function parseSearchLimitField(key: SearchLimitKey, value: string): bigint | null {
  return SEARCH_LIMIT_FIELDS[key].kind === "bytes"
    ? parseByteQuantity(value)
    : parsePositiveInteger(value);
}

/**
 * How a range reads back to the administrator, in the field's own notation --
 * the same spelling `searchLimitText` uses, so a stated bound can be typed back.
 */
function statedRange(key: SearchLimitKey, minimum: bigint, maximum: bigint): string {
  const field = SEARCH_LIMIT_FIELDS[key];
  return field.kind === "bytes"
    ? `${formatByteQuantity(minimum)}–${formatByteQuantity(maximum)}`
    : `${minimum}–${maximum} ${field.unit}`;
}

/**
 * Why one field cannot be accepted, or `null` when it can.
 *
 * The cross-field rule is not here because it is not one field's answer: a
 * per-job ceiling above the total is a disagreement between two values, and
 * `searchLimitErrors` decides which of the two to report it on.
 */
export function searchLimitFieldError(
  key: SearchLimitKey,
  form: SearchLimitForm,
  minimums: SearchLimits,
  maximums: SearchLimits,
): string | null {
  const value = parseSearchLimitField(key, form[key]);
  if (value === null) {
    return SEARCH_LIMIT_FIELDS[key].kind === "bytes"
      ? "Enter a size such as 512 MiB, or a plain number of bytes."
      : "Enter a whole number greater than zero.";
  }
  const minimum = searchLimitValue(minimums, key);
  const maximum = searchLimitValue(maximums, key);
  // The uint32 ceiling `searchLimitsFromForm` enforces needs no test of its own
  // here: `maximum_concurrent_searches` is itself a uint32, so the server's own
  // maximum cannot state a value above it and the range check above covers it.
  if (value < minimum || value > maximum) return `Enter ${statedRange(key, minimum, maximum)}.`;
  return null;
}

/**
 * Every field's error, including the one rule that spans two fields.
 *
 * The per-job/total disagreement is reported on the total rather than on the
 * per-job value because the total is the field that has to move: raising a
 * per-job ceiling is the deliberate act, and the total is the one silently left
 * behind by it.
 *
 * It is reported only when both fields are otherwise acceptable. A per-job value
 * that is itself out of range is already the thing to fix, and comparing against
 * it would put an impossible instruction on a field nobody touched: 16 GiB
 * per job against a 1 GiB ceiling would tell the total, whose own maximum is
 * 8 GiB, to "enter at least 16 GiB".
 */
export function searchLimitErrors(
  form: SearchLimitForm,
  minimums: SearchLimits,
  maximums: SearchLimits,
): Record<SearchLimitKey, string | null> {
  const errors = Object.fromEntries(
    SEARCH_LIMIT_KEYS.map((key) => [key, searchLimitFieldError(key, form, minimums, maximums)]),
  ) as Record<SearchLimitKey, string | null>;
  const perJob = parseSearchLimitField("resultBytes", form.resultBytes);
  const total = parseSearchLimitField("totalResultBytes", form.totalResultBytes);
  if (
    errors.resultBytes === null
    && errors.totalResultBytes === null
    && perJob !== null
    && total !== null
    && perJob > total
  ) {
    errors.totalResultBytes = `Enter at least the per-job limit of ${formatByteQuantity(perJob)}.`;
  }
  return errors;
}

/**
 * The line under a valid field: what it will send, and what it may hold.
 *
 * A byte field states the exact count it parsed to only when the text is not
 * already the canonical spelling of that count -- so `512 MiB` says nothing
 * extra, while `64MB` and `67108864` both explain themselves. That echo is what
 * keeps accepting `MB` as an alias for `MiB` from being a silent reinterpretation
 * of what somebody typed.
 */
export function searchLimitFieldHint(
  key: SearchLimitKey,
  form: SearchLimitForm,
  defaults: SearchLimits,
  minimums: SearchLimits,
  maximums: SearchLimits,
): string {
  const range = statedRange(key, searchLimitValue(minimums, key), searchLimitValue(maximums, key));
  const fallback = searchLimitText(defaults, key);
  const parsed = SEARCH_LIMIT_FIELDS[key].kind === "bytes" ? parseByteQuantity(form[key]) : null;
  const echo = parsed !== null && form[key].trim() !== formatByteQuantity(parsed)
    ? `${describeByteQuantity(parsed)}. `
    : "";
  return `${echo}${range}; default ${fallback}.`;
}

export function sameSearchLimits(left: SearchLimits, right: SearchLimits): boolean {
  return left.maximumRuntime?.seconds === right.maximumRuntime?.seconds
    && left.maximumRuntime?.nanos === right.maximumRuntime?.nanos
    && left.maximumMemoryBytes === right.maximumMemoryBytes
    && left.maximumRowsToRead === right.maximumRowsToRead
    && left.maximumBytesToRead === right.maximumBytesToRead
    && left.maximumGroupedRows === right.maximumGroupedRows
    && left.maximumThreads === right.maximumThreads
    && left.maximumResultRows === right.maximumResultRows
    && left.maximumResultBytes === right.maximumResultBytes
    && left.maximumTotalResultBytes === right.maximumTotalResultBytes
    && left.maximumConcurrentSearches === right.maximumConcurrentSearches
    && left.resultRetention?.seconds === right.resultRetention?.seconds
    && left.resultRetention?.nanos === right.resultRetention?.nanos;
}

export function searchLimitsFromForm(value: SearchLimitForm, exactBase?: SearchLimits): SearchLimits | null {
  const parsed = Object.fromEntries(
    SEARCH_LIMIT_KEYS.map((key) => [key, parseSearchLimitField(key, value[key])]),
  ) as Record<SearchLimitKey, bigint | null>;
  if (Object.values(parsed).some((item) => item === null)) return null;
  const concurrency = parsed.concurrency!;
  if (concurrency > MAXIMUM_CONCURRENCY) return null;
  const result: SearchLimits = {
    maximumRuntime: { seconds: parsed.runtimeSeconds!, nanos: 0 },
    maximumMemoryBytes: parsed.memory!,
    maximumRowsToRead: parsed.rowsRead!,
    maximumBytesToRead: parsed.bytesRead!,
    maximumGroupedRows: parsed.groupedRows!,
    maximumThreads: parsed.threads!,
    maximumResultRows: parsed.resultRows!,
    maximumResultBytes: parsed.resultBytes!,
    maximumTotalResultBytes: parsed.totalResultBytes!,
    maximumConcurrentSearches: Number(concurrency),
    resultRetention: { seconds: parsed.retentionMinutes! * 60n, nanos: 0 },
  };
  if (exactBase !== undefined) {
    // The two fields whose display really is lossy. A runtime shown in whole
    // seconds cannot state the nanoseconds the server may hold, and a retention
    // shown in whole minutes cannot state 901 seconds -- so leaving the field
    // untouched has to mean leaving the value untouched, or opening the page and
    // saving an unrelated field would quietly round one of them. The byte fields
    // used to need the same treatment and no longer do: `formatByteQuantity`
    // writes a quantity that reads back as exactly the value it came from.
    const base = searchLimitsToForm(exactBase);
    if (value.runtimeSeconds === base.runtimeSeconds) result.maximumRuntime = exactBase.maximumRuntime;
    if (value.retentionMinutes === base.retentionMinutes) result.resultRetention = exactBase.resultRetention;
  }
  return result;
}
