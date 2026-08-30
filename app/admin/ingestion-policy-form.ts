import type { IngestionToken } from "@/gen/ts/open_splunk/collector_admin";
import type { Index } from "@/gen/ts/open_splunk/index";
import { describeByteQuantity, formatByteQuantity, parseByteQuantity } from "@/lib/byte-quantity";

/**
 * The optional ingestion policy on an index and on a token, and what it accepts.
 *
 * These fields had the opposite unit defect from the search-limits form. "Maximum
 * event bytes" and "Maximum bytes per second" took a raw byte count while their
 * hints spoke of "1 MiB" and "1 TiB/s", so setting the documented maximum meant
 * typing 1048576 or 1099511627776 by hand -- and the hint's "1 MiB" was prose
 * beside `INDEX_MAX_EVENT_BYTES`, free to drift from it. Both now hold a quantity
 * (`lib/byte-quantity.ts`), and every hint states its ceiling by formatting the
 * constant the parser enforces, so the two cannot disagree.
 *
 * They also had no validation until submit. `indexPolicyFromForm` threw, the
 * console caught it and raised a toast, and the field that caused it was named
 * only in the prose of the message -- while the Create and Save buttons stayed
 * enabled, because nothing had asked whether the form was acceptable. Validation
 * is up front now: `indexPolicyErrors` and `tokenPolicyErrors` mark the field and
 * gate the button, and the `…FromForm` throw is a backstop that routes through
 * the same validators, so the two cannot report different things.
 *
 * Every numeric field here is optional, and blank and "0" mean the same thing.
 * What that thing is differs -- an index limit inherits the server's, a rate
 * limit means unlimited -- which is why each field states its own note rather
 * than sharing one.
 */
export const INDEX_MAX_EVENT_BYTES = 1_048_576n;
export const INDEX_MAX_FIELD_COUNT = 1_024;
export const INDEX_MAX_NESTING_DEPTH = 16;
export const INDEX_MAX_FUTURE_SKEW_SECONDS = 300n;
export const INDEX_MAX_EVENT_AGE_SECONDS = 31_536_000n;
export const INGESTION_MAX_EVENTS_PER_SECOND = 1_000_000n;
export const INGESTION_MAX_BYTES_PER_SECOND = 1_099_511_627_776n;

/** At most 16 unique patterns, 512 UTF-8 bytes each, 4,096 bytes in total. */
const MAXIMUM_PATTERNS = 16;
const MAXIMUM_PATTERN_BYTES = 512;
const MAXIMUM_PATTERNS_TOTAL_BYTES = 4_096;

export interface IndexPolicyForm {
  defaultSourcetype: string;
  maxEventBytes: string;
  maxEventsPerSecond: string;
  maxFieldCount: string;
  maxNestingDepth: string;
  maximumEventAgeSeconds: string;
  maximumFutureSkewSeconds: string;
  maxUncompressedBytesPerSecond: string;
}

export interface TokenPolicyForm {
  allowedHostRegexes: string;
  allowedSourceRegexes: string;
  maxEventsPerSecond: string;
  maxUncompressedBytesPerSecond: string;
}

/**
 * How a field is read, stated back, and complained about.
 *
 * `bytes` holds a unit-bearing quantity, `count` a whole number of the thing
 * `unit` names, and `seconds` a duration with up to three decimal places --
 * which is the protobuf `Duration`'s millisecond floor, not an arbitrary cap.
 */
type PolicyFieldKind = "bytes" | "count" | "seconds";

interface PolicyField {
  kind: PolicyFieldKind;
  /**
   * A `bytes` label never names a unit, because the value carries one. A `count`
   * or `seconds` label may, because the value does not.
   */
  label: string;
  maximum: bigint;
  /** What leaving the field empty means; the ceiling is appended to it. */
  note: string;
  /** What an empty field will do, shown in the field while it is empty. */
  placeholder: string;
  /** The noun a count is measured in, and the empty string for a quantity. */
  unit: string;
}

const INHERITS = "Zero or blank inherits the server limit;";
const INHERIT_PLACEHOLDER = "Inherit server limit";
const UNLIMITED = "Zero or blank is unlimited;";
const UNLIMITED_PLACEHOLDER = "Unlimited";

const RATE_FIELDS = {
  maxEventsPerSecond: {
    kind: "count",
    label: "Maximum events per second",
    maximum: INGESTION_MAX_EVENTS_PER_SECOND,
    note: UNLIMITED,
    placeholder: UNLIMITED_PLACEHOLDER,
    unit: "events per second",
  },
  maxUncompressedBytesPerSecond: {
    kind: "bytes",
    label: "Maximum ingestion rate",
    maximum: INGESTION_MAX_BYTES_PER_SECOND,
    note: "Uncompressed event bytes per second. Zero or blank is unlimited;",
    placeholder: UNLIMITED_PLACEHOLDER,
    unit: "",
  },
} as const satisfies Record<string, PolicyField>;

export const INDEX_POLICY_FIELDS = {
  maxEventBytes: {
    kind: "bytes",
    label: "Maximum event size",
    maximum: INDEX_MAX_EVENT_BYTES,
    note: INHERITS,
    placeholder: INHERIT_PLACEHOLDER,
    unit: "",
  },
  maxFieldCount: {
    kind: "count",
    label: "Maximum field count",
    maximum: BigInt(INDEX_MAX_FIELD_COUNT),
    note: INHERITS,
    placeholder: INHERIT_PLACEHOLDER,
    unit: "fields",
  },
  maxNestingDepth: {
    kind: "count",
    label: "Maximum nesting depth",
    maximum: BigInt(INDEX_MAX_NESTING_DEPTH),
    note: INHERITS,
    placeholder: INHERIT_PLACEHOLDER,
    unit: "path segments",
  },
  maximumFutureSkewSeconds: {
    kind: "seconds",
    label: "Maximum future skew (seconds)",
    maximum: INDEX_MAX_FUTURE_SKEW_SECONDS,
    note: INHERITS,
    placeholder: INHERIT_PLACEHOLDER,
    unit: "seconds",
  },
  maximumEventAgeSeconds: {
    kind: "seconds",
    label: "Maximum event age (seconds)",
    maximum: INDEX_MAX_EVENT_AGE_SECONDS,
    note: INHERITS,
    placeholder: INHERIT_PLACEHOLDER,
    unit: "seconds",
  },
  ...RATE_FIELDS,
} as const satisfies Record<string, PolicyField>;

export const TOKEN_POLICY_FIELDS = RATE_FIELDS;

export type IndexPolicyFieldKey = keyof typeof INDEX_POLICY_FIELDS;
export type TokenPolicyFieldKey = keyof typeof TOKEN_POLICY_FIELDS;

export const INDEX_POLICY_KEYS = Object.keys(INDEX_POLICY_FIELDS) as IndexPolicyFieldKey[];
export const TOKEN_POLICY_KEYS = Object.keys(TOKEN_POLICY_FIELDS) as TokenPolicyFieldKey[];

/** A duration with at most three decimal places: the Duration message's floor. */
const SECONDS = /^(\d+)(?:\.(\d{1,3}))?$/u;

/** Whether the field is empty in the sense both blank and "0" have. */
function isUnset(kind: PolicyFieldKind, value: string): boolean {
  const normalized = value.trim();
  if (normalized.length === 0) return true;
  return kind === "seconds" ? /^0(?:\.0+)?$/u.test(normalized) : normalized === "0";
}

/** The ceiling as the field's own notation states it. */
function statedMaximum(field: PolicyField): string {
  if (field.kind === "bytes") return formatByteQuantity(field.maximum);
  return `${field.maximum.toLocaleString()} ${field.unit}`;
}

function policyFieldError(field: PolicyField, value: string): string | null {
  if (isUnset(field.kind, value)) return null;
  const normalized = value.trim();
  if (field.kind === "bytes") {
    const parsed = parseByteQuantity(normalized);
    if (parsed === null) {
      return `Enter a size such as ${formatByteQuantity(field.maximum)}, or a plain number of bytes.`;
    }
    return parsed > field.maximum ? `Enter ${statedMaximum(field)} or less.` : null;
  }
  if (field.kind === "count") {
    if (!/^\d+$/u.test(normalized)) return "Enter a whole number, or leave the field blank.";
    return BigInt(normalized) > field.maximum ? `Enter ${statedMaximum(field)} or less.` : null;
  }
  const match = SECONDS.exec(normalized);
  if (match === null) return "Enter seconds with at most three decimal places, or leave the field blank.";
  const seconds = BigInt(match[1]);
  const nanos = Number((match[2] ?? "").padEnd(9, "0"));
  return seconds > field.maximum || (seconds === field.maximum && nanos > 0)
    ? `Enter ${statedMaximum(field)} or less.`
    : null;
}

/**
 * The line under a policy field: what leaving it empty does, and its ceiling.
 *
 * The ceiling is formatted from the constant the parser enforces rather than
 * written as prose beside it, which is what let the old hint promise "1 MiB" to
 * a field that measured bytes. A byte field also echoes the exact count it read
 * whenever the text is not already that count's canonical spelling.
 */
function policyFieldHint(field: PolicyField, value: string): string {
  const echo = field.kind !== "bytes" || isUnset(field.kind, value)
    ? null
    : parseByteQuantity(value.trim());
  const parsed = echo === null || value.trim() === formatByteQuantity(echo)
    ? ""
    : `${describeByteQuantity(echo)}. `;
  return `${parsed}${field.note} maximum ${statedMaximum(field)}.`;
}

export function indexPolicyFieldHint(key: IndexPolicyFieldKey, form: IndexPolicyForm): string {
  return policyFieldHint(INDEX_POLICY_FIELDS[key], form[key]);
}

export function tokenPolicyFieldHint(key: TokenPolicyFieldKey, form: TokenPolicyForm): string {
  return policyFieldHint(TOKEN_POLICY_FIELDS[key], form[key]);
}

export function indexPolicyErrors(form: IndexPolicyForm): Record<IndexPolicyFieldKey, string | null> {
  return {
    maxEventBytes: policyFieldError(INDEX_POLICY_FIELDS.maxEventBytes, form.maxEventBytes),
    maxEventsPerSecond: policyFieldError(INDEX_POLICY_FIELDS.maxEventsPerSecond, form.maxEventsPerSecond),
    maxFieldCount: policyFieldError(INDEX_POLICY_FIELDS.maxFieldCount, form.maxFieldCount),
    maxNestingDepth: policyFieldError(INDEX_POLICY_FIELDS.maxNestingDepth, form.maxNestingDepth),
    maximumEventAgeSeconds: policyFieldError(
      INDEX_POLICY_FIELDS.maximumEventAgeSeconds,
      form.maximumEventAgeSeconds,
    ),
    maximumFutureSkewSeconds: policyFieldError(
      INDEX_POLICY_FIELDS.maximumFutureSkewSeconds,
      form.maximumFutureSkewSeconds,
    ),
    maxUncompressedBytesPerSecond: policyFieldError(
      INDEX_POLICY_FIELDS.maxUncompressedBytesPerSecond,
      form.maxUncompressedBytesPerSecond,
    ),
  };
}

export function tokenPolicyErrors(form: TokenPolicyForm): Record<keyof TokenPolicyForm, string | null> {
  return {
    allowedHostRegexes: tokenPatternsError(form.allowedHostRegexes),
    allowedSourceRegexes: tokenPatternsError(form.allowedSourceRegexes),
    maxEventsPerSecond: policyFieldError(TOKEN_POLICY_FIELDS.maxEventsPerSecond, form.maxEventsPerSecond),
    maxUncompressedBytesPerSecond: policyFieldError(
      TOKEN_POLICY_FIELDS.maxUncompressedBytesPerSecond,
      form.maxUncompressedBytesPerSecond,
    ),
  };
}

export function indexPolicyIsValid(form: IndexPolicyForm): boolean {
  return Object.values(indexPolicyErrors(form)).every((error) => error === null);
}

export function tokenPolicyIsValid(form: TokenPolicyForm): boolean {
  return Object.values(tokenPolicyErrors(form)).every((error) => error === null);
}

/** Why a pattern list cannot be accepted, or `null` when it can. */
function patternsError(patterns: Iterable<string>): string | null {
  const unique = new Set(patterns);
  if (unique.size > MAXIMUM_PATTERNS) return `Enter at most ${MAXIMUM_PATTERNS} unique patterns.`;
  const encoder = new TextEncoder();
  let totalBytes = 0;
  for (const pattern of unique) {
    if (pattern.length === 0) return "A pattern cannot be empty.";
    const bytes = encoder.encode(pattern).byteLength;
    if (bytes > MAXIMUM_PATTERN_BYTES) {
      return `Each pattern must be ${MAXIMUM_PATTERN_BYTES.toLocaleString()} UTF-8 bytes or fewer.`;
    }
    if (pattern.includes("\0")) return "A pattern cannot contain a NUL character.";
    totalBytes += bytes;
  }
  return totalBytes > MAXIMUM_PATTERNS_TOTAL_BYTES
    ? `The patterns must be ${MAXIMUM_PATTERNS_TOTAL_BYTES.toLocaleString()} UTF-8 bytes or fewer in total.`
    : null;
}

/** The lines of a pattern textarea, which is where blank lines are dropped. */
function patternLines(value: string): string[] {
  return value.split(/\r?\n/u).filter((pattern) => pattern.length > 0);
}

/** Why the text in a pattern field cannot be accepted, in the parser's words. */
export function tokenPatternsError(value: string): string | null {
  return patternsError(patternLines(value));
}

/**
 * The patterns, sorted and de-duplicated, or a throw naming the same reason.
 *
 * The array form is reached from the persisted token-create guard, whose
 * patterns come back from storage rather than from a textarea and may therefore
 * carry an empty entry a field could never produce.
 */
export function normalizeTokenPatterns(patterns: Iterable<string>, label: string): string[] {
  const unique = new Set(patterns);
  const error = patternsError(unique);
  if (error !== null) throw new Error(`${label} patterns: ${error}`);
  return [...unique].toSorted();
}

/**
 * The same, from the text a field holds.
 *
 * The throw is the backstop for a caller that submitted without checking. It
 * routes through the validator the field displays, so a submit cannot produce a
 * second wording of a message already on screen.
 */
export function tokenPatternsFromForm(value: string, label: string): string[] {
  return normalizeTokenPatterns(patternLines(value), label);
}

/** Reads one optional numeric field, throwing the message the field displays. */
function policyValue(field: PolicyField, value: string): bigint | undefined {
  const error = policyFieldError(field, value);
  if (error !== null) throw new Error(`${field.label}: ${error}`);
  if (isUnset(field.kind, value)) return undefined;
  const normalized = value.trim();
  return field.kind === "bytes" ? parseByteQuantity(normalized)! : BigInt(normalized);
}

function policyDuration(
  field: PolicyField,
  value: string,
): { seconds: bigint; nanos: number } | undefined {
  const error = policyFieldError(field, value);
  if (error !== null) throw new Error(`${field.label}: ${error}`);
  if (isUnset(field.kind, value)) return undefined;
  const match = SECONDS.exec(value.trim())!;
  return { seconds: BigInt(match[1]), nanos: Number((match[2] ?? "").padEnd(9, "0")) };
}

function optionalFormValue(value: bigint | number | undefined): string {
  return value === undefined || value === 0 || value === 0n ? "" : value.toString();
}

/** A byte count as the quantity its field holds, and "" for an unset one. */
function optionalByteFormValue(value: bigint | undefined): string {
  return value === undefined || value === 0n ? "" : formatByteQuantity(value);
}

function durationFormValue(duration: { seconds: bigint; nanos: number } | undefined): string {
  if (duration === undefined || (duration.seconds === 0n && duration.nanos === 0)) return "";
  if (duration.nanos === 0) return duration.seconds.toString();
  return `${duration.seconds}.${duration.nanos.toString().padStart(9, "0").replace(/0+$/u, "")}`;
}

export function indexPolicyFormFromDefinition(definition?: Index["definition"]): IndexPolicyForm {
  return {
    defaultSourcetype: definition?.defaultSourcetype ?? "",
    maxEventBytes: optionalByteFormValue(definition?.limits?.maxEventBytes),
    maxEventsPerSecond: optionalFormValue(definition?.ingestionRateLimits?.maxEventsPerSecond),
    maxFieldCount: optionalFormValue(definition?.limits?.maxFieldCount),
    maxNestingDepth: optionalFormValue(definition?.limits?.maxNestingDepth),
    maximumEventAgeSeconds: durationFormValue(definition?.limits?.maximumEventAge),
    maximumFutureSkewSeconds: durationFormValue(definition?.limits?.maximumFutureSkew),
    maxUncompressedBytesPerSecond: optionalByteFormValue(
      definition?.ingestionRateLimits?.maxUncompressedBytesPerSecond,
    ),
  };
}

export function indexPolicyFromForm(form: IndexPolicyForm) {
  const fields = INDEX_POLICY_FIELDS;
  const fieldCount = policyValue(fields.maxFieldCount, form.maxFieldCount);
  const nestingDepth = policyValue(fields.maxNestingDepth, form.maxNestingDepth);
  return {
    defaultSourcetype: form.defaultSourcetype.trim() || undefined,
    limits: {
      maxEventBytes: policyValue(fields.maxEventBytes, form.maxEventBytes),
      maxFieldCount: fieldCount === undefined ? undefined : Number(fieldCount),
      maxNestingDepth: nestingDepth === undefined ? undefined : Number(nestingDepth),
      maximumFutureSkew: policyDuration(fields.maximumFutureSkewSeconds, form.maximumFutureSkewSeconds),
      maximumEventAge: policyDuration(fields.maximumEventAgeSeconds, form.maximumEventAgeSeconds),
    },
    ingestionRateLimits: {
      maxEventsPerSecond: policyValue(fields.maxEventsPerSecond, form.maxEventsPerSecond),
      maxUncompressedBytesPerSecond: policyValue(
        fields.maxUncompressedBytesPerSecond,
        form.maxUncompressedBytesPerSecond,
      ),
    },
  };
}

export function tokenPolicyFormFromToken(token?: IngestionToken): TokenPolicyForm {
  return {
    allowedHostRegexes: token?.constraints?.allowedHostRegexes.join("\n") ?? "",
    allowedSourceRegexes: token?.constraints?.allowedSourceRegexes.join("\n") ?? "",
    maxEventsPerSecond: optionalFormValue(token?.ingestionRateLimits?.maxEventsPerSecond),
    maxUncompressedBytesPerSecond: optionalByteFormValue(
      token?.ingestionRateLimits?.maxUncompressedBytesPerSecond,
    ),
  };
}

export function tokenPolicyFromForm(form: TokenPolicyForm) {
  return {
    allowedHostRegexes: tokenPatternsFromForm(form.allowedHostRegexes, "Allowed host"),
    allowedSourceRegexes: tokenPatternsFromForm(form.allowedSourceRegexes, "Allowed source"),
    ingestionRateLimits: {
      maxEventsPerSecond: policyValue(TOKEN_POLICY_FIELDS.maxEventsPerSecond, form.maxEventsPerSecond),
      maxUncompressedBytesPerSecond: policyValue(
        TOKEN_POLICY_FIELDS.maxUncompressedBytesPerSecond,
        form.maxUncompressedBytesPerSecond,
      ),
    },
  };
}
