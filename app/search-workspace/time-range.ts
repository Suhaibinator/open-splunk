import type { TimeRange } from "./model";

const RFC3339_EXPRESSION =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-](\d{2}):(\d{2}))$/;
const RELATIVE_EXPRESSION = /^-(\d+)([smhd])$/;
const SNAPPED_DAY_EXPRESSION = /^-(\d+)d@d$/;
const MAXIMUM_EXPRESSION_BYTES = 1_024;
const MAXIMUM_TIMEZONE_BYTES = 255;
const MAXIMUM_SNAPPED_DAYS = 132_218n;
const MAX_INT64_SECONDS = 9_223_372_036_854_775_807n;
const MAX_INT32_DAYS = 2_147_483_647n;
const NANOSECONDS_PER_MILLISECOND = 1_000_000n;
const MILLISECONDS_PER_DAY = 86_400_000;
const MINIMUM_SEARCH_TIME_MS = Date.parse("1900-01-01T00:00:00Z");
const MAXIMUM_SEARCH_TIME_MS = Date.parse("2262-01-01T00:00:00Z");
const MINIMUM_SEARCH_TIME_NS =
  BigInt(MINIMUM_SEARCH_TIME_MS) * NANOSECONDS_PER_MILLISECOND;
const MAXIMUM_SEARCH_TIME_NS =
  BigInt(MAXIMUM_SEARCH_TIME_MS) * NANOSECONDS_PER_MILLISECOND;
const textEncoder = new TextEncoder();

type ParsedTimeExpression =
  | { kind: "now" }
  | { kind: "earliest-data" }
  | { kind: "elapsed"; seconds: bigint }
  | { kind: "calendar-day"; days: bigint }
  | { kind: "snapped-day"; days: bigint }
  | { kind: "absolute"; nanoseconds: bigint };

interface SymbolicCalendarPosition {
  days: bigint;
  phase: 0 | 1;
}

function isServerSpace(codeUnit: number): boolean {
  return (codeUnit >= 0x0009 && codeUnit <= 0x000D)
    || codeUnit === 0x0020
    || codeUnit === 0x0085
    || codeUnit === 0x00A0
    || codeUnit === 0x1680
    || (codeUnit >= 0x2000 && codeUnit <= 0x200A)
    || codeUnit === 0x2028
    || codeUnit === 0x2029
    || codeUnit === 0x202F
    || codeUnit === 0x205F
    || codeUnit === 0x3000;
}

function trimServerSpace(value: string): string {
  // Match Go strings.TrimSpace instead of JavaScript trim(), whose whitespace
  // set differs for values such as U+0085 and U+FEFF.
  let start = 0;
  let end = value.length;
  while (start < end && isServerSpace(value.charCodeAt(start))) start += 1;
  while (end > start && isServerSpace(value.charCodeAt(end - 1))) end -= 1;
  return value.slice(start, end);
}

function strictRfc3339Nanoseconds(expression: string): bigint | null {
  const match = RFC3339_EXPRESSION.exec(expression);
  if (match === null) return null;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const hour = Number(match[4]);
  const minute = Number(match[5]);
  const second = Number(match[6]);
  const offsetHour = match[8] === "Z" ? 0 : Number(match[9]);
  const offsetMinute = match[8] === "Z" ? 0 : Number(match[10]);
  const leapYear = year % 4 === 0 && (year % 100 !== 0 || year % 400 === 0);
  const daysInMonth = [31, leapYear ? 29 : 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31];
  if (
    month < 1
    || month > 12
    || day < 1
    || day > (daysInMonth[month - 1] ?? 0)
    || hour > 23
    || minute > 59
    || second > 59
    || offsetHour > 23
    || offsetMinute > 59
  ) {
    return null;
  }
  const withoutFraction =
    `${match[1]}-${match[2]}-${match[3]}T${match[4]}:${match[5]}:${match[6]}${match[8]}`;
  const milliseconds = Date.parse(withoutFraction);
  if (Number.isNaN(milliseconds)) return null;
  const fractionalNanoseconds = BigInt((match[7] ?? "").padEnd(9, "0") || "0");
  return BigInt(milliseconds) * NANOSECONDS_PER_MILLISECOND + fractionalNanoseconds;
}

function parseServerTimeExpression(expression: string): ParsedTimeExpression | null {
  // The API bounds the authored value before trimming it, so the browser must
  // not admit an oversized padded value that the server will reject.
  if (textEncoder.encode(expression).length > MAXIMUM_EXPRESSION_BYTES) return null;
  const normalized = trimServerSpace(expression);
  if (normalized === "now") return { kind: "now" };
  if (normalized === "0") return { kind: "earliest-data" };
  if (normalized === "@d") return { kind: "snapped-day", days: 0n };

  const snappedDay = SNAPPED_DAY_EXPRESSION.exec(normalized);
  if (snappedDay !== null) {
    const days = BigInt(snappedDay[1]);
    if (days === 0n || days > MAXIMUM_SNAPPED_DAYS) return null;
    return { kind: "snapped-day", days };
  }

  const relative = RELATIVE_EXPRESSION.exec(normalized);
  if (relative !== null) {
    const amount = BigInt(relative[1]);
    if (amount === 0n) return null;
    if (relative[2] === "d") {
      if (amount > MAX_INT32_DAYS) return null;
      return { kind: "calendar-day", days: amount };
    }
    const unitSeconds = {
      s: 1n,
      m: 60n,
      h: 3_600n,
    }[relative[2] as "s" | "m" | "h"];
    const seconds = amount * unitSeconds;
    if (seconds > MAX_INT64_SECONDS) return null;
    return { kind: "elapsed", seconds };
  }

  const nanoseconds = strictRfc3339Nanoseconds(normalized);
  return nanoseconds === null ? null : { kind: "absolute", nanoseconds };
}

function exactNanoseconds(expression: ParsedTimeExpression): bigint | null {
  switch (expression.kind) {
    case "now":
    case "elapsed":
      // The browser receives server time at millisecond precision and submits
      // later, while the resolver captures a nanosecond request-time anchor.
      // Treat anchored forms as symbolic so an exact sub-millisecond boundary
      // cannot be rejected from a lossy/stale client approximation.
      return null;
    case "earliest-data":
      return MINIMUM_SEARCH_TIME_NS;
    case "absolute":
      return expression.nanoseconds;
    case "calendar-day":
    case "snapped-day":
      return null;
  }
}

function elapsedPosition(expression: ParsedTimeExpression): bigint | null {
  switch (expression.kind) {
    case "now":
      return 0n;
    case "elapsed":
      return expression.seconds;
    case "earliest-data":
    case "calendar-day":
    case "snapped-day":
    case "absolute":
      return null;
  }
}

function symbolicCalendarPosition(
  expression: ParsedTimeExpression,
): SymbolicCalendarPosition | null {
  switch (expression.kind) {
    case "now":
      return { days: 0n, phase: 1 };
    case "calendar-day":
      return { days: expression.days, phase: 1 };
    case "snapped-day":
      return { days: expression.days, phase: 0 };
    case "earliest-data":
    case "elapsed":
    case "absolute":
      return null;
  }
}

function symbolicPositionIsBefore(
  earliest: SymbolicCalendarPosition,
  latest: SymbolicCalendarPosition,
): boolean {
  if (earliest.days !== latest.days) return earliest.days > latest.days;
  return earliest.phase < latest.phase;
}

function serverDependentTimezone(timezone: string): boolean {
  return timezone === "Local"
    || timezone === "localtime"
    || timezone === "posixrules"
    || timezone.startsWith("posix/")
    || timezone.startsWith("right/");
}

export function isServerExecutableTimeExpression(expression: string): boolean {
  return parseServerTimeExpression(expression) !== null;
}

export function backendTimeRangeIntent(
  range: TimeRange,
  preserveAbsentTimezone: boolean,
  browserTimezone = Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
): {
  earliest: string;
  latest: string;
  timezone: string | undefined;
} {
  const explicitTimezone = range.timezone === undefined
    ? undefined
    : trimServerSpace(range.timezone);
  const normalizedBrowserTimezone = trimServerSpace(browserTimezone);
  return {
    earliest: range.earliest,
    latest: range.latest,
    timezone: explicitTimezone
      || (preserveAbsentTimezone ? undefined : normalizedBrowserTimezone || "UTC"),
  };
}

export function serverTimeRangeValidationError(
  range: TimeRange,
  now = new Date(),
): string | null {
  const earliest = parseServerTimeExpression(range.earliest);
  const latest = parseServerTimeExpression(range.latest);
  if (earliest === null || latest === null) {
    return "The connected server accepts RFC 3339 timestamps, “now”, -N[s|m|h|d], @d, and -Nd@d; earliest also accepts 0.";
  }
  if (latest.kind === "earliest-data") {
    return "Only the earliest boundary can use 0 (all data).";
  }

  const timezone = range.timezone === undefined
    ? undefined
    : trimServerSpace(range.timezone);
  if (
    timezone !== undefined
    && timezone.length > 0
    && (
      textEncoder.encode(timezone).length > MAXIMUM_TIMEZONE_BYTES
      || serverDependentTimezone(timezone)
    )
  ) {
    return "Choose a valid IANA timezone.";
  }

  const nowMilliseconds = now.valueOf();
  if (!Number.isFinite(nowMilliseconds)) return "Earliest must resolve before latest.";
  const exactEarliest = exactNanoseconds(earliest);
  const exactLatest = exactNanoseconds(latest);
  if (
    (exactEarliest !== null
      && (exactEarliest < MINIMUM_SEARCH_TIME_NS || exactEarliest > MAXIMUM_SEARCH_TIME_NS))
    || (exactLatest !== null
      && (exactLatest < MINIMUM_SEARCH_TIME_NS || exactLatest > MAXIMUM_SEARCH_TIME_NS))
  ) {
    return "The time range is outside the server’s supported 1900–2262 storage interval.";
  }

  // Calendar-day results depend on the submitted IANA timezone. The extra two
  // days keep this browser-side storage check conservative across historical
  // UTC offsets and DST; the server remains authoritative for the exact
  // instant, skipped civil dates, and a repeated local midnight.
  const maximumCalendarDays = BigInt(
    Math.max(
      0,
      Math.ceil((nowMilliseconds - MINIMUM_SEARCH_TIME_MS) / MILLISECONDS_PER_DAY) + 2,
    ),
  );
  for (const expression of [earliest, latest]) {
    if (
      (expression.kind === "calendar-day" || expression.kind === "snapped-day")
      && expression.days > maximumCalendarDays
    ) {
      return "The time range is outside the server’s supported 1900–2262 storage interval.";
    }
  }

  if (
    exactEarliest !== null
    && exactLatest !== null
    && exactEarliest >= exactLatest
  ) {
    return "Earliest must resolve before latest.";
  }
  const elapsedEarliest = elapsedPosition(earliest);
  const elapsedLatest = elapsedPosition(latest);
  if (
    elapsedEarliest !== null
    && elapsedLatest !== null
    && elapsedEarliest <= elapsedLatest
  ) {
    return "Earliest must resolve before latest.";
  }
  const symbolicEarliest = symbolicCalendarPosition(earliest);
  const symbolicLatest = symbolicCalendarPosition(latest);
  if (
    symbolicEarliest !== null
    && symbolicLatest !== null
    && !symbolicPositionIsBefore(symbolicEarliest, symbolicLatest)
  ) {
    return "Earliest must resolve before latest.";
  }
  // Mixed exact/calendar and exact/anchored order depends on the submitted
  // IANA timezone or a later, nanosecond request-time anchor. Avoid false
  // client rejection; the server resolves and validates the interval.
  return null;
}
