import { resolveAbsoluteTimeRange } from "@/lib/search/backend-data";

import type { TimeRange } from "./model";

const RFC3339_EXPRESSION =
  /^(\d{4})-(\d{2})-(\d{2})T(\d{2}):(\d{2}):(\d{2})(?:\.(\d{1,9}))?(Z|[+-](\d{2}):(\d{2}))$/;
const RELATIVE_EXPRESSION = /^-([1-9]\d*)([smhd])$/;
const MAX_INT64_SECONDS = 9_223_372_036_854_775_807n;
const MAX_INT32_DAYS = 2_147_483_647n;
const MINIMUM_SEARCH_TIME_MS = Date.parse("1900-01-01T00:00:00Z");
const MAXIMUM_SEARCH_TIME_MS = Date.parse("2262-01-01T00:00:00Z");

function isStrictRfc3339Expression(expression: string): boolean {
  const match = RFC3339_EXPRESSION.exec(expression);
  if (match === null) return false;
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
    return false;
  }
  return !Number.isNaN(new Date(expression).valueOf());
}

export function isServerExecutableTimeExpression(expression: string): boolean {
  const normalized = expression.trim();
  if (new TextEncoder().encode(normalized).length > 1_024) return false;
  if (normalized === "now") return true;
  const relative = RELATIVE_EXPRESSION.exec(normalized);
  if (relative !== null) {
    const amount = BigInt(relative[1]);
    if (relative[2] === "d") return amount <= MAX_INT32_DAYS;
    const unitSeconds = {
      s: 1n,
      m: 60n,
      h: 3_600n,
      d: 86_400n,
    }[relative[2] as "s" | "m" | "h" | "d"];
    return amount * unitSeconds <= MAX_INT64_SECONDS;
  }
  return isStrictRfc3339Expression(normalized);
}

export function serverTimeRangeValidationError(
  range: TimeRange,
  now = new Date(),
): string | null {
  if (
    !isServerExecutableTimeExpression(range.earliest)
    || !isServerExecutableTimeExpression(range.latest)
  ) {
    return "The connected server accepts RFC 3339 timestamps, “now”, and fixed -N[s|m|h|d] offsets.";
  }
  const timezone = range.timezone?.trim() || "UTC";
  if (new TextEncoder().encode(timezone).length > 255) return "Choose a valid IANA timezone.";
  if (range.timezone !== undefined) {
    try {
      const formatter = new Intl.DateTimeFormat("en-US", { timeZone: timezone });
      formatter.format(new Date(0));
    } catch {
      return "Choose a valid IANA timezone.";
    }
  }
  const expressions = [range.earliest.trim(), range.latest.trim()];
  const dayOffsets = expressions.map((expression) => {
    const match = RELATIVE_EXPRESSION.exec(expression);
    return match?.[2] === "d" ? BigInt(match[1]) : null;
  });
  const maximumCalendarDays = BigInt(
    Math.max(0, Math.ceil((now.valueOf() - MINIMUM_SEARCH_TIME_MS) / 86_400_000) + 1),
  );
  if (dayOffsets.some((days) => days !== null && days > maximumCalendarDays)) {
    return "The time range is outside the server’s supported 1900–2262 storage interval.";
  }
  const calendarComparableOffsets = expressions.map((expression, index) => {
    if (expression === "now") return 0n;
    return dayOffsets[index];
  });
  if (
    calendarComparableOffsets[0] !== null
    && calendarComparableOffsets[1] !== null
    && calendarComparableOffsets[0] <= calendarComparableOffsets[1]
  ) {
    return "Earliest must resolve before latest.";
  }
  // Calendar-day offsets are resolved by the server in the submitted IANA
  // timezone so DST transitions use the same authoritative clock semantics.
  // For all other forms, the browser can validate order and storage bounds
  // exactly without changing the submitted intent.
  if (dayOffsets.every((days) => days === null)) {
    try {
      const resolved = resolveAbsoluteTimeRange(expressions[0], expressions[1], now);
      const earliest = new Date(resolved.earliest).valueOf();
      const latest = new Date(resolved.latest).valueOf();
      if (earliest < MINIMUM_SEARCH_TIME_MS || latest > MAXIMUM_SEARCH_TIME_MS) {
        return "The time range is outside the server’s supported 1900–2262 storage interval.";
      }
    } catch {
      return "Earliest must resolve before latest.";
    }
  }
  return null;
}
