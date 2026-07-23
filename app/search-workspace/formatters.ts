import { COMPACT_NUMBER_FORMAT, NUMBER_FORMAT } from "./constants";

export type IntegerQuantity = number | bigint;

const EXACT_INTEGER_PATTERN = /^[+-]?\d+$/;
const GROUPABLE_DECIMAL_PATTERN = /^([+-]?)(\d+)(\.\d+)?$/;
const DECIMAL_BYTE_UNITS = [
  ["TB", 1_000_000_000_000n],
  ["GB", 1_000_000_000n],
  ["MB", 1_000_000n],
  ["KB", 1_000n],
] as const;
const BINARY_BYTE_UNITS = [
  ["TB", 1_099_511_627_776n],
  ["GB", 1_073_741_824n],
  ["MB", 1_048_576n],
  ["KB", 1_024n],
] as const;
const WHOLE_BYTE_FORMAT = new Intl.NumberFormat("en-US", { maximumFractionDigits: 0 });
const FRACTIONAL_BYTE_FORMAT = new Intl.NumberFormat("en-US", { maximumFractionDigits: 1 });

export function formatExactInteger(value: string, compact = false): string | null {
  if (!EXACT_INTEGER_PATTERN.test(value)) return null;
  try {
    return (compact ? COMPACT_NUMBER_FORMAT : NUMBER_FORMAT).format(BigInt(value));
  } catch {
    return value;
  }
}

export function formatExactNumericText(
  value: string,
  options: {
    compact?: boolean;
    compactSuffix?: string;
  } = {},
): string {
  if (options.compact) {
    const compactSource = options.compactSuffix !== undefined && value.endsWith(options.compactSuffix)
      ? value.slice(0, -options.compactSuffix.length)
      : value;
    const coordinate = Number(compactSource);
    if (Number.isFinite(coordinate)) return COMPACT_NUMBER_FORMAT.format(coordinate);
  }
  return formatExactInteger(value) ?? value;
}

export function formatGroupedNumericText(value: string): string {
  const match = GROUPABLE_DECIMAL_PATTERN.exec(value.trim());
  if (match === null) return value;
  return `${match[1]}${match[2].replace(/\B(?=(\d{3})+(?!\d))/g, ",")}${match[3] ?? ""}`;
}

export function formatNonNegativeIntegerQuantity(
  value: IntegerQuantity,
  invalidLabel = "Unavailable",
): string {
  if (typeof value === "bigint") {
    return value < 0n ? invalidLabel : NUMBER_FORMAT.format(value);
  }
  return !Number.isSafeInteger(value) || value < 0 ? invalidLabel : NUMBER_FORMAT.format(value);
}

export function formatDecimalBytes(
  value: IntegerQuantity,
  invalidLabel = "Unavailable",
): string {
  if (typeof value === "bigint") {
    if (value < 0n) return invalidLabel;
    const unit = DECIMAL_BYTE_UNITS.find(([, threshold]) => value >= threshold);
    if (unit === undefined) return `${formatNonNegativeIntegerQuantity(value, invalidLabel)} B`;
    const [label, threshold] = unit;
    const tenths = (value * 10n + threshold / 2n) / threshold;
    const whole = tenths / 10n;
    const fraction = tenths % 10n;
    return `${formatNonNegativeIntegerQuantity(whole, invalidLabel)}${fraction === 0n ? "" : `.${fraction}`} ${label}`;
  }
  if (!Number.isSafeInteger(value) || value < 0) return invalidLabel;
  const units = ["B", "KB", "MB", "GB", "TB"] as const;
  let amount = value;
  let unitIndex = 0;
  while (amount >= 1_000 && unitIndex < units.length - 1) {
    amount /= 1_000;
    unitIndex += 1;
  }
  const formatter = amount >= 10 || unitIndex === 0 ? WHOLE_BYTE_FORMAT : FRACTIONAL_BYTE_FORMAT;
  return `${formatter.format(amount)} ${units[unitIndex]}`;
}

export function formatBinaryBytes(value: bigint): string {
  if (value <= 0n) return "0 B";
  const unit = BINARY_BYTE_UNITS.find(([, threshold]) => value >= threshold);
  if (unit === undefined) return `${value} B`;
  const [label, threshold] = unit;
  const fractionDigits = value < threshold * 10n ? 1 : 0;
  const scale = fractionDigits === 0 ? 1n : 10n;
  const rounded = (value * scale * 2n + threshold) / (threshold * 2n);
  const whole = rounded / scale;
  if (fractionDigits === 0) return `${whole} ${label}`;
  return `${whole}.${rounded % scale} ${label}`;
}
