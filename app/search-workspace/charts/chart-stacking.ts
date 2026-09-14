import type { StackMode } from "../model";

export interface StackedChartValue {
  end: number;
  raw: number | null;
  start: number;
}

export type StackedChartRow = StackedChartValue[];

export interface StackMagnitudeTotal {
  maximum: number;
  scaledTotal: number;
}

export function createStackMagnitudeTotal(): StackMagnitudeTotal {
  return { maximum: 0, scaledTotal: 0 };
}

/** Accumulate finite same-sign magnitudes without overflowing or losing subnormals. */
export function addStackMagnitude(total: StackMagnitudeTotal, value: number): void {
  const magnitude = Math.abs(value);
  if (!Number.isFinite(magnitude) || magnitude === 0) return;
  if (total.maximum === 0) {
    total.maximum = magnitude;
    total.scaledTotal = 1;
    return;
  }
  if (magnitude > total.maximum) {
    total.scaledTotal = (total.scaledTotal * (total.maximum / magnitude)) + 1;
    total.maximum = magnitude;
    return;
  }
  total.scaledTotal += magnitude / total.maximum;
}

export function stackMagnitudeCoordinate(total: StackMagnitudeTotal): number {
  if (total.maximum === 0 || total.scaledTotal === 0) return 0;
  return total.maximum > Number.MAX_VALUE / total.scaledTotal
    ? Number.MAX_VALUE
    : total.maximum * total.scaledTotal;
}

export function stackMagnitudeIsApproximate(total: StackMagnitudeTotal): boolean {
  return total.maximum !== 0 && total.maximum > Number.MAX_VALUE / total.scaledTotal;
}

/** Add a same-sign stack coordinate while keeping geometry finite. */
export function addStackCoordinate(total: number, value: number): number {
  const sum = total + value;
  if (Number.isFinite(sum)) return sum;
  return value < 0 ? -Number.MAX_VALUE : Number.MAX_VALUE;
}

export function normalizeStackValue(
  value: number,
  positiveTotal: StackMagnitudeTotal,
  negativeTotal: StackMagnitudeTotal,
): number {
  const total = value > 0 ? positiveTotal : negativeTotal;
  if (value !== 0 && total.maximum !== 0 && total.scaledTotal !== 0) {
    return Math.sign(value) * ((Math.abs(value) / total.maximum) / total.scaledTotal) * 100;
  }
  return 0;
}

/**
 * Builds render coordinates without changing the raw values used by chart
 * inspectors. Positive and negative series accumulate on separate baselines.
 */
export function stackChartRows(
  rows: readonly (readonly (number | null)[])[],
  mode: StackMode,
): StackedChartRow[] {
  return rows.map((row) => {
    const finite = row.map((value) => value !== null && Number.isFinite(value) ? value : null);
    const positiveTotal = createStackMagnitudeTotal();
    const negativeTotal = createStackMagnitudeTotal();
    for (const value of finite) {
      if (value === null) continue;
      addStackMagnitude(value < 0 ? negativeTotal : positiveTotal, value);
    }
    let positive = 0;
    let negative = 0;
    return finite.map((raw): StackedChartValue => {
      if (raw === null) return { end: 0, raw: null, start: 0 };
      const value = mode === "stacked100"
        ? normalizeStackValue(raw, positiveTotal, negativeTotal)
        : raw;
      if (mode === "none") return { end: value, raw, start: 0 };
      if (value >= 0) {
        const start = positive;
        positive = addStackCoordinate(positive, value);
        return { end: positive, raw, start };
      }
      const start = negative;
      negative = addStackCoordinate(negative, value);
      return { end: negative, raw, start };
    });
  });
}

export function stackedChartDomain(rows: readonly StackedChartRow[]): number[] {
  return rows.flatMap((row) => row.flatMap((value) =>
    value.raw === null ? [] : [value.start, value.end],
  ));
}
