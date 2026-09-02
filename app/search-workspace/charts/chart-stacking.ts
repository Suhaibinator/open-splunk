import type { StackMode } from "../model";

export interface StackedChartValue {
  end: number;
  raw: number | null;
  start: number;
}

export type StackedChartRow = StackedChartValue[];

function normalizedValue(
  value: number,
  positiveTotal: number,
  negativeTotal: number,
): number {
  if (value > 0) return positiveTotal === 0 ? 0 : (value / positiveTotal) * 100;
  if (value < 0) return negativeTotal === 0 ? 0 : (value / negativeTotal) * 100;
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
    const positiveTotal = finite.reduce<number>(
      (total, value) => total + (value !== null && value > 0 ? value : 0),
      0,
    );
    const negativeTotal = finite.reduce<number>(
      (total, value) => total + (value !== null && value < 0 ? Math.abs(value) : 0),
      0,
    );
    let positive = 0;
    let negative = 0;
    return finite.map((raw): StackedChartValue => {
      if (raw === null) return { end: 0, raw: null, start: 0 };
      const value = mode === "stacked100"
        ? normalizedValue(raw, positiveTotal, negativeTotal)
        : raw;
      if (mode === "none") return { end: value, raw, start: 0 };
      if (value >= 0) {
        const start = positive;
        positive += value;
        return { end: positive, raw, start };
      }
      const start = negative;
      negative += value;
      return { end: negative, raw, start };
    });
  });
}

export function stackedChartDomain(rows: readonly StackedChartRow[]): number[] {
  return rows.flatMap((row) => row.flatMap((value) =>
    value.raw === null ? [] : [value.start, value.end],
  ));
}
