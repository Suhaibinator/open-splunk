export function niceStep(span: number, targetIntervals = 4): number {
  if (!Number.isFinite(span) || span <= 0) return 1;
  const roughStep = span / targetIntervals;
  const power = 10 ** Math.floor(Math.log10(roughStep));
  const fraction = roughStep / power;
  const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return niceFraction * power;
}

export interface LinearTickScale {
  minimum: number;
  maximum: number;
  ticks: number[];
}

function boundedTickScale(rawMinimum: number, rawMaximum: number): LinearTickScale | null {
  const span = rawMaximum - rawMinimum;
  if (!Number.isFinite(span)) return null;
  const step = niceStep(span === 0 ? 1 : span);
  if (!Number.isFinite(step) || step <= 0) return null;
  const minimum = Math.min(rawMinimum, Math.floor(rawMinimum / step) * step);
  const maximum = Math.max(rawMaximum, minimum + step, Math.ceil(rawMaximum / step) * step);
  if (!Number.isFinite(minimum) || !Number.isFinite(maximum)) return null;
  const intervalCount = Math.max(1, Math.round((maximum - minimum) / step));
  if (!Number.isSafeInteger(intervalCount) || intervalCount > 32) return null;
  const ticks = Array.from({ length: intervalCount + 1 }, (_, index) => {
    const value = maximum - (index * step);
    return Math.abs(value) < step / 1_000_000 ? 0 : value;
  });
  return { minimum, maximum, ticks };
}

export function linearTickScale(values: number[]): LinearTickScale {
  let minimum = 0;
  let maximum = 0;
  for (const value of values) {
    if (!Number.isFinite(value)) continue;
    minimum = Math.min(minimum, value);
    maximum = Math.max(maximum, value);
  }
  const ordinary = boundedTickScale(minimum, maximum);
  if (ordinary !== null) return ordinary;

  // Normalize before subtracting or rounding: finite opposite extrema can
  // have an infinite difference, and nice rounding can exceed Number.MAX_VALUE.
  const magnitude = Math.max(Math.abs(minimum), Math.abs(maximum));
  const normalized = boundedTickScale(minimum / magnitude, maximum / magnitude);
  if (normalized === null) throw new RangeError("Normalized chart scale is invalid.");
  const ticks = [...new Set(normalized.ticks.map((value) => value * magnitude))];
  return {
    minimum: Math.min(minimum, normalized.minimum * magnitude),
    maximum: Math.max(maximum, normalized.maximum * magnitude),
    ticks,
  };
}

/** Projects a value onto the axis without overflowing its finite endpoints. */
export function projectScaleValue(value: number, scale: LinearTickScale): number {
  if (value <= scale.minimum) return 0;
  if (value >= scale.maximum) return 1;
  const span = scale.maximum - scale.minimum;
  if (Number.isFinite(span)) return (value - scale.minimum) / span;
  const magnitude = Math.max(Math.abs(scale.minimum), Math.abs(scale.maximum));
  return (value / magnitude - scale.minimum / magnitude)
    / (scale.maximum / magnitude - scale.minimum / magnitude);
}
