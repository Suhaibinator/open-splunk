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

export function linearTickScale(values: number[]): LinearTickScale {
  const dataMinimum = values.length === 0 ? 0 : Math.min(...values);
  const dataMaximum = values.length === 0 ? 0 : Math.max(...values);
  const rawMinimum = Math.min(0, dataMinimum);
  const rawMaximum = Math.max(0, dataMaximum);
  const step = niceStep(rawMaximum === rawMinimum ? 1 : rawMaximum - rawMinimum);
  const minimum = Math.floor(rawMinimum / step) * step;
  const maximum = Math.max(minimum + step, Math.ceil(rawMaximum / step) * step);
  const intervalCount = Math.max(1, Math.round((maximum - minimum) / step));
  const ticks = Array.from({ length: intervalCount + 1 }, (_, index) => {
    const value = maximum - (index * step);
    return Math.abs(value) < step / 1_000_000 ? 0 : value;
  });
  return { minimum, maximum, ticks };
}
