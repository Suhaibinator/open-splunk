export function niceStep(span: number, targetIntervals = 4): number {
  if (!Number.isFinite(span) || span <= 0) return 1;
  const roughStep = span / targetIntervals;
  const power = 10 ** Math.floor(Math.log10(roughStep));
  const fraction = roughStep / power;
  const niceFraction = fraction <= 1 ? 1 : fraction <= 2 ? 2 : fraction <= 5 ? 5 : 10;
  return niceFraction * power;
}
