export const STATS_SPARKLINE_MARKER = "##__SPARKLINE__##";

export function statsSparklineValues(value: unknown): Array<number | null> | null {
  if (!Array.isArray(value) || value[0] !== STATS_SPARKLINE_MARKER) return null;
  return value.slice(1).map((item) => {
    if (typeof item === "number") return Number.isFinite(item) ? item : null;
    if (typeof item !== "string" || item.trim() === "") return null;
    const numeric = Number(item);
    return Number.isFinite(numeric) ? numeric : null;
  });
}

// Require the server-authenticated schema semantic as well as the cell marker.
// The marker is user-representable and therefore only an integrity check, not
// presentation authority.
export function statsSparklineValuesForPresentation(
  value: unknown,
  statsSparkline: boolean,
): Array<number | null> | null {
  return statsSparkline ? statsSparklineValues(value) : null;
}

export function statsSparklineSegments(
  values: Array<number | null>,
  width = 128,
  height = 28,
  padding = 2,
): string[][] {
  const finiteValues = values.filter((value): value is number => value !== null);
  if (finiteValues.length === 0) return [];
  const minimum = Math.min(...finiteValues);
  const maximum = Math.max(...finiteValues);
  const range = maximum - minimum;
  const drawableWidth = width - (2 * padding);
  const drawableHeight = height - (2 * padding);
  const xFor = (index: number) => padding
    + (values.length <= 1 ? drawableWidth / 2 : (index / (values.length - 1)) * drawableWidth);
  const yFor = (value: number) => range === 0
    ? height / 2
    : padding + ((maximum - value) / range) * drawableHeight;
  const segments: string[][] = [];
  let segment: string[] = [];
  values.forEach((value, index) => {
    if (value === null) {
      if (segment.length > 0) segments.push(segment);
      segment = [];
      return;
    }
    segment.push(`${xFor(index).toFixed(2)},${yFor(value).toFixed(2)}`);
  });
  if (segment.length > 0) segments.push(segment);
  return segments;
}
