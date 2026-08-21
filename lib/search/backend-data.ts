import {
  ColumnSemanticType,
  ResultSetKind,
  type ResultRow,
  type ResultSchema,
} from "@/gen/ts/open_splunk/result";
import { ValueType, type TypedValue } from "@/gen/ts/open_splunk/value";
import type {
  DemoEvent,
  DemoField,
  DemoScalar,
  TimelinePoint,
} from "@/lib/demo/search-data";
import { assertBrowserResultColumnCount } from "@/lib/api/pagination";
import { validFlatMultivalueColumnPresentation } from "@/lib/api/result-column-presentation";
import { flatMultivalueDisplay } from "@/lib/search/multivalue-presentation";

export type SearchDataMode = "backend" | "demo";

export interface WorkspaceStatisticSeries {
  /** Stable backend field identity used to align this series across categories. */
  key: string;
  /** Server-provided field label shown in legends and value inspectors. */
  label: string;
  /** Finite plotting coordinate, or null when this category has no numeric value. */
  value: number | null;
  /** Exact server value retained when the plotting coordinate is non-lossless. */
  exactValue?: string;
  coordinateApproximate?: boolean;
}

export interface WorkspaceStatistic {
  id?: string;
  level: string;
  /** Typed server value used for drilldown; the display label may be formatted. */
  pivotValue?: DemoScalar;
  pivotable?: boolean;
  /** Finite coordinate used by compact charts. */
  count: number;
  /** Exact server value retained when the chart coordinate is non-lossless. */
  exactCount?: string;
  coordinateApproximate?: boolean;
  percent: string;
  avgDuration: number;
  /** The server-provided aggregation represented by `count` in categorical charts. */
  measureLabel?: string;
  /**
   * Every chartable backend metric in schema order. The legacy scalar fields
   * above mirror the first series so existing statistics/demo consumers remain
   * compatible without collapsing multi-series visualizations.
   */
  series?: WorkspaceStatisticSeries[];
}

export interface WorkspaceStatisticsColumn {
  /** A unique key for rendering and exporting, even if a backend repeats a field name. */
  key: string;
  /** The SPL field name used for drilldowns. */
  fieldName: string;
  label: string;
  valueType: ValueType;
  semanticType: ColumnSemanticType;
  numeric: boolean;
  pivotable: boolean;
  /** Optional display-only separator for a typed multivalue list. */
  flatMultivalueDelimiter?: string;
  /** Server-authenticated stats sparkline semantics for this list column. */
  statsSparkline: boolean;
}

export type WorkspaceStatisticsValue =
  | null
  | string
  | number
  | boolean
  | WorkspaceStatisticsValue[]
  | { [key: string]: WorkspaceStatisticsValue };

export interface WorkspaceStatisticsRow {
  id: string;
  values: Record<string, WorkspaceStatisticsValue>;
  pivotValues: Record<string, DemoScalar | undefined>;
}

export interface WorkspaceStatisticsTable {
  columns: WorkspaceStatisticsColumn[];
  rows: WorkspaceStatisticsRow[];
}

export interface WorkspaceStatisticsSort {
  key: string;
  direction: "asc" | "desc";
}

export interface WorkspacePattern {
  signature: string;
  count: number;
  percent: number;
}

export interface AdaptedSearchResults {
  events: DemoEvent[];
  fields: DemoField[];
  statistics: WorkspaceStatistic[];
  statisticsTable: WorkspaceStatisticsTable | null;
  statisticDimension: string;
  timeline: TimelinePoint[];
}

const EVENT_TIME_FORMAT_OPTIONS = Object.freeze<Intl.DateTimeFormatOptions>({
  month: "numeric",
  day: "numeric",
  year: "2-digit",
  hour: "numeric",
  minute: "2-digit",
  second: "2-digit",
  fractionalSecondDigits: 3,
});
const TIMELINE_TIME_FORMAT_OPTIONS = Object.freeze<Intl.DateTimeFormatOptions>({
  month: "short",
  day: "numeric",
  hour: "numeric",
  minute: "2-digit",
});

interface ResultDateTimeFormatters {
  event?: Intl.DateTimeFormat;
  timeline?: Intl.DateTimeFormat;
}

function safeNumber(value: bigint): DemoScalar {
  const number = Number(value);
  return Number.isSafeInteger(number) ? number : value.toString();
}

function typedValueToJSON(value: TypedValue | undefined): WorkspaceStatisticsValue {
  switch (value?.kind?.$case) {
    case "nullValue":
    case "missingValue":
      return null;
    case "stringValue":
    case "doubleValue":
    case "boolValue":
      return value.kind.value;
    case "sint64Value":
    case "uint64Value":
      return safeNumber(value.kind.value);
    case "timestampValue":
      return value.kind.value.toISOString();
    case "durationValue": {
      const exact = exactDurationNumericText(value.kind.value.seconds, value.kind.value.nanos);
      const coordinate = Number(exact);
      return Number.isFinite(coordinate)
        && decimalCoordinateIsProvablyExact(exact, coordinate)
        && (!Number.isInteger(coordinate) || Number.isSafeInteger(coordinate))
        ? coordinate
        : exact;
    }
    case "decimalValue":
      return value.kind.value.value;
    case "bytesValue":
      return `[${value.kind.value.byteLength} bytes]`;
    case "listValue":
      return value.kind.value.values.map(typedValueToJSON);
    case "objectValue":
      return Object.fromEntries(value.kind.value.fields.map((field) => [field.name, typedValueToJSON(field.value)]));
    default:
      return null;
  }
}

function typedValueToScalar(value: TypedValue | undefined): DemoScalar {
  return jsonToScalar(typedValueToJSON(value));
}

export function typedValueIsPivotable(value: TypedValue | undefined): boolean {
  switch (value?.kind?.$case) {
    case "nullValue":
    case "stringValue":
    case "boolValue":
      return true;
    case "doubleValue":
      return Number.isFinite(value.kind.value);
    case "sint64Value":
    case "uint64Value":
      return Number.isSafeInteger(Number(value.kind.value));
    default:
      return false;
  }
}

function jsonToScalar(decoded: unknown): DemoScalar {
  if (decoded === null || typeof decoded === "string" || typeof decoded === "number" || typeof decoded === "boolean") {
    return decoded;
  }
  return JSON.stringify(decoded);
}

function formatEventTime(
  value: DemoScalar,
  formatters: ResultDateTimeFormatters,
): { iso: string; label: string } {
  const parsed = typeof value === "string" || typeof value === "number" ? new Date(value) : new Date(Number.NaN);
  if (Number.isNaN(parsed.valueOf())) {
    return { iso: "", label: "Time unavailable" };
  }
  const formatter = formatters.event ??= new Intl.DateTimeFormat(
    "en-US",
    EVENT_TIME_FORMAT_OPTIONS,
  );
  return {
    iso: parsed.toISOString(),
    label: formatter.format(parsed),
  };
}

function rowFields(
  schema: ResultSchema,
  row: ResultRow,
): { fields: Record<string, DemoScalar>; pivotableFields: Record<string, boolean> } {
  const fields: Record<string, DemoScalar> = {};
  const pivotableFields: Record<string, boolean> = {};
  schema.columns.forEach((column, index) => {
    const value = row.cells[index];
    if (column.fieldName === "fields" && value?.kind?.$case === "objectValue") {
      value.kind.value.fields.forEach((field) => {
        fields[field.name] = jsonToScalar(typedValueToJSON(field.value));
        pivotableFields[field.name] = typedValueIsPivotable(field.value);
      });
      return;
    }
    const decoded = typedValueToJSON(value);
    if (column.flatMultivalueDelimiter !== undefined && decoded !== null) {
      const flat = flatMultivalueDisplay(decoded, column.flatMultivalueDelimiter);
      if (flat === undefined) {
        throw new RangeError(
          `Search result column “${column.fieldName}” has an unsupported flat multivalue cell.`,
        );
      }
      fields[column.fieldName] = flat;
    } else {
      fields[column.fieldName] = jsonToScalar(decoded);
    }
    pivotableFields[column.fieldName] = typedValueIsPivotable(value);
  });
  return { fields, pivotableFields };
}

function rowsToEvents(
  schema: ResultSchema,
  rows: ResultRow[],
  formatters: ResultDateTimeFormatters,
): DemoEvent[] {
  return rows.map((row) => {
    const { fields, pivotableFields } = rowFields(schema, row);
    const eventTime = formatEventTime(
      fields["_time"] ?? fields.timestamp ?? null,
      formatters,
    );
    const rawValue = fields["_raw"];
    return {
      id: row.rowId || `row-${row.ordinal.toString()}`,
      time: eventTime.iso,
      timeLabel: eventTime.label,
      raw: typeof rawValue === "string" ? rawValue : JSON.stringify(fields),
      fields,
      pivotableFields,
    };
  });
}

interface ChartNumericValue {
  coordinate: number;
  /** Exact source text when the coordinate is only an approximation. */
  exactText?: string;
  /** Available for exact aggregation even when each individual integer is safe. */
  exactInteger?: bigint;
  approximate: boolean;
}

function integerText(value: string): bigint | undefined {
  if (!/^[+-]?\d+$/.test(value.trim())) return undefined;
  try {
    return BigInt(value.trim());
  } catch {
    return undefined;
  }
}

interface ExactRational {
  numerator: bigint;
  denominator: bigint;
}

function decimalRational(value: string): ExactRational | null {
  const match = /^([+-]?)(\d+)(?:\.(\d+))?(?:e([+-]?\d+))?$/i.exec(value.trim());
  if (match === null) return null;
  const combinedDigits = `${match[2]}${match[3] ?? ""}`;
  if (/^0+$/.test(combinedDigits)) return { numerator: 0n, denominator: 1n };
  if (combinedDigits.length > 768) return null;
  const exponent = Number(match[4] ?? "0");
  if (!Number.isSafeInteger(exponent)) return null;
  const scale = (match[3]?.length ?? 0) - exponent;
  if (Math.abs(scale) > 768) return null;
  const sign = match[1] === "-" ? -1n : 1n;
  const digits = BigInt(combinedDigits);
  return scale >= 0
    ? { numerator: sign * digits, denominator: 10n ** BigInt(scale) }
    : { numerator: sign * digits * (10n ** BigInt(-scale)), denominator: 1n };
}

function numberRational(value: number): ExactRational | null {
  if (!Number.isFinite(value)) return null;
  if (value === 0) return { numerator: 0n, denominator: 1n };
  const bytes = new ArrayBuffer(8);
  const view = new DataView(bytes);
  view.setFloat64(0, value, false);
  const high = view.getUint32(0, false);
  const low = view.getUint32(4, false);
  const negative = (high >>> 31) === 1;
  const exponentBits = (high >>> 20) & 0x7ff;
  const fraction = (BigInt(high & 0x000f_ffff) << 32n) | BigInt(low);
  const significand = exponentBits === 0 ? fraction : (1n << 52n) | fraction;
  const exponent = exponentBits === 0 ? -1074 : exponentBits - 1023 - 52;
  const signedSignificand = negative ? -significand : significand;
  return exponent >= 0
    ? { numerator: signedSignificand << BigInt(exponent), denominator: 1n }
    : { numerator: signedSignificand, denominator: 1n << BigInt(-exponent) };
}

function decimalCoordinateIsProvablyExact(source: string, coordinate: number): boolean {
  const decimal = decimalRational(source);
  const binary = numberRational(coordinate);
  return decimal !== null
    && binary !== null
    && decimal.numerator * binary.denominator === binary.numerator * decimal.denominator;
}

export function exactDurationNumericText(seconds: bigint, nanos: number): string {
  const totalNanos = seconds * 1_000_000_000n + BigInt(nanos);
  const negative = totalNanos < 0n;
  const absoluteNanos = negative ? -totalNanos : totalNanos;
  const wholeSeconds = absoluteNanos / 1_000_000_000n;
  const fractionalNanos = absoluteNanos % 1_000_000_000n;
  return `${negative ? "-" : ""}${wholeSeconds.toString()}.${fractionalNanos.toString().padStart(9, "0")}`;
}

function numericTextValue(source: string): ChartNumericValue | null {
  const trimmed = source.trim();
  if (trimmed.length === 0 || decimalRational(trimmed) === null) return null;
  const coordinate = Number(trimmed);
  if (!Number.isFinite(coordinate)) return null;
  const exactInteger = integerText(trimmed);
  const approximate = !decimalCoordinateIsProvablyExact(trimmed, coordinate)
    || (Number.isInteger(coordinate) && !Number.isSafeInteger(coordinate));
  return {
    coordinate,
    exactText: approximate ? trimmed : undefined,
    exactInteger,
    approximate,
  };
}

/**
 * Convert a typed server value into a finite plotting coordinate without
 * discarding its authoritative representation. Tables continue to use
 * `typedValueToJSON`, so large integer cells remain exact strings.
 */
function chartNumericValue(value: TypedValue | undefined): ChartNumericValue | null {
  switch (value?.kind?.$case) {
    case "sint64Value":
    case "uint64Value": {
      const coordinate = Number(value.kind.value);
      if (!Number.isFinite(coordinate)) return null;
      const approximate = !Number.isSafeInteger(coordinate);
      return {
        coordinate,
        exactText: approximate ? value.kind.value.toString() : undefined,
        exactInteger: value.kind.value,
        approximate,
      };
    }
    case "doubleValue":
      return Number.isFinite(value.kind.value)
        ? { coordinate: value.kind.value, approximate: false }
        : null;
    case "decimalValue": {
      return numericTextValue(value.kind.value.value);
    }
    case "stringValue": {
      return numericTextValue(value.kind.value);
    }
    case "durationValue": {
      const source = exactDurationNumericText(value.kind.value.seconds, value.kind.value.nanos);
      const numeric = numericTextValue(source);
      if (numeric === null) return null;
      return numeric;
    }
    default:
      return null;
  }
}

function numericValueType(valueType: ValueType): boolean {
  return valueType === ValueType.VALUE_TYPE_SINT64
    || valueType === ValueType.VALUE_TYPE_UINT64
    || valueType === ValueType.VALUE_TYPE_DOUBLE
    || valueType === ValueType.VALUE_TYPE_DECIMAL
    || valueType === ValueType.VALUE_TYPE_DURATION;
}

function numericCell(value: TypedValue | undefined): boolean {
  return value?.kind?.$case === "sint64Value"
    || value?.kind?.$case === "uint64Value"
    || value?.kind?.$case === "doubleValue"
    || value?.kind?.$case === "decimalValue"
    || value?.kind?.$case === "durationValue";
}

function statisticsTableFromRows(schema: ResultSchema, rows: ResultRow[]): WorkspaceStatisticsTable | null {
  if (schema.columns.length === 0) return null;
  const metricIndexes = new Set(preferredMetricIndexes(schema, rows));
  const keyCounts = new Map<string, number>();
  const columns = schema.columns.map((column, sourceIndex) => {
    const fieldName = column.fieldName || `column_${sourceIndex + 1}`;
    const occurrence = (keyCounts.get(fieldName) ?? 0) + 1;
    keyCounts.set(fieldName, occurrence);
    const key = occurrence === 1 ? fieldName : `${fieldName}__${occurrence}`;
    const baseLabel = column.displayName || fieldName;
    const numeric = numericValueType(column.valueType) || columnHasNumericValues(rows, sourceIndex);
    const timeLike = columnIsTimeLike(column);
    return {
      key,
      fieldName,
      label: occurrence === 1 ? baseLabel : `${baseLabel} (${occurrence})`,
      valueType: column.valueType,
      semanticType: column.semanticType,
      numeric,
      pivotable: !metricIndexes.has(sourceIndex)
        && column.semanticType !== ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC
        && !AGGREGATE_FIELD_NAME.test(fieldName)
        && !timeLike
        && column.valueType !== ValueType.VALUE_TYPE_LIST
        && column.valueType !== ValueType.VALUE_TYPE_OBJECT
        && fieldName.length > 0,
      flatMultivalueDelimiter: column.flatMultivalueDelimiter,
	  statsSparkline: column.statsSparkline,
      sourceIndex,
    };
  });
  return {
    columns: columns.map(({ sourceIndex: _sourceIndex, ...column }) => column),
    rows: rows.map((row, rowIndex) => ({
      id: `${row.rowId || `row-${row.ordinal.toString()}`}-${rowIndex}`,
      values: Object.fromEntries(columns.map((column) => [column.key, typedValueToJSON(row.cells[column.sourceIndex])])),
      pivotValues: Object.fromEntries(columns.map((column) => {
        const value = row.cells[column.sourceIndex];
        return [column.key, typedValueIsPivotable(value) ? typedValueToScalar(value) : undefined];
      })),
    })),
  };
}

const AGGREGATE_FIELD_NAME = /^(?:(?:count|sum|avg|average|min|max|earliest|latest|median|mode|range|stdev|variance|var|distinct_count|dc|rate|percent)(?:\(|$|_)|p(?:50|75|90|95|98|99|999)$|perc\d+(?:\(|$))/i;

function columnHasNumericValues(rows: ResultRow[], index: number): boolean {
  const observed = rows
    .map((row) => row.cells[index])
    .filter((cell) => cell?.kind?.$case !== "nullValue" && cell?.kind?.$case !== "missingValue");
  return observed.length > 0 && observed.every((cell) => numericCell(cell));
}

function columnIsTimeLike(
  column: { valueType: ValueType; semanticType: ColumnSemanticType; fieldName: string },
): boolean {
  return column.valueType === ValueType.VALUE_TYPE_TIMESTAMP
    || column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME
    || column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_INDEX_TIME
    || /^_?time$/i.test(column.fieldName);
}

function preferredMetricIndexes(schema: ResultSchema, rows: ResultRow[]): number[] {
  const numericIndexes = schema.columns.flatMap((column, index) =>
    numericValueType(column.valueType) || columnHasNumericValues(rows, index) ? [index] : [],
  );
  const explicitMetrics = numericIndexes.filter((index) =>
    schema.columns[index].semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
  );
  const namedAggregates = numericIndexes.filter((index) =>
    AGGREGATE_FIELD_NAME.test(schema.columns[index].fieldName),
  );
  if (explicitMetrics.length > 0) {
    const combined = [...new Set([...explicitMetrics, ...namedAggregates])];
    const combinedSet = new Set(combined);
    const remainingScalarColumns = schema.columns.flatMap((column, index) => {
      if (combinedSet.has(index) || columnIsTimeLike(schema.columns[index])) return [];
      if (column.valueType === ValueType.VALUE_TYPE_LIST || column.valueType === ValueType.VALUE_TYPE_OBJECT) return [];
      return [index];
    });
    // Preserve an untagged aggregate sibling in a mixed schema only when doing
    // so still leaves one defensible category. A runtime chart dimension may
    // itself be named "count", and must not be reclassified as a metric.
    return remainingScalarColumns.length === 1 ? combined : explicitMetrics;
  }
  if (namedAggregates.length > 0) {
    const firstColumnLooksAggregated = namedAggregates.includes(0);
    const hasLaterAliasedNumericColumn = numericIndexes.some(
      (index) => index > 0 && !namedAggregates.includes(index),
    );
    // The current backend orders stats grouping fields before measures. A
    // numeric BY field can legitimately be named "count", while an aliased
    // measure after it may have no aggregate-looking name. In that shape,
    // field-name inference would invert the dimension and measure; declining
    // the chart preserves the authoritative Statistics table instead.
    if (firstColumnLooksAggregated && hasLaterAliasedNumericColumn) return [];
    return namedAggregates;
  }

  // A single numeric field paired with another field is an unambiguous value/dimension shape.
  if (numericIndexes.length === 1 && schema.columns.length > 1) return numericIndexes;

  // Aliased multi-measure statistics lose aggregate-looking field names. They
  // remain unambiguous when exactly one other scalar column can be the category.
  const numericSet = new Set(numericIndexes);
  const nonNumericDimensions = schema.columns.flatMap((column, index) => {
    if (numericSet.has(index) || columnIsTimeLike(schema.columns[index])) return [];
    if (column.valueType === ValueType.VALUE_TYPE_LIST || column.valueType === ValueType.VALUE_TYPE_OBJECT) return [];
    return [index];
  });
  return numericIndexes.length > 1 && nonNumericDimensions.length === 1 ? numericIndexes : [];
}

function preferredDimensionIndex(schema: ResultSchema, metricIndexes: number[]): number | null {
  const metricSet = new Set(metricIndexes);
  const candidates = schema.columns.flatMap((column, index) => {
    if (metricSet.has(index) || columnIsTimeLike(schema.columns[index])) return [];
    if (column.valueType === ValueType.VALUE_TYPE_LIST || column.valueType === ValueType.VALUE_TYPE_OBJECT) return [];
    if (column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC) return [];
    return [index];
  });
  const explicitDimension = candidates.find((index) =>
    schema.columns[index].semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_DIMENSION,
  );
  // More than one dimension requires a split-series or trellis chart. Choosing
  // only the first would collapse distinct backend rows under duplicate labels.
  if (candidates.length !== 1) return null;
  return explicitDimension ?? candidates[0];
}

/**
 * Produce categorical chart rows only when the backend shape has defensible
 * measures and exactly one dimension. An empty row set deliberately signals
 * that Visualization should explain the incompatible result shape instead of
 * inventing an "event count by level" chart.
 */
function statisticsFromRows(schema: ResultSchema, rows: ResultRow[]): { rows: WorkspaceStatistic[]; dimension: string } {
  const metricIndexes = preferredMetricIndexes(schema, rows);
  if (metricIndexes.length === 0) return { rows: [], dimension: "result" };
  const dimensionIndex = preferredDimensionIndex(schema, metricIndexes);
  if (dimensionIndex === null) return { rows: [], dimension: "result" };

  const metricColumns = metricIndexes.map((index) => schema.columns[index]);
  const dimensionColumn = schema.columns[dimensionIndex];
  const averageIndex = schema.columns.findIndex((column) =>
    /^avg(?:erage)?\(/i.test(column.fieldName) || /avg.*duration/i.test(column.fieldName),
  );
  const chartRows = rows.flatMap((row) => {
    const dimensionValue = typedValueToJSON(row.cells[dimensionIndex]);
    if (Array.isArray(dimensionValue) || (typeof dimensionValue === "object" && dimensionValue !== null)) return [];
    const series = metricIndexes.map((metricIndex, index) => {
      const metric = chartNumericValue(row.cells[metricIndex]);
      const column = metricColumns[index];
      return {
        key: column.fieldName || `metric_${metricIndex + 1}`,
        label: column.displayName || column.fieldName || `metric ${metricIndex + 1}`,
        value: metric?.coordinate ?? null,
        exactValue: metric?.exactText,
        coordinateApproximate: metric?.approximate || undefined,
      } satisfies WorkspaceStatisticSeries;
    });
    const primaryMetric = series[0];
    if (primaryMetric === undefined) return [];
    const average = averageIndex < 0 ? null : chartNumericValue(row.cells[averageIndex]);
    return [{
      id: row.rowId || `category-${row.ordinal.toString()}`,
      level: dimensionValue === null ? "(none)" : String(dimensionValue),
      pivotValue: typedValueToScalar(row.cells[dimensionIndex]),
      pivotable: typedValueIsPivotable(row.cells[dimensionIndex]),
      count: primaryMetric.value ?? 0,
      exactCount: primaryMetric.exactValue,
      coordinateApproximate: series.some((metric) => metric.coordinateApproximate) || undefined,
      percent: "0.0%",
      avgDuration: average?.coordinate ?? Number.NaN,
      measureLabel: primaryMetric.label,
      series,
    } satisfies WorkspaceStatistic];
  });
  if (chartRows.length !== rows.length) {
    return { rows: [], dimension: dimensionColumn.fieldName || "result" };
  }
  const total = chartRows.reduce((sum, row) => sum + row.count, 0);
  for (const row of chartRows) {
    row.percent = total > 0 ? `${((row.count / total) * 100).toFixed(1)}%` : "0.0%";
  }
  return {
    dimension: dimensionColumn.fieldName || "result",
    rows: chartRows,
  };
}

function timelineFromRows(
  schema: ResultSchema,
  rows: ResultRow[],
  formatters: ResultDateTimeFormatters,
  knownBucketWidthMs?: number,
): TimelinePoint[] {
  const timeIndex = schema.columns.findIndex((column) => /^_?time$/i.test(column.fieldName));
  if (timeIndex < 0) return [];
  // Timechart columns after _time are independent runtime series. A split value
  // can itself be named "count", so preferring that spelling would discard all
  // of its siblings.
  const numericIndexes = schema.columns.flatMap((column, index) =>
    index !== timeIndex
    && (
      numericValueType(column.valueType)
      || rows.some((row) => chartNumericValue(row.cells[index]) !== null)
    )
      ? [index]
      : [],
  );
  if (numericIndexes.length === 0) return [];
  const points = rows.flatMap((row, index) => {
    const rawTime = typedValueToJSON(row.cells[timeIndex]);
    const date = typeof rawTime === "string" || typeof rawTime === "number" ? new Date(rawTime) : new Date(Number.NaN);
    const chartValues = numericIndexes.flatMap((sourceIndex) => {
      const value = chartNumericValue(row.cells[sourceIndex]);
      return value === null
        ? []
        : [{ name: schema.columns[sourceIndex].fieldName, value }];
    });
    if (Number.isNaN(date.valueOf()) || chartValues.length === 0) return [];
    const series = Object.fromEntries(chartValues.map(({ name, value }) => [name, value.coordinate]));
    const exactSeries = Object.fromEntries(chartValues.flatMap(({ name, value }) =>
      value.exactText === undefined ? [] : [[name, value.exactText]],
    ));
    const count = chartValues.reduce((sum, item) => sum + item.value.coordinate, 0);
    if (!Number.isFinite(count)) return [];
    const exactIntegers = chartValues.map((item) => item.value.exactInteger);
    const exactIntegerTotal = exactIntegers.every((value) => value !== undefined)
      ? exactIntegers.reduce((sum, value) => sum + (value ?? 0n), 0n)
      : undefined;
    const coordinateApproximate = chartValues.some((item) => item.value.approximate)
      || (exactIntegerTotal !== undefined && !Number.isSafeInteger(count));
    const formatter = formatters.timeline ??= new Intl.DateTimeFormat(
      "en-US",
      TIMELINE_TIME_FORMAT_OPTIONS,
    );
    return [{
      id: row.rowId || `bucket-${index}`,
      label: formatter.format(date),
      count,
      series,
      exactCount: coordinateApproximate && exactIntegerTotal !== undefined
        ? exactIntegerTotal.toString()
        : coordinateApproximate && chartValues.length === 1
          ? chartValues[0].value.exactText
          : undefined,
      exactSeries: Object.keys(exactSeries).length > 0 ? exactSeries : undefined,
      coordinateApproximate: coordinateApproximate || undefined,
      earliest: date.toISOString(),
    } satisfies TimelinePoint];
  });
  return points.map((point, index) => {
    const currentTime = point.earliest ? new Date(point.earliest).valueOf() : Number.NaN;
    const previousTime = points[index - 1]?.earliest ? new Date(points[index - 1].earliest as string).valueOf() : Number.NaN;
    const inferredWidth = Number.isFinite(currentTime - previousTime)
      ? currentTime - previousTime
      : knownBucketWidthMs;
    const nextEarliest = points[index + 1]?.earliest;
    return {
      id: point.id,
      label: point.label,
      count: point.count,
      series: point.series,
      exactCount: point.exactCount,
      exactSeries: point.exactSeries,
      coordinateApproximate: point.coordinateApproximate,
      earliest: point.earliest,
      latest: nextEarliest ?? (
        inferredWidth !== undefined && Number.isFinite(inferredWidth) && inferredWidth > 0
          ? new Date(currentTime + inferredWidth).toISOString()
          : undefined
      ),
    };
  });
}

/** Stable field order for a timechart table or export. */
export function timechartValueFields(
  points: TimelinePoint[],
  schema?: ResultSchema,
): string[] {
  if (schema !== undefined) {
    if (schema.resultKind !== ResultSetKind.RESULT_SET_KIND_TIME_SERIES) return [];
    const timeIndex = schema.columns.findIndex((column) => /^_?time$/i.test(column.fieldName));
    if (timeIndex < 0) return [];
    return schema.columns.flatMap((column, index) =>
      index !== timeIndex && column.fieldName.length > 0 ? [column.fieldName] : []
    );
  }
  const fields = new Set<string>();
  for (const point of points) {
    for (const field of Object.keys(point.series ?? {})) fields.add(field);
  }
  return fields.size > 0 ? [...fields] : ["count"];
}

/** Preserve every split-by series instead of exporting only the synthetic total. */
export function timechartRowsForExport(points: TimelinePoint[]): Record<string, WorkspaceStatisticsValue>[] {
  const fields = timechartValueFields(points);
  const hasExplicitSeries = points.some((point) => point.series !== undefined && Object.keys(point.series).length > 0);
  return points.map((point) => ({
    _time: point.earliest ?? point.label,
    ...Object.fromEntries(fields.map((field) => [
      field,
      hasExplicitSeries
        ? point.exactSeries?.[field] ?? point.series?.[field] ?? null
        : point.exactCount ?? point.count,
    ])),
  }));
}

export function timechartSpanMilliseconds(spl: string): number | null {
  const match = /(?:^|\|)\s*timechart\s+span\s*=\s*(\d+)(s|m|h)\b/i.exec(spl);
  if (match === null) return null;
  const magnitude = Number(match[1]);
  const multiplier = match[2].toLowerCase() === "s"
    ? 1_000
    : match[2].toLowerCase() === "m"
      ? 60_000
      : 3_600_000;
  const milliseconds = magnitude * multiplier;
  return Number.isSafeInteger(milliseconds) && milliseconds > 0 ? milliseconds : null;
}

export function adaptSearchResults(
  schema: ResultSchema,
  rows: ResultRow[],
  timechartBucketWidthMs?: number,
): AdaptedSearchResults {
  assertBrowserResultColumnCount(schema.columns.length);
  for (const column of schema.columns) {
    if (!validFlatMultivalueColumnPresentation(column)) {
      throw new RangeError(
        `Search result column “${column.fieldName}” has invalid multivalue presentation metadata.`,
      );
    }
  }
  switch (schema.resultKind) {
    case ResultSetKind.RESULT_SET_KIND_STATISTICS: {
      const transformedStatistics = statisticsFromRows(schema, rows);
      const hasRawEventColumn = schema.columns.some((column) =>
        column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW
        || column.fieldName === "_raw"
      );
      return {
        events: [],
        fields: [],
        statistics: transformedStatistics.rows,
        statisticsTable: hasRawEventColumn ? null : statisticsTableFromRows(schema, rows),
        statisticDimension: transformedStatistics.dimension === "result"
          ? "level"
          : transformedStatistics.dimension,
        timeline: [],
      };
    }
    case ResultSetKind.RESULT_SET_KIND_EVENTS: {
      const dateTimeFormatters: ResultDateTimeFormatters = {};
      return {
        events: rowsToEvents(schema, rows, dateTimeFormatters),
        fields: [],
        statistics: [],
        statisticsTable: null,
        statisticDimension: "level",
        timeline: [],
      };
    }
    case ResultSetKind.RESULT_SET_KIND_TIME_SERIES: {
      const dateTimeFormatters: ResultDateTimeFormatters = {};
      return {
        events: [],
        fields: [],
        statistics: [],
        statisticsTable: null,
        statisticDimension: "level",
        timeline: timelineFromRows(schema, rows, dateTimeFormatters, timechartBucketWidthMs),
      };
    }
    default:
      throw new RangeError(`Search results use unsupported result kind ${schema.resultKind}.`);
  }
}

interface DecimalParts {
  negative: boolean;
  digits: string;
  decimalPosition: bigint;
}

function decimalParts(value: string): DecimalParts | null {
  const match = /^([+-]?)(\d+)(?:\.(\d+))?(?:e([+-]?\d+))?$/i.exec(value.trim());
  if (match === null) return null;
  const fractionLength = BigInt((match[3] ?? "").length);
  let exponent = BigInt(match[4] ?? "0") - fractionLength;
  let digits = `${match[2]}${match[3] ?? ""}`.replace(/^0+/, "");
  if (digits.length === 0) return { negative: false, digits: "0", decimalPosition: 0n };
  const trailingZeroCount = digits.length - digits.replace(/0+$/, "").length;
  if (trailingZeroCount > 0) {
    digits = digits.slice(0, -trailingZeroCount);
    exponent += BigInt(trailingZeroCount);
  }
  return {
    negative: match[1] === "-",
    digits,
    decimalPosition: BigInt(digits.length) + exponent,
  };
}

function compareUnsignedDecimal(left: DecimalParts, right: DecimalParts): number {
  if (left.decimalPosition !== right.decimalPosition) return left.decimalPosition < right.decimalPosition ? -1 : 1;
  const digitLength = Math.max(left.digits.length, right.digits.length);
  return left.digits.padEnd(digitLength, "0").localeCompare(right.digits.padEnd(digitLength, "0"));
}

function sortableString(value: WorkspaceStatisticsValue): string {
  return typeof value === "object" && value !== null ? JSON.stringify(value) : String(value);
}

/** Compare lossless numeric text without first coercing it to IEEE-754. */
export function compareWorkspaceNumericValues(
  left: WorkspaceStatisticsValue,
  right: WorkspaceStatisticsValue,
): number {
  const leftParts = decimalParts(String(left));
  const rightParts = decimalParts(String(right));
  if (leftParts !== null && rightParts !== null) {
    if (leftParts.negative !== rightParts.negative) return leftParts.negative ? -1 : 1;
    const absoluteComparison = compareUnsignedDecimal(leftParts, rightParts);
    return leftParts.negative ? -absoluteComparison : absoluteComparison;
  }
  const leftNumber = Number(left);
  const rightNumber = Number(right);
  if (Number.isFinite(leftNumber) && Number.isFinite(rightNumber)) return leftNumber - rightNumber;
  return String(left).localeCompare(String(right), undefined, { numeric: true, sensitivity: "base" });
}

/** Compare typed statistics cells without coercing lossless 64-bit or decimal strings to IEEE-754 numbers. */
export function compareWorkspaceStatisticValues(
  left: WorkspaceStatisticsValue,
  right: WorkspaceStatisticsValue,
  column: WorkspaceStatisticsColumn,
): number {
  if (left === null) return right === null ? 0 : 1;
  if (right === null) return -1;
  if (column.numeric) return compareWorkspaceNumericValues(left, right);
  const timeLike = columnIsTimeLike(column);
  if (timeLike) {
    const leftTime = new Date(sortableString(left)).valueOf();
    const rightTime = new Date(sortableString(right)).valueOf();
    if (Number.isFinite(leftTime) && Number.isFinite(rightTime)) return leftTime - rightTime;
  }
  if (typeof left === "boolean" && typeof right === "boolean") return Number(left) - Number(right);
  return sortableString(left).localeCompare(sortableString(right), undefined, { numeric: true, sensitivity: "base" });
}

export function patternsFromEvents(
  events: DemoEvent[],
  totalEvents: number,
  sensitivity: "Precise" | "Balanced" | "Broad",
): WorkspacePattern[] {
  const counts = new Map<string, number>();
  for (const event of events) {
    const message = String(event.fields.message ?? event.raw);
    const normalized = sensitivity === "Precise"
      ? message.replace(/\b[0-9a-f]{16,}\b/gi, "*")
      : sensitivity === "Broad"
        ? message.replace(/\b\d+(?:\.\d+)?\b/g, "*").split(/\s+/).slice(0, 2).join(" ") + " *"
        : message.replace(/\b(?:\d+|[0-9a-f]{16,})\b/gi, "*");
    counts.set(normalized, (counts.get(normalized) ?? 0) + 1);
  }
  const denominator = Math.max(1, totalEvents || events.length);
  return [...counts]
    .map(([signature, count]) => ({
      signature,
      count,
      percent: Math.round(((count / denominator) * 100) * 10) / 10,
    }))
    .toSorted((left, right) => right.count - left.count)
    .slice(0, sensitivity === "Precise" ? 8 : sensitivity === "Broad" ? 3 : 5);
}

function startOfLocalDay(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

type RelativeTimeUnit = "s" | "m" | "h" | "d" | "w" | "mon" | "q" | "y";

const RELATIVE_TIME_UNITS: Record<string, RelativeTimeUnit> = {
  s: "s",
  sec: "s",
  second: "s",
  seconds: "s",
  m: "m",
  min: "m",
  minute: "m",
  minutes: "m",
  h: "h",
  hr: "h",
  hour: "h",
  hours: "h",
  d: "d",
  day: "d",
  days: "d",
  w: "w",
  week: "w",
  weeks: "w",
  mon: "mon",
  month: "mon",
  months: "mon",
  q: "q",
  quarter: "q",
  quarters: "q",
  y: "y",
  yr: "y",
  year: "y",
  years: "y",
};

function snapLocalTime(date: Date, unit: RelativeTimeUnit): Date {
  const snapped = new Date(date);
  if (unit === "s") {
    snapped.setMilliseconds(0);
  } else if (unit === "m") {
    snapped.setSeconds(0, 0);
  } else if (unit === "h") {
    snapped.setMinutes(0, 0, 0);
  } else if (unit === "d") {
    return startOfLocalDay(snapped);
  } else if (unit === "w") {
    const day = startOfLocalDay(snapped);
    day.setDate(day.getDate() - day.getDay());
    return day;
  } else if (unit === "mon") {
    return new Date(snapped.getFullYear(), snapped.getMonth(), 1);
  } else if (unit === "q") {
    return new Date(snapped.getFullYear(), Math.floor(snapped.getMonth() / 3) * 3, 1);
  } else {
    return new Date(snapped.getFullYear(), 0, 1);
  }
  return snapped;
}

function daysInLocalMonth(year: number, month: number): number {
  return new Date(year, month + 1, 0).getDate();
}

function shiftCalendarMonths(date: Date, months: number): Date {
  const shifted = new Date(date);
  const originalDay = shifted.getDate();
  shifted.setDate(1);
  shifted.setMonth(shifted.getMonth() + months);
  shifted.setDate(Math.min(originalDay, daysInLocalMonth(shifted.getFullYear(), shifted.getMonth())));
  return shifted;
}

function shiftRelativeTime(date: Date, amount: number, unit: RelativeTimeUnit): Date {
  if (unit === "mon") return shiftCalendarMonths(date, amount);
  if (unit === "q") return shiftCalendarMonths(date, amount * 3);
  if (unit === "y") return shiftCalendarMonths(date, amount * 12);
  const milliseconds = unit === "s"
    ? 1_000
    : unit === "m"
      ? 60_000
      : unit === "h"
        ? 3_600_000
        : unit === "w"
          ? 604_800_000
          : 86_400_000;
  return new Date(date.valueOf() + amount * milliseconds);
}

function resolveTimeExpression(expression: string, now: Date): Date {
  const value = expression.trim();
  if (value === "now") return now;
  if (value === "0") return new Date("1970-01-01T00:00:00.000Z");
  const relative = /^(?:now)?(?:([+-])(\d+)(s|sec|seconds?|m|min|minutes?|h|hr|hours?|d|days?|w|weeks?|mon|months?|q|quarters?|y|yr|years?))?(?:@(s|sec|second|m|min|minute|h|hr|hour|d|day|w|week|mon|month|q|quarter|y|yr|year))?$/i.exec(value);
  if (relative) {
    if (relative[1] === undefined && relative[4] === undefined) throw new Error(`Invalid time expression: ${expression}`);
    const relativeUnit = relative[3] === undefined ? undefined : RELATIVE_TIME_UNITS[relative[3].toLowerCase()];
    const snapUnit = relative[4] === undefined ? undefined : RELATIVE_TIME_UNITS[relative[4].toLowerCase()];
    if ((relative[3] !== undefined && relativeUnit === undefined) || (relative[4] !== undefined && snapUnit === undefined)) {
      throw new Error(`Invalid time expression: ${expression}`);
    }
    const signedAmount = relative[1] === undefined
      ? 0
      : Number(relative[2]) * (relative[1] === "-" ? -1 : 1);
    const shifted = relativeUnit === undefined ? new Date(now) : shiftRelativeTime(now, signedAmount, relativeUnit);
    return snapUnit === undefined ? shifted : snapLocalTime(shifted, snapUnit);
  }
  const parsed = new Date(value);
  if (Number.isNaN(parsed.valueOf())) throw new Error(`Invalid time expression: ${expression}`);
  return parsed;
}

export function resolveAbsoluteTimeRange(earliest: string, latest: string, now = new Date()): { earliest: string; latest: string; timezone: string } {
  const resolvedLatest = resolveTimeExpression(latest, now);
  const resolvedEarliest = resolveTimeExpression(earliest, now);
  if (resolvedEarliest >= resolvedLatest) throw new Error("Earliest time must be before latest time.");
  return {
    earliest: resolvedEarliest.toISOString(),
    latest: resolvedLatest.toISOString(),
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
  };
}
