import {
  ColumnSemanticType,
  ResultSetKind,
  type ResultRow,
  type ResultSchema,
} from "@/gen/ts/open_splunk/v1/result";
import { ValueType, type TypedValue } from "@/gen/ts/open_splunk/v1/value";
import type {
  DemoEvent,
  DemoField,
  DemoScalar,
  TimelinePoint,
} from "@/lib/demo/search-data";

export type SearchDataMode = "backend" | "demo";

export interface WorkspaceStatistic {
  level: string;
  count: number;
  percent: string;
  avgDuration: number;
  /** The server-provided aggregation represented by `count` in categorical charts. */
  measureLabel?: string;
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

const SELECTED_FIELDS = new Set(["host", "source", "sourcetype", "level", "trace_id"]);
const INTERNAL_FIELDS = new Set(["_raw", "_time", "timestamp"]);

function safeNumber(value: bigint): DemoScalar {
  const number = Number(value);
  return Number.isSafeInteger(number) ? number : value.toString();
}

function durationSeconds(seconds: bigint, nanos: number): number {
  return Number(seconds) + nanos / 1_000_000_000;
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
    case "durationValue":
      return durationSeconds(value.kind.value.seconds, value.kind.value.nanos);
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

function jsonToScalar(decoded: unknown): DemoScalar {
  if (decoded === null || typeof decoded === "string" || typeof decoded === "number" || typeof decoded === "boolean") {
    return decoded;
  }
  return JSON.stringify(decoded);
}

function formatEventTime(value: DemoScalar): { iso: string; label: string } {
  const parsed = typeof value === "string" || typeof value === "number" ? new Date(value) : new Date(Number.NaN);
  if (Number.isNaN(parsed.valueOf())) {
    return { iso: "", label: "Time unavailable" };
  }
  return {
    iso: parsed.toISOString(),
    label: new Intl.DateTimeFormat("en-US", {
      month: "numeric",
      day: "numeric",
      year: "2-digit",
      hour: "numeric",
      minute: "2-digit",
      second: "2-digit",
      fractionalSecondDigits: 3,
    }).format(parsed),
  };
}

function rowFields(schema: ResultSchema, row: ResultRow): Record<string, DemoScalar> {
  const fields: Record<string, DemoScalar> = {};
  schema.columns.forEach((column, index) => {
    const value = row.cells[index];
    const decoded = typedValueToJSON(value);
    if (column.fieldName === "fields" && typeof decoded === "object" && decoded !== null && !Array.isArray(decoded)) {
      Object.entries(decoded).forEach(([name, nestedValue]) => {
        fields[name] = jsonToScalar(nestedValue);
      });
      return;
    }
    fields[column.fieldName] = typedValueToScalar(value);
  });
  return fields;
}

function rowsToEvents(schema: ResultSchema, rows: ResultRow[]): DemoEvent[] {
  return rows.map((row) => {
    const fields = rowFields(schema, row);
    const eventTime = formatEventTime(fields["_time"] ?? fields.timestamp ?? null);
    const rawValue = fields["_raw"];
    return {
      id: row.rowId || `row-${row.ordinal.toString()}`,
      time: eventTime.iso,
      timeLabel: eventTime.label,
      raw: typeof rawValue === "string" ? rawValue : JSON.stringify(fields),
      fields,
    };
  });
}

function fieldType(values: DemoScalar[]): DemoField["type"] {
  if (values.some((value) => typeof value === "number")) return "number";
  if (values.some((value) => typeof value === "boolean")) return "boolean";
  return "string";
}

export function deriveFields(events: DemoEvent[]): DemoField[] {
  const names = new Set(events.flatMap((event) => Object.keys(event.fields)));
  return [...names]
    .filter((name) => !INTERNAL_FIELDS.has(name))
    .map((name) => {
      const values = events.map((event) => event.fields[name]).filter((value): value is DemoScalar => value !== undefined);
      const counts = new Map<string, { value: DemoScalar; count: number }>();
      for (const value of values) {
        const key = `${typeof value}:${String(value)}`;
        const current = counts.get(key);
        counts.set(key, { value, count: (current?.count ?? 0) + 1 });
      }
      const topValues = [...counts.values()].toSorted((left, right) => right.count - left.count).slice(0, 5);
      const selected = SELECTED_FIELDS.has(name);
      return {
        name,
        displayName: name,
        distinctCount: counts.size,
        eventCount: values.length,
        selected,
        interesting: !selected,
        type: fieldType(values),
        values: topValues,
      } satisfies DemoField;
    })
    .toSorted((left, right) => {
      const selectedDifference = Number(right.selected) - Number(left.selected);
      return selectedDifference || right.eventCount - left.eventCount || left.name.localeCompare(right.name);
    });
}

function numberField(fields: Record<string, DemoScalar>, candidates: string[]): number | null {
  for (const candidate of candidates) {
    const value = fields[candidate];
    if (typeof value === "number" && Number.isFinite(value)) return value;
    if (typeof value === "string" && value.trim().length > 0) {
      const parsed = Number(value);
      if (Number.isFinite(parsed)) return parsed;
    }
  }
  return null;
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
  const keyCounts = new Map<string, number>();
  const columns = schema.columns.map((column, sourceIndex) => {
    const fieldName = column.fieldName || `column_${sourceIndex + 1}`;
    const occurrence = (keyCounts.get(fieldName) ?? 0) + 1;
    keyCounts.set(fieldName, occurrence);
    const key = occurrence === 1 ? fieldName : `${fieldName}__${occurrence}`;
    const baseLabel = column.displayName || fieldName;
    const observedCells = rows
      .map((row) => row.cells[sourceIndex])
      .filter((cell) => cell?.kind?.$case !== "nullValue" && cell?.kind?.$case !== "missingValue");
    const numeric = numericValueType(column.valueType)
      || (observedCells.length > 0 && observedCells.every((cell) => numericCell(cell)));
    const timeLike = column.valueType === ValueType.VALUE_TYPE_TIMESTAMP
      || column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME
      || column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_INDEX_TIME
      || /^_?time$/i.test(fieldName);
    return {
      key,
      fieldName,
      label: occurrence === 1 ? baseLabel : `${baseLabel} (${occurrence})`,
      valueType: column.valueType,
      semanticType: column.semanticType,
      numeric,
      pivotable: !numeric
        && !timeLike
        && column.valueType !== ValueType.VALUE_TYPE_LIST
        && column.valueType !== ValueType.VALUE_TYPE_OBJECT
        && fieldName.length > 0,
      sourceIndex,
    };
  });
  return {
    columns: columns.map(({ sourceIndex: _sourceIndex, ...column }) => column),
    rows: rows.map((row, rowIndex) => ({
      id: `${row.rowId || `row-${row.ordinal.toString()}`}-${rowIndex}`,
      values: Object.fromEntries(columns.map((column) => [column.key, typedValueToJSON(row.cells[column.sourceIndex])])),
    })),
  };
}

const AGGREGATE_FIELD_NAME = /^(?:(?:count|sum|avg|average|min|max|median|mode|range|stdev|variance|var|distinct_count|dc|rate)(?:\(|$|_)|p(?:50|75|90|95|98|99|999)$|perc\d+(?:\(|$))/i;

function columnHasNumericValues(rows: ResultRow[], index: number): boolean {
  const observed = rows
    .map((row) => row.cells[index])
    .filter((cell) => cell?.kind?.$case !== "nullValue" && cell?.kind?.$case !== "missingValue");
  return observed.length > 0 && observed.every((cell) => numericCell(cell));
}

function columnIsTimeLike(schema: ResultSchema, index: number): boolean {
  const column = schema.columns[index];
  return column.valueType === ValueType.VALUE_TYPE_TIMESTAMP
    || column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME
    || column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_INDEX_TIME
    || /^_?time$/i.test(column.fieldName);
}

function preferredMetricIndex(schema: ResultSchema, rows: ResultRow[]): number | null {
  const numericIndexes = schema.columns.flatMap((column, index) =>
    numericValueType(column.valueType) || columnHasNumericValues(rows, index) ? [index] : [],
  );
  const explicitMetric = numericIndexes.find((index) =>
    schema.columns[index].semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
  );
  if (explicitMetric !== undefined) return explicitMetric;
  const namedAggregate = numericIndexes.find((index) => AGGREGATE_FIELD_NAME.test(schema.columns[index].fieldName));
  if (namedAggregate !== undefined) return namedAggregate;

  // A single numeric field paired with another field is an unambiguous value/dimension shape.
  // With multiple unnamed numeric fields there is no defensible way to choose a measure.
  return numericIndexes.length === 1 && schema.columns.length > 1 ? numericIndexes[0] : null;
}

function preferredDimensionIndex(schema: ResultSchema, metricIndex: number): number | null {
  const candidates = schema.columns.flatMap((column, index) => {
    if (index === metricIndex || columnIsTimeLike(schema, index)) return [];
    if (column.valueType === ValueType.VALUE_TYPE_LIST || column.valueType === ValueType.VALUE_TYPE_OBJECT) return [];
    if (column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC || AGGREGATE_FIELD_NAME.test(column.fieldName)) return [];
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
 * Produce the legacy categorical chart rows only when the backend shape has one
 * defensible measure and one dimension. An empty row set deliberately signals
 * that Visualization should explain the incompatible result shape instead of
 * inventing an "event count by level" chart.
 */
function statisticsFromRows(schema: ResultSchema, rows: ResultRow[]): { rows: WorkspaceStatistic[]; dimension: string } {
  const metricIndex = preferredMetricIndex(schema, rows);
  if (metricIndex === null) return { rows: [], dimension: "result" };
  const dimensionIndex = preferredDimensionIndex(schema, metricIndex);
  if (dimensionIndex === null) return { rows: [], dimension: "result" };

  const metricColumn = schema.columns[metricIndex];
  const dimensionColumn = schema.columns[dimensionIndex];
  const averageIndex = schema.columns.findIndex((column) =>
    /^avg(?:erage)?\(/i.test(column.fieldName) || /avg.*duration/i.test(column.fieldName),
  );
  const chartRows = rows.flatMap((row) => {
    const count = numberField(
      { metric: typedValueToScalar(row.cells[metricIndex]) },
      ["metric"],
    );
    if (count === null || count < 0) return [];
    const dimensionValue = typedValueToJSON(row.cells[dimensionIndex]);
    if (Array.isArray(dimensionValue) || (typeof dimensionValue === "object" && dimensionValue !== null)) return [];
    const average = averageIndex < 0
      ? Number.NaN
      : numberField({ average: typedValueToScalar(row.cells[averageIndex]) }, ["average"]) ?? Number.NaN;
    return [{
      level: dimensionValue === null ? "(none)" : String(dimensionValue),
      count,
      percent: "0.0%",
      avgDuration: average,
      measureLabel: metricColumn.displayName || metricColumn.fieldName,
    } satisfies WorkspaceStatistic];
  });
  // Mixed positive/negative measures require a zero-baseline chart, which this
  // compact Splunk-style visualization intentionally does not pretend to render.
  if (chartRows.length !== rows.length) return { rows: [], dimension: dimensionColumn.fieldName || "result" };
  const total = chartRows.reduce((sum, row) => sum + row.count, 0);
  for (const row of chartRows) {
    row.percent = total > 0 ? `${((row.count / total) * 100).toFixed(1)}%` : "0.0%";
  }
  return {
    dimension: dimensionColumn.fieldName || "result",
    rows: chartRows,
  };
}

function statisticsFromEvents(events: DemoEvent[]): WorkspaceStatistic[] {
  const counts = new Map<string, { count: number; durations: number[] }>();
  for (const event of events) {
    const level = String(event.fields.level ?? "OTHER");
    const current = counts.get(level) ?? { count: 0, durations: [] };
    current.count += 1;
    const duration = event.fields.duration_ms;
    if (typeof duration === "number") current.durations.push(duration);
    counts.set(level, current);
  }
  return [...counts].map(([level, summary]) => ({
    level,
    count: summary.count,
    percent: events.length > 0 ? `${((summary.count / events.length) * 100).toFixed(1)}%` : "0.0%",
    avgDuration: summary.durations.length > 0
      ? summary.durations.reduce((sum, value) => sum + value, 0) / summary.durations.length
      : 0,
  }));
}

function timelineFromRows(schema: ResultSchema, rows: ResultRow[]): TimelinePoint[] {
  const timeName = schema.columns.find((column) => /^_?time$/i.test(column.fieldName))?.fieldName;
  const countName = schema.columns.find((column) => /^(?:count|count\(.+\))$/i.test(column.fieldName))?.fieldName;
  if (!timeName) return [];
  const decodedRows = rows.map((row) => rowFields(schema, row));
  const numericNames = countName === undefined
    ? schema.columns
      .map((column) => column.fieldName)
      .filter((name) => name !== timeName && decodedRows.some((fields) => numberField(fields, [name]) !== null))
    : [countName];
  if (numericNames.length === 0) return [];
  const points = rows.flatMap((row, index) => {
    const fields = decodedRows[index];
    const rawTime = fields[timeName];
    const date = typeof rawTime === "string" || typeof rawTime === "number" ? new Date(rawTime) : new Date(Number.NaN);
    const series = Object.fromEntries(numericNames.flatMap((name) => {
      const value = numberField(fields, [name]);
      return value === null ? [] : [[name, value]];
    }));
    const values = Object.values(series);
    if (Number.isNaN(date.valueOf()) || values.length === 0) return [];
    return [{
      id: row.rowId || `bucket-${index}`,
      label: new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date),
      count: values.reduce((sum, value) => sum + value, 0),
      series,
      earliest: date.toISOString(),
    } satisfies TimelinePoint];
  });
  return points.map((point, index) => {
    const currentTime = point.earliest ? new Date(point.earliest).valueOf() : Number.NaN;
    const previousTime = points[index - 1]?.earliest ? new Date(points[index - 1].earliest as string).valueOf() : Number.NaN;
    const inferredWidth = Number.isFinite(currentTime - previousTime) ? currentTime - previousTime : 60_000;
    return {
      id: point.id,
      label: point.label,
      count: point.count,
      series: point.series,
      earliest: point.earliest,
      latest: points[index + 1]?.earliest ?? new Date(currentTime + Math.max(1, inferredWidth)).toISOString(),
    };
  });
}

function timelineFromEvents(events: DemoEvent[]): TimelinePoint[] {
  const dated = events
    .map((event) => ({ event, date: new Date(event.time) }))
    .filter(({ date }) => !Number.isNaN(date.valueOf()));
  if (dated.length === 0) return [];
  const earliest = Math.min(...dated.map(({ date }) => date.valueOf()));
  const latest = Math.max(...dated.map(({ date }) => date.valueOf()));
  const bucketCount = Math.min(48, Math.max(1, Math.ceil(Math.sqrt(dated.length) * 3)));
  const width = Math.max(1, Math.ceil((latest - earliest + 1) / bucketCount));
  const counts = Array.from({ length: bucketCount }, () => 0);
  for (const { date } of dated) counts[Math.min(bucketCount - 1, Math.floor((date.valueOf() - earliest) / width))] += 1;
  return counts.map((count, index) => {
    const start = new Date(earliest + index * width);
    const end = new Date(earliest + (index + 1) * width);
    return {
      id: `bucket-${index}`,
      label: new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(start),
      count,
      earliest: start.toISOString(),
      latest: end.toISOString(),
    };
  });
}

/** Stable backend field order for a timechart table or export. */
export function timechartValueFields(points: TimelinePoint[]): string[] {
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
      hasExplicitSeries ? point.series?.[field] ?? null : point.count,
    ])),
  }));
}

export function adaptSearchResults(schema: ResultSchema, rows: ResultRow[], timechart: boolean): AdaptedSearchResults {
  const events = rowsToEvents(schema, rows);
  const transformedStatistics = statisticsFromRows(schema, rows);
  const timeline = timechart ? timelineFromRows(schema, rows) : timelineFromEvents(events);
  const hasRawEventColumn = schema.columns.some((column) => column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW || column.fieldName === "_raw");
  const statisticsTable = !timechart
    && schema.resultKind !== ResultSetKind.RESULT_SET_KIND_EVENTS
    && !hasRawEventColumn
    ? statisticsTableFromRows(schema, rows)
    : null;
  return {
    events,
    fields: deriveFields(events),
    statistics: timechart || schema.resultKind === ResultSetKind.RESULT_SET_KIND_EVENTS
      ? statisticsFromEvents(events)
      : transformedStatistics.rows,
    statisticsTable,
    statisticDimension: transformedStatistics.dimension === "result" ? "level" : transformedStatistics.dimension,
    timeline,
  };
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

function compareNumericValues(left: WorkspaceStatisticsValue, right: WorkspaceStatisticsValue): number {
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
  if (column.numeric) return compareNumericValues(left, right);
  const timeLike = column.valueType === ValueType.VALUE_TYPE_TIMESTAMP
    || column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME
    || column.semanticType === ColumnSemanticType.COLUMN_SEMANTIC_TYPE_INDEX_TIME
    || /^_?time$/i.test(column.fieldName);
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

export function indexesFromSPL(spl: string): string[] {
  const indexes = new Set<string>();
  const baseSearch = spl.split("|", 1)[0] ?? spl;
  let includesAllAuthorizedIndexes = false;
  for (const match of baseSearch.matchAll(/\bindex\s*=\s*(?:"([^"]+)"|([a-zA-Z0-9_.*-]+))/gi)) {
    const matchIndex = match.index ?? 0;
    const before = baseSearch.slice(0, matchIndex);
    const quoteCount = (before.match(/(?<!\\)"/g) ?? []).length;
    if (quoteCount % 2 !== 0 || /(?:\bNOT|-)\s*$/i.test(before)) continue;
    const value = (match[1] ?? match[2] ?? "").trim();
    if (value.includes("*")) includesAllAuthorizedIndexes = true;
    else if (value.length > 0) indexes.add(value);
  }
  if (includesAllAuthorizedIndexes) return [];
  return indexes.size > 0 ? [...indexes] : ["gradethis"];
}
