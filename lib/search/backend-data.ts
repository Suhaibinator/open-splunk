import { ResultSetKind, type ResultRow, type ResultSchema } from "@/gen/ts/open_splunk/v1/result";
import type { TypedValue } from "@/gen/ts/open_splunk/v1/value";
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

function typedValueToJSON(value: TypedValue | undefined): unknown {
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

function statisticsFromRows(schema: ResultSchema, rows: ResultRow[]): { rows: WorkspaceStatistic[]; dimension: string } {
  const names = schema.columns.map((column) => column.fieldName);
  const countName = names.find((name) => /^(?:count|count\(.+\))$/i.test(name)) ?? names.find((name) => /count/i.test(name));
  const averageName = names.find((name) => /^avg(?:erage)?\(/i.test(name) || /avg.*duration/i.test(name));
  const dimension = names.find((name) => name !== countName && name !== averageName && !/^_?time$/i.test(name)) ?? "result";
  const decoded = rows.map((row) => rowFields(schema, row));
  const counts = decoded.map((fields) => numberField(fields, countName ? [countName] : []) ?? 0);
  const total = counts.reduce((sum, count) => sum + count, 0);
  return {
    dimension,
    rows: decoded.map((fields, index) => {
      const count = counts[index];
      return {
        level: String(fields[dimension] ?? "(none)"),
        count,
        percent: total > 0 ? `${((count / total) * 100).toFixed(1)}%` : "0.0%",
        avgDuration: numberField(fields, averageName ? [averageName] : ["avg(duration_ms)", "avg_duration_ms"]) ?? Number.NaN,
      };
    }),
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
  if (!timeName || !countName) return [];
  const points = rows.flatMap((row, index) => {
    const fields = rowFields(schema, row);
    const rawTime = fields[timeName];
    const date = typeof rawTime === "string" || typeof rawTime === "number" ? new Date(rawTime) : new Date(Number.NaN);
    const count = numberField(fields, [countName]);
    if (Number.isNaN(date.valueOf()) || count === null) return [];
    return [{
      id: row.rowId || `bucket-${index}`,
      label: new Intl.DateTimeFormat("en-US", { month: "short", day: "numeric", hour: "numeric", minute: "2-digit" }).format(date),
      count,
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

export function adaptSearchResults(schema: ResultSchema, rows: ResultRow[], timechart: boolean): AdaptedSearchResults {
  const events = rowsToEvents(schema, rows);
  const transformedStatistics = statisticsFromRows(schema, rows);
  const timeline = timechart ? timelineFromRows(schema, rows) : timelineFromEvents(events);
  return {
    events,
    fields: deriveFields(events),
    statistics: timechart || schema.resultKind === ResultSetKind.RESULT_SET_KIND_EVENTS
      ? statisticsFromEvents(events)
      : transformedStatistics.rows,
    statisticDimension: transformedStatistics.dimension === "result" ? "level" : transformedStatistics.dimension,
    timeline,
  };
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
    .map(([signature, count]) => ({ signature, count, percent: (count / denominator) * 100 }))
    .toSorted((left, right) => right.count - left.count)
    .slice(0, sensitivity === "Precise" ? 8 : sensitivity === "Broad" ? 3 : 5);
}

function startOfLocalDay(date: Date): Date {
  return new Date(date.getFullYear(), date.getMonth(), date.getDate());
}

function resolveTimeExpression(expression: string, now: Date, fallback: Date): Date {
  const value = expression.trim();
  if (value === "now") return now;
  if (value === "0") return new Date("1970-01-01T00:00:00.000Z");
  if (value === "@d") return startOfLocalDay(now);
  const relative = /^-(\d+)(m|h|d)(@d)?$/.exec(value);
  if (relative) {
    const amount = Number(relative[1]);
    const milliseconds = amount * (relative[2] === "m" ? 60_000 : relative[2] === "h" ? 3_600_000 : 86_400_000);
    const result = new Date(now.valueOf() - milliseconds);
    return relative[3] ? startOfLocalDay(result) : result;
  }
  const parsed = new Date(value);
  return Number.isNaN(parsed.valueOf()) ? fallback : parsed;
}

export function resolveAbsoluteTimeRange(earliest: string, latest: string, now = new Date()): { earliest: string; latest: string; timezone: string } {
  const resolvedLatest = resolveTimeExpression(latest, now, now);
  const resolvedEarliest = resolveTimeExpression(earliest, now, new Date(now.valueOf() - 86_400_000));
  if (resolvedEarliest >= resolvedLatest) throw new Error("Earliest time must be before latest time.");
  return {
    earliest: resolvedEarliest.toISOString(),
    latest: resolvedLatest.toISOString(),
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
  };
}

export function indexesFromSPL(spl: string): string[] {
  const indexes = new Set<string>();
  for (const match of spl.matchAll(/\bindex\s*=\s*(?:"([^"]+)"|([a-zA-Z0-9_.-]+))/gi)) {
    const value = (match[1] ?? match[2] ?? "").trim();
    if (value) indexes.add(value);
  }
  return indexes.size > 0 ? [...indexes] : ["gradethis"];
}
