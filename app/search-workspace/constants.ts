import type { ResultTab, TimeRange } from "./model";

export const DEFAULT_QUERY = "index=gradethis\n| sort -_time";
export const NUMBER_FORMAT = new Intl.NumberFormat("en-US");
export const COMPACT_NUMBER_FORMAT = new Intl.NumberFormat("en-US", {
  notation: "compact",
  maximumFractionDigits: 1,
});

export const TIME_PRESETS: TimeRange[] = [
  { label: "Last 15 minutes", earliest: "-15m", latest: "now" },
  { label: "Last 60 minutes", earliest: "-60m", latest: "now" },
  { label: "Last 4 hours", earliest: "-4h", latest: "now" },
  { label: "Last 24 hours", earliest: "-24h", latest: "now" },
  { label: "Last 7 days", earliest: "-7d", latest: "now" },
  { label: "Last 30 days", earliest: "-30d", latest: "now" },
  { label: "Today", earliest: "@d", latest: "now" },
  { label: "Yesterday", earliest: "-1d@d", latest: "@d" },
  { label: "All time", earliest: "0", latest: "now" },
];

export const COMPLETIONS = [
  { label: "stats", insertion: "stats count by level", detail: "Calculate aggregate statistics." },
  { label: "timechart", insertion: "timechart span=5m count by level", detail: "Create a time-series result." },
  { label: "table", insertion: "table _time level logger message trace_id", detail: "Keep fields in the listed order." },
  { label: "sort", insertion: "sort -_time", detail: "Sort newest events first." },
  { label: "where", insertion: "where status >= 500", detail: "Filter with an evaluated expression." },
];

export const EVENT_EXPORT_FIELDS = ["_time", "level", "logger", "message", "trace_id"];
export const EXPORT_FIELDS_BY_TAB: Record<ResultTab, string[]> = {
  events: EVENT_EXPORT_FIELDS,
  patterns: ["pattern", "count", "percent"],
  statistics: ["level", "count", "percent", "avgDuration"],
  visualization: ["level", "count", "percent", "avgDuration"],
};

export const EXPORT_FIELD_LABELS: Record<string, string> = {
  avgDuration: "avg(duration_ms)",
  percent: "% of results",
};
