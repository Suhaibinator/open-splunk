import type { SearchDataMode } from "@/lib/search/backend-data";

export type ResultTab = "events" | "patterns" | "statistics" | "visualization";
export type ModalName = "time" | "save" | "open" | "history" | "export" | "settings" | "jobs" | "inspect";
export type MenuName =
  | "app"
  | "user"
  | "activity"
  | "help"
  | "search-actions"
  | "search-mode"
  | "save-as"
  | "stats-format"
  | "pattern-sensitivity"
  | "event-display"
  | "event-page-size"
  | "timeline-format";
export type JobPhase =
  | "queued"
  | "parsing"
  | "planning"
  | "running"
  | "finalizing"
  | "completed"
  | "failed"
  | "canceled";
export type SearchMode = "Smart" | "Fast" | "Verbose";
export type ExportStage = "configure" | "preparing" | "ready";
export type TimePickerSection = "presets" | "relative" | "range" | "advanced";
export type StatsDensity = "compact" | "standard";
export type PatternSensitivity = "Precise" | "Balanced" | "Broad";
export type EventDisplay = "List" | "Raw";
export type TimelineDisplay = "Columns" | "Compact";
export type ChartStyle = "column" | "horizontal" | "line";
export type LegendPosition = "bottom" | "right" | "none";

export interface TimeRange {
  label: string;
  earliest: string;
  latest: string;
}

export interface ToastState {
  message: string;
  tone: "success" | "info" | "warning";
}

export interface SearchWorkspaceProps {
  dataMode: SearchDataMode;
  apiBaseUrl?: string;
}
