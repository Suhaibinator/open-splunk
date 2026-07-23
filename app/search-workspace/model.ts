import type { SearchDataMode } from "@/lib/search/backend-data";

export type ResultTab = "events" | "patterns" | "statistics" | "visualization";
export type ModalName =
  | "time"
  | "save"
  | "open"
  | "rename-saved-search"
  | "delete-saved-search"
  | "history"
  | "clear-history"
  | "export"
  | "settings"
  | "jobs"
  | "inspect";
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
  | "canceled"
  | "expired";
export type SearchMode = "Smart" | "Fast" | "Verbose";
export type ExportStage = "configure" | "pending" | "ready" | "error";
export type DialogActionStatus = "idle" | "pending" | "error";
export type ExportFormatChoice = "csv" | "jsonl";
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
  timezone?: string;
}

export interface ToastState {
  message: string;
  tone: "success" | "info" | "warning";
}

export type DialogActionState =
  | { status: "idle" }
  | { status: "pending" }
  | { status: "error"; error: string };

export type TargetedDialogActionState =
  | { status: "idle" }
  | { status: "pending"; targetId: string }
  | { status: "error"; error: string; targetId?: string | null };

export type ExportQuantity = number | bigint;

export interface ExportArtifactDetails {
  /** Opaque value passed back to the owner when this exact artifact is downloaded. */
  downloadHandle: string;
  fileName: string;
  mediaType: string;
  rowCount: ExportQuantity;
  sizeBytes: ExportQuantity;
  expiresAt: Date | null;
}

interface ExportDialogStateBase {
  description: string;
  sourceTab: ResultTab;
  format: ExportFormatChoice;
  maximumRows?: ExportQuantity | null;
  maximumBytes?: ExportQuantity | null;
}

export type ExportDialogState =
  | (ExportDialogStateBase & {
    status: "configure";
    available: boolean;
    unavailableReason?: string | null;
  })
  | (ExportDialogStateBase & {
    status: "pending";
    requestId: string;
    exportJobId: string | null;
    rowsWritten?: ExportQuantity | null;
    bytesWritten?: ExportQuantity | null;
    percentComplete?: number | null;
    cancel: DialogActionState;
  })
  | (ExportDialogStateBase & {
    status: "ready";
    requestId: string;
    exportJobId: string;
    artifact: ExportArtifactDetails;
    download: DialogActionState;
  })
  | (ExportDialogStateBase & {
    status: "error";
    requestId: string | null;
    exportJobId: string | null;
    error: string;
    retryable: boolean;
  });

export interface SearchCapabilityState {
  supported: boolean;
  enabled: boolean;
  configurable: boolean;
  detail: string;
  update: DialogActionState;
}

export interface SearchSettingsCapabilities {
  context: string;
  fieldDiscovery: SearchCapabilityState;
  liveResultPreviews: SearchCapabilityState;
  eventSampling: SearchCapabilityState;
}

export type SearchCapabilityName = keyof Pick<
  SearchSettingsCapabilities,
  "fieldDiscovery" | "liveResultPreviews" | "eventSampling"
>;

export interface SearchWorkspaceProps {
  dataMode: SearchDataMode;
  apiBaseUrl?: string;
}
