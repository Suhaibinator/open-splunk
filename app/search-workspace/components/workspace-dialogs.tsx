import type { DemoHistoryEntry, DemoSavedSearch } from "@/lib/demo/search-data";

import { EXPORT_FIELD_LABELS, NUMBER_FORMAT } from "../constants";
import { Modal } from "../modal";
import type { ExportStage, JobPhase, ModalName, ResultTab, SearchMode, TimeRange } from "../model";
import { phaseLabel, stateClass } from "../workspace-utils";

interface WorkspaceDialogsProps {
  activeSavedSearchId: string | null;
  activeTab: ResultTab;
  displayedExportRows: number;
  elapsed: string;
  exportFieldOptions: string[];
  exportFields: string[];
  exportFormat: "csv" | "jsonl";
  exportSourceTab: ResultTab;
  exportStage: ExportStage;
  history: DemoHistoryEntry[];
  historyFilter: string;
  isRunning: boolean;
  modal: ModalName | null;
  phase: JobPhase;
  saveDescription: string;
  saveDialogReturnFocus: HTMLElement | null;
  saveName: string;
  savedSearchFilter: string;
  savedSearches: DemoSavedSearch[];
  scannedBytes: string;
  scannedRows: number;
  searchId: string;
  searchMode: SearchMode;
  submittedQuery: string;
  submittedTimeRange: TimeRange;
  timeRange: TimeRange;
  visibleEventCount: number;
  onCancelSearch: () => void;
  onClearHistory: () => void;
  onDeleteHistoryEntry: (id: string) => void;
  onDeleteSavedSearch: (id: string) => void;
  onDownloadExport: () => void;
  onExportFieldToggle: (field: string) => void;
  onExportFormatChange: (format: "csv" | "jsonl") => void;
  onHistoryEntryOpen: (entry: DemoHistoryEntry, runImmediately: boolean) => void;
  onHistoryFilterChange: (filter: string) => void;
  onModalChange: (modal: ModalName | null) => void;
  onOpenSavedSearch: (saved: DemoSavedSearch) => void;
  onPrepareExport: () => void;
  onResetExport: () => void;
  onSaveDescriptionChange: (description: string) => void;
  onSaveNameChange: (name: string) => void;
  onSaveSearch: () => void;
  onSavedSearchFilterChange: (filter: string) => void;
}

export function WorkspaceDialogs({
  activeSavedSearchId,
  activeTab,
  displayedExportRows,
  elapsed,
  exportFieldOptions,
  exportFields,
  exportFormat,
  exportSourceTab,
  exportStage,
  history,
  historyFilter,
  isRunning,
  modal,
  phase,
  saveDescription,
  saveDialogReturnFocus,
  saveName,
  savedSearchFilter,
  savedSearches,
  scannedBytes,
  scannedRows,
  searchId,
  searchMode,
  submittedQuery,
  submittedTimeRange,
  timeRange,
  visibleEventCount,
  onCancelSearch,
  onClearHistory,
  onDeleteHistoryEntry,
  onDeleteSavedSearch,
  onDownloadExport,
  onExportFieldToggle,
  onExportFormatChange,
  onHistoryEntryOpen,
  onHistoryFilterChange,
  onModalChange,
  onOpenSavedSearch,
  onPrepareExport,
  onResetExport,
  onSaveDescriptionChange,
  onSaveNameChange,
  onSaveSearch,
  onSavedSearchFilterChange,
}: WorkspaceDialogsProps) {
  if (modal === "save") {
    return (
      <Modal
        title={activeSavedSearchId === null ? "Save search" : "Save search as"}
        subtitle="Preserve the SPL and relative time range for reuse."
        onClose={() => onModalChange(null)}
        returnFocus={saveDialogReturnFocus}
        footer={<><button className="button secondary" type="button" onClick={() => onModalChange(null)}>Cancel</button><button className="button primary" type="button" disabled={saveName.trim().length === 0} onClick={onSaveSearch}>Save</button></>}
      >
        <div className="form-stack" data-testid="save-search-dialog">
          <label><span>Name</span><input value={saveName} onChange={(event) => onSaveNameChange(event.target.value)} /></label>
          <label><span>Description <small>optional</small></span><textarea value={saveDescription} onChange={(event) => onSaveDescriptionChange(event.target.value)} rows={3} /></label>
          <div className="form-summary"><span>App</span><strong>Search &amp; Reporting</strong><span>Time range</span><strong>{timeRange.label}</strong><span>Result view</span><strong>{activeTab[0].toUpperCase() + activeTab.slice(1)}</strong></div>
        </div>
      </Modal>
    );
  }

  if (modal === "open") {
    const filtered = savedSearches.filter((item) => `${item.name} ${item.description} ${item.query}`.toLowerCase().includes(savedSearchFilter.toLowerCase()));
    return (
      <Modal title="Open a saved search" subtitle={`${savedSearches.length} searches in this app`} wide onClose={() => onModalChange(null)}>
        <div className="library-toolbar"><label className="filter-input"><span aria-hidden="true">⌕</span><input aria-label="Filter saved searches" placeholder="Filter saved searches" value={savedSearchFilter} onChange={(event) => onSavedSearchFilterChange(event.target.value)} /></label><select aria-label="Sort saved searches" defaultValue="updated"><option value="updated">Recently updated</option><option value="name">Name</option></select></div>
        <div className="saved-list" data-testid="saved-search-list">
          {filtered.map((saved) => (
            <article className="saved-row" key={saved.id}>
              <button className="saved-row-main" type="button" onClick={() => onOpenSavedSearch(saved)}><span className="saved-icon" aria-hidden="true">⌕</span><span><strong>{saved.name}</strong><small>{saved.description}</small><code>{saved.query.replaceAll("\n", " ")}</code></span></button>
              <div className="saved-row-meta"><span>{saved.updatedAt}</span><button className="icon-button" aria-label={`Delete ${saved.name}`} type="button" onClick={() => onDeleteSavedSearch(saved.id)}>×</button></div>
            </article>
          ))}
          {filtered.length === 0 ? <div className="empty-state"><strong>No saved searches match</strong><span>Try another name or SPL term.</span></div> : null}
        </div>
      </Modal>
    );
  }

  if (modal === "history") {
    const filtered = history.filter((item) => item.query.toLowerCase().includes(historyFilter.toLowerCase()));
    return (
      <Modal title="Search history" subtitle="Every attempted search, including canceled and failed jobs." wide onClose={() => onModalChange(null)}>
        <div className="library-toolbar"><label className="filter-input"><span aria-hidden="true">⌕</span><input aria-label="Filter search history" placeholder="Filter by SPL" value={historyFilter} onChange={(event) => onHistoryFilterChange(event.target.value)} /></label><button className="button secondary compact" type="button" onClick={onClearHistory}>Clear history</button></div>
        <div className="history-table" data-testid="history-list">
          <div className="history-head"><span>Search</span><span>Status</span><span>Events</span><span>Duration</span><span>Ran</span><span>Actions</span></div>
          {filtered.map((entry) => (
            <article className="history-row" key={entry.id}><code title={entry.query}>{entry.query}</code><span className={`history-state history-${entry.state.toLowerCase()}`}>{entry.state}</span><span>{NUMBER_FORMAT.format(entry.events)}</span><span>{entry.duration}</span><span>{entry.ranAt}</span><div className="row-actions"><button type="button" onClick={() => onHistoryEntryOpen(entry, false)}>Open</button><button type="button" onClick={() => onHistoryEntryOpen(entry, true)}>Run again</button><button aria-label="Delete history entry" type="button" onClick={() => onDeleteHistoryEntry(entry.id)}>×</button></div></article>
          ))}
        </div>
      </Modal>
    );
  }

  if (modal === "export") {
    return (
      <Modal
        title={`Export ${exportSourceTab}`}
        subtitle="Create a bounded artifact from the displayed search result."
        onClose={() => { onModalChange(null); onResetExport(); }}
        footer={exportStage === "ready"
          ? <><span className="export-ready-note">✓ Artifact ready · expires in 10 minutes</span><button className="button primary" type="button" onClick={onDownloadExport}>Download .{exportFormat}</button></>
          : <><button className="button secondary" type="button" onClick={() => onModalChange(null)}>Cancel</button><button className="button primary" type="button" disabled={exportStage === "preparing" || exportFields.length === 0} onClick={onPrepareExport}>{exportStage === "preparing" ? "Preparing…" : "Create export"}</button></>}
      >
        <div className="form-stack" data-testid="export-dialog">
          <fieldset className="segmented-fieldset" disabled={exportStage !== "configure"}>
            <legend>Format</legend>
            <label className={exportFormat === "csv" ? "selected" : ""} htmlFor="export-format-csv"><input id="export-format-csv" aria-label="Export as CSV" type="radio" name="format" checked={exportFormat === "csv"} onChange={() => onExportFormatChange("csv")} /><span><strong>CSV</strong><small>Spreadsheet-compatible table</small></span></label>
            <label className={exportFormat === "jsonl" ? "selected" : ""} htmlFor="export-format-jsonl"><input id="export-format-jsonl" aria-label="Export as JSON Lines" type="radio" name="format" checked={exportFormat === "jsonl"} onChange={() => onExportFormatChange("jsonl")} /><span><strong>JSON Lines</strong><small>Typed record per line</small></span></label>
          </fieldset>
          <fieldset className="export-fields" disabled={exportStage !== "configure"}>
            <legend>Columns <small>{exportFields.length} selected</small></legend>
            {exportFieldOptions.map((field) => <label key={field} htmlFor={`export-field-${field}`}><input id={`export-field-${field}`} aria-label={`Include ${field} in export`} type="checkbox" checked={exportFields.includes(field)} onChange={() => onExportFieldToggle(field)} /><code>{EXPORT_FIELD_LABELS[field] ?? field}</code></label>)}
          </fieldset>
          <div className="export-limit"><span>Maximum rows</span><strong>50,000</strong><small>{displayedExportRows} displayed {exportSourceTab === "events" ? "events" : "rows"}</small></div>
          {exportStage === "preparing" ? <div className="export-progress"><span style={{ width: "72%" }} /><strong>Materializing results…</strong></div> : null}
        </div>
      </Modal>
    );
  }

  if (modal === "settings") {
    return <Modal title="Search preferences" subtitle="Settings apply to this browser." onClose={() => onModalChange(null)} footer={<button className="button primary" type="button" onClick={() => onModalChange(null)}>Done</button>}><div className="settings-list" data-testid="settings-dialog"><label htmlFor="setting-wrap-events"><span><strong>Wrap event text</strong><small>Keep long raw events within the results pane.</small></span><input id="setting-wrap-events" aria-label="Wrap event text" type="checkbox" defaultChecked /></label><label htmlFor="setting-discover-fields"><span><strong>Discover interesting fields</strong><small>Profile fields that appear in at least 20% of events.</small></span><input id="setting-discover-fields" aria-label="Discover interesting fields" type="checkbox" defaultChecked /></label><label htmlFor="setting-live-previews"><span><strong>Live result previews</strong><small>Render bounded rows while a search runs.</small></span><input id="setting-live-previews" aria-label="Live result previews" type="checkbox" defaultChecked /></label><label htmlFor="setting-compact-density"><span><strong>Compact density</strong><small>Show more events in the available viewport.</small></span><input id="setting-compact-density" aria-label="Compact density" type="checkbox" defaultChecked /></label></div></Modal>;
  }

  if (modal === "jobs") {
    return (
      <Modal title="Activity & jobs" subtitle="Search jobs retained for this session." wide onClose={() => onModalChange(null)}>
        <div className="jobs-list" data-testid="jobs-dialog">
          <article className="job-card active-job-card"><div className={`job-card-state ${stateClass(phase)}`}><span />{phaseLabel(phase)}</div><code>{submittedQuery.replaceAll("\n", " ")}</code><div className="job-card-stats"><span>{NUMBER_FORMAT.format(visibleEventCount)} events</span><span>{scannedBytes} scanned</span><span>{elapsed}</span></div>{isRunning ? <button className="button danger compact" type="button" onClick={onCancelSearch}>Cancel</button> : null}</article>
          {history.slice(0, 4).map((entry) => <article className="job-card" key={entry.id}><div className={`job-card-state history-${entry.state.toLowerCase()}`}><span />{entry.state}</div><code>{entry.query}</code><div className="job-card-stats"><span>{NUMBER_FORMAT.format(entry.events)} events</span><span>{entry.duration}</span><span>{entry.ranAt}</span></div></article>)}
        </div>
      </Modal>
    );
  }

  if (modal === "inspect") {
    return (
      <Modal title="Search job inspector" subtitle="Dispatch and execution details for the displayed result." wide onClose={() => onModalChange(null)} footer={<button className="button primary" type="button" onClick={() => onModalChange(null)}>Done</button>}>
        <div className="job-inspector" data-testid="job-inspector"><section><span>Status</span><strong className={`inspector-state ${stateClass(phase)}`}><i />{phaseLabel(phase)}</strong></section><section><span>Search ID</span><code>{searchId}</code></section><section><span>Search mode</span><strong>{searchMode}</strong></section><section><span>Time range</span><strong>{submittedTimeRange.label}</strong></section><section><span>Scanned</span><strong>{NUMBER_FORMAT.format(scannedRows)} rows · {scannedBytes}</strong></section><section><span>Elapsed</span><strong>{elapsed}</strong></section><div className="inspector-query"><span>Dispatched SPL</span><code>{submittedQuery}</code></div><p>No skipped events, sequence gaps, or execution warnings were reported for this job.</p></div>
      </Modal>
    );
  }

  return null;
}
