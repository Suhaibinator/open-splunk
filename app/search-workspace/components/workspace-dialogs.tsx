import type { DemoHistoryEntry, DemoSavedSearch } from "@/lib/demo/search-data";

import { NUMBER_FORMAT } from "../constants";
import {
  formatDecimalBytes,
  formatExactNumericText,
  formatNonNegativeIntegerQuantity,
} from "../formatters";
import { Modal } from "../modal";
import type {
  DialogActionState,
  ExportArtifactDetails,
  ExportDialogState,
  JobPhase,
  ModalName,
  ResultTab,
  SearchCapabilityName,
  SearchCapabilityState,
  SearchMode,
  SearchSettingsCapabilities,
  TargetedDialogActionState,
  TimeRange,
} from "../model";
import { phaseLabel, stateClass } from "../workspace-utils";

import styles from "./workspace-dialogs.module.css";

function formatExpiry(date: Date): string | null {
  if (Number.isNaN(date.valueOf())) return null;
  return new Intl.DateTimeFormat("en-US", {
    year: "numeric",
    month: "short",
    day: "numeric",
    hour: "numeric",
    minute: "2-digit",
    second: "2-digit",
    timeZoneName: "short",
  }).format(date);
}

function formatHistoryResultCount(entry: DemoHistoryEntry): string {
  return entry.eventsExact === undefined
    ? NUMBER_FORMAT.format(entry.events)
    : formatExactNumericText(entry.eventsExact);
}

function CapabilitySetting({
  capability,
  label,
  name,
  onChange,
}: {
  capability: SearchCapabilityState;
  label: string;
  name: SearchCapabilityName;
  onChange: (name: SearchCapabilityName, enabled: boolean) => void;
}) {
  const controlId = `setting-${name}`;
  const unavailable = !capability.supported;
  const updating = capability.update.status === "pending";
  return (
    <label className={unavailable ? styles.unavailableSetting : undefined} htmlFor={controlId}>
      <span>
        <strong>{label}</strong>
        <small>{capability.detail}</small>
        {capability.update.status === "error" ? <small className={styles.settingError} role="alert">{capability.update.error}</small> : null}
      </span>
      <span className={styles.capabilityControl}>
        <small className={unavailable ? styles.unavailableBadge : styles.availableBadge}>
          {updating ? "Updating" : unavailable ? "Unavailable" : capability.configurable ? "Available" : "Read only"}
        </small>
        <input
          id={controlId}
          aria-label={label}
          type="checkbox"
          checked={capability.supported && capability.enabled}
          aria-busy={updating}
          disabled={!capability.supported || !capability.configurable || updating}
          onChange={(event) => onChange(name, event.target.checked)}
        />
      </span>
    </label>
  );
}

interface WorkspaceDialogsProps {
  activeSavedSearchId: string | null;
  activeTab: ResultTab;
  appName: string;
  currentTimeMs: number;
  dataMetricLabel: string;
  displayedExportRows: number;
  elapsed: string;
  exportFieldLabels: Record<string, string>;
  exportFieldOptions: string[];
  exportFields: string[];
  exportState: ExportDialogState;
  history: DemoHistoryEntry[];
  historyHasMore: boolean;
  historyLibraryStatus: "loading" | "available" | "unavailable";
  historyLoadingMore: boolean;
  historyClearState: DialogActionState;
  historyDeleteState: TargetedDialogActionState;
  historyFilter: string;
  jobCancelState: DialogActionState;
  jobInspectorNotices: string[] | null;
  isRunning: boolean;
  modal: ModalName | null;
  phase: JobPhase;
  resultCountLabel: string;
  resultCountPrefix: string;
  saveDescription: string;
  saveDialogReturnFocus: HTMLElement | null;
  saveName: string;
  saveState: DialogActionState;
  savedSearchFilter: string;
  savedSearchDeleteState: TargetedDialogActionState;
  savedSearchDuplicateState: TargetedDialogActionState;
  savedSearchDeleteTarget: DemoSavedSearch | null;
  savedSearchRenameName: string;
  savedSearchRenameState: TargetedDialogActionState;
  savedSearchRenameTarget: DemoSavedSearch | null;
  savedSearchHasMore: boolean;
  savedSearchLibraryStatus: "loading" | "available" | "unavailable";
  savedSearchLoadingMore: boolean;
  savedSearches: DemoSavedSearch[];
  resolvedTimeRangeLabel: string | null;
  scannedBytes: string;
  scannedRows: number | null;
  scannedRowsApproximate: boolean;
  searchId: string;
  searchMode: SearchMode | "Server controlled";
  searchSettingsCapabilities: SearchSettingsCapabilities;
  submittedQuery: string;
  submittedTimeRange: TimeRange;
  timeRange: TimeRange;
  visibleEventCount: number;
  onCancelSearch: () => void;
  onCancelExport: () => void;
  onClearHistory: () => void;
  onDeleteHistoryEntry: (id: string) => void;
  onDeleteSavedSearch: (id: string) => void;
  onDuplicateSavedSearch: (id: string) => void;
  onRenameSavedSearch: () => void;
  onRequestRenameSavedSearch: (id: string) => void;
  onRequestDeleteSavedSearch: (id: string) => void;
  onDownloadExport: (artifact: ExportArtifactDetails) => void;
  onExportFieldToggle: (field: string) => void;
  onExportFormatChange: (format: "csv" | "jsonl") => void;
  onHistoryEntryOpen: (entry: DemoHistoryEntry, runImmediately: boolean) => void;
  onHistoryEntrySave: (entry: DemoHistoryEntry) => void;
  onHistoryFilterChange: (filter: string) => void;
  onLoadMoreHistory: () => void;
  onLoadMoreSavedSearches: () => void;
  onModalChange: (modal: ModalName | null) => void;
  onOpenSavedSearch: (saved: DemoSavedSearch) => void;
  onPrepareExport: () => void;
  onResetExport: () => void;
  onSaveDescriptionChange: (description: string) => void;
  onSaveNameChange: (name: string) => void;
  onSaveSearch: () => void;
  onSavedSearchFilterChange: (filter: string) => void;
  onSavedSearchRenameNameChange: (name: string) => void;
  onSearchCapabilityChange: (capability: SearchCapabilityName, enabled: boolean) => void;
}

export function WorkspaceDialogs({
  activeSavedSearchId,
  activeTab,
  appName,
  currentTimeMs,
  dataMetricLabel,
  displayedExportRows,
  elapsed,
  exportFieldLabels,
  exportFieldOptions,
  exportFields,
  exportState,
  history,
  historyHasMore,
  historyLibraryStatus,
  historyLoadingMore,
  historyClearState,
  historyDeleteState,
  historyFilter,
  jobCancelState,
  jobInspectorNotices,
  isRunning,
  modal,
  phase,
  resultCountLabel,
  resultCountPrefix,
  saveDescription,
  saveDialogReturnFocus,
  saveName,
  saveState,
  savedSearchFilter,
  savedSearchDeleteState,
  savedSearchDuplicateState,
  savedSearchDeleteTarget,
  savedSearchRenameName,
  savedSearchRenameState,
  savedSearchRenameTarget,
  savedSearchHasMore,
  savedSearchLibraryStatus,
  savedSearchLoadingMore,
  savedSearches,
  resolvedTimeRangeLabel,
  scannedBytes,
  scannedRows,
  scannedRowsApproximate,
  searchId,
  searchMode,
  searchSettingsCapabilities,
  submittedQuery,
  submittedTimeRange,
  timeRange,
  visibleEventCount,
  onCancelSearch,
  onCancelExport,
  onClearHistory,
  onDeleteHistoryEntry,
  onDeleteSavedSearch,
  onDuplicateSavedSearch,
  onRenameSavedSearch,
  onRequestRenameSavedSearch,
  onRequestDeleteSavedSearch,
  onDownloadExport,
  onExportFieldToggle,
  onExportFormatChange,
  onHistoryEntryOpen,
  onHistoryEntrySave,
  onHistoryFilterChange,
  onLoadMoreHistory,
  onLoadMoreSavedSearches,
  onModalChange,
  onOpenSavedSearch,
  onPrepareExport,
  onResetExport,
  onSaveDescriptionChange,
  onSaveNameChange,
  onSaveSearch,
  onSavedSearchFilterChange,
  onSavedSearchRenameNameChange,
  onSearchCapabilityChange,
}: WorkspaceDialogsProps) {
  if (modal === "save") {
    const saving = saveState.status === "pending";
    return (
      <Modal
        title={activeSavedSearchId === null ? "Save search" : "Save search as"}
        subtitle="Preserve the SPL and selected time range for reuse."
        onClose={() => {
          if (!saving) onModalChange(null);
        }}
        returnFocus={saveDialogReturnFocus}
        footer={<><button className="button secondary" type="button" disabled={saving} onClick={() => onModalChange(null)}>Cancel</button><button className="button primary" type="button" aria-busy={saving} disabled={saving || saveName.trim().length === 0} onClick={onSaveSearch}>{saving ? "Saving…" : "Save"}</button></>}
      >
        <div className="form-stack" data-testid="save-search-dialog" aria-busy={saving}>
          {saveState.status === "error" ? <p className={styles.actionError} role="alert">{saveState.error}</p> : null}
          <label><span>Name</span><input value={saveName} disabled={saving} onChange={(event) => onSaveNameChange(event.target.value)} /></label>
          <label><span>Description <small>optional</small></span><textarea value={saveDescription} disabled={saving} onChange={(event) => onSaveDescriptionChange(event.target.value)} rows={3} /></label>
          <div className="form-summary"><span>App</span><strong>{appName}</strong><span>Time range</span><strong>{timeRange.label}</strong><span>Result view</span><strong>{activeTab[0].toUpperCase() + activeTab.slice(1)}</strong></div>
        </div>
      </Modal>
    );
  }

  if (modal === "open") {
    const filtered = savedSearches.filter((item) => `${item.name} ${item.description} ${item.query}`.toLowerCase().includes(savedSearchFilter.toLowerCase()));
    const deletingSavedSearchId = savedSearchDeleteState.status === "pending"
      ? savedSearchDeleteState.targetId
      : null;
    const duplicatingSavedSearchId = savedSearchDuplicateState.status === "pending"
      ? savedSearchDuplicateState.targetId
      : null;
    const savedSearchActionPending = deletingSavedSearchId !== null
      || duplicatingSavedSearchId !== null;
    return (
      <Modal title="Open a saved search" subtitle="Searches currently available in this app." wide onClose={() => onModalChange(null)}>
        <div className="library-toolbar"><label className="filter-input"><span aria-hidden="true">⌕</span><input aria-label="Filter saved searches" placeholder="Filter saved searches" disabled={savedSearchLibraryStatus !== "available"} value={savedSearchFilter} onChange={(event) => onSavedSearchFilterChange(event.target.value)} /></label></div>
        {savedSearchDeleteState.status === "error" && savedSearchDeleteState.error
          ? <p className={styles.actionError} role="alert">{savedSearchDeleteState.error}</p>
          : savedSearchDuplicateState.status === "error" && savedSearchDuplicateState.error
            ? <p className={styles.actionError} role="alert">{savedSearchDuplicateState.error}</p>
          : null}
        <div className="saved-list" data-testid="saved-search-list">
          {savedSearchLibraryStatus === "loading" ? <output className="empty-state"><strong>Loading saved searches…</strong><span>Reading the connected server library.</span></output> : null}
          {savedSearchLibraryStatus === "unavailable" ? <div className="empty-state" role="alert"><strong>Saved searches unavailable</strong><span>The connected server did not expose this capability or the request failed.</span></div> : null}
          {filtered.map((saved) => {
            const deletingThisSearch = deletingSavedSearchId === saved.id;
            const duplicatingThisSearch = duplicatingSavedSearchId === saved.id;
            return (
              <article className="saved-row" aria-busy={deletingThisSearch || duplicatingThisSearch} key={saved.id}>
                <button className="saved-row-main" type="button" disabled={savedSearchActionPending} onClick={() => onOpenSavedSearch(saved)}><span className="saved-icon" aria-hidden="true">⌕</span><span><strong>{saved.name}</strong><small>{saved.description}</small><code>{saved.query.replaceAll("\n", " ")}</code></span></button>
                <div className="saved-row-meta">
                  <span>{saved.updatedAt}</span>
                  <button
                    className="icon-button"
                    aria-label={duplicatingThisSearch ? `Duplicating ${saved.name}` : `Duplicate ${saved.name}`}
                    type="button"
                    disabled={savedSearchActionPending}
                    onClick={() => onDuplicateSavedSearch(saved.id)}
                  >
                    {duplicatingThisSearch ? "…" : "⧉"}
                  </button>
                  <button
                    className="icon-button"
                    aria-label={`Rename ${saved.name}`}
                    type="button"
                    disabled={savedSearchActionPending}
                    onClick={() => onRequestRenameSavedSearch(saved.id)}
                  >
                    ✎
                  </button>
                  <button
                    className="icon-button"
                    aria-label={deletingThisSearch ? `Deleting ${saved.name}` : `Delete ${saved.name}`}
                    type="button"
                    disabled={savedSearchActionPending}
                    onClick={() => onRequestDeleteSavedSearch(saved.id)}
                  >
                    {deletingThisSearch ? "…" : "×"}
                  </button>
                </div>
              </article>
            );
          })}
          {savedSearchLibraryStatus === "available" && filtered.length === 0 ? <div className="empty-state"><strong>{savedSearches.length === 0 ? "No saved searches yet" : "No loaded saved searches match"}</strong><span>{savedSearches.length === 0 ? "Save a search to make it available here." : savedSearchHasMore ? "Load more results or try another name or SPL term." : "Try another name or SPL term."}</span></div> : null}
          {savedSearchLibraryStatus === "available" && savedSearchHasMore ? (
            <div className={styles.loadMoreRow}>
              <button
                className="button secondary compact"
                type="button"
                aria-busy={savedSearchLoadingMore}
                disabled={savedSearchLoadingMore || savedSearchActionPending}
                onClick={onLoadMoreSavedSearches}
              >
                {savedSearchLoadingMore ? "Loading more…" : "Load more saved searches"}
              </button>
            </div>
          ) : null}
        </div>
      </Modal>
    );
  }

  if (modal === "rename-saved-search" && savedSearchRenameTarget !== null) {
    const renaming = savedSearchRenameState.status === "pending"
      && savedSearchRenameState.targetId === savedSearchRenameTarget.id;
    return (
      <Modal
        title="Rename saved search"
        subtitle={`Choose a new name for “${savedSearchRenameTarget.name}”. The SPL and sharing settings will not change.`}
        initialFocus="#rename-saved-search-name"
        onClose={() => {
          if (!renaming) onModalChange("open");
        }}
        footer={(
          <>
            <button className="button secondary" type="button" disabled={renaming} onClick={() => onModalChange("open")}>Cancel</button>
            <button
              className="button primary"
              type="submit"
              form="rename-saved-search-form"
              aria-busy={renaming}
              disabled={renaming
                || savedSearchRenameName.trim().length === 0
                || savedSearchRenameName.trim() === savedSearchRenameTarget.name}
            >
              {renaming ? "Renaming…" : "Rename"}
            </button>
          </>
        )}
      >
        <form
          className="form-stack"
          id="rename-saved-search-form"
          aria-busy={renaming}
          onSubmit={(event) => {
            event.preventDefault();
            if (
              !renaming
              && savedSearchRenameName.trim().length > 0
              && savedSearchRenameName.trim() !== savedSearchRenameTarget.name
            ) onRenameSavedSearch();
          }}
        >
          {savedSearchRenameState.status === "error" && savedSearchRenameState.error
            ? <p className={styles.actionError} role="alert">{savedSearchRenameState.error}</p>
            : null}
          <label>
            <span>Name</span>
            <input
              id="rename-saved-search-name"
              value={savedSearchRenameName}
              disabled={renaming}
              onChange={(event) => onSavedSearchRenameNameChange(event.target.value)}
              onKeyDown={(event) => {
                if (
                  event.key === "Enter"
                  && !renaming
                  && savedSearchRenameName.trim().length > 0
                  && savedSearchRenameName.trim() !== savedSearchRenameTarget.name
                ) {
                  event.preventDefault();
                  onRenameSavedSearch();
                }
              }}
            />
          </label>
        </form>
      </Modal>
    );
  }

  if (modal === "delete-saved-search" && savedSearchDeleteTarget !== null) {
    const deleting = savedSearchDeleteState.status === "pending"
      && savedSearchDeleteState.targetId === savedSearchDeleteTarget.id;
    return (
      <Modal
        title="Delete saved search?"
        subtitle={`“${savedSearchDeleteTarget.name}” will be permanently removed from ${appName}.`}
        onClose={() => {
          if (!deleting) onModalChange("open");
        }}
        footer={(
          <>
            <button className="button secondary" type="button" disabled={deleting} onClick={() => onModalChange("open")}>Keep saved search</button>
            <button className="button danger" type="button" aria-busy={deleting} disabled={deleting} onClick={() => onDeleteSavedSearch(savedSearchDeleteTarget.id)}>
              {deleting ? "Deleting…" : "Delete saved search"}
            </button>
          </>
        )}
      >
        <p>This cannot be undone. Existing search history is not removed.</p>
        {savedSearchDeleteState.status === "error" && savedSearchDeleteState.error
          ? <p className={styles.actionError} role="alert">{savedSearchDeleteState.error}</p>
          : null}
      </Modal>
    );
  }

  if (modal === "history") {
    const filtered = history.filter((item) => item.query.toLowerCase().includes(historyFilter.toLowerCase()));
    const clearingHistory = historyClearState.status === "pending";
    const deletingHistoryEntryId = historyDeleteState.status === "pending"
      ? historyDeleteState.targetId
      : null;
    const deletingHistory = deletingHistoryEntryId !== null;
    return (
      <Modal title="Search history" subtitle="Recent completed, canceled, and failed searches." wide onClose={() => onModalChange(null)}>
        <div className="library-toolbar"><label className="filter-input"><span aria-hidden="true">⌕</span><input aria-label="Filter search history" placeholder="Filter by SPL" disabled={historyLibraryStatus !== "available"} value={historyFilter} onChange={(event) => onHistoryFilterChange(event.target.value)} /></label><button className="button secondary compact" type="button" disabled={historyLibraryStatus !== "available" || history.length === 0 || clearingHistory || deletingHistory || historyLoadingMore} onClick={() => onModalChange("clear-history")}>Clear history</button></div>
        {historyClearState.status === "error" && historyClearState.error
          ? <p className={styles.actionError} role="alert">{historyClearState.error}</p>
          : historyDeleteState.status === "error" && historyDeleteState.error
            ? <p className={styles.actionError} role="alert">{historyDeleteState.error}</p>
            : null}
        <div className={styles.historyTableScroll} data-testid="history-list">
          <table className={styles.historyTable}>
            <thead>
              <tr><th scope="col">Search</th><th scope="col">Status</th><th scope="col">Results</th><th scope="col">Duration</th><th scope="col">Ran</th><th scope="col">Actions</th></tr>
            </thead>
            <tbody>
              {historyLibraryStatus === "loading" ? <tr><td aria-label="Search history loading" colSpan={6}><output className="empty-state"><strong>Loading search history…</strong><span>Reading terminal jobs from the connected server.</span></output></td></tr> : null}
              {historyLibraryStatus === "unavailable" ? <tr><td aria-label="Search history unavailable" colSpan={6}><div className="empty-state" role="alert"><strong>Search history unavailable</strong><span>The connected server did not expose this capability or the request failed.</span></div></td></tr> : null}
              {filtered.map((entry) => {
                const deletingThisEntry = deletingHistoryEntryId === entry.id;
                return (
                  <tr aria-busy={deletingThisEntry} key={entry.id}>
                    <td>
                      <code title={entry.query}>{entry.query}</code>
                      {entry.sourceLabel || entry.appId || entry.resolvedTimeRange ? (
                        <small className={styles.historyMeta} title={entry.resolvedTimeRange}>
                          {[entry.sourceLabel, entry.appId, entry.resolvedTimeRange].filter(Boolean).join(" · ")}
                        </small>
                      ) : null}
                    </td>
                    <td><span className={`history-state history-${entry.state.toLowerCase()}`}>{entry.state}</span></td>
                    <td className={styles.numericCell}>{formatHistoryResultCount(entry)}</td>
                    <td>{entry.duration}</td>
                    <td>{entry.ranAt}</td>
                    <td>
                      <div className="row-actions">
                        <button type="button" disabled={clearingHistory || deletingThisEntry} onClick={() => onHistoryEntryOpen(entry, false)}>Open</button>
                        <button type="button" disabled={clearingHistory || deletingThisEntry} onClick={() => onHistoryEntryOpen(entry, true)}>Run again</button>
                        <button type="button" disabled={clearingHistory || deletingThisEntry} onClick={() => onHistoryEntrySave(entry)}>Save</button>
                        <button aria-label={deletingThisEntry ? "Deleting history entry" : "Delete history entry"} type="button" disabled={clearingHistory || deletingHistory} onClick={() => onDeleteHistoryEntry(entry.id)}>{deletingThisEntry ? "…" : "Delete"}</button>
                      </div>
                    </td>
                  </tr>
                );
              })}
              {historyLibraryStatus === "available" && filtered.length === 0 ? (
                <tr>
                  <td aria-label="No history entries" colSpan={6}>
                    <div className="empty-state"><strong>No loaded history entries match</strong><span>{history.length === 0 ? "Run a search to create history." : historyHasMore ? "Load more results or try another SPL term." : "Try another SPL term."}</span></div>
                  </td>
                </tr>
              ) : null}
            </tbody>
          </table>
        </div>
        {historyLibraryStatus === "available" && historyHasMore ? (
          <div className={styles.loadMoreRow}>
            <button
              className="button secondary compact"
              type="button"
              aria-busy={historyLoadingMore}
              disabled={historyLoadingMore || clearingHistory || deletingHistory}
              onClick={onLoadMoreHistory}
            >
              {historyLoadingMore ? "Loading more…" : "Load more search history"}
            </button>
          </div>
        ) : null}
      </Modal>
    );
  }

  if (modal === "clear-history") {
    const clearingHistory = historyClearState.status === "pending";
    const clearBlocked = clearingHistory || historyLoadingMore;
    return (
      <Modal
        title="Clear search history?"
        subtitle={`This removes all search history for ${appName}. ${NUMBER_FORMAT.format(history.length)} ${history.length === 1 ? "entry is" : "entries are"} currently loaded in this view.`}
        onClose={() => {
          if (!clearingHistory) onModalChange("history");
        }}
        footer={(
          <>
            <button className="button secondary" type="button" disabled={clearingHistory} onClick={() => onModalChange("history")}>Keep history</button>
            <button className="button danger" type="button" aria-busy={clearingHistory} disabled={clearBlocked} onClick={onClearHistory}>
              {clearingHistory ? "Clearing…" : historyLoadingMore ? "Wait for history load" : "Clear app history"}
            </button>
          </>
        )}
      >
        <p>This cannot be undone. Searches belonging to other apps are not included.</p>
        {historyClearState.status === "error" && historyClearState.error
          ? <p className={styles.actionError} role="alert">{historyClearState.error}</p>
          : null}
      </Modal>
    );
  }

  if (modal === "export") {
    const exportPending = exportState.status === "pending";
    const exportCancelState = exportState.status === "pending" ? exportState.cancel : null;
    const exportCancelPending = exportCancelState?.status === "pending";
    const exportReady = exportState.status === "ready";
    const exportFailed = exportState.status === "error";
    const downloadState = exportState.status === "ready" ? exportState.download : null;
    const downloadPending = downloadState?.status === "pending";
    const artifact = exportState.status === "ready" ? exportState.artifact : null;
    const expiry = artifact?.expiresAt === null || artifact?.expiresAt === undefined
      ? null
      : formatExpiry(artifact.expiresAt);
    const artifactExpired = artifact !== null
      && artifact.expiresAt !== null
      && expiry !== null
      && artifact.expiresAt.valueOf() <= currentTimeMs;
    const artifactMetadataValid = artifact !== null
      && artifact.downloadHandle.trim().length > 0
      && artifact.fileName.trim().length > 0
      && formatNonNegativeIntegerQuantity(artifact.rowCount) !== "Unavailable"
      && formatDecimalBytes(artifact.sizeBytes) !== "Unavailable"
      && (artifact.expiresAt === null || expiry !== null);
    const rawPercentComplete = exportState.status === "pending" ? exportState.percentComplete : null;
    const pendingRowsWritten = exportState.status === "pending" ? exportState.rowsWritten : null;
    const pendingBytesWritten = exportState.status === "pending" ? exportState.bytesWritten : null;
    const exportError = exportState.status === "error" ? exportState.error : null;
    const percentComplete = rawPercentComplete === null
      || rawPercentComplete === undefined
      || !Number.isFinite(rawPercentComplete)
      ? null
      : Math.max(0, Math.min(100, rawPercentComplete));
    const exportAvailable = exportState.status === "configure" ? exportState.available : true;
    const exportRetryable = exportState.status === "error" ? exportState.retryable : true;
    const canChooseFormat = !exportPending && !exportReady;
    const canConfigure = exportAvailable && canChooseFormat;
    const canCreate = canConfigure
      && exportFields.length > 0
      && exportRetryable;
    const maximumRows = exportState.maximumRows === null || exportState.maximumRows === undefined
      ? "Not advertised"
      : formatNonNegativeIntegerQuantity(exportState.maximumRows);
    const maximumBytes = exportState.maximumBytes === null || exportState.maximumBytes === undefined
      ? null
      : formatDecimalBytes(exportState.maximumBytes);
    const closeExport = () => {
      if (exportState.status === "configure" || exportState.status === "error") onResetExport();
      onModalChange(null);
    };

    return (
      <Modal
        title={`Export ${exportState.sourceTab}`}
        subtitle={exportState.description}
        onClose={closeExport}
        footer={exportReady
          ? <>
              <output className="export-ready-note">
                {!artifactMetadataValid
                  ? "Export artifact metadata is invalid"
                  : artifactExpired
                    ? <>Export artifact expired · {expiry}</>
                    : <>✓ Artifact ready · {formatNonNegativeIntegerQuantity(artifact.rowCount)} rows · {formatDecimalBytes(artifact.sizeBytes)}{expiry === null ? null : <> · expires {expiry}</>}</>}
              </output>
              <button className="button secondary" type="button" disabled={downloadPending} onClick={onResetExport}>New export</button>
              <button className="button primary" type="button" aria-busy={downloadPending} disabled={!artifactMetadataValid || artifactExpired || downloadPending} onClick={() => { if (artifact !== null) onDownloadExport(artifact); }}>{downloadPending ? "Downloading…" : artifact?.fileName ? `Download ${artifact.fileName}` : `Download .${exportState.format}`}</button>
            </>
          : exportPending
            ? <>
                <output className={styles.pendingFooter}>Preparation will continue if this dialog is closed.</output>
                <button
                  className="button secondary"
                  type="button"
                  aria-busy={exportCancelPending}
                  disabled={exportCancelPending}
                  onClick={onCancelExport}
                >
                  {exportCancelPending ? "Canceling…" : exportCancelState?.status === "error" ? "Retry cancellation" : "Cancel export"}
                </button>
                <button className="button secondary" type="button" disabled={exportCancelPending} onClick={closeExport}>Continue in background</button>
              </>
            : <><button className="button secondary" type="button" onClick={closeExport}>Cancel</button><button className="button primary" type="button" disabled={!canCreate} onClick={onPrepareExport}>{exportFailed ? exportRetryable ? "Retry export" : "Export unavailable" : "Create export"}</button></>}
      >
        <div className="form-stack" data-testid="export-dialog" aria-busy={exportPending || downloadPending || exportCancelPending}>
          {exportState.status === "configure" && !exportState.available ? (
            <aside className={styles.capabilityNotice}>
              <strong>Export unavailable</strong>
              <span>{exportState.unavailableReason || "This data source did not advertise an export capability for the selected format."}</span>
            </aside>
          ) : null}
          {exportError === null ? null : <p className={styles.actionError} role="alert">{exportError}</p>}
          {exportCancelState?.status === "error" ? <p className={styles.actionError} role="alert">{exportCancelState.error}</p> : null}
          {downloadState?.status === "error"
            ? <p className={styles.actionError} role="alert">{downloadState.error}</p>
            : null}
          <fieldset className="segmented-fieldset" disabled={!canChooseFormat}>
            <legend>Format</legend>
            <label className={exportState.format === "csv" ? "selected" : ""} htmlFor="export-format-csv"><input id="export-format-csv" aria-label="Export as CSV" type="radio" name="format" checked={exportState.format === "csv"} onChange={() => onExportFormatChange("csv")} /><span><strong>CSV</strong><small>Spreadsheet-compatible table</small></span></label>
            <label className={exportState.format === "jsonl" ? "selected" : ""} htmlFor="export-format-jsonl"><input id="export-format-jsonl" aria-label="Export as JSON Lines" type="radio" name="format" checked={exportState.format === "jsonl"} onChange={() => onExportFormatChange("jsonl")} /><span><strong>JSON Lines</strong><small>Typed record per line</small></span></label>
          </fieldset>
          <fieldset className="export-fields" disabled={!canConfigure}>
            <legend>Columns <small>{exportFields.length} selected</small></legend>
            {exportFieldOptions.map((field, index) => {
              const fieldLabel = exportFieldLabels[field] ?? field;
              const inputId = `export-field-${index}`;
              return <label key={field} htmlFor={inputId}><input id={inputId} aria-label={`Include ${fieldLabel} in export`} type="checkbox" checked={exportFields.includes(field)} onChange={() => onExportFieldToggle(field)} /><code>{fieldLabel}</code></label>;
            })}
          </fieldset>
          <div className="export-limit">
            <span>Maximum rows</span>
            <strong>{maximumRows}</strong>
            <small>
              {NUMBER_FORMAT.format(displayedExportRows)} displayed {exportState.sourceTab === "events" ? "events" : "rows"}
              {maximumBytes === null ? null : <> · {maximumBytes} byte limit</>}
            </small>
          </div>
          {exportPending ? (
            <div className={styles.exportProgress}>
              <progress aria-label="Server export progress" max={100} value={percentComplete ?? undefined} />
              <span>
                {percentComplete === null ? "Materializing results…" : `${Math.round(percentComplete)}% complete`}
                {pendingRowsWritten === null || pendingRowsWritten === undefined ? null : <> · {formatNonNegativeIntegerQuantity(pendingRowsWritten)} rows</>}
                {pendingBytesWritten === null || pendingBytesWritten === undefined ? null : <> · {formatDecimalBytes(pendingBytesWritten)}</>}
              </span>
            </div>
          ) : null}
        </div>
      </Modal>
    );
  }

  if (modal === "settings") {
    return (
      <Modal
        title="Search capabilities"
        subtitle={searchSettingsCapabilities.context}
        onClose={() => onModalChange(null)}
        footer={<button className="button primary" type="button" onClick={() => onModalChange(null)}>Done</button>}
      >
        <div className="settings-list" data-testid="settings-dialog">
          <CapabilitySetting
            capability={searchSettingsCapabilities.fieldDiscovery}
            label="Interesting field discovery"
            name="fieldDiscovery"
            onChange={onSearchCapabilityChange}
          />
          <CapabilitySetting
            capability={searchSettingsCapabilities.liveResultPreviews}
            label="Live result previews"
            name="liveResultPreviews"
            onChange={onSearchCapabilityChange}
          />
          <CapabilitySetting
            capability={searchSettingsCapabilities.eventSampling}
            label="Event sampling"
            name="eventSampling"
            onChange={onSearchCapabilityChange}
          />
        </div>
      </Modal>
    );
  }

  if (modal === "jobs") {
    const cancelPending = jobCancelState.status === "pending";
    return (
      <Modal
        title="Activity & jobs"
        subtitle={searchMode === "Server controlled"
          ? "Recent terminal jobs persisted by the connected server."
          : "Search jobs retained for this session."}
        wide
        onClose={() => onModalChange(null)}
      >
        <div className="jobs-list" data-testid="jobs-dialog">
          {jobCancelState.status === "error" ? <p className={styles.actionError} role="alert">{jobCancelState.error}</p> : null}
          <article className="job-card active-job-card" aria-busy={cancelPending}><div className={`job-card-state ${stateClass(phase)}`}><span />{phaseLabel(phase)}</div><code>{submittedQuery.replaceAll("\n", " ")}</code><div className="job-card-stats"><span>{resultCountPrefix}{NUMBER_FORMAT.format(visibleEventCount)} {resultCountLabel}</span><span>{scannedBytes} {dataMetricLabel.toLowerCase()}</span><span>{elapsed}</span></div>{isRunning ? <button className="button danger compact" type="button" aria-busy={cancelPending} disabled={cancelPending} onClick={onCancelSearch}>{cancelPending ? "Canceling…" : "Cancel"}</button> : null}</article>
          {history.slice(0, 4).map((entry) => <article className="job-card" key={entry.id}><div className={`job-card-state history-${entry.state.toLowerCase()}`}><span />{entry.state}</div><code>{entry.query}</code><div className="job-card-stats"><span>{formatHistoryResultCount(entry)} results</span><span>{entry.duration}</span><span>{entry.ranAt}</span></div></article>)}
        </div>
      </Modal>
    );
  }

  if (modal === "inspect") {
    return (
      <Modal title="Search job inspector" subtitle="Dispatch and execution details for the displayed result." wide onClose={() => onModalChange(null)} footer={<button className="button primary" type="button" onClick={() => onModalChange(null)}>Done</button>}>
        <div className="job-inspector" data-testid="job-inspector">
          <section><span>Status</span><strong className={`inspector-state ${stateClass(phase)}`}><i />{phaseLabel(phase)}</strong></section>
          <section><span>Search ID</span><code>{searchId}</code></section>
          <section><span>Search mode</span><strong>{searchMode}</strong></section>
          <section><span>Time range</span><strong>{resolvedTimeRangeLabel ?? submittedTimeRange.label}</strong></section>
          <section><span>Scanned</span><strong>{scannedRows === null ? "Unavailable" : `${scannedRowsApproximate ? "≈ " : ""}${NUMBER_FORMAT.format(scannedRows)} rows`}</strong></section>
          <section><span>{dataMetricLabel}</span><strong>{scannedBytes}</strong></section>
          <section><span>Elapsed</span><strong>{elapsed}</strong></section>
          <div className="inspector-query"><span>Dispatched SPL</span><code>{submittedQuery}</code></div>
          {jobInspectorNotices === null
            ? <p>Execution warning and sequence-gap telemetry was not supplied for this job.</p>
            : jobInspectorNotices.length === 0
              ? <p>No execution warnings or sequence gaps were reported by the data source.</p>
              : <div className={styles.inspectorNotices}><strong>Execution notices</strong><ul>{jobInspectorNotices.map((notice) => <li key={notice}>{notice}</li>)}</ul></div>}
        </div>
      </Modal>
    );
  }

  return null;
}
