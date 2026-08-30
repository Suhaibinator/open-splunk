import { useEffect, useRef } from "react";

import { Modal } from "../../_components/modal";
import type { ServerJobSettings } from "@/lib/search/server-job-settings";

import type { DialogActionState } from "../model";
import type { SearchSharingDialog } from "../use-search-sharing";

interface SearchSharingDialogProps {
  dialog: SearchSharingDialog;
  settings: ServerJobSettings | null;
  loadState: DialogActionState;
  mutationState: DialogActionState;
  manualCopyValue: string | null;
  canCopyJob: boolean;
  canCopySavedSearch: boolean;
  onClose: () => void;
  onCopyJobLink: () => void;
  onCopyQueryLink: () => void;
  onCopySavedSearchLink: () => void;
  onMakeShared: () => void;
  onMakePrivate: () => void;
}

const RETENTION_LABELS: Record<ServerJobSettings["retentionClass"], string> = {
  manual: "Manual job",
  shared: "Shared job",
  "scheduled-report": "Scheduled report",
  "scheduled-alert": "Scheduled alert",
  "triggered-webhook": "Triggered webhook",
};

const RESULT_LABELS: Record<ServerJobSettings["retainedResultState"], string> = {
  pending: "Pending",
  available: "Available",
  expired: "Expired",
  missing: "Missing",
  corrupt: "Corrupt",
};

function lifetimeLabel(milliseconds: number): string {
  if (milliseconds === 7 * 24 * 60 * 60 * 1_000) return "7 days (sliding)";
  if (milliseconds === 10 * 60 * 1_000) return "10 minutes (sliding)";
  const minutes = milliseconds / 60_000;
  return `${Number.isInteger(minutes) ? minutes : minutes.toFixed(1)} minutes (sliding)`;
}

function actionError(state: DialogActionState): string | null {
  return state.status === "error" ? state.error : null;
}

export function SearchSharingDialog({
  dialog,
  settings,
  loadState,
  mutationState,
  manualCopyValue,
  canCopyJob,
  canCopySavedSearch,
  onClose,
  onCopyJobLink,
  onCopyQueryLink,
  onCopySavedSearchLink,
  onMakeShared,
  onMakePrivate,
}: SearchSharingDialogProps) {
  const manualCopyRef = useRef<HTMLInputElement>(null);
  const busy = loadState.status === "pending" || mutationState.status === "pending";
  const error = actionError(loadState) ?? actionError(mutationState);
  const jobLinkUnavailable = !canCopyJob
    ? "Exact-result sharing is unavailable. You can still copy a query or saved-search link."
    : settings?.retainedResultState === "expired"
      ? "These exact results expired. Run the search again to create a shareable result snapshot."
      : settings !== null && settings.retainedResultState !== "available"
        ? `These exact results are ${RESULT_LABELS[settings.retainedResultState].toLocaleLowerCase()} and cannot be shared.`
        : null;
  useEffect(() => {
    if (manualCopyValue === null) return;
    manualCopyRef.current?.focus();
    manualCopyRef.current?.select();
  }, [manualCopyValue]);
  if (dialog === "share") {
    return (
      <Modal
        title="Share search"
        subtitle="Choose whether recipients open exact results, rerun the query, or follow the saved definition."
        initialFocus="#search-sharing-copy-job:not(:disabled), #search-sharing-copy-query"
        dismissible={!busy}
        onClose={onClose}
      >
        <div className="search-sharing-options" aria-busy={busy}>
          {error === null ? null : <p className="search-sharing-error" role="alert">{error}</p>}
          <button id="search-sharing-copy-job" className="button button--secondary search-sharing-option" type="button" aria-describedby={jobLinkUnavailable === null ? undefined : "search-sharing-job-unavailable"} disabled={!canCopyJob || busy || settings?.retainedResultState !== "available"} onClick={onCopyJobLink}>
            <strong>Copy job link</strong>
            <span>Share these exact retained results. This changes visibility to Everyone and the sliding lifetime to seven days.</span>
          </button>
          {jobLinkUnavailable === null ? null : <p className="search-sharing-note" id="search-sharing-job-unavailable">{jobLinkUnavailable}</p>}
          <button id="search-sharing-copy-query" className="button button--secondary search-sharing-option" type="button" disabled={busy} onClick={onCopyQueryLink}>
            <strong>Copy query link</strong>
            <span>Share the SPL and time-range intent. Opening it creates a new job under the recipient&apos;s permissions.</span>
          </button>
          <button className="button button--secondary search-sharing-option" type="button" disabled={!canCopySavedSearch || busy} onClick={onCopySavedSearchLink}>
            <strong>Copy saved-search link</strong>
            <span>Open the latest persisted definition. Editing that saved search changes what this link opens.</span>
          </button>
          {manualCopyValue === null ? null : (
            <label className="search-sharing-manual-copy" htmlFor="search-sharing-manual-copy">
              <span>Copy this link manually</span>
              <input
                ref={manualCopyRef}
                id="search-sharing-manual-copy"
                readOnly
                value={manualCopyValue}
                onClick={(event) => event.currentTarget.select()}
                onFocus={(event) => event.currentTarget.select()}
              />
              <small>Clipboard access failed. The complete link is selected when you focus this field.</small>
            </label>
          )}
        </div>
      </Modal>
    );
  }
  return (
    <Modal
      title="Job settings"
      subtitle="Visibility and retention apply to this exact result snapshot."
      dismissible={!busy}
      onClose={onClose}
      footer={settings === null ? undefined : settings.visibility === "everyone"
        ? <button className="button button--secondary" type="button" disabled={busy} onClick={onMakePrivate}>Make private</button>
        : <button className="button button--secondary" type="button" disabled={busy || settings.retainedResultState !== "available"} onClick={onMakeShared}>Make shared · 7 days</button>}
    >
      {error === null ? null : <p className="search-sharing-error" role="alert">{error}</p>}
      {loadState.status === "pending" ? <output className="empty-state"><strong>Loading job settings…</strong></output> : settings === null ? <div className="empty-state"><strong>Job settings unavailable</strong><span>Run a durable backend search to manage an exact result snapshot.</span></div> : (
        <dl className="search-sharing-settings">
          <div><dt>Visibility</dt><dd>{settings.visibility === "everyone" ? "Everyone" : "Private"}</dd></div>
          <div><dt>Lifetime</dt><dd>{lifetimeLabel(settings.lifetimeMs)}</dd></div>
          <div><dt>Expires</dt><dd>{settings.expiresAt?.toLocaleString() ?? "Not set"}</dd></div>
          <div><dt>Last access</dt><dd>{settings.lastAccessedAt?.toLocaleString() ?? "Not accessed"}</dd></div>
          <div><dt>Provenance</dt><dd>{settings.provenance}</dd></div>
          <div><dt>Retention class</dt><dd>{RETENTION_LABELS[settings.retentionClass]}</dd></div>
          <div><dt>Retained result</dt><dd>{RESULT_LABELS[settings.retainedResultState]}</dd></div>
        </dl>
      )}
    </Modal>
  );
}
