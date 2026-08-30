"use client";

import { useEffect, useMemo, useRef, useState } from "react";
import Link from "next/link";

import { ScheduleValidationMode } from "@/gen/ts/open_splunk/schedule_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import type { SystemBootstrapModel } from "@/lib/api/system-bootstrap";
import { createErrorMessage } from "@/lib/error-message";
import { searchJobLaunchHref } from "@/lib/search/launch-url";
import {
  listServerScheduledReportRuns,
  runServerSavedSearch,
  scheduledReportRetainedResultPresentation,
  scheduledReportOutcomePresentation,
  setServerSavedSearchSchedule,
  validateServerSchedule,
  validateScheduledReportConfigurationFields,
  type ScheduledReportConfiguration,
  type ServerScheduledReportRun,
} from "@/lib/search/server-scheduled-reports";
import type { ServerSavedSearch } from "@/lib/search/server-objects";
import { formatMediumDateTime } from "../_components/date-format";
import { FieldNote, fieldControlProps } from "../_components/field-validation";
import { Modal } from "../_components/modal";
import { StatusLabel } from "../_components/status";

type ScheduledReportDialog = "schedule" | "history";
type ScheduledReportAction = "save" | "state" | "run" | "history";

interface ScheduledReportStatusProps {
  savedSearch: ServerSavedSearch;
}

export function ScheduledReportStatus({ savedSearch }: ScheduledReportStatusProps) {
  const schedule = savedSearch.schedule;
  const status = savedSearch.scheduleStatus;
  if (schedule === null) {
    return <div className="reports-schedule-cell"><StatusLabel tone="neutral">Manual</StatusLabel><small>No schedule</small></div>;
  }
  const presentation = scheduledReportOutcomePresentation(status?.lastOutcome ?? 0);
  const latestResultPresentation = status?.latestRetainedResultState === null || status?.latestRetainedResultState === undefined
    ? null
    : scheduledReportRetainedResultPresentation(status.latestRetainedResultState, status.latestResultExpiresAt);
  return (
    <div className="reports-schedule-cell">
      <StatusLabel tone={schedule.enabled ? "success" : "warning"}>{schedule.enabled ? "Scheduled" : "Paused"}</StatusLabel>
      <code>{schedule.cron} · {schedule.timezone}</code>
      <small>dispatch.ttl: {schedule.dispatchTtl || "2p"}</small>
      <small>
        {schedule.enabled
          ? `Next: ${formatMediumDateTime(status?.nextRunAt, "calculating")}`
          : "Future runs disabled"}
      </small>
      {status?.lastRunAt === null || status?.lastRunAt === undefined ? null : (
        <small>Last: {presentation.label}, {formatMediumDateTime(status.lastRunAt, "unknown")}</small>
      )}
      {status?.latestSearchJobId === null || status?.latestSearchJobId === undefined ? null : (
        <small>
          {latestResultPresentation === "pending" ? <span>Latest result pending</span> : null}
          {latestResultPresentation === "available" ? <Link href={searchJobLaunchHref(status.latestSearchJobId)}>Latest result</Link> : null}
          {latestResultPresentation === "expired" ? <span>Latest result expired</span> : null}
          {latestResultPresentation === "missing" ? <span>Latest result unavailable</span> : null}
          {latestResultPresentation === "corrupt" ? <span>Latest result corrupt</span> : null}
          {status.latestResultExpiresAt === null
            ? ""
            : ` · ${latestResultPresentation === "expired" ? "expired" : "expires"} ${formatMediumDateTime(status.latestResultExpiresAt, "unknown")}`}
        </small>
      )}
    </div>
  );
}

interface ScheduledReportActionsProps {
  autoOpenSchedule?: boolean;
  bootstrap: SystemBootstrapModel;
  client: OpenSplunkApiClient;
  disabled: boolean;
  onNotice: (message: string) => void;
  onScheduleOpened?: () => void;
  onRefresh: () => void;
  onUpdated: (savedSearch: ServerSavedSearch) => void;
  savedSearch: ServerSavedSearch;
}

const scheduledReportError = createErrorMessage("The scheduled-report request failed.");

function initialConfiguration(savedSearch: ServerSavedSearch): ScheduledReportConfiguration {
  return savedSearch.schedule ?? {
    enabled: false,
    cron: "0 * * * *",
    timezone: Intl.DateTimeFormat().resolvedOptions().timeZone || "UTC",
    dispatchTtl: "2p",
  };
}

export function ScheduledReportActions({
  autoOpenSchedule = false,
  bootstrap,
  client,
  disabled,
  onNotice,
  onScheduleOpened,
  onRefresh,
  onUpdated,
  savedSearch,
}: ScheduledReportActionsProps) {
  const [dialog, setDialog] = useState<ScheduledReportDialog | null>(null);
  const [configuration, setConfiguration] = useState(() => initialConfiguration(savedSearch));
  const [pending, setPending] = useState<ScheduledReportAction | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [serverValidationErrors, setServerValidationErrors] = useState<ReturnType<typeof validateScheduledReportConfigurationFields>>({});
  const [history, setHistory] = useState<ServerScheduledReportRun[]>([]);
  const autoOpenedRef = useRef(false);

  useEffect(() => {
    if (!autoOpenSchedule || autoOpenedRef.current) return;
    autoOpenedRef.current = true;
    setError(null);
    setDialog("schedule");
    onScheduleOpened?.();
  }, [autoOpenSchedule, onScheduleOpened]);

  useEffect(() => {
    if (dialog === null) setConfiguration(initialConfiguration(savedSearch));
  }, [dialog, savedSearch]);

  const validationErrors = useMemo(() => dialog === "schedule" ? {
    ...serverValidationErrors,
    ...validateScheduledReportConfigurationFields(configuration),
  } : {}, [configuration, dialog, serverValidationErrors]);
  const validationError = validationErrors.cron ?? validationErrors.timezone ?? validationErrors.dispatchTtl ?? null;

  function closeDialog() {
    if (pending !== null) return;
    setDialog(null);
    setError(null);
    setServerValidationErrors({});
  }

  async function authoritativeValidation(next: ScheduledReportConfiguration) {
    return validateServerSchedule(client, {
      mode: ScheduleValidationMode.SCHEDULE_VALIDATION_MODE_SCHEDULED_REPORT,
      cron: next.cron,
      timezone: next.timezone,
      dispatchTtl: next.dispatchTtl,
    });
  }

  async function saveSchedule() {
    if (validationError !== null || pending !== null) return;
    setPending("save");
    setError(null);
    setServerValidationErrors({});
    try {
      const authoritativeErrors = await authoritativeValidation(configuration);
      if (Object.keys(authoritativeErrors).length > 0) {
        setServerValidationErrors(authoritativeErrors);
        return;
      }
      const result = await setServerSavedSearchSchedule(client, bootstrap, savedSearch, configuration);
      if (result.status === "unavailable") throw new Error("Scheduled searches are unavailable on this server.");
      onUpdated(result.value);
      setDialog(null);
      onNotice(`${configuration.enabled ? "Scheduled" : "Saved paused schedule for"} “${savedSearch.name}”.`);
    } catch (reason) {
      setError(scheduledReportError(reason));
    } finally {
      setPending(null);
    }
  }

  async function toggleEnabled() {
    const schedule = savedSearch.schedule;
    if (schedule === null || pending !== null) return;
    setPending("state");
    setError(null);
    try {
      const authoritativeErrors = await authoritativeValidation({ ...schedule, enabled: !schedule.enabled });
      if (Object.keys(authoritativeErrors).length > 0) {
        throw new TypeError(Object.values(authoritativeErrors)[0]);
      }
      const result = await setServerSavedSearchSchedule(client, bootstrap, savedSearch, {
        ...schedule,
        enabled: !schedule.enabled,
      });
      if (result.status === "unavailable") throw new Error("Scheduled searches are unavailable on this server.");
      onUpdated(result.value);
      onNotice(`${result.value.schedule?.enabled ? "Enabled" : "Paused"} “${savedSearch.name}”.`);
    } catch (reason) {
      setError(scheduledReportError(reason));
    } finally {
      setPending(null);
    }
  }

  async function runNow() {
    if (pending !== null) return;
    setPending("run");
    setError(null);
    try {
      const result = await runServerSavedSearch(client, bootstrap, savedSearch.id);
      if (result.status === "unavailable") throw new Error("Scheduled searches are unavailable on this server.");
      onNotice(`Started “${savedSearch.name}” as job ${result.value.searchJobId}.`);
      onRefresh();
    } catch (reason) {
      setError(scheduledReportError(reason));
    } finally {
      setPending(null);
    }
  }

  async function openHistory() {
    if (pending !== null) return;
    setDialog("history");
    setHistory([]);
    setPending("history");
    setError(null);
    try {
      const result = await listServerScheduledReportRuns(client, bootstrap, savedSearch.id);
      if (result.status === "unavailable") throw new Error("Scheduled-search history is unavailable on this server.");
      setHistory(result.value.items);
    } catch (reason) {
      setError(scheduledReportError(reason));
    } finally {
      setPending(null);
    }
  }

  const controlsDisabled = disabled || pending !== null;
  return (
    <>
      <button type="button" aria-label={`${savedSearch.schedule === null ? "Schedule" : "Edit schedule for"} ${savedSearch.name}`} disabled={controlsDisabled} onClick={() => { setError(null); setServerValidationErrors({}); setDialog("schedule"); }}>
        {savedSearch.schedule === null ? "Schedule" : "Edit schedule"}
      </button>
      {savedSearch.schedule === null ? null : (
        <button type="button" aria-label={`${savedSearch.schedule.enabled ? "Pause" : "Enable"} ${savedSearch.name}`} disabled={controlsDisabled} aria-busy={pending === "state"} onClick={() => void toggleEnabled()}>
          {savedSearch.schedule.enabled ? "Pause" : "Enable"}
        </button>
      )}
      <button type="button" aria-label={`Run ${savedSearch.name} now`} disabled={controlsDisabled} aria-busy={pending === "run"} onClick={() => void runNow()}>Run now</button>
      <button type="button" aria-label={`View ${savedSearch.name} run history`} disabled={controlsDisabled} aria-busy={pending === "history"} onClick={() => void openHistory()}>History</button>
      {dialog === null && error !== null ? <span className="reports-action-error" role="alert">{error}</span> : null}

      {dialog === "schedule" ? (
        <Modal
          title={savedSearch.schedule === null ? "Schedule saved search" : "Edit saved-search schedule"}
          subtitle={`Run “${savedSearch.name}” on a durable Splunk-compatible schedule.`}
          initialFocus="#reports-schedule-cron"
          dismissible={pending === null}
          onClose={closeDialog}
          footer={(
            <>
              <button className="button button--secondary" type="button" disabled={pending !== null} onClick={closeDialog}>Cancel</button>
              <button className="button button--primary" type="submit" form="reports-schedule-form" disabled={pending !== null || validationError !== null} aria-busy={pending === "save"}>
                {pending === "save" ? "Saving…" : "Save schedule"}
              </button>
            </>
          )}
        >
          <form className="form-stack" id="reports-schedule-form" onSubmit={(event) => { event.preventDefault(); void saveSchedule(); }}>
            <label>
              <span>Cron schedule</span>
              <input id="reports-schedule-cron" value={configuration.cron} autoComplete="off" disabled={pending !== null} onChange={(event) => { setServerValidationErrors({}); setConfiguration((current) => ({ ...current, cron: event.target.value })); }} {...fieldControlProps("reports-schedule-cron", validationErrors.cron ?? null)} />
              <FieldNote fieldId="reports-schedule-cron" error={validationErrors.cron ?? null}>Five numeric fields: minute, hour, day of month, month, weekday.</FieldNote>
            </label>
            <label>
              <span>Timezone</span>
              <input id="reports-schedule-timezone" value={configuration.timezone} autoComplete="off" disabled={pending !== null} onChange={(event) => { setServerValidationErrors({}); setConfiguration((current) => ({ ...current, timezone: event.target.value })); }} {...fieldControlProps("reports-schedule-timezone", validationErrors.timezone ?? null)} />
              <FieldNote fieldId="reports-schedule-timezone" error={validationErrors.timezone ?? null}>Use an IANA name such as UTC or America/Los_Angeles.</FieldNote>
            </label>
            <label>
              <span>Result retention</span>
              <input id="reports-schedule-retention" value={configuration.dispatchTtl} autoComplete="off" disabled={pending !== null} onChange={(event) => { setServerValidationErrors({}); setConfiguration((current) => ({ ...current, dispatchTtl: event.target.value })); }} {...fieldControlProps("reports-schedule-retention", validationErrors.dispatchTtl ?? null)} />
              <FieldNote fieldId="reports-schedule-retention" error={validationErrors.dispatchTtl ?? null}>Positive seconds or Np; 2p retains results for twice the actual schedule period.</FieldNote>
            </label>
            <div className="settings-list">
              <label>
                <input aria-label="Enable scheduled report after saving" type="checkbox" checked={configuration.enabled} disabled={pending !== null} onChange={(event) => setConfiguration((current) => ({ ...current, enabled: event.target.checked }))} />
                <span><strong>Enable after saving</strong><small>Disabled schedules remain configured but do not claim future runs.</small></span>
              </label>
            </div>
            {error === null ? null : <p className="reports-action-error" role="alert">{error}</p>}
          </form>
        </Modal>
      ) : null}

      {dialog === "history" ? (
        <Modal title={`${savedSearch.name} run history`} subtitle="Newest scheduled occurrences and retained result links." wide dismissible={pending === null} onClose={closeDialog}>
          {pending === "history" ? <output className="reports-action-hint">Loading run history…</output> : null}
          {error === null ? null : <p className="reports-action-error" role="alert">{error}</p>}
          {pending !== "history" && error === null && history.length === 0 ? <p className="reports-action-hint">This saved search has not recorded a scheduled run.</p> : null}
          {history.length === 0 ? null : (
            <div className="table-wrap">
              <table className="table table--compact table--cards">
                <thead><tr><th scope="col">Scheduled</th><th scope="col">Outcome</th><th scope="col">Skipped</th><th scope="col">Result</th></tr></thead>
                <tbody>{history.map((run) => {
                  const presentation = scheduledReportOutcomePresentation(run.outcome);
                  const resultPresentation = run.retainedResultState === null
                    ? null
                    : scheduledReportRetainedResultPresentation(run.retainedResultState, run.searchJobExpiresAt);
                  const resultExpired = resultPresentation === "expired";
                  const resultLabel = run.searchJobId === null
                    ? "Unavailable"
                    : resultExpired
                      ? "Expired"
                      : resultPresentation === "available"
                        ? <Link href={searchJobLaunchHref(run.searchJobId)}>Open results</Link>
                        : resultPresentation === "pending"
                          ? "Pending"
                          : "Unavailable";
                  return (
                    <tr key={run.id}>
                      <td data-label="Scheduled">{formatMediumDateTime(run.scheduledAt, "Unknown")}</td>
                      <td data-label="Outcome"><StatusLabel tone={presentation.tone}>{presentation.label}</StatusLabel></td>
                      <td data-label="Skipped">{run.skippedOccurrenceCount.toLocaleString()}</td>
                      <td data-label="Result">{resultLabel}{run.searchJobExpiresAt === null ? null : <small>{resultExpired ? "Expired" : "Expires"} {formatMediumDateTime(run.searchJobExpiresAt, "unknown")}</small>}</td>
                    </tr>
                  );
                })}</tbody>
              </table>
            </div>
          )}
        </Modal>
      ) : null}
    </>
  );
}
