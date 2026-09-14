"use client";

import Link from "next/link";
import { useCallback, useEffect, useMemo, useRef, useState } from "react";

import { AlertConditionOperator, AlertRunOutcome } from "@/gen/ts/open_splunk/alert";
import {
  createOpenSplunkApiClient,
  getSystemBootstrap,
  isHttpStatus,
  type SystemBootstrapModel,
} from "@/lib/api";
import { createErrorMessage } from "@/lib/error-message";
import { validateServerAlertSchedule, type AlertFormValue } from "@/lib/search/alert-form";
import { searchJobLaunchHref } from "@/lib/search/launch-url";
import {
  alertRunOutcomeLabel,
  AlertCreationSession,
  deleteServerAlert,
  listServerAlertRuns,
  listServerAlerts,
  rotateServerAlertSecret,
  runServerAlert,
  setServerAlertEnabled,
  testServerAlertWebhook,
  updateServerAlert,
  type ServerAlert,
  type ServerAlertRun,
} from "@/lib/search/server-alerts";

import { BackendResourceState } from "../_components/backend-resource-state";
import { formatMediumDateTime } from "../_components/date-format";
import { Modal } from "../_components/modal";
import { StatusLabel } from "../_components/status";
import { AlertSecretRecovery } from "./alert-secret-recovery";
import { AlertWizard } from "./alert-wizard";
import {
  alertDefinitionFromForm,
  alertEffectiveDispatchTTL,
  alertEffectiveWebhookTTL,
  alertFormFromServer,
  alertOutcomeTone,
  alertRunResultPresentation,
  defaultAlertForm,
  ONE_TIME_SECRET_NAVIGATION_BLOCKED_MESSAGE,
} from "./alerts-ui-state";
import { useAlertSecretIssuance } from "./use-alert-secret-issuance";

interface AlertsPanelProps {
  apiBaseUrl: string;
  initialDraft?: Partial<AlertFormValue>;
}

function newAlertForm(
  bootstrap: SystemBootstrapModel | null,
  initialDraft: Partial<AlertFormValue> | undefined,
): AlertFormValue {
  const appId = initialDraft?.appId ?? bootstrap?.selectedAppId ?? bootstrap?.apps[0]?.appId ?? "search";
  const application = bootstrap?.apps.find((candidate) => candidate.appId === appId);
  return defaultAlertForm({
    ...initialDraft,
    appId,
    indexScope: initialDraft?.indexScope ?? application?.defaultIndexNames ?? [],
  });
}

type LoadState = "loading" | "available" | "unavailable" | "error";
type AlertMutationAction = "delete" | "rotate" | "run" | "state" | "test";
type AlertWizardTarget = { kind: "create" } | { kind: "edit"; alert: ServerAlert };
type PendingAlertAction =
  | { kind: "history"; alertId: string }
  | { kind: "save"; alertId: string | null }
  | { kind: AlertMutationAction; alertId: string };

const errorMessage = createErrorMessage("The alert request failed.");
type AlertNotice = { message: string; tone: "success" | "warning" };

function isPendingAlertAction(
  pending: PendingAlertAction | null,
  alertId: string,
  kind: PendingAlertAction["kind"],
): boolean {
  return pending?.alertId === alertId && pending.kind === kind;
}

function conditionLabel(alert: ServerAlert): string {
  const condition = alert.definition.condition;
  if (condition === undefined) return "Unknown";
  let operator: string;
  switch (condition.operator) {
    case AlertConditionOperator.ALERT_CONDITION_OPERATOR_GREATER_THAN: operator = ">"; break;
    case AlertConditionOperator.ALERT_CONDITION_OPERATOR_LESS_THAN: operator = "<"; break;
    case AlertConditionOperator.ALERT_CONDITION_OPERATOR_EQUAL: operator = "="; break;
    case AlertConditionOperator.ALERT_CONDITION_OPERATOR_NOT_EQUAL: operator = "≠"; break;
    case AlertConditionOperator.ALERT_CONDITION_OPERATOR_UNSPECIFIED:
    case AlertConditionOperator.UNRECOGNIZED:
    default: return "Unknown";
  }
  return `Results ${operator} ${condition.threshold.toString()}`;
}

export function AlertsPanel({ apiBaseUrl, initialDraft }: AlertsPanelProps) {
  const client = useMemo(() => createOpenSplunkApiClient({ baseUrl: apiBaseUrl }), [apiBaseUrl]);
  const [bootstrap, setBootstrap] = useState<SystemBootstrapModel | null>(null);
  const [state, setState] = useState<LoadState>("loading");
  const [alerts, setAlerts] = useState<ServerAlert[]>([]);
  const [error, setError] = useState<string | null>(null);
  const [administratorSignInRequired, setAdministratorSignInRequired] = useState(false);
  const [notice, setNotice] = useState<AlertNotice | null>(null);
  const [generation, setGeneration] = useState(0);
  const [wizardTarget, setWizardTarget] = useState<AlertWizardTarget | null>(null);
  const [deleteTarget, setDeleteTarget] = useState<ServerAlert | null>(null);
  const [pendingAction, setPendingAction] = useState<PendingAlertAction | null>(null);
  const [history, setHistory] = useState<{ alert: ServerAlert; runs: ServerAlertRun[] } | null>(null);
  const creationRef = useRef(new AlertCreationSession());
  const [secretReturnFocus, setSecretReturnFocus] = useState<HTMLElement | null>(null);
  const issuance = useAlertSecretIssuance();

  const reload = useCallback(() => setGeneration((current) => current + 1), []);
  const loadInput = { client, generation };
  const [activeLoadInput, setActiveLoadInput] = useState(loadInput);
  if (activeLoadInput.client !== loadInput.client || activeLoadInput.generation !== loadInput.generation) {
    setActiveLoadInput(loadInput);
    setState("loading");
    setError(null);
    setAdministratorSignInRequired(false);
  }
  useEffect(() => {
    if (activeLoadInput.generation !== generation) return;
    const controller = new AbortController();
    void (async () => {
      try {
        const nextBootstrap = await getSystemBootstrap(client, undefined, { signal: controller.signal });
        const result = await listServerAlerts(client, nextBootstrap, { signal: controller.signal });
        if (controller.signal.aborted) return;
        setBootstrap(nextBootstrap);
        if (result.status === "unavailable") {
          setState("unavailable");
          return;
        }
        setAlerts(result.value.items);
        setState("available");
      } catch (reason) {
        if (controller.signal.aborted) return;
        setAdministratorSignInRequired(isHttpStatus(reason, 401) || isHttpStatus(reason, 403));
        setError(errorMessage(reason));
        setState("error");
      }
    })();
    return () => controller.abort();
  }, [activeLoadInput, client, generation]);

  function replaceAlert(next: ServerAlert) {
    setAlerts((current) => current.map((alert) => alert.id === next.id ? next : alert));
  }

  function openNewAlert() {
    setSecretReturnFocus(document.activeElement instanceof HTMLElement
      ? document.activeElement
      : null);
    creationRef.current.reset();
    setError(null);
    setAdministratorSignInRequired(false);
    setWizardTarget({ kind: "create" });
  }

  async function createAlert(value: AlertFormValue) {
    if (bootstrap === null) return;
    const editingAlert = wizardTarget?.kind === "edit" ? wizardTarget.alert : null;
    const issuingSecret = editingAlert === null;
    if (issuingSecret && !issuance.begin("create")) return;
    setPendingAction({ kind: "save", alertId: editingAlert?.id ?? null });
    setError(null);
    try {
      if (editingAlert !== null) {
        const updated = await updateServerAlert(client, bootstrap, editingAlert, alertDefinitionFromForm(value));
        if (updated.status === "unavailable") throw new Error("Alert updates are unavailable.");
        replaceAlert(updated.value);
        setWizardTarget(null);
        setNotice({ message: `${updated.value.definition.name} updated. The new definition applies to future claims.`, tone: "success" });
        return;
      }
      const definition = alertDefinitionFromForm(value);
      const result = await creationRef.current.create(
        client,
        bootstrap,
        definition,
      );
      if (result.status === "unavailable") throw new Error("Alert creation is unavailable.");
      setAlerts((current) => current.some((candidate) => candidate.id === result.value.alert.id)
        ? current.map((candidate) => candidate.id === result.value.alert.id ? result.value.alert : candidate)
        : [result.value.alert, ...current]);
      setWizardTarget(null);
      if (result.value.secret === null) issuance.finishIssuing();
      else issuance.recover(result.value.secret);
      setNotice({ message: result.value.notice, tone: result.value.noticeTone });
    } catch (reason) {
      if (issuingSecret) issuance.failIssuing();
      setAdministratorSignInRequired(isHttpStatus(reason, 401) || isHttpStatus(reason, 403));
      setError(errorMessage(reason));
    } finally {
      setPendingAction(null);
    }
  }

  async function mutate(alert: ServerAlert, action: AlertMutationAction) {
    if (bootstrap === null) return;
    if (action === "rotate" && !issuance.begin("rotate")) return;
    setPendingAction({ kind: action, alertId: alert.id });
    setError(null);
    try {
      switch (action) {
        case "state": {
          const result = await setServerAlertEnabled(client, bootstrap, alert, !alert.enabled);
          if (result.status === "unavailable") throw new Error("Alert state changes are unavailable.");
          replaceAlert(result.value);
          setNotice({ message: `${alert.definition.name} ${result.value.enabled ? "enabled" : "disabled"}.`, tone: "success" });
          break;
        }
        case "run": {
          const result = await runServerAlert(client, bootstrap, alert.id);
          if (result.status === "unavailable") throw new Error("Manual alert runs are unavailable.");
          const skipped = result.value.outcome === AlertRunOutcome.ALERT_RUN_OUTCOME_SKIPPED_OVERLAP;
          setNotice({
            message: skipped
              ? `Run ${result.value.id} was skipped because another run is active.`
              : `Run ${result.value.id} was queued.`,
            tone: skipped ? "warning" : "success",
          });
          reload();
          break;
        }
        case "test": {
          const result = await testServerAlertWebhook(client, bootstrap, alert.id);
          if (result.status === "unavailable") throw new Error("Webhook testing is unavailable.");
          setNotice({
            message: result.value.delivered
              ? `Test delivery ${result.value.deliveryId} succeeded.`
              : `Test delivery failed: ${result.value.failureCategory ?? "unknown failure"}.`,
            tone: result.value.delivered ? "success" : "warning",
          });
          break;
        }
        case "rotate": {
          setSecretReturnFocus(document.activeElement instanceof HTMLElement
            ? document.activeElement
            : null);
          const result = await rotateServerAlertSecret(client, bootstrap, alert);
          if (result.status === "unavailable") throw new Error("Secret rotation is unavailable.");
          replaceAlert(result.value.alert);
          if (result.value.signingSecret === null) throw new Error("The rotated signing secret was unavailable.");
          issuance.recover({ name: result.value.alert.definition.name, value: result.value.signingSecret });
          break;
        }
        case "delete": {
          const result = await deleteServerAlert(client, bootstrap, alert);
          if (result.status === "unavailable") throw new Error("Alert deletion is unavailable.");
          setAlerts((current) => current.filter((candidate) => candidate.id !== result.value));
          setDeleteTarget(null);
          setNotice({ message: `${alert.definition.name} deleted.`, tone: "success" });
          break;
        }
      }
    } catch (reason) {
      if (action === "rotate") issuance.failIssuing();
      setAdministratorSignInRequired(isHttpStatus(reason, 401) || isHttpStatus(reason, 403));
      setError(errorMessage(reason));
    } finally {
      setPendingAction(null);
    }
  }

  const controlsBlocked = pendingAction !== null || issuance.active;

  async function openHistory(alert: ServerAlert) {
    if (bootstrap === null) return;
    setPendingAction({ kind: "history", alertId: alert.id });
    try {
      const result = await listServerAlertRuns(client, bootstrap, alert.id);
      if (result.status === "unavailable") throw new Error("Alert run history is unavailable.");
      setHistory({ alert, runs: result.value.items });
    } catch (reason) {
      setAdministratorSignInRequired(isHttpStatus(reason, 401) || isHttpStatus(reason, 403));
      setError(errorMessage(reason));
    } finally {
      setPendingAction(null);
    }
  }

  return (
    <section className="suite-card alerts-panel" aria-labelledby="alerts-panel-title" aria-busy={pendingAction !== null}>
      <header className="alerts-panel-header"><div><h2 id="alerts-panel-title">Alerts</h2><p>Scheduled result-count conditions with signed webhook delivery.</p></div><button className="button button--primary" type="button" disabled={state !== "available" || controlsBlocked} onClick={openNewAlert}>New alert</button></header>
      {notice ? <output className="alerts-notice" data-tone={notice.tone} aria-live="polite">{notice.message}</output> : null}
      {error ? <div className="alerts-error" role="alert"><span>{error}</span>{administratorSignInRequired ? <Link href="/signin/">Administrator sign in</Link> : null}</div> : null}
      {state === "loading" ? <BackendResourceState kind="loading" title="Loading alerts" message="Reading schedules and recent delivery state." /> : null}
      {state === "unavailable" ? <BackendResourceState kind="unavailable" title="Alerts unavailable" message="This server does not advertise the alerts capability." /> : null}
      {state === "error" ? <BackendResourceState kind="error" title="Could not load alerts" message={error ?? "The request failed."} action={<button className="button" type="button" onClick={reload}>Retry</button>} /> : null}
      {state === "available" && alerts.length === 0 ? <BackendResourceState kind="empty" title="No alerts" message="Create a disabled alert, configure its receiver, then enable its schedule." /> : null}
      {state === "available" && alerts.length > 0 ? <div className="table-wrap"><table className="table table--cards alerts-table"><thead><tr><th scope="col">Alert</th><th scope="col">Schedule</th><th scope="col">Condition</th><th scope="col">Webhook</th><th scope="col">Last activity</th><th scope="col">Actions</th></tr></thead><tbody>{alerts.map((alert) => <tr key={alert.id}><td data-label="Alert"><strong>{alert.definition.name}</strong><small>{alert.definition.search?.spl}</small></td><td data-label="Schedule"><StatusLabel tone={alert.enabled ? "success" : "neutral"}>{alert.enabled ? "Enabled" : "Disabled"}</StatusLabel><small>{alert.definition.cron} · {alert.definition.timezone}</small><small>Next: {formatMediumDateTime(alert.nextRunAt, "Not scheduled")}</small></td><td data-label="Condition">{conditionLabel(alert)}<small>dispatch {alertEffectiveDispatchTTL(alert.definition)} · webhook {alertEffectiveWebhookTTL(alert.definition)}</small></td><td data-label="Webhook">{alert.webhookHostname}<small>Secret generation {alert.secretGeneration.toString()} · {formatMediumDateTime(alert.secretRotatedAt, "Unknown")}</small></td><td data-label="Last activity"><StatusLabel tone={alertOutcomeTone(alert.lastOutcome)}>{alert.lastOutcome === null ? "Not run" : alertRunOutcomeLabel(alert.lastOutcome)}</StatusLabel><small>Evaluated: {formatMediumDateTime(alert.lastEvaluatedAt, "Never")}</small><small>Delivered successfully: {formatMediumDateTime(alert.lastDeliveredAt, "Never")}</small></td><td data-label="Actions"><div className="alerts-actions"><button className="button button--ghost button--compact" aria-label={`Edit ${alert.definition.name}`} type="button" disabled={controlsBlocked} onClick={() => { setWizardTarget({ kind: "edit", alert }); setAdministratorSignInRequired(false); }}>Edit</button><button className="button button--ghost button--compact" aria-label={`${alert.enabled ? "Disable" : "Enable"} ${alert.definition.name}`} aria-busy={isPendingAlertAction(pendingAction, alert.id, "state")} type="button" disabled={controlsBlocked} onClick={() => void mutate(alert, "state")}>{alert.enabled ? "Disable" : "Enable"}</button><button className="button button--ghost button--compact" aria-label={`Run ${alert.definition.name} now`} aria-busy={isPendingAlertAction(pendingAction, alert.id, "run")} type="button" disabled={controlsBlocked} onClick={() => void mutate(alert, "run")}>Run now</button><button className="button button--ghost button--compact" aria-label={`Test ${alert.definition.name} webhook`} aria-busy={isPendingAlertAction(pendingAction, alert.id, "test")} type="button" disabled={controlsBlocked} onClick={() => void mutate(alert, "test")}>Test</button><button className="button button--ghost button--compact" aria-label={`Rotate ${alert.definition.name} secret`} aria-busy={isPendingAlertAction(pendingAction, alert.id, "rotate")} type="button" disabled={controlsBlocked} onClick={() => void mutate(alert, "rotate")}>Rotate secret</button><button className="button button--ghost button--compact" aria-label={`View ${alert.definition.name} history`} aria-busy={isPendingAlertAction(pendingAction, alert.id, "history")} type="button" disabled={controlsBlocked} onClick={() => void openHistory(alert)}>History</button><button aria-label={`Delete ${alert.definition.name}`} className="button button--danger button--compact" type="button" disabled={controlsBlocked} onClick={() => setDeleteTarget(alert)}>Delete</button></div></td></tr>)}</tbody></table></div> : null}
      {wizardTarget ? <AlertWizard applications={bootstrap?.apps.map((app) => ({ defaultIndexNames: app.defaultIndexNames, id: app.appId, name: app.displayName || app.slug || app.appId })) ?? []} administratorSignInRequired={administratorSignInRequired} existingWebhookHostname={wizardTarget.kind === "edit" ? wizardTarget.alert.webhookHostname : undefined} title={wizardTarget.kind === "create" ? "Save as alert" : `Edit ${wizardTarget.alert.definition.name}`} submitLabel={wizardTarget.kind === "create" ? "Create disabled alert" : "Save changes"} initialValue={wizardTarget.kind === "create" ? newAlertForm(bootstrap, initialDraft) : alertFormFromServer(wizardTarget.alert)} pending={pendingAction?.kind === "save"} submitError={pendingAction?.kind === "save" ? null : error} navigationBlocked={wizardTarget.kind === "create" && issuance.navigationBlocked} validateSchedule={(value, signal) => validateServerAlertSchedule(client, value, { signal })} onClose={() => { if (wizardTarget.kind === "create") creationRef.current.reset(); setWizardTarget(null); }} onSubmit={(value) => void createAlert(value)} /> : null}
      {pendingAction?.kind === "rotate" && issuance.phase === "issuing" ? <Modal title="Rotating webhook secret" subtitle="Keep this page open while the new one-time secret is issued." dismissible={false} onClose={() => {}}><output className="empty-state" aria-live="polite"><strong>Rotating secret…</strong><span>The recovery screen will appear as soon as the server responds.</span>{issuance.navigationBlocked ? <span role="alert">{ONE_TIME_SECRET_NAVIGATION_BLOCKED_MESSAGE}</span> : null}</output></Modal> : null}
      {issuance.secret ? <AlertSecretRecovery alertName={issuance.secret.name} secret={issuance.secret.value} navigationBlocked={issuance.navigationBlocked} returnFocus={secretReturnFocus} onClose={issuance.closeRecovery} /> : null}
      {history ? <AlertHistoryDialog alert={history.alert} runs={history.runs} onClose={() => setHistory(null)} /> : null}
      {deleteTarget ? <Modal title="Delete alert" subtitle={`Delete “${deleteTarget.definition.name}” and its run history?`} dismissible={pendingAction === null} onClose={() => setDeleteTarget(null)} footer={<><button className="button button--secondary" type="button" disabled={pendingAction !== null} onClick={() => setDeleteTarget(null)}>Cancel</button><button className="button button--danger" type="button" aria-busy={isPendingAlertAction(pendingAction, deleteTarget.id, "delete")} disabled={pendingAction !== null} onClick={() => void mutate(deleteTarget, "delete")}>Delete alert</button></>}><p>Deletion is rejected while a run is active. This action cannot be undone.</p></Modal> : null}
    </section>
  );
}

function AlertHistoryDialog({ alert, runs, onClose }: { alert: ServerAlert; runs: ServerAlertRun[]; onClose: () => void }) {
  const now = new Date();
  return <Modal title={`${alert.definition.name} history`} subtitle="Newest retained runs first." wide onClose={onClose} footer={<button className="button button--primary" type="button" onClick={onClose}>Done</button>}><div className="table-wrap"><table className="table table--cards alerts-history-table"><thead><tr><th scope="col">Scheduled</th><th scope="col">Outcome</th><th scope="col">Retained results</th><th scope="col">Details</th></tr></thead><tbody>{runs.length === 0 ? <tr><td colSpan={4}>No runs recorded.</td></tr> : runs.map((run) => {
    const resultPresentation = alertRunResultPresentation(run, now);
    const result = resultPresentation === "available" && run.searchJobId !== null
      ? <Link href={searchJobLaunchHref(run.searchJobId)}>Open results</Link>
      : resultPresentation === "expired"
        ? "Expired"
        : resultPresentation === "pending"
          ? "Pending"
          : "Unavailable";
    return <tr key={run.id}><td data-label="Scheduled">{formatMediumDateTime(run.scheduledAt, "Unknown")}{run.missedOccurrenceCount > 0 ? <small>{run.missedOccurrenceCount} missed occurrences coalesced</small> : null}</td><td data-label="Outcome"><StatusLabel tone={alertOutcomeTone(run.outcome)}>{alertRunOutcomeLabel(run.outcome)}</StatusLabel></td><td data-label="Retained results">{result}{run.searchJobExpiresAt === null ? null : <small>{resultPresentation === "expired" ? "Expired" : "Expires"} {formatMediumDateTime(run.searchJobExpiresAt, "Unknown")}</small>}</td><td data-label="Details">{run.failureCategory ?? run.deliveryId ?? "—"}</td></tr>;
  })}</tbody></table></div></Modal>;
}
