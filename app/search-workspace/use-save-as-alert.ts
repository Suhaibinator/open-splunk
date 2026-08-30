import { useEffect, useRef, useState } from "react";

import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import {
  isHttpStatus,
  supportsServerFeature,
  type OpenSplunkApiClient,
  type SystemBootstrapModel,
} from "@/lib/api";
import {
  validateServerAlertSchedule,
  type AlertFormErrors,
  type AlertFormValue,
} from "@/lib/search/alert-form";
import { AlertCreationSession, type AlertSigningSecret } from "@/lib/search/server-alerts";

import { alertDefinitionFromForm } from "../reports/alerts-ui-state";
import { useAlertSecretIssuance } from "../reports/use-alert-secret-issuance";

interface UseSaveAsAlertOptions {
  backendEnabled: boolean;
  client: OpenSplunkApiClient;
  loadBootstrap: () => Promise<SystemBootstrapModel>;
  onNotice: (message: string, tone: "success" | "warning") => void;
}

export interface SaveAsAlertController {
  administratorSignInRequired: boolean;
  applications: SystemBootstrapModel["apps"];
  close: () => void;
  closeSecret: () => void;
  draft: AlertFormValue | null;
  error: string | null;
  open: (prepare: (bootstrap: SystemBootstrapModel) => AlertFormValue) => Promise<void>;
  pending: boolean;
  secret: AlertSigningSecret | null;
  submit: (value: AlertFormValue) => Promise<void>;
  validateSchedule: (value: AlertFormValue, signal: AbortSignal) => Promise<AlertFormErrors>;
}

/** Owns Save As Alert preparation, retry identity, mutation, and recovery UI state. */
export function useSaveAsAlert({
  backendEnabled,
  client,
  loadBootstrap,
  onNotice,
}: UseSaveAsAlertOptions): SaveAsAlertController {
  const [bootstrap, setBootstrap] = useState<SystemBootstrapModel | null>(null);
  const [draft, setDraft] = useState<AlertFormValue | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [administratorSignInRequired, setAdministratorSignInRequired] = useState(false);
  const abortRef = useRef<AbortController | null>(null);
  const creationRef = useRef(new AlertCreationSession());
  const issuance = useAlertSecretIssuance();
  const pending = issuance.phase === "issuing";

  useEffect(() => () => abortRef.current?.abort(), []);

  async function open(prepare: (current: SystemBootstrapModel) => AlertFormValue): Promise<void> {
    if (!backendEnabled) {
      onNotice("Alerts require a connected backend.", "warning");
      return;
    }
    try {
      const current = await loadBootstrap();
      if (!supportsServerFeature(current, ServerFeature.SERVER_FEATURE_ALERTS)) {
        onNotice("This server does not advertise webhook alerts.", "warning");
        return;
      }
      creationRef.current.reset();
      setBootstrap(current);
      setError(null);
      setAdministratorSignInRequired(false);
      setDraft(prepare(current));
    } catch (reason) {
      onNotice(reason instanceof Error ? reason.message : "Unable to prepare the alert.", "warning");
    }
  }

  function close(): void {
    if (issuance.active) return;
    creationRef.current.reset();
    setDraft(null);
    setError(null);
    setAdministratorSignInRequired(false);
  }

  async function submit(value: AlertFormValue): Promise<void> {
    if (!issuance.begin("create")) return;
    const controller = new AbortController();
    abortRef.current?.abort();
    abortRef.current = controller;
    setError(null);
    setAdministratorSignInRequired(false);
    try {
      const current = bootstrap ?? await loadBootstrap();
      setBootstrap(current);
      const definition = alertDefinitionFromForm(value);
      const result = await creationRef.current.create(
        client,
        current,
        definition,
        { signal: controller.signal },
      );
      if (controller.signal.aborted) return;
      if (result.status === "unavailable") throw new Error("Webhook alerts are unavailable on this server.");
      setDraft(null);
      if (result.value.secret === null) issuance.finishIssuing();
      else issuance.recover(result.value.secret);
      onNotice(result.value.notice, result.value.noticeTone);
    } catch (reason) {
      issuance.failIssuing();
      if (!controller.signal.aborted) {
        const needsSignIn = isHttpStatus(reason, 401) || isHttpStatus(reason, 403);
        setAdministratorSignInRequired(needsSignIn);
        setError(needsSignIn
          ? "Administrator sign-in is required to create an alert."
          : reason instanceof Error ? reason.message : "Unable to create the alert.");
      }
    } finally {
      if (abortRef.current === controller) abortRef.current = null;
    }
  }

  return {
    administratorSignInRequired,
    applications: bootstrap?.apps ?? [],
    close,
    closeSecret: issuance.closeRecovery,
    draft,
    error,
    open,
    pending,
    secret: issuance.secret,
    submit,
    validateSchedule: (value, signal) => validateServerAlertSchedule(client, value, { signal }),
  };
}
