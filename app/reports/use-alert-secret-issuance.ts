"use client";

import { useCallback, useEffect, useRef, useState } from "react";

import {
  AlertSecretIssuanceController,
  type AlertSecretIssuanceOperation,
  type AlertSecretIssuanceState,
  type AlertSigningSecret,
} from "@/lib/search/server-alerts";

import { installOneTimeSecretNavigationProtection } from "./use-one-time-secret-unload-protection";

export interface AlertSecretIssuance {
  active: boolean;
  begin: (operation: AlertSecretIssuanceOperation) => boolean;
  closeRecovery: () => void;
  failIssuing: () => void;
  finishIssuing: () => void;
  navigationBlocked: boolean;
  phase: AlertSecretIssuanceState["phase"];
  recover: (secret: AlertSigningSecret) => void;
  secret: AlertSigningSecret | null;
}

/** React adapter for the synchronous issuance controller. */
export function useAlertSecretIssuance(): AlertSecretIssuance {
  const [controller] = useState(() => new AlertSecretIssuanceController());
  const [state, setState] = useState<AlertSecretIssuanceState>(() => controller.state());
  const [navigationBlocked, setNavigationBlocked] = useState(false);
  const guardCleanupRef = useRef<(() => void) | null>(null);

  const publish = useCallback(() => setState(controller.state()), [controller]);
  const deactivateGuard = useCallback(() => {
    guardCleanupRef.current?.();
    guardCleanupRef.current = null;
    setNavigationBlocked(false);
  }, []);
  const begin = useCallback((operation: AlertSecretIssuanceOperation) => {
    const started = controller.begin(operation);
    if (started) {
      setNavigationBlocked(false);
      guardCleanupRef.current = installOneTimeSecretNavigationProtection(
        window,
        document,
        () => setNavigationBlocked(true),
      );
      publish();
    }
    return started;
  }, [controller, publish]);
  const recover = useCallback((secret: AlertSigningSecret) => {
    controller.recover(secret);
    publish();
  }, [controller, publish]);
  const finishIssuing = useCallback(() => {
    controller.finishIssuing();
    if (controller.state().phase === "idle") deactivateGuard();
    publish();
  }, [controller, deactivateGuard, publish]);
  const failIssuing = useCallback(() => {
    controller.failIssuing();
    deactivateGuard();
    publish();
  }, [controller, deactivateGuard, publish]);
  const closeRecovery = useCallback(() => {
    controller.acknowledgeRecovery();
    if (controller.state().phase === "idle") deactivateGuard();
    publish();
  }, [controller, deactivateGuard, publish]);
  useEffect(() => () => guardCleanupRef.current?.(), []);
  const active = state.phase === "issuing" || state.phase === "recovery";

  return {
    active,
    begin,
    closeRecovery,
    failIssuing,
    finishIssuing,
    navigationBlocked,
    phase: state.phase,
    recover,
    secret: state.phase === "recovery" ? state.secret : null,
  };
}
