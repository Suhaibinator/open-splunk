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
  const controllerRef = useRef<AlertSecretIssuanceController | null>(null);
  controllerRef.current ??= new AlertSecretIssuanceController();
  const [state, setState] = useState<AlertSecretIssuanceState>(() => controllerRef.current!.state());
  const [navigationBlocked, setNavigationBlocked] = useState(false);
  const guardCleanupRef = useRef<(() => void) | null>(null);

  const publish = useCallback(() => setState(controllerRef.current!.state()), []);
  const deactivateGuard = useCallback(() => {
    guardCleanupRef.current?.();
    guardCleanupRef.current = null;
    setNavigationBlocked(false);
  }, []);
  const begin = useCallback((operation: AlertSecretIssuanceOperation) => {
    const started = controllerRef.current!.begin(operation);
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
  }, [publish]);
  const recover = useCallback((secret: AlertSigningSecret) => {
    controllerRef.current!.recover(secret);
    publish();
  }, [publish]);
  const finishIssuing = useCallback(() => {
    controllerRef.current!.finishIssuing();
    if (controllerRef.current!.state().phase === "idle") deactivateGuard();
    publish();
  }, [deactivateGuard, publish]);
  const failIssuing = useCallback(() => {
    controllerRef.current!.failIssuing();
    deactivateGuard();
    publish();
  }, [deactivateGuard, publish]);
  const closeRecovery = useCallback(() => {
    controllerRef.current!.acknowledgeRecovery();
    if (controllerRef.current!.state().phase === "idle") deactivateGuard();
    publish();
  }, [deactivateGuard, publish]);
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
