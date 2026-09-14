import { useEffect, useMemo, useSyncExternalStore } from "react";
import { BackendPatterns, type BackendPatternsState, type PatternClient, type PatternContext } from "./backend-patterns";

const EMPTY_STATE: BackendPatternsState = {
  rows: [], coverage: null, pageNumber: 1, pageSize: 20,
  hasNextPage: false, loading: false, error: null, members: null,
};
const emptySnapshot = () => EMPTY_STATE;
const emptySubscribe = () => () => undefined;

/** A job/snapshot/sensitivity transition drops every old page and member filter. */
export function useBackendPatterns(
  client: PatternClient | null,
  context: PatternContext | null,
  enabled: boolean,
) {
  const searchJobId = context?.searchJobId;
  const snapshotRef = context?.snapshotRef;
  const sensitivity = context?.sensitivity;
  const controller = useMemo(() => client && searchJobId && snapshotRef && sensitivity
    ? new BackendPatterns({ searchJobId, snapshotRef, sensitivity }, client)
    : null, [client, searchJobId, snapshotRef, sensitivity]);
  const state = useSyncExternalStore(controller?.subscribe ?? emptySubscribe, controller?.getSnapshot ?? emptySnapshot, emptySnapshot);
  useEffect(() => {
    if (enabled && controller && controller.getSnapshot().coverage === null) void controller.loadGroups();
  }, [controller, enabled]);
  useEffect(() => () => controller?.cancelPending(), [controller]);
  return { controller, ...state };
}
