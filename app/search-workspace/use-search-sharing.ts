import { useCallback, useEffect, useRef, useState } from "react";

import type { SearchJob } from "@/gen/ts/open_splunk/search";
import { ServerFeature } from "@/gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "@/lib/api/open-splunk-client";
import {
  savedSearchLaunchHref,
  searchJobLaunchHref,
  searchLaunchHref,
} from "@/lib/search/launch-url";
import {
  adaptServerJobSettings,
  getServerJobSettings,
  makeServerSearchJobPrivate,
  shareServerSearchJob,
  type ServerJobSettings,
} from "@/lib/search/server-job-settings";
import type { SystemBootstrapModel } from "@/lib/api/system-bootstrap";
import type { ProtobufRequestOptions } from "@/lib/api/protobuf-transport";
import { supportsServerFeature } from "@/lib/api/system-bootstrap";
import type { SearchResultView } from "@/lib/search/result-view-navigation";

import type { DialogActionState, TimeRange } from "./model";

export type SearchSharingDialog = "share" | "settings";

interface UseSearchSharingOptions {
  client: OpenSplunkApiClient;
  bootstrap: SystemBootstrapModel | null;
  backendEnabled: boolean;
  job: SearchJob | null;
  activeSavedSearchId: string | null;
  query: string;
  resultView: SearchResultView;
  timeRange: TimeRange;
  copyText: (text: string, successMessage: string) => Promise<boolean>;
  onJobUpdated: (job: SearchJob) => void;
}

export interface SearchSharingController {
  dialog: SearchSharingDialog | null;
  settings: ServerJobSettings | null;
  loadState: DialogActionState;
  mutationState: DialogActionState;
  manualCopyValue: string | null;
  canCopyJob: boolean;
  canCopySavedSearch: boolean;
  open: (dialog: SearchSharingDialog) => void;
  close: () => void;
  copyJobLink: () => Promise<void>;
  copyQueryLink: () => Promise<void>;
  copySavedSearchLink: () => Promise<void>;
  makeShared: () => Promise<void>;
  makePrivate: () => Promise<void>;
}

function absoluteHref(relativeHref: string): string {
  return new URL(relativeHref, window.location.origin).toString();
}

export async function shareSearchJobForLink(
  client: OpenSplunkApiClient,
  bootstrap: SystemBootstrapModel,
  searchJobId: string,
  expectedStateVersion: bigint,
  origin: string,
  resultView: SearchResultView = "events",
  options?: ProtobufRequestOptions,
): Promise<Awaited<ReturnType<typeof shareServerSearchJob>> & { href: string }> {
  const result = await shareServerSearchJob(
    client,
    bootstrap,
    searchJobId,
    expectedStateVersion,
    options,
  );
  return {
    ...result,
    href: new URL(searchJobLaunchHref(result.job.searchJobId, resultView), origin).toString(),
  };
}

export function useSearchSharing(options: UseSearchSharingOptions): SearchSharingController {
  const [dialog, setDialog] = useState<SearchSharingDialog | null>(null);
  const [settings, setSettings] = useState<ServerJobSettings | null>(null);
  const [loadState, setLoadState] = useState<DialogActionState>({ status: "idle" });
  const [mutationState, setMutationState] = useState<DialogActionState>({ status: "idle" });
  const [manualCopyValue, setManualCopyValue] = useState<string | null>(null);
  const requestAbortRef = useRef<AbortController | null>(null);
  const requestEpochRef = useRef(0);
  const jobID = options.job?.searchJobId ?? null;
  const activeRequestJobIDRef = useRef(jobID);
  const durableJobsSupported = options.bootstrap !== null && supportsServerFeature(
    options.bootstrap,
    ServerFeature.SERVER_FEATURE_DURABLE_SEARCH_JOBS,
  );

  const beginRequest = useCallback(() => {
    requestAbortRef.current?.abort();
    const controller = new AbortController();
    const epoch = requestEpochRef.current + 1;
    requestEpochRef.current = epoch;
    requestAbortRef.current = controller;
    return { controller, epoch, jobID };
  }, [jobID]);

  const requestIsCurrent = useCallback((epoch: number, expectedJobID: string | null) => (
    requestEpochRef.current === epoch
    && expectedJobID === (options.job?.searchJobId ?? null)
    && !requestAbortRef.current?.signal.aborted
  ), [options.job?.searchJobId]);

  const [dialogJobID, setDialogJobID] = useState(jobID);
  if (dialogJobID !== jobID) {
    setDialogJobID(jobID);
    setDialog(null);
    setSettings(null);
    setLoadState({ status: "idle" });
    setMutationState({ status: "idle" });
    setManualCopyValue(null);
  }

  useEffect(() => {
    if (activeRequestJobIDRef.current === jobID) return;
    activeRequestJobIDRef.current = jobID;
    requestAbortRef.current?.abort();
    requestAbortRef.current = null;
    requestEpochRef.current += 1;
  }, [jobID]);

  useEffect(() => () => {
    requestAbortRef.current?.abort();
    requestEpochRef.current += 1;
  }, []);

  const load = useCallback(async () => {
    if (
      !options.backendEnabled
      || options.bootstrap === null
      || options.job === null
      || !durableJobsSupported
    ) {
      requestAbortRef.current?.abort();
      requestAbortRef.current = null;
      requestEpochRef.current += 1;
      setSettings(
        !options.backendEnabled && options.job !== null
          ? adaptServerJobSettings(options.job)
          : null,
      );
      setLoadState({ status: "idle" });
      return;
    }
    const request = beginRequest();
    setLoadState({ status: "pending" });
    try {
      const result = await getServerJobSettings(
        options.client,
        options.bootstrap,
        options.job.searchJobId,
        { signal: request.controller.signal },
      );
      if (!requestIsCurrent(request.epoch, request.jobID)) return;
      setSettings(result.settings);
      options.onJobUpdated(result.job);
      setLoadState({ status: "idle" });
    } catch (error) {
      if (!requestIsCurrent(request.epoch, request.jobID)) return;
      setLoadState({
        status: "error",
        error: error instanceof Error ? error.message : "Unable to load job settings.",
      });
    }
  }, [beginRequest, durableJobsSupported, options, requestIsCurrent]);

  const open = useCallback((nextDialog: SearchSharingDialog) => {
    setDialog(nextDialog);
    setMutationState({ status: "idle" });
    setManualCopyValue(null);
    void load();
  }, [load]);

  const close = useCallback(() => {
    if (mutationState.status !== "pending") {
      requestAbortRef.current?.abort();
      requestAbortRef.current = null;
      requestEpochRef.current += 1;
      setDialog(null);
    }
  }, [mutationState.status]);

  const copyLink = useCallback(async (href: string, successMessage: string) => {
    const copied = await options.copyText(href, successMessage);
    setManualCopyValue(copied ? null : href);
  }, [options]);

  const copyQueryLink = useCallback(async () => {
    await copyLink(absoluteHref(searchLaunchHref(options.query, {
      earliest: options.timeRange.earliest,
      latest: options.timeRange.latest,
      label: options.timeRange.label,
      timezone: options.timeRange.timezone,
      view: options.resultView,
    })), "Query link copied to the clipboard.");
  }, [copyLink, options.query, options.resultView, options.timeRange]);

  const copySavedSearchLink = useCallback(async () => {
    if (options.activeSavedSearchId === null) return;
    await copyLink(
      absoluteHref(savedSearchLaunchHref(options.activeSavedSearchId, true, options.resultView)),
      "Saved-search link copied to the clipboard.",
    );
  }, [copyLink, options.activeSavedSearchId, options.resultView]);

  const share = useCallback(async (copyAfterShare: boolean) => {
    if (options.job === null || options.bootstrap === null || settings === null) return;
    const request = beginRequest();
    setMutationState({ status: "pending" });
    try {
      let result: Awaited<ReturnType<typeof shareServerSearchJob>>;
      let sharedHref: string | null = null;
      if (copyAfterShare) {
        const shared = await shareSearchJobForLink(
          options.client,
          options.bootstrap,
          options.job.searchJobId,
          settings.stateVersion,
          window.location.origin,
          options.resultView,
          { signal: request.controller.signal },
        );
        result = shared;
        sharedHref = shared.href;
      } else {
        result = await shareServerSearchJob(
          options.client,
          options.bootstrap,
          options.job.searchJobId,
          settings.stateVersion,
          { signal: request.controller.signal },
        );
      }
      if (!requestIsCurrent(request.epoch, request.jobID)) return;
      setSettings(result.settings);
      options.onJobUpdated(result.job);
      if (copyAfterShare) {
        if (sharedHref === null) throw new TypeError("The shared job link was unavailable.");
        const copied = await options.copyText(
          sharedHref,
          "Job link copied. These exact results now have a sliding seven-day lifetime.",
        );
        if (!requestIsCurrent(request.epoch, request.jobID)) return;
        setManualCopyValue(copied ? null : sharedHref);
      }
      setMutationState({ status: "idle" });
    } catch (error) {
      if (!requestIsCurrent(request.epoch, request.jobID)) return;
      setMutationState({
        status: "error",
        error: error instanceof Error ? error.message : "Unable to share this search job.",
      });
    }
  }, [beginRequest, options, requestIsCurrent, settings]);

  const copyJobLink = useCallback(() => share(true), [share]);
  const makeShared = useCallback(() => share(false), [share]);

  const makePrivate = useCallback(async () => {
    if (options.job === null || options.bootstrap === null || settings === null) return;
    const request = beginRequest();
    setMutationState({ status: "pending" });
    try {
      const result = await makeServerSearchJobPrivate(
        options.client,
        options.bootstrap,
        options.job.searchJobId,
        settings.stateVersion,
        { signal: request.controller.signal },
      );
      if (!requestIsCurrent(request.epoch, request.jobID)) return;
      setSettings(result.settings);
      options.onJobUpdated(result.job);
      setMutationState({ status: "idle" });
    } catch (error) {
      if (!requestIsCurrent(request.epoch, request.jobID)) return;
      setMutationState({
        status: "error",
        error: error instanceof Error ? error.message : "Unable to update this search job.",
      });
    }
  }, [beginRequest, options, requestIsCurrent, settings]);

  return {
    dialog,
    settings,
    loadState,
    mutationState,
    manualCopyValue,
    canCopyJob: options.backendEnabled && options.job !== null && durableJobsSupported,
    canCopySavedSearch: options.activeSavedSearchId !== null,
    open,
    close,
    copyJobLink,
    copyQueryLink,
    copySavedSearchLink,
    makeShared,
    makePrivate,
  };
}
