import type { ResultSchema } from "@/gen/ts/open_splunk/result";
import type { SearchJob } from "@/gen/ts/open_splunk/search";
import type { SearchWebSocketClient } from "@/lib/api";

import type { LivePreviewSnapshot } from "./live-preview";
import type { ProgressRevisionState } from "./progress-revision";

export type RunningPreviewStatus =
  | "disabled"
  | "waiting"
  | "live"
  | "paused"
  | "resyncing"
  | "limited"
  | "finalizing"
  | "finalization-error";

export class RunningSearchController {
  readonly abortRef = { current: null as AbortController | null };
  readonly cancelPendingRef = { current: false };
  readonly cancelRequestedRef = { current: false };
  readonly generationRef = { current: 0 };
  readonly jobIdRef = { current: null as string | null };
  readonly jobRef = { current: null as SearchJob | null };
  readonly jobVersionRef = { current: 0n };
  readonly launchRef = { current: false };
  readonly liveUpdateEpochRef = { current: 0n };
  readonly previewRef = { current: null as LivePreviewSnapshot | null };
  readonly previewRowLimitRef = { current: 0 };
  readonly previewSchemasRef = { current: new Map<string, ResultSchema>() };
  readonly previewStatusRef = { current: "disabled" as RunningPreviewStatus };
  readonly progressRevisionRef = { current: null as ProgressRevisionState };
  readonly socketRef = { current: null as SearchWebSocketClient | null };
  private timers: number[] = [];

  beginGeneration(): number {
    this.generationRef.current += 1;
    return this.generationRef.current;
  }

  resetBackendRun(abortRelatedRequests: () => void) {
    const supersededJobId = this.jobIdRef.current;
    this.abortRef.current?.abort();
    abortRelatedRequests();
    this.stopLiveUpdates();
    const controller = new AbortController();
    this.abortRef.current = controller;
    this.clearJob();
    this.cancelPendingRef.current = false;
    this.cancelRequestedRef.current = false;
    return { controller, supersededJobId };
  }

  beginCancel(): number | null {
    if (this.cancelPendingRef.current) return null;
    this.cancelPendingRef.current = true;
    this.cancelRequestedRef.current = true;
    return this.generationRef.current;
  }

  acceptAuthoritativeJob(job: SearchJob, progressRevision: NonNullable<ProgressRevisionState>) {
    this.jobVersionRef.current = job.stateVersion;
    this.progressRevisionRef.current = progressRevision;
    this.jobRef.current = job;
  }

  attachSocket(socket: SearchWebSocketClient) {
    this.socketRef.current = socket;
  }

  captureLiveUpdateEpoch(): bigint {
    return this.liveUpdateEpochRef.current;
  }

  finishCancel(generation: number) {
    if (this.generationRef.current !== generation) return;
    this.cancelPendingRef.current = false;
    this.cancelRequestedRef.current = false;
  }

  isCurrent(generation: number, searchJobId?: string): boolean {
    return this.generationRef.current === generation
      && (searchJobId === undefined || this.jobIdRef.current === searchJobId);
  }

  liveUpdateEpochIs(epoch: bigint): boolean {
    return this.liveUpdateEpochRef.current === epoch;
  }

  recordUnversionedLiveUpdate() {
    this.liveUpdateEpochRef.current += 1n;
  }

  releaseSocket(socket: SearchWebSocketClient | null) {
    if (this.socketRef.current === socket) this.socketRef.current = null;
  }

  launchIsLocked(): boolean {
    return this.launchRef.current;
  }

  lockLaunch() {
    this.launchRef.current = true;
    window.setTimeout(() => {
      this.launchRef.current = false;
    }, 0);
  }

  supersede(): number {
    return this.beginGeneration();
  }

  stopLiveUpdates() {
    this.socketRef.current?.dispose();
    this.socketRef.current = null;
  }

  clearJob() {
    this.jobIdRef.current = null;
    this.jobRef.current = null;
    this.jobVersionRef.current = 0n;
    this.liveUpdateEpochRef.current = 0n;
  }

  resetProgress() {
    this.progressRevisionRef.current = null;
  }

  clearTimers = () => {
    for (const timer of this.timers) window.clearTimeout(timer);
    this.timers = [];
  };

  schedule = (callback: () => void, delay: number) => {
    const timer = window.setTimeout(callback, delay);
    this.timers.push(timer);
  };
}
