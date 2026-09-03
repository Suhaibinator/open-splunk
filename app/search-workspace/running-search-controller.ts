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
  private abortController: AbortController | null = null;
  private cancelPending = false;
  private cancelRequested = false;
  private generation = 0;
  private jobId: string | null = null;
  private job: SearchJob | null = null;
  private jobVersion = 0n;
  private launchLocked = false;
  private liveUpdateEpoch = 0n;
  private preview: LivePreviewSnapshot | null = null;
  private previewRowLimit = 0;
  private readonly previewSchemas = new Map<string, ResultSchema>();
  private previewStatus: RunningPreviewStatus = "disabled";
  private progressRevision: ProgressRevisionState = null;
  private socket: SearchWebSocketClient | null = null;
  private timers: number[] = [];

  beginGeneration(): number {
    this.generation += 1;
    return this.generation;
  }

  resetBackendRun(abortRelatedRequests: () => void) {
    const supersededJobId = this.jobId;
    this.abortController?.abort();
    abortRelatedRequests();
    this.stopLiveUpdates();
    const controller = new AbortController();
    this.abortController = controller;
    this.clearJob();
    this.cancelPending = false;
    this.cancelRequested = false;
    return { controller, supersededJobId };
  }

  beginCancel(): number | null {
    if (this.cancelPending) return null;
    this.cancelPending = true;
    this.cancelRequested = true;
    return this.generation;
  }

  acceptAuthoritativeJob(job: SearchJob, progressRevision: NonNullable<ProgressRevisionState>) {
    this.jobVersion = job.stateVersion;
    this.progressRevision = progressRevision;
    this.job = job;
  }

  attachSocket(socket: SearchWebSocketClient) {
    this.socket = socket;
  }

  captureLiveUpdateEpoch(): bigint {
    return this.liveUpdateEpoch;
  }

  finishCancel(generation: number) {
    if (this.generation !== generation) return;
    this.cancelPending = false;
    this.cancelRequested = false;
  }

  isCurrent(generation: number, searchJobId?: string): boolean {
    return this.generation === generation
      && (searchJobId === undefined || this.jobId === searchJobId);
  }

  liveUpdateEpochIs(epoch: bigint): boolean {
    return this.liveUpdateEpoch === epoch;
  }

  recordUnversionedLiveUpdate() {
    this.liveUpdateEpoch += 1n;
  }

  releaseSocket(socket: SearchWebSocketClient | null) {
    if (this.socket === socket) this.socket = null;
  }

  launchIsLocked(): boolean {
    return this.launchLocked;
  }

  lockLaunch() {
    this.launchLocked = true;
    window.setTimeout(() => {
      this.launchLocked = false;
    }, 0);
  }

  supersede(): number {
    return this.beginGeneration();
  }

  stopLiveUpdates() {
    this.socket?.dispose();
    this.socket = null;
  }

  clearJob() {
    this.jobId = null;
    this.job = null;
    this.jobVersion = 0n;
    this.liveUpdateEpoch = 0n;
  }

  resetProgress() {
    this.progressRevision = null;
  }

  abortRequest() {
    this.abortController?.abort();
  }

  cancelIsPending(): boolean {
    return this.cancelPending;
  }

  cancelWasRequested(): boolean {
    return this.cancelRequested;
  }

  currentGeneration(): number {
    return this.generation;
  }

  currentJob(): SearchJob | null {
    return this.job;
  }

  currentJobId(): string | null {
    return this.jobId;
  }

  currentJobVersion(): bigint {
    return this.jobVersion;
  }

  currentPreview(): LivePreviewSnapshot | null {
    return this.preview;
  }

  currentPreviewRowLimit(): number {
    return this.previewRowLimit;
  }

  currentPreviewStatus(): RunningPreviewStatus {
    return this.previewStatus;
  }

  currentProgressRevision(): ProgressRevisionState {
    return this.progressRevision;
  }

  previewSchema(schemaId: string): ResultSchema | undefined {
    return this.previewSchemas.get(schemaId);
  }

  recordJobId(jobId: string | null) {
    this.jobId = jobId;
  }

  recordJobVersion(version: bigint) {
    this.jobVersion = version;
  }

  recordPreview(preview: LivePreviewSnapshot | null) {
    this.preview = preview;
  }

  recordPreviewRowLimit(limit: number) {
    this.previewRowLimit = limit;
  }

  recordPreviewSchema(schema: ResultSchema) {
    this.previewSchemas.set(schema.schemaId, schema);
  }

  recordPreviewStatus(status: RunningPreviewStatus) {
    this.previewStatus = status;
  }

  recordProgressRevision(revision: ProgressRevisionState) {
    this.progressRevision = revision;
  }

  releaseRequest(controller: AbortController) {
    if (this.abortController === controller) this.abortController = null;
  }

  replaceRequest(controller: AbortController) {
    this.abortController = controller;
  }

  resetPreview() {
    this.preview = null;
    this.previewRowLimit = 0;
    this.previewSchemas.clear();
    this.previewStatus = "disabled";
  }

  clearPreviewSchemas() {
    this.previewSchemas.clear();
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
