import type { ResultSchema } from "@/gen/ts/open_splunk/result";
import type { SearchJob } from "@/gen/ts/open_splunk/search";
import type { SearchWebSocketClient } from "@/lib/api";

import type { LivePreviewSnapshot } from "./live-preview";
import {
  isVersionedSearchRevision,
  reconcileSearchProgress,
  type ProgressDecision,
  type ProgressRevisionState,
  type SearchProgressSource,
} from "./progress-revision";

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
    this.abortRequest();
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

  adoptAuthoritativeJob(
    generation: number,
    job: SearchJob,
  ): NonNullable<ProgressRevisionState> {
    if (!this.canAdoptJob(generation, job.searchJobId)) {
      throw new DOMException("Search was superseded.", "AbortError");
    }
    if (!isVersionedSearchRevision(job.stateVersion)) {
      throw new Error("The server returned a search job without a valid state revision.");
    }
    if (
      job.stateVersion < this.jobVersion
      || (
        this.progressRevision !== null
        && job.stateVersion < this.progressRevision.revision
      )
    ) {
      throw new Error("The search job snapshot was older than the applied live state.");
    }
    if (job.progress === undefined) {
      throw new Error("The server returned a search job without progress.");
    }
    const decision = reconcileSearchProgress(
      this.progressRevision,
      job.progress,
      { kind: "authoritative", envelopeRevision: job.stateVersion },
    );
    if (decision.kind === "ignore") {
      throw new Error("The search job progress was older than the applied live progress.");
    }
    if (decision.kind === "recover") {
      throw new Error(`The server returned inconsistent search progress (${decision.reason}).`);
    }
    this.jobId = job.searchJobId;
    this.jobVersion = job.stateVersion;
    this.progressRevision = decision.state;
    this.job = job;
    return decision.state;
  }

  replaceSocket(socket: SearchWebSocketClient) {
    const replaced = this.socket;
    this.socket = socket;
    if (replaced !== socket) replaced?.dispose();
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

  advanceLiveUpdateEpoch() {
    this.liveUpdateEpoch += 1n;
  }

  disposeSocket(socket: SearchWebSocketClient | null) {
    if (this.socket !== socket) return;
    this.socket = null;
    socket?.dispose();
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
    this.disposeSocket(this.socket);
  }

  clearJob() {
    this.jobId = null;
    this.job = null;
    this.jobVersion = 0n;
    this.liveUpdateEpoch = 0n;
    this.progressRevision = null;
  }

  abortRequest() {
    const controller = this.abortController;
    this.abortController = null;
    controller?.abort();
  }

  cancelIsPending(): boolean {
    return this.cancelPending;
  }

  cancelWasRequested(): boolean {
    return this.cancelRequested;
  }

  generationSnapshot(): number {
    return this.generation;
  }

  jobSnapshot() {
    return {
      id: this.jobId,
      job: this.job,
      version: this.jobVersion,
    } as const;
  }

  previewSchema(schemaId: string): ResultSchema | undefined {
    return this.previewSchemas.get(schemaId);
  }

  previewSnapshot() {
    return {
      rowLimit: this.previewRowLimit,
      snapshot: this.preview,
      status: this.previewStatus,
    } as const;
  }

  private canAdoptJob(generation: number, searchJobId: string): boolean {
    return this.generation === generation
      && (this.jobId === null || this.jobId === searchJobId);
  }

  applyPreview(preview: LivePreviewSnapshot, status: RunningPreviewStatus) {
    this.preview = preview;
    this.previewStatus = status;
  }

  configurePreview(limit: number) {
    this.previewRowLimit = limit;
  }

  registerPreviewSchema(schema: ResultSchema) {
    this.previewSchemas.set(schema.schemaId, schema);
  }

  transitionPreview(status: RunningPreviewStatus) {
    this.previewStatus = status;
  }

  clearPreview(status: RunningPreviewStatus) {
    this.preview = null;
    this.previewStatus = status;
  }

  reconcileProgress(
    progress: Parameters<typeof reconcileSearchProgress>[1],
    source: SearchProgressSource,
  ): ProgressDecision {
    const decision = reconcileSearchProgress(this.progressRevision, progress, source);
    if (decision.kind === "apply") this.progressRevision = decision.state;
    return decision;
  }

  reconcileLiveJobVersion(version: bigint): "stale" | "current" | "advanced" {
    if (version < this.jobVersion) return "stale";
    if (version === this.jobVersion) return "current";
    this.jobVersion = version;
    return "advanced";
  }

  releaseRequest(controller: AbortController) {
    if (this.abortController === controller) this.abortController = null;
  }

  replaceRequest(controller: AbortController) {
    if (this.abortController !== controller) this.abortController?.abort();
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
