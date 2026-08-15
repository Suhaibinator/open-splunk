import {
  type JobTarget,
  type ResynchronizationRequired,
  ResynchronizationReason,
  SearchWebSocketCommand,
  type SearchWebSocketEvent,
  SearchWebSocketEvent as SearchWebSocketEventCodec,
  type SearchWebSocketProtocolError,
  SearchWebSocketProtocolErrorCode,
} from "@/gen/ts/open_splunk/v1/search_ws";

export const DEFAULT_SEARCH_WEBSOCKET_PATH = "/api/v1/search/ws";

const SOCKET_CONNECTING = 0;
const SOCKET_OPEN = 1;
const MAXIMUM_UINT64 = (1n << 64n) - 1n;
const APPLICATION_STABILITY_DELAY_MS = 1_000;
const DEFAULT_MAXIMUM_INBOUND_FRAMES = 64;
const DEFAULT_MAXIMUM_INBOUND_BYTES = 4 << 20;
const INBOUND_QUEUE_COMPACTION_HEAD = 64;

export type SearchWebSocketConnectionState =
  | "idle"
  | "connecting"
  | "open"
  | "reconnecting"
  | "closed";

export type WebSocketFactory = (url: string) => WebSocket;
export type SearchWebSocketEventListener = (event: SearchWebSocketEvent) => void | Promise<void>;
export type SearchWebSocketErrorListener = (error: Error) => void;
export type SearchWebSocketStateListener = (state: SearchWebSocketConnectionState) => void;
export type SearchWebSocketProtocolErrorListener = (error: SearchWebSocketProtocolError) => void;

export interface SearchWebSocketSequenceGap {
  readonly target: JobTarget;
  readonly targetKey: string;
  readonly lastSequence: bigint;
  readonly expectedSequence: bigint;
  readonly receivedSequence: bigint;
}

export type SearchWebSocketSequenceGapListener = (gap: SearchWebSocketSequenceGap) => void;

export interface SearchWebSocketResynchronizationNotice {
  readonly event: SearchWebSocketEvent;
  readonly required: ResynchronizationRequired;
  readonly target: JobTarget;
  readonly targetKey: string;
  readonly lastSequence: bigint;
  /**
   * The client keeps the target suspended until a recovery listener explicitly
   * acknowledges a safe boundary. Call this only after restoring authoritative
   * state. Omission preserves the prior checkpoint and retries recovery.
   */
  acknowledge(sequence?: bigint): void;
}

export type SearchWebSocketResynchronizationListener = (
  notice: SearchWebSocketResynchronizationNotice,
) => void | Promise<void>;

export interface SearchWebSocketClientOptions {
  /** Usually supplied by GetSystemBootstrapResponse.searchWebsocketPath. */
  readonly path?: string;
  /** Browser defaults to the current origin. A full HTTP(S)/WS(S) URL supports local development. */
  readonly baseUrl?: string;
  readonly autoReconnect?: boolean;
  readonly reconnectDelayMs?: number;
  readonly maximumReconnectDelayMs?: number;
  /** Maximum received frames retained across the active listener and pending backlog. */
  readonly maximumInboundFrames?: number;
  /** Maximum received binary bytes retained across the active listener and pending backlog. */
  readonly maximumInboundBytes?: number;
  readonly webSocketFactory?: WebSocketFactory;
}

export interface SearchWebSocketSubscriptionOptions {
  readonly subscriptionId?: string;
  readonly includePreviews?: boolean;
  readonly previewRowLimit?: number;
}

interface ActiveSubscription {
  readonly subscriptionId: string;
  readonly target: JobTarget;
  readonly includePreviews: boolean;
  readonly previewRowLimit?: number;
}

type RetainedBinaryFrame = Uint8Array | Blob;

interface InboundFrame {
  readonly data: RetainedBinaryFrame;
  readonly bytes: number;
}

interface InboundGeneration {
  readonly socket: WebSocket;
  readonly frames: Array<InboundFrame | undefined>;
  frameHead: number;
  invalidated: boolean;
}

function assertIdentifier(value: string, label: string): void {
  if (value.trim().length === 0) {
    throw new TypeError(`${label} must not be empty`);
  }
}

export function searchJobTarget(searchJobId: string): JobTarget {
  assertIdentifier(searchJobId, "Search job ID");
  return { target: { $case: "searchJobId", value: searchJobId } };
}

export function exportJobTarget(exportJobId: string): JobTarget {
  assertIdentifier(exportJobId, "Export job ID");
  return { target: { $case: "exportJobId", value: exportJobId } };
}

export function searchWebSocketTargetKey(target: JobTarget): string {
  switch (target.target?.$case) {
    case "searchJobId":
      assertIdentifier(target.target.value, "Search job ID");
      return `search:${target.target.value}`;
    case "exportJobId":
      assertIdentifier(target.target.value, "Export job ID");
      return `export:${target.target.value}`;
    default:
      throw new TypeError("Search WebSocket target is missing its search or export job ID");
  }
}

function cloneTarget(target: JobTarget): JobTarget {
  switch (target.target?.$case) {
    case "searchJobId":
      return searchJobTarget(target.target.value);
    case "exportJobId":
      return exportJobTarget(target.target.value);
    default:
      throw new TypeError("Search WebSocket target is missing its search or export job ID");
  }
}

function defaultWebSocketFactory(url: string): WebSocket {
  const WebSocketConstructor = globalThis.WebSocket;
  if (typeof WebSocketConstructor !== "function") {
    throw new Error("WebSocket is unavailable in this runtime");
  }
  return new WebSocketConstructor(url);
}

function normalizeError(error: unknown, fallback: string): Error {
  if (error instanceof Error) {
    return error;
  }
  return new Error(fallback, { cause: error });
}

function eventError(event: Event): Error {
  if (typeof ErrorEvent !== "undefined" && event instanceof ErrorEvent && event.message.length > 0) {
    return new Error(event.message, { cause: event.error });
  }
  return new Error("Search WebSocket connection error");
}

function randomIdentifier(prefix: string, counter: bigint): string {
  const randomUUID = globalThis.crypto?.randomUUID;
  if (typeof randomUUID === "function") {
    return `${prefix}-${randomUUID.call(globalThis.crypto)}`;
  }
  return `${prefix}-${Date.now().toString(36)}-${counter.toString(36)}`;
}

function validateReconnectDelay(value: number, label: string): void {
  if (!Number.isFinite(value) || value < 0) {
    throw new RangeError(`${label} must be a non-negative number of milliseconds`);
  }
}

function validatePositiveSafeInteger(value: number, label: string): void {
  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new RangeError(`${label} must be a positive safe integer`);
  }
}

function isSubscriptionScopedEvent(event: SearchWebSocketEvent): boolean {
  switch (event.payload?.$case) {
    case "searchStateChanged":
    case "searchProgress":
    case "resultSchemaAvailable":
    case "resultPreview":
    case "searchTerminal":
    case "exportStateChanged":
    case "exportProgress":
    case "exportTerminal":
    case "warning":
    case "resynchronizationRequired":
      return true;
    default:
      return false;
  }
}

function isUnknownTargetEvent(event: SearchWebSocketEvent): boolean {
  return event.payload === undefined && event.sequence > 0n;
}

function isTargetEvent(event: SearchWebSocketEvent): boolean {
  return isSubscriptionScopedEvent(event) || isUnknownTargetEvent(event);
}

function validateEventEnvelope(event: SearchWebSocketEvent): string | undefined {
  const payload = event.payload?.$case;
  if (payload === undefined) {
    // ts-proto intentionally discards unknown oneof fields. Preserve protocol
    // forward compatibility by treating a positively sequenced, fully routed
    // unknown variant as target-scoped opaque progress. Sequence-zero unknown
    // controls are likewise safe for generic observers to ignore.
    if (!isUnknownTargetEvent(event)) return undefined;
    return event.subscriptionId && event.target
      ? undefined
      : "Search WebSocket unknown target event is missing routing metadata";
  }
  if (isSubscriptionScopedEvent(event)) {
    if (payload === "resynchronizationRequired") {
      return event.sequence === 0n
        ? undefined
        : "Search WebSocket resynchronization control event has a target sequence";
    }
    return event.sequence > 0n
      ? undefined
      : "Search WebSocket target event has no positive sequence";
  }
  return event.sequence === 0n
    ? undefined
    : "Search WebSocket control event has an unexpected target sequence";
}

function payloadTarget(event: SearchWebSocketEvent): JobTarget | undefined {
  switch (event.payload?.$case) {
    case "searchStateChanged":
      return searchJobTarget(event.payload.value.searchJobId);
    case "resultSchemaAvailable":
      return searchJobTarget(event.payload.value.searchJobId);
    case "resultPreview":
      return searchJobTarget(event.payload.value.searchJobId);
    case "searchTerminal":
      return searchJobTarget(event.payload.value.searchJobId);
    case "exportStateChanged":
      return exportJobTarget(event.payload.value.exportJobId);
    case "exportTerminal":
      return exportJobTarget(event.payload.value.exportJobId);
    case "warning":
      return event.payload.value.target;
    case "resynchronizationRequired":
      return event.payload.value.target;
    default:
      return undefined;
  }
}

function eventMatchesSubscriptionTarget(
  event: SearchWebSocketEvent,
  subscription: ActiveSubscription,
): boolean {
  const expected = searchWebSocketTargetKey(subscription.target);
  const candidates = [event.target, payloadTarget(event)].filter(
    (target): target is JobTarget => target !== undefined,
  );
  // Progress messages carry no embedded ID, so their outer target is the only
  // routing proof. Other payloads are checked against both when both exist.
  if (candidates.length === 0) return false;
  try {
    return candidates.every((target) => searchWebSocketTargetKey(target) === expected);
  } catch {
    return false;
  }
}

function assertUint64Sequence(sequence: bigint): void {
  if (sequence < 0n) {
    throw new RangeError("Search WebSocket sequence must not be negative");
  }
  if (sequence > MAXIMUM_UINT64) {
    throw new RangeError("Search WebSocket sequence must fit protobuf uint64");
  }
}

export function resolveSearchWebSocketUrl(path: string, baseUrl?: string): string {
  if (/^wss?:\/\//i.test(path)) {
    return path;
  }

  let url: URL;
  if (/^[a-z][a-z\d+.-]*:\/\//i.test(path)) {
    url = new URL(path);
  } else {
    let base = baseUrl;
    if (!base) {
      if (typeof window === "undefined") {
        throw new Error("A base URL is required to connect to the search WebSocket outside a browser");
      }
      base = window.location.href;
    } else if (!/^[a-z][a-z\d+.-]*:\/\//i.test(base)) {
      if (typeof window === "undefined") {
        throw new Error("A relative WebSocket base URL cannot be resolved outside a browser");
      }
      base = new URL(base, window.location.href).toString();
    }
    url = new URL(path, base);
  }

  if (url.protocol === "http:") {
    url.protocol = "ws:";
  } else if (url.protocol === "https:") {
    url.protocol = "wss:";
  } else if (url.protocol !== "ws:" && url.protocol !== "wss:") {
    throw new TypeError(`Unsupported Search WebSocket URL protocol: ${url.protocol}`);
  }
  return url.toString();
}

function retainBinaryFrame(data: unknown, maximumBytes: number): InboundFrame | undefined {
  if (data instanceof ArrayBuffer) {
    if (data.byteLength > maximumBytes) return undefined;
    const owned = new Uint8Array(data).slice();
    return { data: owned, bytes: owned.byteLength };
  }
  if (ArrayBuffer.isView(data)) {
    if (data.byteLength > maximumBytes) return undefined;
    // A small view can otherwise retain an arbitrarily large or resizable
    // backing buffer while being charged only for the visible slice.
    const owned = new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice();
    return { data: owned, bytes: owned.byteLength };
  }
  if (typeof Blob !== "undefined" && data instanceof Blob) {
    if (data.size > maximumBytes) return undefined;
    return { data, bytes: data.size };
  }
  if (typeof data === "string") {
    throw new TypeError("Search WebSocket rejected a text frame; binary protobuf is required");
  }
  throw new TypeError("Search WebSocket received an unsupported frame type");
}

async function readBinaryFrame(data: RetainedBinaryFrame): Promise<Uint8Array> {
  if (data instanceof Uint8Array) {
    return data;
  }
  return new Uint8Array(await data.arrayBuffer());
}

/**
 * Binary, resumable client for the Search WebSocket protocol.
 * Sequence numbers remain bigint and are scoped independently to each job target.
 */
export class SearchWebSocketClient {
  private readonly path: string;
  private readonly baseUrl?: string;
  private readonly autoReconnect: boolean;
  private readonly reconnectDelayMs: number;
  private readonly maximumReconnectDelayMs: number;
  private readonly maximumInboundFrames: number;
  private readonly maximumInboundBytes: number;
  private readonly webSocketFactory: WebSocketFactory;

  private socket?: WebSocket;
  private state: SearchWebSocketConnectionState = "idle";
  private connectionPromise?: Promise<void>;
  private connectionSocket?: WebSocket;
  private rejectConnection?: (reason?: unknown) => void;
  private reconnectTimer?: ReturnType<typeof globalThis.setTimeout>;
  private applicationStabilityTimer?: ReturnType<typeof globalThis.setTimeout>;
  private reconnectAttempt = 0;
  private intentionallyClosed = false;
  private identifierCounter = 0n;
  private inboundGeneration?: InboundGeneration;
  private activeInboundGeneration?: InboundGeneration;
  private retainedInboundFrames = 0;
  private retainedInboundBytes = 0;
  private reconnectDeferredForInbound = false;

  private readonly subscriptions = new Map<string, ActiveSubscription>();
  private readonly usedSubscriptionIds = new Set<string>();
  private readonly pendingSubscribeRequests = new Map<string, readonly ActiveSubscription[]>();
  private readonly lastSequences = new Map<string, bigint>();
  private readonly suspendedTargets = new Set<string>();
  private readonly eventListeners = new Set<SearchWebSocketEventListener>();
  private readonly resynchronizationListeners = new Set<SearchWebSocketResynchronizationListener>();
  private readonly protocolErrorListeners = new Set<SearchWebSocketProtocolErrorListener>();
  private readonly sequenceGapListeners = new Set<SearchWebSocketSequenceGapListener>();
  private readonly errorListeners = new Set<SearchWebSocketErrorListener>();
  private readonly stateListeners = new Set<SearchWebSocketStateListener>();

  public constructor(options: SearchWebSocketClientOptions = {}) {
    this.path = options.path ?? DEFAULT_SEARCH_WEBSOCKET_PATH;
    this.baseUrl = options.baseUrl;
    this.autoReconnect = options.autoReconnect ?? true;
    this.reconnectDelayMs = options.reconnectDelayMs ?? 750;
    this.maximumReconnectDelayMs = options.maximumReconnectDelayMs ?? 15_000;
    this.maximumInboundFrames = options.maximumInboundFrames ?? DEFAULT_MAXIMUM_INBOUND_FRAMES;
    this.maximumInboundBytes = options.maximumInboundBytes ?? DEFAULT_MAXIMUM_INBOUND_BYTES;
    this.webSocketFactory = options.webSocketFactory ?? defaultWebSocketFactory;

    validateReconnectDelay(this.reconnectDelayMs, "Reconnect delay");
    validateReconnectDelay(this.maximumReconnectDelayMs, "Maximum reconnect delay");
    validatePositiveSafeInteger(this.maximumInboundFrames, "Maximum inbound frame count");
    validatePositiveSafeInteger(this.maximumInboundBytes, "Maximum inbound byte count");
    if (this.maximumReconnectDelayMs < this.reconnectDelayMs) {
      throw new RangeError("Maximum reconnect delay must be greater than or equal to reconnect delay");
    }
  }

  public get connectionState(): SearchWebSocketConnectionState {
    return this.state;
  }

  public get connected(): boolean {
    return this.socket?.readyState === SOCKET_OPEN;
  }

  public connect(): Promise<void> {
    if (this.socket?.readyState === SOCKET_OPEN) {
      return Promise.resolve();
    }
    if (this.socket?.readyState === SOCKET_CONNECTING && this.connectionPromise) {
      return this.connectionPromise;
    }

    const previousSocket = this.socket;
    this.invalidateInboundGeneration(this.inboundGeneration);
    this.socket = undefined;
    this.connectionPromise = undefined;
    if (this.connectionSocket === previousSocket && this.rejectConnection) {
      this.rejectConnection(new Error("Search WebSocket was replaced before connecting"));
    }
    this.connectionSocket = undefined;
    this.rejectConnection = undefined;
    this.pendingSubscribeRequests.clear();
    this.clearApplicationStabilityTimer();
    this.intentionallyClosed = false;
    this.clearReconnectTimer();
    this.setState(this.reconnectAttempt > 0 ? "reconnecting" : "connecting");

    let socket: WebSocket;
    try {
      const url = resolveSearchWebSocketUrl(this.path, this.baseUrl);
      socket = this.webSocketFactory(url);
      socket.binaryType = "arraybuffer";
      this.socket = socket;
    } catch (error) {
      const normalized = normalizeError(error, "Unable to create the Search WebSocket");
      this.setState("closed");
      this.emitError(normalized);
      this.scheduleReconnect();
      return Promise.reject(normalized);
    }
    const inboundGeneration: InboundGeneration = {
      socket,
      frames: [],
      frameHead: 0,
      invalidated: false,
    };
    this.inboundGeneration = inboundGeneration;

    this.connectionPromise = new Promise<void>((resolve, reject) => {
      this.connectionSocket = socket;
      this.rejectConnection = reject;

      socket.addEventListener("open", () => {
        if (this.socket !== socket) {
          return;
        }
        if (this.subscriptions.size === 0) {
          this.reconnectAttempt = 0;
        }
        this.connectionPromise = undefined;
        this.connectionSocket = undefined;
        this.rejectConnection = undefined;
        this.setState("open");
        resolve();
        this.sendActiveSubscriptions(socket);
      });

      socket.addEventListener("message", (message) => {
        if (this.socket !== socket) {
          return;
        }
        this.enqueueInboundFrame(inboundGeneration, message.data);
      });

      socket.addEventListener("error", (event) => {
        if (this.socket !== socket) {
          return;
        }
        const error = eventError(event);
        this.emitError(error);
        if (socket.readyState === SOCKET_CONNECTING) {
          this.connectionPromise = undefined;
          this.connectionSocket = undefined;
          this.rejectConnection = undefined;
          reject(error);
        }
      });

      socket.addEventListener("close", () => {
        this.invalidateInboundGeneration(inboundGeneration);
        if (this.socket !== socket) {
          return;
        }
        this.clearApplicationStabilityTimer();
        this.socket = undefined;
        this.connectionPromise = undefined;
        if (this.connectionSocket === socket && this.rejectConnection) {
          this.rejectConnection(new Error("Search WebSocket closed before connecting"));
        }
        this.connectionSocket = undefined;
        this.rejectConnection = undefined;
        this.pendingSubscribeRequests.clear();
        this.setState("closed");
        this.scheduleReconnect();
      });
    });

    return this.connectionPromise;
  }

  /** Stops reconnecting while retaining subscriptions and sequence checkpoints. */
  public disconnect(code = 1000, reason = "Client disconnected"): void {
    this.intentionallyClosed = true;
    this.clearReconnectTimer();
    this.clearApplicationStabilityTimer();
    const socket = this.socket;
    this.invalidateInboundGeneration(this.inboundGeneration);
    this.socket = undefined;
    this.connectionPromise = undefined;
    if (this.connectionSocket === socket && this.rejectConnection) {
      this.rejectConnection(new Error(reason));
    }
    this.connectionSocket = undefined;
    this.rejectConnection = undefined;
    this.pendingSubscribeRequests.clear();
    if (socket && (socket.readyState === SOCKET_CONNECTING || socket.readyState === SOCKET_OPEN)) {
      try {
        socket.close(code, reason);
      } catch (error) {
        this.emitError(normalizeError(error, "Unable to close the Search WebSocket"));
      }
    }
    this.setState("closed");
  }

  /** Fully releases the client and its retained replay checkpoints. */
  public dispose(): void {
    this.disconnect(1000, "Client disposed");
    this.subscriptions.clear();
    this.usedSubscriptionIds.clear();
    this.pendingSubscribeRequests.clear();
    this.lastSequences.clear();
    this.suspendedTargets.clear();
    this.eventListeners.clear();
    this.resynchronizationListeners.clear();
    this.protocolErrorListeners.clear();
    this.sequenceGapListeners.clear();
    this.errorListeners.clear();
    this.stateListeners.clear();
  }

  public subscribe(target: JobTarget, options: SearchWebSocketSubscriptionOptions = {}): string {
    const normalizedTarget = cloneTarget(target);
    const subscriptionId = options.subscriptionId ?? this.nextIdentifier("subscription");
    assertIdentifier(subscriptionId, "Subscription ID");
    if (this.usedSubscriptionIds.has(subscriptionId)) {
      throw new Error(`Search WebSocket subscription ID was already used: ${subscriptionId}`);
    }
    if (
      options.previewRowLimit !== undefined
      && (!Number.isInteger(options.previewRowLimit)
        || options.previewRowLimit <= 0
        || options.previewRowLimit > 0xffff_ffff)
    ) {
      throw new RangeError("Preview row limit must be an integer between 1 and 4294967295");
    }

    const searchTarget = normalizedTarget.target?.$case === "searchJobId";
    const includePreviews = options.includePreviews ?? searchTarget;
    if (!searchTarget && includePreviews) {
      throw new TypeError("Search WebSocket previews require a search-job target");
    }
    if (options.previewRowLimit !== undefined && !includePreviews) {
      throw new TypeError("Preview row limit requires preview inclusion");
    }

    const targetKey = searchWebSocketTargetKey(normalizedTarget);
    for (const active of this.subscriptions.values()) {
      if (searchWebSocketTargetKey(active.target) === targetKey) {
        throw new Error(`Search WebSocket target already has an active subscription: ${targetKey}`);
      }
    }

    const subscription: ActiveSubscription = {
      subscriptionId,
      target: normalizedTarget,
      includePreviews,
      previewRowLimit: options.previewRowLimit,
    };
    this.usedSubscriptionIds.add(subscriptionId);
    this.subscriptions.set(subscriptionId, subscription);

    const sourceSocket = this.socket;
    if (sourceSocket?.readyState === SOCKET_OPEN) {
      if (!this.sendSubscriptions([subscription])) {
        this.restartForReplay(sourceSocket);
      }
    } else {
      void this.connect().catch(() => {
        // Connection errors are delivered through onError; reconnect remains automatic.
      });
    }
    return subscriptionId;
  }

  public unsubscribe(subscriptionId: string): boolean {
    const subscription = this.subscriptions.get(subscriptionId);
    if (subscription === undefined) {
      return false;
    }
    this.subscriptions.delete(subscriptionId);
    // A recovery callback for this subscription may still be in flight. Its
    // completion is fenced by object identity; clear the target suspension so
    // a replacement subscription can replay from the last committed sequence.
    this.suspendedTargets.delete(searchWebSocketTargetKey(subscription.target));
    const sourceSocket = this.socket;
    if (sourceSocket?.readyState === SOCKET_OPEN) {
      if (!this.sendCommand({
        requestId: this.nextIdentifier("unsubscribe"),
        payload: {
          $case: "unsubscribe",
          value: { subscriptionIds: [subscriptionId] },
        },
      })) {
        this.restartForReplay(sourceSocket);
      }
    }
    return true;
  }

  public getLastSequence(target: JobTarget): bigint {
    return this.lastSequences.get(searchWebSocketTargetKey(target)) ?? 0n;
  }

  /** Seeds or advances a replay checkpoint; checkpoints are never allowed to move backward. */
  public setLastSequence(target: JobTarget, sequence: bigint): void {
    assertUint64Sequence(sequence);
    const key = searchWebSocketTargetKey(target);
    const previous = this.lastSequences.get(key) ?? 0n;
    if (sequence < previous) {
      throw new RangeError(`Search WebSocket sequence for ${key} cannot move backward`);
    }
    this.lastSequences.set(key, sequence);
    this.suspendedTargets.delete(key);
  }

  public onEvent(listener: SearchWebSocketEventListener): () => void {
    this.eventListeners.add(listener);
    return () => this.eventListeners.delete(listener);
  }

  public onResynchronizationRequired(listener: SearchWebSocketResynchronizationListener): () => void {
    this.resynchronizationListeners.add(listener);
    return () => this.resynchronizationListeners.delete(listener);
  }

  public onProtocolError(listener: SearchWebSocketProtocolErrorListener): () => void {
    this.protocolErrorListeners.add(listener);
    return () => this.protocolErrorListeners.delete(listener);
  }

  public onSequenceGap(listener: SearchWebSocketSequenceGapListener): () => void {
    this.sequenceGapListeners.add(listener);
    return () => this.sequenceGapListeners.delete(listener);
  }

  public onError(listener: SearchWebSocketErrorListener): () => void {
    this.errorListeners.add(listener);
    return () => this.errorListeners.delete(listener);
  }

  public onConnectionStateChange(listener: SearchWebSocketStateListener): () => void {
    this.stateListeners.add(listener);
    return () => this.stateListeners.delete(listener);
  }

  private enqueueInboundFrame(generation: InboundGeneration, data: unknown): void {
    if (
      generation.invalidated
      || this.inboundGeneration !== generation
      || this.socket !== generation.socket
    ) {
      return;
    }
    if (this.retainedInboundFrames >= this.maximumInboundFrames) {
      this.handleInboundPressure(generation.socket);
      return;
    }
    let frame: InboundFrame | undefined;
    try {
      frame = retainBinaryFrame(data, this.maximumInboundBytes - this.retainedInboundBytes);
    } catch (error) {
      this.emitError(normalizeError(error, "Unable to inspect a Search WebSocket frame"));
      this.restartForReplay(generation.socket);
      return;
    }
    if (frame === undefined) {
      this.handleInboundPressure(generation.socket);
      return;
    }

    generation.frames.push(frame);
    this.retainedInboundFrames += 1;
    this.retainedInboundBytes += frame.bytes;
    this.scheduleInboundDrain(generation);
  }

  private handleInboundPressure(socket: WebSocket): void {
    this.emitError(
      new Error(
        `Search WebSocket inbound backlog exceeded ${this.maximumInboundFrames} frames or ${this.maximumInboundBytes} bytes`,
      ),
    );
    this.restartForReplay(socket, "Inbound backlog exceeded; replay required");
  }

  private scheduleInboundDrain(generation: InboundGeneration): void {
    if (
      generation.invalidated
      || generation.frameHead === generation.frames.length
      || this.activeInboundGeneration !== undefined
    ) {
      return;
    }
    this.activeInboundGeneration = generation;
    void this.drainInboundFrames(generation);
  }

  private async drainInboundFrames(generation: InboundGeneration): Promise<void> {
    try {
      while (!generation.invalidated) {
        const frame = generation.frames[generation.frameHead];
        if (frame === undefined) {
          return;
        }
        generation.frames[generation.frameHead] = undefined;
        generation.frameHead += 1;
        if (generation.frameHead === generation.frames.length) {
          generation.frames.length = 0;
          generation.frameHead = 0;
        } else if (
          generation.frameHead >= INBOUND_QUEUE_COMPACTION_HEAD
          && generation.frameHead * 2 >= generation.frames.length
        ) {
          const pending = generation.frames.length - generation.frameHead;
          generation.frames.copyWithin(0, generation.frameHead);
          generation.frames.length = pending;
          generation.frameHead = 0;
        }
        try {
          // Inbound checkpoints require strict wire order; parallel listeners could commit a later frame first.
          // oxlint-disable-next-line no-await-in-loop
          await this.handleMessage(frame.data, generation.socket);
        } catch (error) {
          this.emitError(normalizeError(error, "Unable to process a Search WebSocket frame"));
          this.restartForReplay(generation.socket);
        } finally {
          this.retainedInboundFrames -= 1;
          this.retainedInboundBytes -= frame.bytes;
        }
      }
    } finally {
      if (this.activeInboundGeneration === generation) {
        this.activeInboundGeneration = undefined;
      }
      const current = this.inboundGeneration;
      if (current !== undefined) {
        this.reconnectDeferredForInbound = false;
        this.scheduleInboundDrain(current);
      } else if (this.reconnectDeferredForInbound) {
        this.reconnectDeferredForInbound = false;
        this.scheduleReconnect();
      }
    }
  }

  private invalidateInboundGeneration(generation: InboundGeneration | undefined): void {
    if (generation === undefined || generation.invalidated) {
      return;
    }
    generation.invalidated = true;
    for (let index = generation.frameHead; index < generation.frames.length; index += 1) {
      const frame = generation.frames[index];
      if (frame !== undefined) {
        this.retainedInboundFrames -= 1;
        this.retainedInboundBytes -= frame.bytes;
        generation.frames[index] = undefined;
      }
    }
    generation.frames.length = 0;
    generation.frameHead = 0;
    if (this.inboundGeneration === generation) {
      this.inboundGeneration = undefined;
    }
  }

  private async handleMessage(data: RetainedBinaryFrame, sourceSocket: WebSocket): Promise<void> {
    if (this.socket !== sourceSocket) {
      return;
    }
    const bytes = await readBinaryFrame(data);
    if (this.socket !== sourceSocket) {
      return;
    }
    const event = SearchWebSocketEventCodec.decode(bytes);
    const envelopeError = validateEventEnvelope(event);
    if (envelopeError !== undefined) {
      this.emitError(new Error(envelopeError));
      this.restartForReplay(sourceSocket);
      return;
    }
    let routedSubscription: ActiveSubscription | undefined;
    if (isTargetEvent(event)) {
      const subscriptionId = event.subscriptionId;
      if (!subscriptionId) {
        this.emitError(new Error("Search WebSocket target event is missing its subscription ID"));
        this.restartForReplay(sourceSocket);
        return;
      }
      routedSubscription = this.subscriptions.get(subscriptionId);
      if (!routedSubscription) {
        return;
      }
      if (!eventMatchesSubscriptionTarget(event, routedSubscription)) {
        this.emitError(
          new Error("Search WebSocket event does not match its subscription target"),
        );
        this.restartForReplay(sourceSocket);
        return;
      }
    }
    const target = routedSubscription
      ? cloneTarget(routedSubscription.target)
      : this.resolveEventTarget(event);
    const resynchronization = event.payload?.$case === "resynchronizationRequired"
      ? event.payload.value
      : undefined;
    if (
      resynchronization !== undefined
      && resynchronization.subscriptionId !== event.subscriptionId
    ) {
      this.emitError(
        new Error("Search WebSocket resynchronization payload has a mismatched subscription ID"),
      );
      this.restartForReplay(sourceSocket);
      return;
    }
    if (
      resynchronization !== undefined
      && resynchronization.earliestAvailableSequence > resynchronization.latestSequence
    ) {
      this.emitError(new Error("Search WebSocket resynchronization replay bounds are inverted"));
      this.restartForReplay(sourceSocket);
      return;
    }

    if (target && event.sequence > 0n && !resynchronization) {
      this.clearApplicationStabilityTimer();
      const targetKey = searchWebSocketTargetKey(target);
      if (this.suspendedTargets.has(targetKey)) {
        return;
      }
      const previous = this.lastSequences.get(targetKey) ?? 0n;
      if (event.sequence <= previous) {
        return;
      }
      if (previous > 0n && event.sequence > previous + 1n) {
        const gap: SearchWebSocketSequenceGap = {
          target,
          targetKey,
          lastSequence: previous,
          expectedSequence: previous + 1n,
          receivedSequence: event.sequence,
        };
        for (const listener of this.sequenceGapListeners) {
          try {
            listener(gap);
          } catch (error) {
            this.emitError(normalizeError(error, "Search WebSocket sequence-gap listener failed"));
          }
        }
        this.restartForReplay(sourceSocket);
        return;
      }

      if (!await this.emitEvent(event)) {
        this.restartForReplay(sourceSocket);
        return;
      }
      if (!this.isCurrentInboundContext(sourceSocket, routedSubscription)) {
        return;
      }
      const current = this.lastSequences.get(targetKey) ?? 0n;
      if (event.sequence > current) {
        this.lastSequences.set(targetKey, event.sequence);
      }
      this.markApplicationStable();
    } else {
      if (!await this.emitEvent(event)) {
        this.restartForReplay(sourceSocket);
        return;
      }
      if (!this.isCurrentInboundContext(sourceSocket, routedSubscription)) {
        return;
      }
    }

    this.reconcileSubscriptionCommand(event);
    if (event.payload?.$case === "protocolError") {
      for (const listener of this.protocolErrorListeners) {
        try {
          listener(event.payload.value);
        } catch (error) {
          this.emitError(normalizeError(error, "Search WebSocket protocol-error listener failed"));
        }
      }
    }

    if (resynchronization) {
      await this.emitResynchronization(
        event,
        resynchronization,
        target,
        routedSubscription,
        sourceSocket,
      );
    }
  }

  private async emitEvent(event: SearchWebSocketEvent): Promise<boolean> {
    const outcomes = await Promise.allSettled(
      Array.from(this.eventListeners, async (listener) => listener(event)),
    );
    let delivered = true;
    for (const outcome of outcomes) {
      if (outcome.status === "rejected") {
        delivered = false;
        this.emitError(normalizeError(outcome.reason, "Search WebSocket event listener failed"));
      }
    }
    return delivered;
  }

  private isCurrentInboundContext(
    sourceSocket: WebSocket,
    routedSubscription: ActiveSubscription | undefined,
  ): boolean {
    return this.socket === sourceSocket
      && (
        routedSubscription === undefined
        || this.subscriptions.get(routedSubscription.subscriptionId) === routedSubscription
      );
  }

  private async emitResynchronization(
    event: SearchWebSocketEvent,
    required: ResynchronizationRequired,
    resolvedTarget: JobTarget | undefined,
    routedSubscription: ActiveSubscription | undefined,
    sourceSocket: WebSocket,
  ): Promise<void> {
    const target = required.target ?? resolvedTarget;
    if (!target) {
      this.emitError(new Error("Resynchronization event did not identify a job target"));
      return;
    }
    const normalizedTarget = cloneTarget(target);
    const targetKey = searchWebSocketTargetKey(normalizedTarget);
    const lastSequence = this.lastSequences.get(targetKey) ?? 0n;
    this.clearApplicationStabilityTimer();
    if (
      required.reason !== ResynchronizationReason.RESYNCHRONIZATION_REASON_SERVER_RESTARTED
      && required.latestSequence < lastSequence
    ) {
      this.emitError(
        new Error(
          `Search WebSocket resynchronization for ${targetKey} cannot regress checkpoint ${lastSequence.toString()} to ${required.latestSequence.toString()}`,
        ),
      );
      this.restartForReplay(sourceSocket);
      return;
    }
    this.suspendedTargets.add(targetKey);
    let acknowledgedSequence: bigint | undefined;
    const acknowledge = (sequence = required.latestSequence) => {
      assertUint64Sequence(sequence);
      if (sequence < required.latestSequence) {
        throw new RangeError(
          `Resynchronization checkpoint for ${targetKey} cannot precede the server's latest sequence`,
        );
      }
      acknowledgedSequence = sequence;
    };
    const notice: SearchWebSocketResynchronizationNotice = {
      event,
      required,
      target: normalizedTarget,
      targetKey,
      lastSequence,
      acknowledge,
    };

    if (this.resynchronizationListeners.size === 0) {
      this.emitError(
        new Error(
          `Search WebSocket target ${targetKey} requires authoritative resynchronization; register an onResynchronizationRequired handler`,
        ),
      );
      this.restartForReplay(sourceSocket);
      return;
    }

    const outcomes = await Promise.allSettled(
      Array.from(
        this.resynchronizationListeners,
        async (listener) => listener(notice),
      ),
    );
    let recoveryFailed = false;
    for (const outcome of outcomes) {
      if (outcome.status === "rejected") {
        recoveryFailed = true;
        this.emitError(normalizeError(outcome.reason, "Search WebSocket resynchronization handler failed"));
      }
    }
    if (!this.isCurrentInboundContext(sourceSocket, routedSubscription)) {
      return;
    }
    if (recoveryFailed) {
      // Keep the old checkpoint and establish a fresh subscription. The server
      // will replay the same resynchronization requirement, which gives a
      // transient authoritative GET failure another bounded reconnect attempt
      // instead of leaving this target suspended forever on a live socket.
      this.restartForReplay(sourceSocket);
      return;
    }
    if (acknowledgedSequence === undefined) {
      this.emitError(
        new Error(`Search WebSocket recovery listener did not acknowledge ${targetKey}`),
      );
      this.restartForReplay(sourceSocket);
      return;
    }

    this.commitResynchronization(normalizedTarget, acknowledgedSequence);
  }

  /** A server restart may establish a new sequence epoch, so recovery can move this boundary backward. */
  private commitResynchronization(target: JobTarget, sequence: bigint): void {
    assertUint64Sequence(sequence);
    const targetKey = searchWebSocketTargetKey(target);
    this.lastSequences.set(targetKey, sequence);
    this.suspendedTargets.delete(targetKey);
    this.markApplicationStable();
  }

  private resolveEventTarget(event: SearchWebSocketEvent): JobTarget | undefined {
    if (event.target) {
      return cloneTarget(event.target);
    }
    if (event.payload?.$case === "resynchronizationRequired" && event.payload.value.target) {
      return cloneTarget(event.payload.value.target);
    }
    if (event.payload?.$case === "subscriptionAcknowledged" && event.payload.value.target) {
      return cloneTarget(event.payload.value.target);
    }
    if (event.subscriptionId) {
      const subscription = this.subscriptions.get(event.subscriptionId);
      return subscription ? cloneTarget(subscription.target) : undefined;
    }
    return undefined;
  }

  private sendActiveSubscriptions(sourceSocket: WebSocket): void {
    // Subscribe commands are atomic on the server. Keep reconnect requests
    // isolated so one expired or otherwise rejected target cannot evict every
    // valid local subscription carried in the same command.
    for (const subscription of this.subscriptions.values()) {
      if (this.socket !== sourceSocket) {
        return;
      }
      if (!this.sendSubscriptions([subscription])) {
        this.restartForReplay(sourceSocket);
        return;
      }
    }
  }

  private sendSubscriptions(subscriptions: readonly ActiveSubscription[]): boolean {
    if (subscriptions.length === 0) {
      return true;
    }
    const requestId = this.nextIdentifier("subscribe");
    if (this.sendCommand({
      requestId,
      payload: {
        $case: "subscribe",
        value: {
          subscriptions: subscriptions.map((subscription) => ({
            subscriptionId: subscription.subscriptionId,
            target: cloneTarget(subscription.target),
            afterSequence: this.getLastSequence(subscription.target),
            includePreviews: subscription.includePreviews,
            previewRowLimit: subscription.previewRowLimit,
          })),
        },
      },
    })) {
      this.pendingSubscribeRequests.set(requestId, subscriptions);
      return true;
    }
    return false;
  }

  private sendCommand(command: SearchWebSocketCommand): boolean {
    const socket = this.socket;
    if (!socket || socket.readyState !== SOCKET_OPEN) {
      return false;
    }
    try {
      socket.send(SearchWebSocketCommand.encode(command).finish());
      return true;
    } catch (error) {
      this.emitError(normalizeError(error, "Unable to send a Search WebSocket command"));
      return false;
    }
  }

  private reconcileSubscriptionCommand(event: SearchWebSocketEvent): void {
    if (event.payload?.$case === "subscriptionAcknowledged") {
      const acknowledged = event.payload.value;
      const pending = this.pendingSubscribeRequests.get(acknowledged.requestId);
      if (!pending) {
        return;
      }
      const remaining = pending.filter(
        (subscription) => subscription.subscriptionId !== acknowledged.subscriptionId,
      );
      if (remaining.length === 0) {
        this.pendingSubscribeRequests.delete(acknowledged.requestId);
      } else {
        this.pendingSubscribeRequests.set(acknowledged.requestId, remaining);
      }
      if (this.pendingSubscribeRequests.size === 0) {
        this.scheduleApplicationStabilityReset();
      }
      return;
    }
    if (event.payload?.$case !== "protocolError") {
      return;
    }
    const protocolError = event.payload.value;
    const rejected = this.pendingSubscribeRequests.get(protocolError.requestId);
    if (!rejected) {
      return;
    }
    this.pendingSubscribeRequests.delete(protocolError.requestId);
    if (
      protocolError.connectionWillClose
      && protocolError.code
        === SearchWebSocketProtocolErrorCode.SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS
    ) {
      // The server exhausted its bounded connection-lifetime identity set.
      // Preserve the rejected local subscription; the required close rotates
      // the socket and retries it against a fresh identity set.
      return;
    }
    for (const subscription of rejected) {
      if (this.subscriptions.get(subscription.subscriptionId) === subscription) {
        this.subscriptions.delete(subscription.subscriptionId);
      }
    }
    if (this.pendingSubscribeRequests.size === 0) {
      this.markApplicationStable();
    }
  }

  /** Drops the current generation without advancing checkpoints so retained events can replay. */
  private restartForReplay(
    expectedSocket?: WebSocket,
    closeReason = "Sequence gap; replay required",
  ): void {
    const socket = this.socket;
    if (!socket || (expectedSocket !== undefined && socket !== expectedSocket)) {
      return;
    }
    this.invalidateInboundGeneration(this.inboundGeneration);
    this.socket = undefined;
    this.connectionPromise = undefined;
    this.connectionSocket = undefined;
    this.rejectConnection = undefined;
    this.pendingSubscribeRequests.clear();
    this.clearApplicationStabilityTimer();
    try {
      socket.close(4000, closeReason);
    } catch (error) {
      this.emitError(normalizeError(error, "Unable to restart Search WebSocket replay"));
    }
    this.setState("closed");
    this.scheduleReconnect();
  }

  private scheduleReconnect(): void {
    if (!this.autoReconnect || this.intentionallyClosed || this.reconnectTimer !== undefined) {
      return;
    }
    if (this.activeInboundGeneration !== undefined) {
      this.reconnectDeferredForInbound = true;
      this.setState("reconnecting");
      return;
    }
    const exponent = Math.min(this.reconnectAttempt, 16);
    const delay = Math.min(this.reconnectDelayMs * 2 ** exponent, this.maximumReconnectDelayMs);
    this.reconnectAttempt += 1;
    this.setState("reconnecting");
    this.reconnectTimer = globalThis.setTimeout(() => {
      this.reconnectTimer = undefined;
      void this.connect().catch(() => {
        // The connection/close handlers notify listeners and schedule the next retry.
      });
    }, delay);
  }

  private clearReconnectTimer(): void {
    this.reconnectDeferredForInbound = false;
    if (this.reconnectTimer !== undefined) {
      globalThis.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
    }
  }

  private scheduleApplicationStabilityReset(): void {
    this.clearApplicationStabilityTimer();
    const socket = this.socket;
    if (!socket || socket.readyState !== SOCKET_OPEN) {
      return;
    }
    this.applicationStabilityTimer = globalThis.setTimeout(() => {
      this.applicationStabilityTimer = undefined;
      if (this.socket === socket && socket.readyState === SOCKET_OPEN) {
        this.reconnectAttempt = 0;
      }
    }, APPLICATION_STABILITY_DELAY_MS);
  }

  private markApplicationStable(): void {
    this.clearApplicationStabilityTimer();
    this.reconnectAttempt = 0;
  }

  private clearApplicationStabilityTimer(): void {
    if (this.applicationStabilityTimer !== undefined) {
      globalThis.clearTimeout(this.applicationStabilityTimer);
      this.applicationStabilityTimer = undefined;
    }
  }

  private nextIdentifier(prefix: string): string {
    this.identifierCounter += 1n;
    return randomIdentifier(prefix, this.identifierCounter);
  }

  private setState(state: SearchWebSocketConnectionState): void {
    if (this.state === state) {
      return;
    }
    this.state = state;
    for (const listener of this.stateListeners) {
      try {
        listener(state);
      } catch (error) {
        this.emitError(normalizeError(error, "Search WebSocket state listener failed"));
      }
    }
  }

  private emitError(error: Error): void {
    for (const listener of this.errorListeners) {
      try {
        listener(error);
      } catch {
        // Error listeners are terminal observers; one cannot safely report its own failure.
      }
    }
  }
}
