import {
  type JobTarget,
  type ResynchronizationRequired,
  SearchWebSocketCommand,
  type SearchWebSocketEvent,
  SearchWebSocketEvent as SearchWebSocketEventCodec,
  type SearchWebSocketProtocolError,
} from "@/gen/ts/open_splunk/v1/search_ws";

export const DEFAULT_SEARCH_WEBSOCKET_PATH = "/api/v1/search/ws";

const SOCKET_CONNECTING = 0;
const SOCKET_OPEN = 1;

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
   * The client commits latestSequence after every recovery listener resolves.
   * Call this after restoring authoritative state only when that state establishes
   * a newer explicit sequence boundary.
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

async function readBinaryFrame(data: unknown): Promise<Uint8Array> {
  if (data instanceof ArrayBuffer) {
    return new Uint8Array(data);
  }
  if (ArrayBuffer.isView(data)) {
    return new Uint8Array(data.buffer, data.byteOffset, data.byteLength);
  }
  if (typeof Blob !== "undefined" && data instanceof Blob) {
    return new Uint8Array(await data.arrayBuffer());
  }
  if (typeof data === "string") {
    throw new TypeError("Search WebSocket rejected a text frame; binary protobuf is required");
  }
  throw new TypeError("Search WebSocket received an unsupported frame type");
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
  private readonly webSocketFactory: WebSocketFactory;

  private socket?: WebSocket;
  private state: SearchWebSocketConnectionState = "idle";
  private connectionPromise?: Promise<void>;
  private connectionSocket?: WebSocket;
  private rejectConnection?: (reason?: unknown) => void;
  private reconnectTimer?: ReturnType<typeof globalThis.setTimeout>;
  private reconnectAttempt = 0;
  private intentionallyClosed = false;
  private identifierCounter = 0n;
  private messageChain: Promise<void> = Promise.resolve();

  private readonly subscriptions = new Map<string, ActiveSubscription>();
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
    this.webSocketFactory = options.webSocketFactory ?? defaultWebSocketFactory;

    validateReconnectDelay(this.reconnectDelayMs, "Reconnect delay");
    validateReconnectDelay(this.maximumReconnectDelayMs, "Maximum reconnect delay");
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

    this.connectionPromise = new Promise<void>((resolve, reject) => {
      this.connectionSocket = socket;
      this.rejectConnection = reject;

      socket.addEventListener("open", () => {
        if (this.socket !== socket) {
          return;
        }
        this.reconnectAttempt = 0;
        this.connectionPromise = undefined;
        this.connectionSocket = undefined;
        this.rejectConnection = undefined;
        this.setState("open");
        resolve();
        this.sendActiveSubscriptions();
      });

      socket.addEventListener("message", (message) => {
        if (this.socket !== socket) {
          return;
        }
        this.messageChain = this.messageChain
          .then(() => this.handleMessage(message.data, socket))
          .catch((error: unknown) => {
            this.emitError(normalizeError(error, "Unable to process a Search WebSocket frame"));
          });
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
        if (this.socket !== socket) {
          return;
        }
        this.socket = undefined;
        this.connectionPromise = undefined;
        if (this.connectionSocket === socket && this.rejectConnection) {
          this.rejectConnection(new Error("Search WebSocket closed before connecting"));
        }
        this.connectionSocket = undefined;
        this.rejectConnection = undefined;
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
    const socket = this.socket;
    this.socket = undefined;
    this.connectionPromise = undefined;
    if (this.connectionSocket === socket && this.rejectConnection) {
      this.rejectConnection(new Error(reason));
    }
    this.connectionSocket = undefined;
    this.rejectConnection = undefined;
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
    if (this.subscriptions.has(subscriptionId)) {
      throw new Error(`Search WebSocket subscription already exists: ${subscriptionId}`);
    }
    if (
      options.previewRowLimit !== undefined
      && (!Number.isInteger(options.previewRowLimit)
        || options.previewRowLimit <= 0
        || options.previewRowLimit > 0xffff_ffff)
    ) {
      throw new RangeError("Preview row limit must be an integer between 1 and 4294967295");
    }

    const subscription: ActiveSubscription = {
      subscriptionId,
      target: normalizedTarget,
      includePreviews: options.includePreviews ?? true,
      previewRowLimit: options.previewRowLimit,
    };
    this.subscriptions.set(subscriptionId, subscription);

    if (this.connected) {
      this.sendSubscriptions([subscription]);
    } else {
      void this.connect().catch(() => {
        // Connection errors are delivered through onError; reconnect remains automatic.
      });
    }
    return subscriptionId;
  }

  public unsubscribe(subscriptionId: string): boolean {
    const existed = this.subscriptions.delete(subscriptionId);
    if (!existed) {
      return false;
    }
    if (this.connected) {
      this.sendCommand({
        requestId: this.nextIdentifier("unsubscribe"),
        payload: {
          $case: "unsubscribe",
          value: { subscriptionIds: [subscriptionId] },
        },
      });
    }
    return true;
  }

  public getLastSequence(target: JobTarget): bigint {
    return this.lastSequences.get(searchWebSocketTargetKey(target)) ?? 0n;
  }

  /** Seeds or advances a replay checkpoint; checkpoints are never allowed to move backward. */
  public setLastSequence(target: JobTarget, sequence: bigint): void {
    if (sequence < 0n) {
      throw new RangeError("Search WebSocket sequence must not be negative");
    }
    const key = searchWebSocketTargetKey(target);
    const previous = this.lastSequences.get(key) ?? 0n;
    if (sequence < previous) {
      throw new RangeError(`Search WebSocket sequence for ${key} cannot move backward`);
    }
    this.lastSequences.set(key, sequence);
    this.suspendedTargets.delete(key);
  }

  /** Removes a retained checkpoint so a future subscription starts from current state. */
  public forgetTarget(target: JobTarget): void {
    const key = searchWebSocketTargetKey(target);
    this.lastSequences.delete(key);
    this.suspendedTargets.delete(key);
  }

  public ping(nonce = this.nextIdentifier("ping")): boolean {
    if (!this.connected) {
      return false;
    }
    this.sendCommand({
      requestId: this.nextIdentifier("ping-request"),
      payload: { $case: "ping", value: { nonce } },
    });
    return true;
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

  private async handleMessage(data: unknown, sourceSocket: WebSocket): Promise<void> {
    if (this.socket !== sourceSocket) {
      return;
    }
    const bytes = await readBinaryFrame(data);
    if (this.socket !== sourceSocket) {
      return;
    }
    const event = SearchWebSocketEventCodec.decode(bytes);
    const target = this.resolveEventTarget(event);
    const resynchronization = event.payload?.$case === "resynchronizationRequired"
      ? event.payload.value
      : undefined;

    if (target && event.sequence > 0n && !resynchronization) {
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
        this.restartForReplay();
        return;
      }

      await this.emitEvent(event);
      if (this.socket !== sourceSocket) {
        return;
      }
      this.lastSequences.set(targetKey, event.sequence);
    } else {
      await this.emitEvent(event);
    }

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
      await this.emitResynchronization(event, resynchronization, target);
    }
  }

  private async emitEvent(event: SearchWebSocketEvent): Promise<void> {
    const outcomes = await Promise.allSettled(
      Array.from(this.eventListeners, async (listener) => listener(event)),
    );
    for (const outcome of outcomes) {
      if (outcome.status === "rejected") {
        this.emitError(normalizeError(outcome.reason, "Search WebSocket event listener failed"));
      }
    }
  }

  private async emitResynchronization(
    event: SearchWebSocketEvent,
    required: ResynchronizationRequired,
    resolvedTarget: JobTarget | undefined,
  ): Promise<void> {
    const target = required.target ?? resolvedTarget;
    if (!target) {
      this.emitError(new Error("Resynchronization event did not identify a job target"));
      return;
    }
    const normalizedTarget = cloneTarget(target);
    const targetKey = searchWebSocketTargetKey(normalizedTarget);
    const lastSequence = this.lastSequences.get(targetKey) ?? 0n;
    this.suspendedTargets.add(targetKey);
    let acknowledgedSequence: bigint | undefined;
    const acknowledge = (sequence = required.latestSequence) => {
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
    if (recoveryFailed) {
      return;
    }

    const checkpoint = acknowledgedSequence ?? required.latestSequence;
    this.commitResynchronization(normalizedTarget, checkpoint);
  }

  /** A server restart may establish a new sequence epoch, so recovery can move this boundary backward. */
  private commitResynchronization(target: JobTarget, sequence: bigint): void {
    if (sequence < 0n) {
      throw new RangeError("Search WebSocket sequence must not be negative");
    }
    const targetKey = searchWebSocketTargetKey(target);
    this.lastSequences.set(targetKey, sequence);
    this.suspendedTargets.delete(targetKey);
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

  private sendActiveSubscriptions(): void {
    this.sendSubscriptions(Array.from(this.subscriptions.values()));
  }

  private sendSubscriptions(subscriptions: readonly ActiveSubscription[]): void {
    if (subscriptions.length === 0) {
      return;
    }
    this.sendCommand({
      requestId: this.nextIdentifier("subscribe"),
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
    });
  }

  private sendCommand(command: SearchWebSocketCommand): void {
    const socket = this.socket;
    if (!socket || socket.readyState !== SOCKET_OPEN) {
      return;
    }
    try {
      socket.send(SearchWebSocketCommand.encode(command).finish());
    } catch (error) {
      this.emitError(normalizeError(error, "Unable to send a Search WebSocket command"));
    }
  }

  /** Drops the current generation without advancing checkpoints so retained events can replay. */
  private restartForReplay(): void {
    const socket = this.socket;
    if (!socket) {
      return;
    }
    this.socket = undefined;
    this.connectionPromise = undefined;
    this.connectionSocket = undefined;
    this.rejectConnection = undefined;
    try {
      socket.close(4000, "Sequence gap; replay required");
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
    if (this.reconnectTimer !== undefined) {
      globalThis.clearTimeout(this.reconnectTimer);
      this.reconnectTimer = undefined;
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
