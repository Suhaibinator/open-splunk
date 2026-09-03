import {
  browserSupportsTokenCreateLock,
  parsePersistedTokenCreateGuard,
  readTokenCreateGuardRaw,
  requestTokenCreateLock,
  type UnreadableTokenCreateRecovery,
} from "./token-creation";

export interface TokenRecoveryStartupRecord {
  restored: ReturnType<typeof parsePersistedTokenCreateGuard>;
  unreadableRecovery: UnreadableTokenCreateRecovery | null;
}

export type TokenRecoveryStartupSnapshot =
  | { readonly kind: "idle"; readonly lockAvailable: null }
  | { readonly kind: "preflight"; readonly lockAvailable: boolean }
  | {
      readonly kind: "storage-unavailable";
      readonly lockAvailable: boolean;
      readonly error: unknown;
    }
  | { readonly kind: "empty"; readonly lockAvailable: boolean }
  | {
      readonly kind: "stored";
      readonly lockAvailable: boolean;
      readonly record: TokenRecoveryStartupRecord;
    }
  | {
      readonly kind: "lock-unavailable" | "acquiring" | "contended";
      readonly lockAvailable: boolean;
      readonly record: TokenRecoveryStartupRecord;
    }
  | {
      readonly kind: "lock-failed";
      readonly lockAvailable: boolean;
      readonly record: TokenRecoveryStartupRecord;
      readonly error: unknown;
    };

export interface TokenRecoveryStartupContext {
  isCurrent: () => boolean;
  signal: AbortSignal;
}

export interface TokenRecoveryStartupHandlers {
  onCleanup: () => void;
  onOwned: (
    record: TokenRecoveryStartupRecord,
    context: TokenRecoveryStartupContext,
  ) => Promise<void>;
}

const SERVER_SNAPSHOT: TokenRecoveryStartupSnapshot = Object.freeze({
  kind: "idle",
  lockAvailable: null,
});

export interface TokenRecoveryStartupDependencies {
  lockAvailable: () => boolean;
  parse: typeof parsePersistedTokenCreateGuard;
  randomId: () => string;
  read: typeof readTokenCreateGuardRaw;
  requestLock: (
    normalizedApiBaseUrl: string,
    signal: AbortSignal,
    callback: (lock: Lock | null) => Promise<void>,
  ) => Promise<void>;
}

const browserDependencies: TokenRecoveryStartupDependencies = {
  lockAvailable: browserSupportsTokenCreateLock,
  parse: parsePersistedTokenCreateGuard,
  randomId: () => crypto.randomUUID(),
  read: readTokenCreateGuardRaw,
  requestLock: (normalizedApiBaseUrl, signal, callback) => requestTokenCreateLock(
    normalizedApiBaseUrl,
    { mode: "exclusive", ifAvailable: true, signal },
    callback,
  ),
};

export class TokenRecoveryStartupController {
  private active: {
    controller: AbortController;
    handlers: TokenRecoveryStartupHandlers;
    id: number;
  } | null = null;
  private readonly listeners = new Set<() => void>();
  private nextId = 0;
  private snapshot = SERVER_SNAPSHOT;

  constructor(
    private readonly dependencies: TokenRecoveryStartupDependencies = browserDependencies,
  ) {}

  getServerSnapshot = (): TokenRecoveryStartupSnapshot => SERVER_SNAPSHOT;

  getSnapshot = (): TokenRecoveryStartupSnapshot => this.snapshot;

  subscribe = (listener: () => void): (() => void) => {
    this.listeners.add(listener);
    return () => this.listeners.delete(listener);
  };

  start(
    normalizedApiBaseUrl: string,
    observedServerTimeMs: number | null,
    handlers: TokenRecoveryStartupHandlers,
  ): () => void {
    this.stop();
    const controller = new AbortController();
    const id = ++this.nextId;
    this.active = { controller, handlers, id };
    const isCurrent = () => this.active?.id === id && !controller.signal.aborted;
    const publish = (snapshot: TokenRecoveryStartupSnapshot) => {
      if (!isCurrent()) return;
      this.snapshot = Object.freeze(snapshot);
      for (const listener of this.listeners) listener();
    };
    const lockAvailable = this.dependencies.lockAvailable();
    publish({ kind: "preflight", lockAvailable });
    let raw: string | null;
    try {
      raw = this.dependencies.read(normalizedApiBaseUrl);
    } catch (error) {
      publish({ kind: "storage-unavailable", lockAvailable, error });
      return () => this.stop(id);
    }
    if (raw === null) {
      publish({ kind: "empty", lockAvailable });
      return () => this.stop(id);
    }
    const restored = this.dependencies.parse(raw, normalizedApiBaseUrl);
    const unreadableRecovery = restored === null
      ? {
          attemptId: this.dependencies.randomId(),
          raw,
          observedServerTimeMs,
          candidates: [],
          reconciliationError: "The saved token safety record is unreadable.",
        }
      : null;
    const record = { restored, unreadableRecovery };
    publish({ kind: "stored", lockAvailable, record });
    if (!lockAvailable) {
      publish({ kind: "lock-unavailable", lockAvailable, record });
      return () => this.stop(id);
    }
    publish({ kind: "acquiring", lockAvailable, record });
    void this.dependencies.requestLock(
      normalizedApiBaseUrl,
      controller.signal,
      async (lock) => {
        if (!isCurrent()) return;
        if (lock === null) {
          publish({ kind: "contended", lockAvailable, record });
          return;
        }
        await handlers.onOwned(record, { isCurrent, signal: controller.signal });
      },
    ).catch((error: unknown) => {
      publish({ kind: "lock-failed", lockAvailable, record, error });
    });
    return () => this.stop(id);
  }

  stop(expectedId?: number) {
    const active = this.active;
    if (active === null || (expectedId !== undefined && active.id !== expectedId)) return;
    this.active = null;
    active.controller.abort();
    active.handlers.onCleanup();
    this.snapshot = SERVER_SNAPSHOT;
    for (const listener of this.listeners) listener();
  }
}
