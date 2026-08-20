import assert from "node:assert/strict";
import test from "node:test";

import {
  ExportFormat,
  ExportJobState,
  type ExportJob,
  type ExportProgress,
} from "../../gen/ts/open_splunk/export";
import {
  SearchWebSocketCommand,
  type SearchWebSocketEvent,
  SearchWebSocketEvent as SearchWebSocketEventCodec,
} from "../../gen/ts/open_splunk/search_ws";
import { ServerFeature } from "../../gen/ts/open_splunk/system_api";
import type { OpenSplunkApiClient } from "../api/open-splunk-client";
import { exportJobTarget } from "../api/search-websocket";
import type { SystemBootstrapModel } from "../api/system-bootstrap";
import {
  reconcileExportProgress,
  type ExportProgressRevisionState,
  type ExportProgressSource,
  waitForServerExport,
} from "./server-exports";

const SOCKET_CONNECTING = 0;
const SOCKET_OPEN = 1;
const SOCKET_CLOSING = 2;
const SOCKET_CLOSED = 3;

class FakeWebSocket extends EventTarget {
  public binaryType: BinaryType = "blob";
  public readyState = SOCKET_CONNECTING;
  public readonly sent: Uint8Array[] = [];

  public send(data: string | ArrayBufferLike | Blob | ArrayBufferView): void {
    if (this.readyState !== SOCKET_OPEN) throw new Error("fake WebSocket is not open");
    if (typeof data === "string" || data instanceof Blob) {
      throw new TypeError("test client sent a non-binary frame");
    }
    if (ArrayBuffer.isView(data)) {
      this.sent.push(new Uint8Array(data.buffer, data.byteOffset, data.byteLength).slice());
      return;
    }
    this.sent.push(new Uint8Array(data).slice());
  }

  public close(): void {
    if (this.readyState === SOCKET_CLOSED) return;
    this.readyState = SOCKET_CLOSING;
    this.readyState = SOCKET_CLOSED;
    this.dispatchEvent(new Event("close"));
  }

  public open(): void {
    assert.equal(this.readyState, SOCKET_CONNECTING);
    this.readyState = SOCKET_OPEN;
    this.dispatchEvent(new Event("open"));
  }

  public receive(event: SearchWebSocketEvent): void {
    const message = new Event("message");
    Object.defineProperty(message, "data", {
      value: SearchWebSocketEventCodec.encode(event).finish(),
    });
    this.dispatchEvent(message);
  }

  public fail(): void {
    this.dispatchEvent(new Event("error"));
  }
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void;
  let reject!: (reason?: unknown) => void;
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, resolve, reject };
}

async function waitFor(
  predicate: () => boolean,
  description: string,
  timeoutMilliseconds = 2_000,
): Promise<void> {
  const deadline = Date.now() + timeoutMilliseconds;
  await new Promise<void>((resolve, reject) => {
    const poll = (): void => {
      if (predicate()) {
        resolve();
        return;
      }
      if (Date.now() >= deadline) {
        reject(new Error(`timed out waiting for ${description}`));
        return;
      }
      setTimeout(poll, 0);
    };
    poll();
  });
}

const authoritative = (envelopeRevision: bigint): ExportProgressSource => ({
  kind: "authoritative",
  envelopeRevision,
});
const terminal = (envelopeRevision: bigint): ExportProgressSource => ({
  kind: "terminal",
  envelopeRevision,
});
const live: ExportProgressSource = { kind: "live" };

function progress(
  updatedAt: string,
  overrides: Partial<ExportProgress> = {},
): ExportProgress {
  return {
    rowsWritten: 10n,
    bytesWritten: 100n,
    percentComplete: 40,
    elapsed: { seconds: 2n, nanos: 3 },
    updatedAt: new Date(updatedAt),
    ...overrides,
  };
}

function exportJob(
  stateVersion: bigint,
  state: ExportJobState,
  jobProgress: ExportProgress,
): ExportJob {
  return {
    exportJobId: "export-1",
    stateVersion,
    definition: {
      searchJobId: "search-1",
      columns: [],
      rowLimit: undefined,
      byteLimit: undefined,
      formatOptions: {
        $case: "csv",
        value: { headerMode: 1 },
      },
    },
    format: ExportFormat.EXPORT_FORMAT_CSV,
    state,
    progress: jobProgress,
    artifact: undefined,
    failure: undefined,
    createdAt: new Date("2026-07-26T00:00:00.000Z"),
    startedAt: new Date("2026-07-26T00:00:00.000Z"),
    finishedAt: undefined,
    expiresAt: undefined,
  };
}

function bootstrapModel(searchWebsocketPath: string | null): SystemBootstrapModel {
  return {
    build: null,
    features: new Set([ServerFeature.SERVER_FEATURE_EXPORT_CSV]),
    searchWebsocketPath,
    limits: {
      maximumPageSize: 100,
      maximumPreviewRows: 100,
      maximumWebsocketSubscriptions: 4,
      maximumWebsocketFrameBytes: 1_048_576n,
      maximumExportRows: 10_000n,
      maximumExportBytes: 10_000_000n,
      defaultSearchTimeoutMs: 30_000,
      searchResultRetentionMs: 60_000,
      maximumTimelineBuckets: 100,
      maximumFieldSummaryValues: 10,
    },
    apps: [],
    indexes: [],
    selectedAppId: null,
    serverTime: new Date("2026-07-26T00:00:00.000Z"),
  };
}

function applied(
  current: ExportProgressRevisionState,
  candidate: ExportProgress,
  source: ExportProgressSource,
): NonNullable<ExportProgressRevisionState> {
  const decision = reconcileExportProgress(current, candidate, source);
  assert.equal(decision.kind, "apply");
  if (decision.kind !== "apply") throw new Error("expected export progress to apply");
  return decision.state;
}

test("an older replayed live frame cannot regress an authoritative export snapshot", () => {
  const current = applied(
    null,
    progress("2026-07-26T00:00:10.000Z", {
      rowsWritten: 50n,
      bytesWritten: 500n,
      elapsed: { seconds: 10n, nanos: 0 },
    }),
    authoritative(10n),
  );
  const decision = reconcileExportProgress(
    current,
    progress("2026-07-26T00:00:09.000Z", {
      rowsWritten: 40n,
      bytesWritten: 400n,
      elapsed: { seconds: 9n, nanos: 0 },
    }),
    live,
  );

  assert.deepEqual(decision, {
    kind: "ignore",
    reason: "lower",
    state: current,
  });
  assert.strictEqual(decision.state, current);
});

test("a newer live watermark applies without inventing a server revision", () => {
  const current = applied(
    null,
    progress("2026-07-26T00:00:10.000Z"),
    authoritative(10n),
  );
  const next = applied(
    current,
    progress("2026-07-26T00:00:11.000Z", {
      rowsWritten: 11n,
      bytesWritten: 110n,
      elapsed: { seconds: 3n, nanos: 0 },
    }),
    live,
  );

  assert.equal(next.revision, 10n);
  assert.equal(next.epoch, current.epoch + 1n);
  assert.equal(next.progress.rowsWritten, 11n);
});

test("same-watermark monotonic advances survive millisecond timestamp collapse", () => {
  const current = applied(
    null,
    progress("2026-07-26T00:00:10.000Z"),
    authoritative(10n),
  );
  const next = applied(
    current,
    progress("2026-07-26T00:00:10.000Z", {
      rowsWritten: 11n,
      bytesWritten: 110n,
      elapsed: { seconds: 3n, nanos: 0 },
    }),
    live,
  );

  assert.equal(next.progress.rowsWritten, 11n);
});

test("a higher REST revision cannot overwrite a newer live watermark", () => {
  const authoritativeState = applied(
    null,
    progress("2026-07-26T00:00:10.000Z"),
    authoritative(10n),
  );
  const liveState = applied(
    authoritativeState,
    progress("2026-07-26T00:00:12.000Z", {
      rowsWritten: 12n,
      bytesWritten: 120n,
      elapsed: { seconds: 4n, nanos: 0 },
    }),
    live,
  );
  const decision = reconcileExportProgress(
    liveState,
    progress("2026-07-26T00:00:11.000Z", {
      rowsWritten: 11n,
      bytesWritten: 110n,
      elapsed: { seconds: 3n, nanos: 0 },
    }),
    authoritative(11n),
  );

  assert.deepEqual(decision, {
    kind: "recover",
    reason: "watermark-regression",
    state: liveState,
  });
});

test("an equal progress snapshot may advance only the authoritative revision", () => {
  const candidate = progress("2026-07-26T00:00:10.000Z");
  const current = applied(null, candidate, authoritative(10n));
  const next = applied(current, candidate, authoritative(11n));

  assert.equal(next.revision, 11n);
  assert.equal(next.epoch, current.epoch + 1n);
});

test("ambiguous progress is recovered rather than applied", () => {
  const current = applied(
    null,
    progress("2026-07-26T00:00:10.000Z"),
    authoritative(10n),
  );
  const cases: Array<[ExportProgress, ExportProgressSource, string]> = [
    [progress("invalid"), live, "invalid-watermark"],
    [progress("2026-07-26T00:00:11.000Z", { elapsed: undefined }), live, "invalid-watermark"],
    [progress("2026-07-26T00:00:11.000Z"), authoritative(0n), "invalid-revision"],
    [
      progress("2026-07-26T00:00:11.000Z", { rowsWritten: 9n }),
      terminal(11n),
      "watermark-regression",
    ],
  ];
  for (const [candidate, source, reason] of cases) {
    const decision = reconcileExportProgress(current, candidate, source);
    assert.equal(decision.kind, "recover");
    if (decision.kind === "recover") assert.equal(decision.reason, reason);
    assert.strictEqual(decision.state, current);
  }

  const unversioned = reconcileExportProgress(
    null,
    progress("2026-07-26T00:00:11.000Z"),
    live,
  );
  assert.deepEqual(unversioned, {
    kind: "recover",
    reason: "unversioned",
    state: null,
  });
});

test("applied export progress is isolated from decoded-object mutation", () => {
  const candidate = progress("2026-07-26T00:00:10.000Z");
  const state = applied(null, candidate, authoritative(10n));
  candidate.rowsWritten = 999n;
  candidate.elapsed!.nanos = 999;
  candidate.updatedAt!.setUTCFullYear(2000);

  assert.equal(state.progress.rowsWritten, 10n);
  assert.deepEqual(state.progress.elapsed, { seconds: 2n, nanos: 3 });
  assert.equal(state.progress.updatedAt?.toISOString(), "2026-07-26T00:00:10.000Z");
});

test("an already-aborted WebSocket export wait rejects before opening a socket", async () => {
  const controller = new AbortController();
  controller.abort(new DOMException("Canceled before monitoring.", "AbortError"));
  const client = {
    exports: {
      get: () => {
        throw new Error("an aborted wait must not issue a recovery request");
      },
    },
  } as unknown as OpenSplunkApiClient;

  await assert.rejects(
    waitForServerExport(
      client,
      bootstrapModel("/api/search/ws"),
      exportJob(
        1n,
        ExportJobState.EXPORT_JOB_STATE_RUNNING,
        progress("2026-07-26T00:00:01.000Z"),
      ),
      { signal: controller.signal },
    ),
    /Canceled before monitoring/,
  );
});

test("REST-only export monitoring rejects foreign and regressing snapshots", async () => {
  const initial = exportJob(
    3n,
    ExportJobState.EXPORT_JOB_STATE_RUNNING,
    progress("2026-07-26T00:00:03.000Z", {
      rowsWritten: 30n,
      bytesWritten: 300n,
      elapsed: { seconds: 3n, nanos: 0 },
    }),
  );
  const cases: Array<{
    name: string;
    candidate: ExportJob;
    message: RegExp;
  }> = [
    {
      name: "foreign job",
      candidate: {
        ...exportJob(
          4n,
          ExportJobState.EXPORT_JOB_STATE_COMPLETED,
          progress("2026-07-26T00:00:04.000Z"),
        ),
        exportJobId: "export-foreign",
      },
      message: /different export job/,
    },
    {
      name: "lower state revision",
      candidate: exportJob(
        2n,
        ExportJobState.EXPORT_JOB_STATE_RUNNING,
        progress("2026-07-26T00:00:04.000Z", {
          rowsWritten: 31n,
          bytesWritten: 310n,
          elapsed: { seconds: 4n, nanos: 0 },
        }),
      ),
      message: /older than the applied state/,
    },
    {
      name: "regressing progress",
      candidate: exportJob(
        4n,
        ExportJobState.EXPORT_JOB_STATE_RUNNING,
        progress("2026-07-26T00:00:04.000Z", {
          rowsWritten: 29n,
          bytesWritten: 290n,
          elapsed: { seconds: 4n, nanos: 0 },
        }),
      ),
      message: /progress is inconsistent/,
    },
  ];

  await Promise.all(cases.map(async (testCase) => {
    let updates = 0;
    const client = {
      exports: {
        get: async () => ({ exportJob: testCase.candidate }),
      },
    } as unknown as OpenSplunkApiClient;
    await assert.rejects(
      waitForServerExport(client, bootstrapModel(null), initial, {
        pollIntervalMs: 100,
        onUpdate: () => {
          updates += 1;
        },
      }),
      testCase.message,
      testCase.name,
    );
    assert.equal(updates, 0, `${testCase.name} must not publish a rejected snapshot`);
  }));
});

test("one recovery cycle owns a shared REST response and retries after a live epoch advance", async (t) => {
  const sockets: FakeWebSocket[] = [];
  const originalWebSocket = Object.getOwnPropertyDescriptor(globalThis, "WebSocket");
  class TestWebSocket extends FakeWebSocket {
    public constructor(_url: string | URL) {
      super();
      sockets.push(this);
    }
  }
  Object.defineProperty(globalThis, "WebSocket", {
    configurable: true,
    writable: true,
    value: TestWebSocket,
  });
  t.after(() => {
    if (originalWebSocket === undefined) Reflect.deleteProperty(globalThis, "WebSocket");
    else Object.defineProperty(globalThis, "WebSocket", originalWebSocket);
  });

  const firstRecovery = deferred<{ exportJob: ExportJob | undefined }>();
  const secondRecovery = deferred<{ exportJob: ExportJob | undefined }>();
  let recoveryCalls = 0;
  const client = {
    exports: {
      get: () => {
        recoveryCalls += 1;
        if (recoveryCalls === 1) return firstRecovery.promise;
        if (recoveryCalls === 2) return secondRecovery.promise;
        throw new Error("unexpected duplicate export recovery");
      },
    },
  } as unknown as OpenSplunkApiClient;
  const bootstrap = bootstrapModel("/api/search/ws");
  const initial = exportJob(
    1n,
    ExportJobState.EXPORT_JOB_STATE_QUEUED,
    progress("2026-07-26T00:00:01.000Z", {
      rowsWritten: 1n,
      bytesWritten: 10n,
      percentComplete: 10,
      elapsed: { seconds: 1n, nanos: 0 },
    }),
  );
  const updates: ExportJob[] = [];
  const controller = new AbortController();
  t.after(() => controller.abort());
  const completion = waitForServerExport(client, bootstrap, initial, {
    signal: controller.signal,
    websocketBaseUrl: "http://127.0.0.1:8080/",
    websocketRecoveryIntervalMs: 1_000,
    onUpdate: (job) => updates.push(job),
  });

  await waitFor(() => sockets.length === 1, "export WebSocket creation");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "export subscription");
  const subscribe = SearchWebSocketCommand.decode(sockets[0].sent[0]);
  assert.equal(subscribe.payload?.$case, "subscribe");
  if (subscribe.payload?.$case !== "subscribe") throw new Error("expected export subscription");
  const subscriptionId = subscribe.payload.value.subscriptions[0]?.subscriptionId;
  assert.ok(subscriptionId);
  await waitFor(() => recoveryCalls === 1, "initial export recovery");

  const liveProgress = progress("2026-07-26T00:00:02.000Z", {
    rowsWritten: 2n,
    bytesWritten: 20n,
    percentComplete: 20,
    elapsed: { seconds: 2n, nanos: 0 },
  });
  sockets[0].receive({
    sequence: 1n,
    occurredAt: new Date("2026-07-26T00:00:02.000Z"),
    subscriptionId,
    target: exportJobTarget("export-1"),
    payload: { $case: "exportProgress", value: liveProgress },
  });
  await waitFor(() => updates.length === 1, "live export progress");
  sockets[0].fail();
  sockets[0].fail();

  firstRecovery.resolve({
    exportJob: exportJob(
      2n,
      ExportJobState.EXPORT_JOB_STATE_RUNNING,
      progress("2026-07-26T00:00:01.500Z", {
        rowsWritten: 1n,
        bytesWritten: 15n,
        percentComplete: 15,
        elapsed: { seconds: 1n, nanos: 500_000_000 },
      }),
    ),
  });
  await waitFor(() => recoveryCalls === 2, "single coalesced follow-up recovery");
  assert.deepEqual(updates.map((job) => job.progress?.rowsWritten), [2n]);

  secondRecovery.resolve({
    exportJob: exportJob(
      3n,
      ExportJobState.EXPORT_JOB_STATE_COMPLETED,
      progress("2026-07-26T00:00:03.000Z", {
        rowsWritten: 3n,
        bytesWritten: 30n,
        percentComplete: 100,
        elapsed: { seconds: 3n, nanos: 0 },
      }),
    ),
  });
  const result = await completion;
  assert.equal(result.status, "available");
  if (result.status === "available") {
    assert.equal(result.value.state, ExportJobState.EXPORT_JOB_STATE_COMPLETED);
    assert.equal(result.value.progress?.rowsWritten, 3n);
  }
  assert.deepEqual(updates.map((job) => job.progress?.rowsWritten), [2n, 3n]);
  await new Promise((resolve) => setTimeout(resolve, 5));
  assert.equal(recoveryCalls, 2);
});

test("a conflicting terminal progress frame cannot publish its terminal artifact", async (t) => {
  const sockets: FakeWebSocket[] = [];
  const originalWebSocket = Object.getOwnPropertyDescriptor(globalThis, "WebSocket");
  class TestWebSocket extends FakeWebSocket {
    public constructor(_url: string | URL) {
      super();
      sockets.push(this);
    }
  }
  Object.defineProperty(globalThis, "WebSocket", {
    configurable: true,
    writable: true,
    value: TestWebSocket,
  });
  t.after(() => {
    if (originalWebSocket === undefined) Reflect.deleteProperty(globalThis, "WebSocket");
    else Object.defineProperty(globalThis, "WebSocket", originalWebSocket);
  });

  const initial = exportJob(
    1n,
    ExportJobState.EXPORT_JOB_STATE_RUNNING,
    progress("2026-07-26T00:00:01.000Z", {
      rowsWritten: 10n,
      bytesWritten: 100n,
      percentComplete: 40,
      elapsed: { seconds: 1n, nanos: 0 },
    }),
  );
  const authoritativeTerminal: ExportJob = {
    ...exportJob(
      2n,
      ExportJobState.EXPORT_JOB_STATE_COMPLETED,
      progress("2026-07-26T00:00:03.000Z", {
        rowsWritten: 11n,
        bytesWritten: 110n,
        percentComplete: 100,
        elapsed: { seconds: 3n, nanos: 0 },
      }),
    ),
    artifact: {
      fileName: "authoritative.csv",
      mediaType: "text/csv",
      sizeBytes: 110n,
      rowCount: 11n,
      expiresAt: new Date("2026-07-26T01:00:00.000Z"),
    },
  };
  const terminalRecovery = deferred<{ exportJob: ExportJob | undefined }>();
  let recoveryCalls = 0;
  const client = {
    exports: {
      get: () => {
        recoveryCalls += 1;
        if (recoveryCalls === 1) return Promise.resolve({ exportJob: initial });
        if (recoveryCalls === 2) return terminalRecovery.promise;
        throw new Error("unexpected duplicate export recovery");
      },
    },
  } as unknown as OpenSplunkApiClient;
  const updates: ExportJob[] = [];
  const controller = new AbortController();
  t.after(() => controller.abort());
  const completion = waitForServerExport(
    client,
    bootstrapModel("/api/search/ws"),
    initial,
    {
      signal: controller.signal,
      websocketBaseUrl: "http://127.0.0.1:8080/",
      websocketRecoveryIntervalMs: 1_000,
      onUpdate: (job) => updates.push(job),
    },
  );

  await waitFor(() => sockets.length === 1, "export WebSocket creation");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "export subscription");
  const subscribe = SearchWebSocketCommand.decode(sockets[0].sent[0]);
  assert.equal(subscribe.payload?.$case, "subscribe");
  if (subscribe.payload?.$case !== "subscribe") throw new Error("expected export subscription");
  const subscriptionId = subscribe.payload.value.subscriptions[0]?.subscriptionId;
  assert.ok(subscriptionId);
  await waitFor(() => updates.length === 1, "initial authoritative recovery");
  assert.equal(recoveryCalls, 1);
  updates.length = 0;

  sockets[0].receive({
    sequence: 1n,
    occurredAt: new Date("2026-07-26T00:00:02.000Z"),
    subscriptionId,
    target: exportJobTarget("export-1"),
    payload: {
      $case: "exportTerminal",
      value: {
        exportJobId: "export-1",
        state: ExportJobState.EXPORT_JOB_STATE_COMPLETED,
        stateVersion: 2n,
        finalProgress: progress("2026-07-26T00:00:02.000Z", {
          rowsWritten: 9n,
          bytesWritten: 90n,
          percentComplete: 100,
          elapsed: { seconds: 2n, nanos: 0 },
        }),
        failure: undefined,
        artifact: {
          fileName: "suspect.csv",
          mediaType: "text/csv",
          sizeBytes: 90n,
          rowCount: 9n,
          expiresAt: new Date("2026-07-26T01:00:00.000Z"),
        },
      },
    },
  });

  await waitFor(() => recoveryCalls === 2, "terminal conflict recovery");
  assert.equal(
    updates.length,
    0,
    "the terminal state and artifact must remain private until REST confirms the envelope",
  );

  terminalRecovery.resolve({ exportJob: authoritativeTerminal });
  const result = await completion;
  assert.equal(result.status, "available");
  if (result.status === "available") {
    assert.equal(result.value.state, ExportJobState.EXPORT_JOB_STATE_COMPLETED);
    assert.equal(result.value.artifact?.fileName, "authoritative.csv");
    assert.equal(result.value.progress?.rowsWritten, 11n);
  }
  assert.deepEqual(
    updates.map((job) => job.artifact?.fileName),
    ["authoritative.csv"],
  );
});
