import assert from "node:assert/strict";
import test from "node:test";

import { SearchJobState } from "../../gen/ts/open_splunk/v1/search";
import {
  ResynchronizationReason,
  SearchWebSocketCommand,
  type SearchWebSocketEvent,
  SearchWebSocketEvent as SearchWebSocketEventCodec,
} from "../../gen/ts/open_splunk/v1/search_ws";
import {
  SearchWebSocketClient,
  searchJobTarget,
} from "./search-websocket";

const SOCKET_CONNECTING = 0;
const SOCKET_OPEN = 1;
const SOCKET_CLOSING = 2;
const SOCKET_CLOSED = 3;

class FakeWebSocket extends EventTarget {
  public binaryType: BinaryType = "blob";
  public readyState = SOCKET_CONNECTING;
  public readonly sent: Uint8Array[] = [];
  public readonly closes: Array<{ code?: number; reason?: string }> = [];

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

  public close(code?: number, reason?: string): void {
    if (this.readyState === SOCKET_CLOSED) return;
    this.closes.push({ code, reason });
    this.readyState = SOCKET_CLOSING;
    this.finishClose();
  }

  public open(): void {
    assert.equal(this.readyState, SOCKET_CONNECTING);
    this.readyState = SOCKET_OPEN;
    this.dispatchEvent(new Event("open"));
  }

  public receive(event: SearchWebSocketEvent): void {
    const frame = SearchWebSocketEventCodec.encode(event).finish();
    this.receiveRaw(frame);
  }

  public receiveRaw(data: unknown): void {
    const message = new Event("message");
    Object.defineProperty(message, "data", { value: data });
    this.dispatchEvent(message);
  }

  public serverClose(): void {
    if (this.readyState === SOCKET_CLOSED) return;
    this.readyState = SOCKET_CLOSING;
    this.finishClose();
  }

  private finishClose(): void {
    this.readyState = SOCKET_CLOSED;
    this.dispatchEvent(new Event("close"));
  }
}

function stateEvent(
  subscriptionId: string,
  searchJobId: string,
  sequence: bigint,
  stateVersion = sequence,
): SearchWebSocketEvent {
  const target = searchJobTarget(searchJobId);
  return {
    sequence,
    occurredAt: new Date("2026-07-25T00:00:00.000Z"),
    subscriptionId,
    target,
    payload: {
      $case: "searchStateChanged",
      value: {
        searchJobId,
        state: SearchJobState.SEARCH_JOB_STATE_RUNNING,
        stateVersion,
      },
    },
  };
}

function resynchronizationEvent(
  subscriptionId: string,
  searchJobId: string,
  latestSequence: bigint,
): SearchWebSocketEvent {
  const target = searchJobTarget(searchJobId);
  return {
    sequence: 0n,
    occurredAt: new Date("2026-07-25T00:00:01.000Z"),
    subscriptionId,
    target,
    payload: {
      $case: "resynchronizationRequired",
      value: {
        subscriptionId,
        target,
        reason: ResynchronizationReason.RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED,
        earliestAvailableSequence: latestSequence,
        latestSequence,
        recoveryPath: "/api/v1/search/jobs/get",
      },
    },
  };
}

function subscribeCommand(socket: FakeWebSocket, index = 0) {
  const command = SearchWebSocketCommand.decode(socket.sent[index]);
  assert.equal(command.payload?.$case, "subscribe");
  if (command.payload?.$case !== "subscribe") throw new Error("expected subscribe command");
  assert.equal(command.payload.value.subscriptions.length, 1);
  return command.payload.value.subscriptions[0];
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

function clientFixture() {
  const sockets: FakeWebSocket[] = [];
  const client = new SearchWebSocketClient({
    autoReconnect: true,
    reconnectDelayMs: 0,
    maximumReconnectDelayMs: 0,
    baseUrl: "http://127.0.0.1:8080/",
    webSocketFactory: () => {
      const socket = new FakeWebSocket();
      sockets.push(socket);
      return socket as unknown as WebSocket;
    },
  });
  return { client, sockets };
}

test("reconnect retains one subscription and resumes strictly after its processed sequence", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-resume");
  const received: bigint[] = [];
  client.onEvent((event) => {
    if (event.payload?.$case === "searchStateChanged") received.push(event.sequence);
  });

  client.subscribe(target, {
    subscriptionId: "resume-subscription",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "initial WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "initial subscription");
  const initial = subscribeCommand(sockets[0]);
  assert.equal(initial.subscriptionId, "resume-subscription");
  assert.equal(initial.afterSequence, 0n);

  sockets[0].receive(stateEvent(initial.subscriptionId, "search-resume", 101n));
  await waitFor(() => client.getLastSequence(target) === 101n, "first sequence checkpoint");
  sockets[0].serverClose();

  await waitFor(() => sockets.length === 2, "reconnected WebSocket");
  sockets[1].open();
  await waitFor(() => sockets[1].sent.length === 1, "resume subscription");
  const resumed = subscribeCommand(sockets[1]);
  assert.equal(resumed.subscriptionId, initial.subscriptionId);
  assert.equal(resumed.afterSequence, 101n);

  sockets[0].receive(stateEvent(initial.subscriptionId, "search-resume", 102n));
  sockets[1].receive(stateEvent(resumed.subscriptionId, "search-resume", 102n));
  await waitFor(() => client.getLastSequence(target) === 102n, "replayed sequence checkpoint");
  assert.deepEqual(received, [101n, 102n]);
});

test("a sequence gap reconnects from the last processed event and applies the contiguous replay", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-gap");
  const received: bigint[] = [];
  const gaps: Array<{ expected: bigint; received: bigint }> = [];
  client.onEvent((event) => {
    if (event.payload?.$case === "searchStateChanged") received.push(event.sequence);
  });
  client.onSequenceGap((gap) => {
    gaps.push({ expected: gap.expectedSequence, received: gap.receivedSequence });
  });

  client.subscribe(target, {
    subscriptionId: "gap-subscription",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "initial WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "initial subscription");

  sockets[0].receive(stateEvent("gap-subscription", "search-gap", 501n));
  await waitFor(() => client.getLastSequence(target) === 501n, "initial gap checkpoint");
  sockets[0].receive(stateEvent("gap-subscription", "search-gap", 503n));

  await waitFor(() => sockets.length === 2, "gap replay WebSocket");
  assert.deepEqual(gaps, [{ expected: 502n, received: 503n }]);
  assert.equal(client.getLastSequence(target), 501n);
  sockets[1].open();
  await waitFor(() => sockets[1].sent.length === 1, "gap replay subscription");
  assert.equal(subscribeCommand(sockets[1]).afterSequence, 501n);

  sockets[1].receive(stateEvent("gap-subscription", "search-gap", 502n));
  sockets[1].receive(stateEvent("gap-subscription", "search-gap", 503n));
  await waitFor(() => client.getLastSequence(target) === 503n, "contiguous replay checkpoint");
  assert.deepEqual(received, [501n, 502n, 503n]);
});

test("failed authoritative resynchronization retries without advancing or permanently suspending the target", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-resynchronize");
  const errors: string[] = [];
  const received: bigint[] = [];
  let attempts = 0;
  client.onError((error) => errors.push(error.message));
  client.onEvent((event) => {
    if (event.payload?.$case === "searchStateChanged") received.push(event.sequence);
  });
  client.onResynchronizationRequired(async (notice) => {
    attempts += 1;
    if (attempts === 1) throw new Error("transient authoritative GET failure");
    notice.acknowledge();
  });

  client.subscribe(target, {
    subscriptionId: "resynchronize-subscription",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "initial WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "initial subscription");
  sockets[0].receive(stateEvent("resynchronize-subscription", "search-resynchronize", 701n));
  await waitFor(() => client.getLastSequence(target) === 701n, "pre-resynchronization checkpoint");

  sockets[0].receive(resynchronizationEvent(
    "resynchronize-subscription",
    "search-resynchronize",
    710n,
  ));
  await waitFor(() => attempts === 1, "first authoritative recovery attempt");
  await waitFor(() => sockets.length === 2, "resynchronization retry WebSocket");
  assert.equal(client.getLastSequence(target), 701n);

  sockets[1].open();
  await waitFor(() => sockets[1].sent.length === 1, "resynchronization retry subscription");
  assert.equal(subscribeCommand(sockets[1]).afterSequence, 701n);
  sockets[1].receive(resynchronizationEvent(
    "resynchronize-subscription",
    "search-resynchronize",
    710n,
  ));
  await waitFor(() => client.getLastSequence(target) === 710n, "authoritative recovery checkpoint");
  assert.equal(attempts, 2);

  sockets[1].receive(stateEvent("resynchronize-subscription", "search-resynchronize", 711n));
  await waitFor(() => client.getLastSequence(target) === 711n, "post-recovery live checkpoint");
  assert.deepEqual(received, [701n, 711n]);
  assert.ok(errors.some((message) => message.includes("transient authoritative GET failure")));
});

test("a rejected event listener leaves the checkpoint unchanged and replays the event", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-listener-retry");
  const errors: string[] = [];
  let attempts = 0;
  client.onError((error) => errors.push(error.message));
  client.onEvent((event) => {
    if (event.payload?.$case !== "searchStateChanged") return;
    attempts += 1;
    if (attempts === 1) throw new Error("transient event consumer failure");
  });

  client.subscribe(target, {
    subscriptionId: "listener-retry-subscription",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "initial listener WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "initial listener subscription");
  sockets[0].receive(stateEvent(
    "listener-retry-subscription",
    "search-listener-retry",
    801n,
  ));

  await waitFor(() => sockets.length === 2, "listener replay WebSocket");
  assert.equal(client.getLastSequence(target), 0n);
  sockets[1].open();
  await waitFor(() => sockets[1].sent.length === 1, "listener replay subscription");
  assert.equal(subscribeCommand(sockets[1]).afterSequence, 0n);
  sockets[1].receive(stateEvent(
    "listener-retry-subscription",
    "search-listener-retry",
    801n,
  ));

  await waitFor(() => client.getLastSequence(target) === 801n, "listener replay checkpoint");
  assert.equal(attempts, 2);
  assert.ok(errors.some((message) => message.includes("transient event consumer failure")));
});

test("unsubscribing during recovery does not suspend a later subscription to the same target", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-resubscribe");
  let recoveryStarted = false;
  let resolveRecovery!: () => void;
  const recovery = new Promise<void>((resolve) => {
    resolveRecovery = resolve;
  });
  client.onResynchronizationRequired(async () => {
    recoveryStarted = true;
    await recovery;
  });

  client.subscribe(target, {
    subscriptionId: "old-subscription",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "resubscribe WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "old subscription");
  sockets[0].receive(stateEvent("old-subscription", "search-resubscribe", 901n));
  await waitFor(() => client.getLastSequence(target) === 901n, "old subscription checkpoint");
  sockets[0].receive(resynchronizationEvent(
    "old-subscription",
    "search-resubscribe",
    910n,
  ));
  await waitFor(() => recoveryStarted, "authoritative recovery start");

  assert.equal(client.unsubscribe("old-subscription"), true);
  client.subscribe(target, {
    subscriptionId: "new-subscription",
    includePreviews: false,
  });
  resolveRecovery();
  sockets[0].receive(stateEvent("new-subscription", "search-resubscribe", 902n));

  await waitFor(() => client.getLastSequence(target) === 902n, "new subscription checkpoint");
});

test("an invalid server frame reconnects without advancing a target checkpoint", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-invalid-frame");
  const errors: string[] = [];
  client.onError((error) => errors.push(error.message));
  client.subscribe(target, {
    subscriptionId: "invalid-frame-subscription",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "invalid-frame WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "invalid-frame subscription");

  sockets[0].receiveRaw("not a binary protobuf frame");
  await waitFor(() => sockets.length === 2, "invalid-frame replay WebSocket");
  assert.equal(client.getLastSequence(target), 0n);
  sockets[1].open();
  await waitFor(() => sockets[1].sent.length === 1, "invalid-frame replay subscription");
  assert.equal(subscribeCommand(sockets[1]).afterSequence, 0n);
  assert.ok(errors.some((message) => message.includes("binary protobuf is required")));
});

test("a subscription-scoped event cannot advance another target checkpoint", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-target-a");
  const errors: string[] = [];
  client.onError((error) => errors.push(error.message));
  client.subscribe(target, {
    subscriptionId: "target-a-subscription",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "target-coherence WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "target-coherence subscription");

  sockets[0].receive(stateEvent(
    "target-a-subscription",
    "search-target-b",
    1_001n,
  ));
  await waitFor(() => sockets.length === 2, "target-coherence replay WebSocket");
  assert.equal(client.getLastSequence(target), 0n);
  assert.ok(errors.some((message) => message.includes("does not match its subscription target")));
});

test("checkpoints reject values outside protobuf uint64", () => {
  const { client } = clientFixture();
  const target = searchJobTarget("search-sequence-bound");
  try {
    assert.throws(() => client.setLastSequence(target, -1n), /must not be negative/);
    assert.throws(() => client.setLastSequence(target, 1n << 64n), /uint64/);
    client.setLastSequence(target, (1n << 64n) - 1n);
    assert.equal(client.getLastSequence(target), (1n << 64n) - 1n);
  } finally {
    client.dispose();
  }
});

test("open-close flapping backs off until the application subscription is acknowledged", () => {
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const delays: number[] = [];
  const callbacks: Array<() => void> = [];
  const canceled = new Set<number>();
  globalThis.setTimeout = ((handler: TimerHandler, delay?: number, ...arguments_: unknown[]) => {
    if (typeof handler !== "function") throw new TypeError("test timer requires a function");
    const id = callbacks.length;
    delays.push(delay ?? 0);
    callbacks.push(() => handler(...arguments_));
    return id;
  }) as unknown as typeof globalThis.setTimeout;
  globalThis.clearTimeout = ((identifier: number | undefined) => {
    if (identifier !== undefined) canceled.add(identifier);
  }) as typeof globalThis.clearTimeout;

  const sockets: FakeWebSocket[] = [];
  const client = new SearchWebSocketClient({
    autoReconnect: true,
    reconnectDelayMs: 10,
    maximumReconnectDelayMs: 40,
    baseUrl: "http://127.0.0.1:8080/",
    webSocketFactory: () => {
      const socket = new FakeWebSocket();
      sockets.push(socket);
      return socket as unknown as WebSocket;
    },
  });
  const runTimer = (index: number): void => {
    assert.equal(canceled.has(index), false);
    callbacks[index]();
  };
  try {
    client.subscribe(searchJobTarget("search-flapping"), {
      subscriptionId: "flapping-subscription",
      includePreviews: false,
    });
    assert.equal(sockets.length, 1);
    sockets[0].open();
    sockets[0].serverClose();
    assert.deepEqual(delays, [10]);

    runTimer(0);
    assert.equal(sockets.length, 2);
    sockets[1].open();
    sockets[1].serverClose();
    assert.deepEqual(delays, [10, 20]);

    runTimer(1);
    assert.equal(sockets.length, 3);
    sockets[2].open();
    sockets[2].serverClose();
    assert.deepEqual(delays, [10, 20, 40]);
  } finally {
    client.dispose();
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
  }
});
