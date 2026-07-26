import assert from "node:assert/strict";
import test from "node:test";

import { SearchJobState } from "../../gen/ts/open_splunk/v1/search";
import {
  ResynchronizationReason,
  SearchWebSocketCommand,
  type SearchWebSocketEvent,
  SearchWebSocketEvent as SearchWebSocketEventCodec,
  SearchWebSocketProtocolErrorCode,
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
  reason = ResynchronizationReason.RESYNCHRONIZATION_REASON_SEQUENCE_EXPIRED,
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
        reason,
        earliestAvailableSequence: latestSequence,
        latestSequence,
        recoveryPath: "/api/v1/search/jobs/get",
      },
    },
  };
}

function subscriptionAcknowledgedEvent(
  requestId: string,
  subscriptionId: string,
  searchJobId: string,
): SearchWebSocketEvent {
  return {
    sequence: 0n,
    occurredAt: new Date("2026-07-25T00:00:01.000Z"),
    subscriptionId,
    target: searchJobTarget(searchJobId),
    payload: {
      $case: "subscriptionAcknowledged",
      value: {
        requestId,
        subscriptionId,
        target: searchJobTarget(searchJobId),
        earliestAvailableSequence: 1n,
        latestSequence: 1n,
        replayWillFollow: false,
      },
    },
  };
}

function closingSubscriptionLimitEvent(requestId: string): SearchWebSocketEvent {
  return {
    sequence: 0n,
    occurredAt: new Date("2026-07-25T00:00:01.000Z"),
    subscriptionId: "",
    target: undefined,
    payload: {
      $case: "protocolError",
      value: {
        requestId,
        code: SearchWebSocketProtocolErrorCode.SEARCH_WEB_SOCKET_PROTOCOL_ERROR_CODE_TOO_MANY_SUBSCRIPTIONS,
        message: "connection subscription identity limit exceeded",
        violations: [],
        connectionWillClose: true,
      },
    },
  };
}

function unknownSequencedEvent(
  subscriptionId: string,
  searchJobId: string,
  sequence: bigint,
): Uint8Array {
  const knownEnvelope = SearchWebSocketEventCodec.encode({
    sequence,
    occurredAt: new Date("2026-07-25T00:00:02.000Z"),
    subscriptionId,
    target: searchJobTarget(searchJobId),
    payload: undefined,
  }).finish();
  // Future length-delimited oneof field 24 with an empty message. Current
  // generated code skips it and exposes payload=undefined.
  const wire = new Uint8Array(knownEnvelope.length + 3);
  wire.set(knownEnvelope);
  wire.set([0xc2, 0x01, 0x00], knownEnvelope.length);
  return wire;
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

test("a sequence gap applies contiguous replay and ignores stale duplicates on the recovered socket", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-gap");
  const received: bigint[] = [];
  const gaps: Array<{ expected: bigint; received: bigint }> = [];
  const errors: string[] = [];
  client.onError((error) => errors.push(error.message));
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
  sockets[1].receive(stateEvent("gap-subscription", "search-gap", 503n));
  sockets[1].receive(stateEvent("gap-subscription", "search-gap", 504n));
  await waitFor(() => client.getLastSequence(target) === 504n, "post-duplicate checkpoint");
  assert.deepEqual(received, [501n, 502n, 503n, 504n]);
  assert.deepEqual(gaps, [{ expected: 502n, received: 503n }]);
  assert.deepEqual(errors, []);
  assert.equal(sockets.length, 2);
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

test("an acknowledgment remains provisional until asynchronous recovery finishes", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-provisional-acknowledgment");
  const received: bigint[] = [];
  let recoveryStarted = false;
  let releaseRecovery!: () => void;
  const recovery = new Promise<void>((resolve) => {
    releaseRecovery = resolve;
  });
  client.onEvent((event) => {
    if (event.payload?.$case === "searchStateChanged") received.push(event.sequence);
  });
  client.onResynchronizationRequired(async (notice) => {
    notice.acknowledge();
    recoveryStarted = true;
    await recovery;
  });

  client.subscribe(target, {
    subscriptionId: "provisional-acknowledgment",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "provisional-acknowledgment WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "provisional-acknowledgment subscription");
  sockets[0].receive(stateEvent(
    "provisional-acknowledgment",
    "search-provisional-acknowledgment",
    601n,
  ));
  await waitFor(() => client.getLastSequence(target) === 601n, "pre-recovery checkpoint");
  sockets[0].receive(resynchronizationEvent(
    "provisional-acknowledgment",
    "search-provisional-acknowledgment",
    610n,
  ));
  await waitFor(() => recoveryStarted, "provisional acknowledgment");
  sockets[0].receive(stateEvent(
    "provisional-acknowledgment",
    "search-provisional-acknowledgment",
    611n,
  ));

  assert.equal(client.getLastSequence(target), 601n);
  assert.deepEqual(received, [601n]);
  releaseRecovery();
  await waitFor(() => client.getLastSequence(target) === 611n, "post-recovery checkpoint");
  assert.deepEqual(received, [601n, 611n]);
});

test("a resolved but unacknowledged recovery listener cannot advance the checkpoint", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-unhandled-resynchronization");
  const errors: string[] = [];
  client.onError((error) => errors.push(error.message));
  client.onResynchronizationRequired(() => {
    // Model a global listener that owns a different target and deliberately
    // does not acknowledge this notice.
  });

  client.subscribe(target, {
    subscriptionId: "unhandled-resynchronization",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "unhandled-resynchronization WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "unhandled-resynchronization subscription");
  sockets[0].receive(stateEvent(
    "unhandled-resynchronization",
    "search-unhandled-resynchronization",
    751n,
  ));
  await waitFor(() => client.getLastSequence(target) === 751n, "safe pre-recovery checkpoint");
  sockets[0].receive(resynchronizationEvent(
    "unhandled-resynchronization",
    "search-unhandled-resynchronization",
    760n,
  ));

  await waitFor(() => sockets.length === 2, "unacknowledged recovery retry");
  assert.equal(client.getLastSequence(target), 751n);
  sockets[1].open();
  await waitFor(() => sockets[1].sent.length === 1, "unacknowledged recovery subscription");
  assert.equal(subscribeCommand(sockets[1]).afterSequence, 751n);
  assert.ok(errors.some((message) => message.includes("did not acknowledge")));
});

test("a same-epoch resynchronization cannot regress its processed checkpoint", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-stale-resynchronization");
  const errors: string[] = [];
  let recoveries = 0;
  client.onError((error) => errors.push(error.message));
  client.onResynchronizationRequired((notice) => {
    recoveries++;
    notice.acknowledge();
  });
  client.subscribe(target, {
    subscriptionId: "stale-resynchronization",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "stale-resynchronization WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "stale-resynchronization subscription");
  sockets[0].receive(stateEvent(
    "stale-resynchronization",
    "search-stale-resynchronization",
    900n,
  ));
  await waitFor(() => client.getLastSequence(target) === 900n, "stale-resynchronization checkpoint");

  sockets[0].receive(resynchronizationEvent(
    "stale-resynchronization",
    "search-stale-resynchronization",
    899n,
  ));
  await waitFor(() => sockets.length === 2, "stale-resynchronization replay");
  assert.equal(client.getLastSequence(target), 900n);
  assert.equal(recoveries, 0);
  assert.ok(errors.some((message) => message.includes("cannot regress")));
});

test("a resynchronization payload cannot name a different subscription", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-resynchronization-subscription-fence");
  const errors: string[] = [];
  let recoveries = 0;
  client.onError((error) => errors.push(error.message));
  client.onResynchronizationRequired((notice) => {
    recoveries += 1;
    notice.acknowledge();
  });
  client.subscribe(target, {
    subscriptionId: "resynchronization-subscription-fence",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "subscription-fence WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "subscription-fence subscription");
  sockets[0].receive(stateEvent(
    "resynchronization-subscription-fence",
    "search-resynchronization-subscription-fence",
    41n,
  ));
  await waitFor(() => client.getLastSequence(target) === 41n, "subscription-fence checkpoint");

  const malformed = resynchronizationEvent(
    "resynchronization-subscription-fence",
    "search-resynchronization-subscription-fence",
    45n,
  );
  assert.equal(malformed.payload?.$case, "resynchronizationRequired");
  if (malformed.payload?.$case !== "resynchronizationRequired") {
    throw new Error("expected resynchronization event");
  }
  malformed.payload.value.subscriptionId = "different-subscription";
  sockets[0].receive(malformed);

  await waitFor(() => sockets.length === 2, "subscription-fence replay");
  assert.equal(client.getLastSequence(target), 41n);
  assert.equal(recoveries, 0);
  assert.ok(errors.some((message) => message.includes("subscription ID")));
});

test("a resynchronization envelope rejects inverted replay bounds", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-resynchronization-bounds");
  const errors: string[] = [];
  let recoveries = 0;
  client.onError((error) => errors.push(error.message));
  client.onResynchronizationRequired((notice) => {
    recoveries += 1;
    notice.acknowledge();
  });
  client.subscribe(target, {
    subscriptionId: "resynchronization-bounds",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "resynchronization-bounds WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "resynchronization-bounds subscription");
  sockets[0].receive(stateEvent(
    "resynchronization-bounds",
    "search-resynchronization-bounds",
    51n,
  ));
  await waitFor(() => client.getLastSequence(target) === 51n, "resynchronization-bounds checkpoint");

  const malformed = resynchronizationEvent(
    "resynchronization-bounds",
    "search-resynchronization-bounds",
    55n,
  );
  assert.equal(malformed.payload?.$case, "resynchronizationRequired");
  if (malformed.payload?.$case !== "resynchronizationRequired") {
    throw new Error("expected resynchronization event");
  }
  malformed.payload.value.earliestAvailableSequence = 56n;
  sockets[0].receive(malformed);

  await waitFor(() => sockets.length === 2, "resynchronization-bounds replay");
  assert.equal(client.getLastSequence(target), 51n);
  assert.equal(recoveries, 0);
  assert.ok(errors.some((message) => message.includes("replay bounds")));
});

test("a server restart may establish a lower sequence epoch after authoritative recovery", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-restarted-epoch");
  client.onResynchronizationRequired((notice) => {
    notice.acknowledge();
  });
  client.subscribe(target, {
    subscriptionId: "restarted-epoch",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "restarted-epoch WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "restarted-epoch subscription");
  sockets[0].receive(stateEvent("restarted-epoch", "search-restarted-epoch", 950n));
  await waitFor(() => client.getLastSequence(target) === 950n, "old epoch checkpoint");

  sockets[0].receive(resynchronizationEvent(
    "restarted-epoch",
    "search-restarted-epoch",
    25n,
    ResynchronizationReason.RESYNCHRONIZATION_REASON_SERVER_RESTARTED,
  ));
  await waitFor(() => client.getLastSequence(target) === 25n, "new epoch checkpoint");
  assert.equal(sockets.length, 1);
});

test("a close-required subscription limit preserves the rejected subscription for reconnect", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  let protocolErrors = 0;
  client.onProtocolError(() => {
    protocolErrors++;
  });
  client.subscribe(searchJobTarget("search-identity-reconnect"), {
    subscriptionId: "identity-reconnect",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "identity-limit WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "identity-limit subscription");
  const rejected = SearchWebSocketCommand.decode(sockets[0].sent[0]);
  sockets[0].receive(closingSubscriptionLimitEvent(rejected.requestId));
  await waitFor(() => protocolErrors === 1, "identity-limit protocol error");
  sockets[0].serverClose();

  await waitFor(() => sockets.length === 2, "identity-limit reconnect");
  sockets[1].open();
  await waitFor(() => sockets[1].sent.length === 1, "identity-limit retry");
  const retried = subscribeCommand(sockets[1]);
  assert.equal(retried.subscriptionId, "identity-reconnect");
  assert.equal(retried.target?.target?.$case, "searchJobId");
  assert.equal(retried.target?.target?.value, "search-identity-reconnect");
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

test("a future sequenced event variant remains forward compatible and advances its routed checkpoint", async (t) => {
  const { client, sockets } = clientFixture();
  t.after(() => client.dispose());
  const target = searchJobTarget("search-future-event");
  const observed: SearchWebSocketEvent[] = [];
  client.onEvent((event) => {
    observed.push(event);
  });
  client.subscribe(target, {
    subscriptionId: "future-event-subscription",
    includePreviews: false,
  });
  await waitFor(() => sockets.length === 1, "future-event WebSocket");
  sockets[0].open();
  await waitFor(() => sockets[0].sent.length === 1, "future-event subscription");

  sockets[0].receiveRaw(unknownSequencedEvent(
    "future-event-subscription",
    "search-future-event",
    1_101n,
  ));
  await waitFor(() => client.getLastSequence(target) === 1_101n, "future-event checkpoint");
  assert.equal(sockets.length, 1);
  assert.equal(observed.length, 1);
  assert.equal(observed[0].payload, undefined);
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

test("open-close and unhandled recovery failures retain exponential reconnect backoff", async () => {
  const originalSetTimeout = globalThis.setTimeout;
  const originalClearTimeout = globalThis.clearTimeout;
  const delays: number[] = [];
  const callbacks: Array<() => void> = [];
  const canceled = new Set<number>();
  let holdAcknowledgment = false;
  let acknowledgmentStarted = false;
  let releaseAcknowledgment: (() => void) | undefined;
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
  client.onEvent(async (event) => {
    if (!holdAcknowledgment || event.payload?.$case !== "subscriptionAcknowledged") return;
    acknowledgmentStarted = true;
    await new Promise<void>((resolve) => {
      releaseAcknowledgment = resolve;
    });
    throw new Error("slow acknowledgment listener rejected");
  });
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

    runTimer(2);
    assert.equal(sockets.length, 4);
    sockets[3].open();
    const thirdCommand = SearchWebSocketCommand.decode(sockets[3].sent[0]);
    sockets[3].receive(subscriptionAcknowledgedEvent(
      thirdCommand.requestId,
      "flapping-subscription",
      "search-flapping",
    ));
    sockets[3].receive(resynchronizationEvent(
      "flapping-subscription",
      "search-flapping",
      1n,
    ));
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(canceled.has(3), true);
    assert.deepEqual(delays, [10, 20, 40, 1_000, 40]);

    runTimer(4);
    assert.equal(sockets.length, 5);
    sockets[4].open();
    const fourthCommand = SearchWebSocketCommand.decode(sockets[4].sent[0]);
    sockets[4].receive(subscriptionAcknowledgedEvent(
      fourthCommand.requestId,
      "flapping-subscription",
      "search-flapping",
    ));
    sockets[4].receive(resynchronizationEvent(
      "flapping-subscription",
      "search-flapping",
      1n,
    ));
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(canceled.has(5), true);
    assert.deepEqual(delays, [10, 20, 40, 1_000, 40, 1_000, 40]);

    runTimer(6);
    assert.equal(sockets.length, 6);
    sockets[5].open();
    holdAcknowledgment = true;
    const fifthCommand = SearchWebSocketCommand.decode(sockets[5].sent[0]);
    sockets[5].receive(subscriptionAcknowledgedEvent(
      fifthCommand.requestId,
      "flapping-subscription",
      "search-flapping",
    ));
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.equal(acknowledgmentStarted, true);
    assert.deepEqual(delays, [10, 20, 40, 1_000, 40, 1_000, 40]);

    releaseAcknowledgment?.();
    await new Promise<void>((resolve) => setImmediate(resolve));
    assert.deepEqual(delays, [10, 20, 40, 1_000, 40, 1_000, 40, 40]);
  } finally {
    client.dispose();
    globalThis.setTimeout = originalSetTimeout;
    globalThis.clearTimeout = originalClearTimeout;
  }
});
