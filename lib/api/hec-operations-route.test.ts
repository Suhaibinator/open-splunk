import assert from "node:assert/strict";
import test from "node:test";

import { GetHECOperationalSnapshotRequest } from "@/gen/ts/open_splunk/hec_admin_api";

import { clearAdministratorBearerToken, setAdministratorBearerToken } from "./administrator-session";
import { OpenSplunkApiClient } from "./open-splunk-client";
import {
  PROTOBUF_CONTENT_TYPE,
  ProtobufResponseTooLargeError,
  ProtobufTransport,
} from "./protobuf-transport";
import { hecOperationsRoutes } from "./routes";

test("HEC operations rejects an oversized response before protobuf decode", async () => {
  const maximumResponseBytes = 64 << 10;
  let cancelled = false;
  const body = new ReadableStream<Uint8Array>({
    start(controller) {
      controller.enqueue(new Uint8Array(maximumResponseBytes + 1));
      controller.close();
    },
    cancel() {
      cancelled = true;
    },
  });
  const client = new OpenSplunkApiClient(new ProtobufTransport({
    fetch: async () => new Response(body, {
      status: 200,
      headers: {
        "Content-Length": String(maximumResponseBytes + 1),
        "Content-Type": PROTOBUF_CONTENT_TYPE,
      },
    }),
  }));
  setAdministratorBearerToken("hec-operations-test-administrator");
  try {
    await assert.rejects(
      client.hec.getOperationalSnapshot(GetHECOperationalSnapshotRequest.fromPartial({})),
      ProtobufResponseTooLargeError,
    );
    assert.equal(hecOperationsRoutes.get.maximumResponseBytes, maximumResponseBytes);
    assert.equal(cancelled, true);
  } finally {
    clearAdministratorBearerToken();
  }
});

test("HEC operations bounds chunked responses without a declared length", async () => {
  const maximumResponseBytes = 64 << 10;
  let cancelled = false;
  let sent = 0;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      sent += 1;
      controller.enqueue(new Uint8Array(4 << 10));
    },
    cancel() {
      cancelled = true;
    },
  }, { highWaterMark: 0 });
  const client = new OpenSplunkApiClient(new ProtobufTransport({
    fetch: async () => new Response(body, {
      status: 200,
      headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
    }),
  }));
  setAdministratorBearerToken("hec-operations-test-administrator");
  try {
    await assert.rejects(
      client.hec.getOperationalSnapshot(GetHECOperationalSnapshotRequest.fromPartial({})),
      ProtobufResponseTooLargeError,
    );
    assert.equal(hecOperationsRoutes.get.maximumResponseBytes, maximumResponseBytes);
    assert.equal(sent, 17);
    assert.equal(cancelled, true);
  } finally {
    clearAdministratorBearerToken();
  }
});
