import assert from "node:assert/strict";
import test from "node:test";
import { BinaryWriter } from "@bufbuild/protobuf/wire";
import { currentIdentity, currentSessionRevision, clearIdentitySession, publishIdentity, selectOrganization, subscribeToSessionUnauthorized } from "./identity-session";
import { HttpError, MAXIMUM_ERROR_RESPONSE_BYTES, PROTOBUF_CONTENT_TYPE, ProtobufResponseTooLargeError, ProtobufTransport, defineProtobufRoute } from "./protobuf-transport";
import { ListIndexesRequest, ListIndexesResponse } from "@/gen/ts/open_splunk/index_api";
import { ListKnowledgeObjectsRequest, ListKnowledgeObjectsResponse } from "@/gen/ts/open_splunk/knowledge_api";
import { ValidateSearchRequest, ValidateSearchResponse } from "@/gen/ts/open_splunk/search_api";

test.afterEach(() => clearIdentitySession());

test("organization selection requires membership and advances cache ownership", () => {
  publishIdentity({ status: "authenticated", user: { id: 7n, email: "viewer@example.com", name: "Viewer", userType: 1 }, organizations: [{ id: 8n, name: "Org", role: 1 }], organizationId: null, error: null });
  const before = currentSessionRevision();
  assert.throws(() => selectOrganization(9n), /membership/);
  selectOrganization(8n);
  assert.equal(currentIdentity().organizationId, 8n);
  assert.ok(currentSessionRevision() > before);
  clearIdentitySession();
  assert.equal(currentIdentity().user, null);
  assert.equal(currentIdentity().organizationId, null);
});

test("protobuf calls send cookies and organization context without bearer authority", async () => {
  publishIdentity({ status: "authenticated", user: { id: 7n, email: "viewer@example.com", name: "Viewer", userType: 1 }, organizations: [{ id: 8n, name: "Org", role: 1 }], organizationId: 8n, error: null });
  let options: RequestInit | undefined;
  const transport = new ProtobufTransport({ headers: { aUtHoRiZaTiOn: "Bearer injected", "x-opensplunk-organization-id": "999" }, fetch: async (_url, init) => { options = init; return new Response(ListIndexesResponse.encode(ListIndexesResponse.fromPartial({})).finish(), { headers: { "Content-Type": PROTOBUF_CONTENT_TYPE } }); } });
  await transport.post(defineProtobufRoute("/api/indexes/list", ListIndexesRequest, ListIndexesResponse), ListIndexesRequest.fromPartial({}), { headers: { AUTHORIZATION: "Bearer injected-request", "X-OpenSplunk-Organization-ID": "888" } });
  const headers = new Headers(options?.headers);
  assert.equal(headers.get("Authorization"), null);
  assert.equal(headers.get("X-OpenSplunk-Organization-ID"), "8");
  assert.equal(options?.credentials, "include");
});

test("401 responses signal session revalidation", async () => {
  let signals = 0;
  const unsubscribe = subscribeToSessionUnauthorized(() => { signals += 1; });
  const transport = new ProtobufTransport({ fetch: async () => new Response("expired", { status: 401 }) });
  try { await assert.rejects(transport.post(defineProtobufRoute("/api/indexes/list", ListIndexesRequest, ListIndexesResponse), ListIndexesRequest.fromPartial({})), HttpError); assert.equal(signals, 1); } finally { unsubscribe(); }
});

test("declared oversized unknown protobuf fields are rejected before decode", async () => {
  const maximumResponseBytes = 8 << 20;
  const unknownField = new BinaryWriter()
    .uint32((100 << 3) | 2)
    .bytes(new Uint8Array(maximumResponseBytes))
    .finish();
  let decodeCalls = 0;
  const countedResponseCodec = {
    encode: ListKnowledgeObjectsResponse.encode,
    decode(
      input: Parameters<typeof ListKnowledgeObjectsResponse.decode>[0],
      length?: number,
    ) {
      decodeCalls += 1;
      return ListKnowledgeObjectsResponse.decode(input, length);
    },
  };
  let cancelled = false;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      controller.enqueue(unknownField);
    },
    cancel() {
      cancelled = true;
      return new Promise<void>(() => {
        // A declared-size violation must not wait for an adversarial source.
      });
    },
  }, { highWaterMark: 0 });
  const route = defineProtobufRoute(
    "/api/knowledge/objects/list",
    ListKnowledgeObjectsRequest,
    countedResponseCodec,
    { maximumResponseBytes },
  );
  const transport = new ProtobufTransport({
    fetch: async () => new Response(body, {
      status: 200,
      headers: {
        "Content-Length": String(unknownField.byteLength),
        "Content-Type": PROTOBUF_CONTENT_TYPE,
      },
    }),
  });

  await assert.rejects(
    transport.post(route, ListKnowledgeObjectsRequest.fromPartial({})),
    ProtobufResponseTooLargeError,
  );
  assert.equal(decodeCalls, 0);
  assert.equal(cancelled, true);
});

test("many tiny oversized chunks reject before decode without awaiting cancellation", async () => {
  const maximumResponseBytes = 4 << 10;
  let decodeCalls = 0;
  let cancelled = false;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      controller.enqueue(new Uint8Array(1));
    },
    cancel() {
      cancelled = true;
      return new Promise<void>(() => {
        // An adversarial source may never settle cancellation.
      });
    },
  }, { highWaterMark: 0 });
  const countedResponseCodec = {
    encode: ListKnowledgeObjectsResponse.encode,
    decode(
      input: Parameters<typeof ListKnowledgeObjectsResponse.decode>[0],
      length?: number,
    ) {
      decodeCalls += 1;
      return ListKnowledgeObjectsResponse.decode(input, length);
    },
  };
  const route = defineProtobufRoute(
    "/api/knowledge/objects/list",
    ListKnowledgeObjectsRequest,
    countedResponseCodec,
    { maximumResponseBytes },
  );
  const transport = new ProtobufTransport({
    fetch: async () => new Response(body, {
      status: 200,
      headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
    }),
  });

  await assert.rejects(
    transport.post(route, ListKnowledgeObjectsRequest.fromPartial({})),
    ProtobufResponseTooLargeError,
  );
  assert.equal(decodeCalls, 0);
  assert.equal(cancelled, true);
});

test("error response bodies use a fixed global streaming cap", async () => {
  let cancelled = false;
  const body = new ReadableStream<Uint8Array>({
    pull(controller) {
      controller.enqueue(new Uint8Array(MAXIMUM_ERROR_RESPONSE_BYTES));
    },
    cancel() {
      cancelled = true;
    },
  }, { highWaterMark: 0 });
  const route = defineProtobufRoute(
    "/api/search/validate",
    ValidateSearchRequest,
    ValidateSearchResponse,
  );
  const transport = new ProtobufTransport({
    fetch: async () => new Response(body, { status: 500, statusText: "Failure" }),
  });

  await assert.rejects(
    transport.post(route, ValidateSearchRequest.fromPartial({})),
    (error: unknown) => {
      assert.ok(error instanceof HttpError);
      assert.equal(error.status, 500);
      assert.equal(error.message, "HTTP 500: Failure");
      assert.equal(error.responseBody, undefined);
      return true;
    },
  );
  assert.equal(cancelled, true);
});
