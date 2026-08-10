import assert from "node:assert/strict";
import test from "node:test";

import { BinaryWriter } from "@bufbuild/protobuf/wire";

import {
  clearAdministratorBearerToken,
  getAdministratorBearerToken,
  isAdministratorRoutePath,
  isValidAdministratorBearerToken,
  setAdministratorBearerToken,
} from "./administrator-session";
import {
  HttpError,
  MAXIMUM_ERROR_RESPONSE_BYTES,
  PROTOBUF_CONTENT_TYPE,
  ProtobufResponseTooLargeError,
  ProtobufTransport,
  defineProtobufRoute,
} from "./protobuf-transport";
import { ListIndexesRequest, ListIndexesResponse } from "@/gen/ts/open_splunk/v1/index_api";
import {
  SetIngestionTokenEnabledRequest,
  SetIngestionTokenEnabledResponse,
} from "@/gen/ts/open_splunk/v1/collector_admin_api";
import {
  ListKnowledgeObjectDependenciesRequest,
  ListKnowledgeObjectDependenciesResponse,
  ListKnowledgeObjectsRequest,
  ListKnowledgeObjectsResponse,
} from "@/gen/ts/open_splunk/v1/knowledge_api";
import { ValidateSearchRequest, ValidateSearchResponse } from "@/gen/ts/open_splunk/v1/search_api";
import { ingestionTokenRoutes, knowledgeRoutes, searchRoutes } from "./routes";

const administratorToken = "admin-token-0123456789-abcdefghijkl";

test.afterEach(() => clearAdministratorBearerToken());

test("administrator bearer tokens match backend admission and remain memory-only", () => {
  assert.equal(isValidAdministratorBearerToken(administratorToken), true);
  assert.equal(isValidAdministratorBearerToken("short"), false);
  assert.equal(isValidAdministratorBearerToken(`${"a".repeat(32)} whitespace`), false);
  assert.equal(isValidAdministratorBearerToken(`=${"a".repeat(31)}`), false);
  assert.equal(isValidAdministratorBearerToken(`${"a".repeat(32)}=a`), false);

  setAdministratorBearerToken(administratorToken);
  assert.equal(getAdministratorBearerToken(), administratorToken);
  clearAdministratorBearerToken();
  assert.equal(getAdministratorBearerToken(), null);
  assert.throws(() => setAdministratorBearerToken("invalid"), TypeError);
});

test("administrator route allowlist excludes ordinary search and WebSocket paths", () => {
  assert.equal(isAdministratorRoutePath("/api/v1/indexes/list"), true);
  assert.equal(isAdministratorRoutePath("/api/v1/audit/events/list"), true);
  assert.equal(isAdministratorRoutePath("/api/v1/ingestion-tokens/state/set"), true);
  assert.equal(isAdministratorRoutePath("/api/v1/hec/operations/get"), true);
  assert.equal(isAdministratorRoutePath("/api/v1/knowledge/objects/get"), true);
  assert.equal(isAdministratorRoutePath("/api/v1/knowledge/objects/list"), true);
  assert.equal(isAdministratorRoutePath("/api/v1/knowledge/objects/dependencies"), true);
  assert.equal(isAdministratorRoutePath("/api/v1/knowledge/objects/dependents"), true);
  assert.equal(isAdministratorRoutePath("/api/v1/knowledge/objects/create"), false);
  assert.equal(isAdministratorRoutePath("/api/v1/knowledge/objects/validate"), false);
  assert.equal(isAdministratorRoutePath("/api/v1/knowledge/objects/update"), false);
  assert.equal(isAdministratorRoutePath("/api/v1/knowledge/objects/delete"), false);
  assert.equal(isAdministratorRoutePath("/api/v1/search/jobs/inspect"), true);
  assert.equal(isAdministratorRoutePath("/api/v1/search/jobs/create"), false);
  assert.equal(isAdministratorRoutePath("/api/v1/search/suggestions"), false);
  assert.equal(isAdministratorRoutePath("/api/v1/search/ws"), false);
  assert.equal(knowledgeRoutes.validate.path, "/api/v1/knowledge/objects/validate");
  assert.equal(knowledgeRoutes.validate.maximumResponseBytes, 8 << 20);
  assert.equal(searchRoutes.inspect.maximumResponseBytes, 8 << 20);
});

test("token state route is administrator-authenticated and protobuf-bound", async () => {
  let requestPath = "";
  let authorization: string | null = null;
  let decoded: SetIngestionTokenEnabledRequest | undefined;
  const transport = new ProtobufTransport({
    fetch: async (input, init) => {
      requestPath = new URL(String(input), "https://example.test").pathname;
      authorization = new Headers(init?.headers).get("Authorization");
      const bytes = init?.body;
      assert.ok(bytes instanceof Uint8Array);
      decoded = SetIngestionTokenEnabledRequest.decode(bytes);
      return new Response(
        SetIngestionTokenEnabledResponse.encode(
          SetIngestionTokenEnabledResponse.fromPartial({}),
        ).finish(),
        { status: 200, headers: { "Content-Type": PROTOBUF_CONTENT_TYPE } },
      );
    },
  });

  setAdministratorBearerToken(administratorToken);
  await transport.post(
    ingestionTokenRoutes.setState,
    SetIngestionTokenEnabledRequest.fromPartial({
      ingestionTokenId: "token-1",
      expectedVersion: 7n,
      enabled: false,
    }),
  );

  assert.equal(requestPath, "/api/v1/ingestion-tokens/state/set");
  assert.equal(authorization, `Bearer ${administratorToken}`);
  assert.deepEqual(decoded, {
    ingestionTokenId: "token-1",
    expectedVersion: 7n,
    enabled: false,
  });
});

test("transport attaches the memory-only token only to protected protobuf calls", async () => {
  const requests: Array<{ url: string; headers: Headers }> = [];
  const fetchImplementation: typeof fetch = async (input, init) => {
    const url = String(input);
    requests.push({ url, headers: new Headers(init?.headers) });
    const body = url.endsWith("/indexes/list")
      ? ListIndexesResponse.encode(ListIndexesResponse.fromPartial({})).finish()
      : url.endsWith("/knowledge/objects/dependencies")
        ? ListKnowledgeObjectDependenciesResponse.encode(
          ListKnowledgeObjectDependenciesResponse.fromPartial({}),
        ).finish()
      : url.endsWith("/knowledge/objects/list")
        ? ListKnowledgeObjectsResponse.encode(ListKnowledgeObjectsResponse.fromPartial({})).finish()
        : ValidateSearchResponse.encode(ValidateSearchResponse.fromPartial({ valid: true })).finish();
    return new Response(body, { status: 200, headers: { "Content-Type": PROTOBUF_CONTENT_TYPE } });
  };
  const transport = new ProtobufTransport({
    fetch: fetchImplementation,
    headers: { "X-Test": "present" },
  });
  const protectedRoute = defineProtobufRoute(
    "/api/v1/indexes/list",
    ListIndexesRequest,
    ListIndexesResponse,
  );
  const searchRoute = defineProtobufRoute(
    "/api/v1/search/validate",
    ValidateSearchRequest,
    ValidateSearchResponse,
  );
  const knowledgeRoute = defineProtobufRoute(
    "/api/v1/knowledge/objects/list",
    ListKnowledgeObjectsRequest,
    ListKnowledgeObjectsResponse,
  );

  setAdministratorBearerToken(administratorToken);
  await transport.post(protectedRoute, ListIndexesRequest.fromPartial({}), {
    headers: { Authorization: "Bearer also-replaced" },
  });
  await transport.post(knowledgeRoute, ListKnowledgeObjectsRequest.fromPartial({}));
  await transport.post(
    knowledgeRoutes.dependencies,
    ListKnowledgeObjectDependenciesRequest.fromPartial({}),
  );
  await transport.post(searchRoute, ValidateSearchRequest.fromPartial({}));

  assert.equal(requests[0]?.headers.get("Authorization"), `Bearer ${administratorToken}`);
  assert.equal(requests[0]?.headers.get("X-Test"), "present");
  assert.equal(requests[1]?.headers.get("Authorization"), `Bearer ${administratorToken}`);
  assert.equal(requests[2]?.headers.get("Authorization"), `Bearer ${administratorToken}`);
  assert.equal(requests[3]?.headers.get("Authorization"), null);
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
    "/api/v1/knowledge/objects/list",
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
    "/api/v1/knowledge/objects/list",
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
    "/api/v1/search/validate",
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
