import assert from "node:assert/strict";
import { existsSync, readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import process from "node:process";
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
import { ListIndexesRequest, ListIndexesResponse } from "@/gen/ts/open_splunk/index_api";
import {
  SetIngestionTokenEnabledRequest,
  SetIngestionTokenEnabledResponse,
} from "@/gen/ts/open_splunk/collector_admin_api";
import {
  CreateKnowledgeObjectRequest,
  CreateKnowledgeObjectResponse,
  DeleteKnowledgeObjectRequest,
  DeleteKnowledgeObjectResponse,
  ListKnowledgeObjectDependenciesRequest,
  ListKnowledgeObjectDependenciesResponse,
  ListKnowledgeObjectsRequest,
  ListKnowledgeObjectsResponse,
  PrepareKnowledgeObjectQuarantineRequest,
  PrepareKnowledgeObjectQuarantineResponse,
  PreviewKnowledgeObjectRequest,
  PreviewKnowledgeObjectResponse,
  QuarantineKnowledgeObjectRequest,
  QuarantineKnowledgeObjectResponse,
  SetKnowledgeObjectStateRequest,
  SetKnowledgeObjectStateResponse,
  UpdateKnowledgeObjectRequest,
  UpdateKnowledgeObjectResponse,
  ValidateKnowledgeObjectRequest,
  ValidateKnowledgeObjectResponse,
} from "@/gen/ts/open_splunk/knowledge_api";
import { ValidateSearchRequest, ValidateSearchResponse } from "@/gen/ts/open_splunk/search_api";
import { ingestionTokenRoutes, knowledgeRoutes, lookupRoutes, searchRoutes, serverSettingsRoutes } from "./routes";

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
  assert.equal(isAdministratorRoutePath("/api/indexes/list"), true);
  assert.equal(isAdministratorRoutePath("/api/audit/events/list"), true);
  assert.equal(isAdministratorRoutePath("/api/ingestion-tokens/state/set"), true);
  assert.equal(isAdministratorRoutePath("/api/hec/operations/get"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/get"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/list"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/dependencies"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/dependents"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/create"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/validate"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/update"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/set-state"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/delete"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/quarantine/prepare"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/quarantine"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/state/set"), false);
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/preview"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/lookups/create"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/lookups/delete"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/lookups/get"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/lookups/list"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/lookups/preview"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/lookups/replace"), true);
  assert.equal(isAdministratorRoutePath("/api/knowledge/lookups/state/set"), true);
  assert.equal(isAdministratorRoutePath("/api/search/jobs/inspect"), true);
  assert.equal(isAdministratorRoutePath("/api/server/settings/get"), true);
  assert.equal(isAdministratorRoutePath("/api/server/settings/update"), true);
  assert.equal(isAdministratorRoutePath("/api/server/appearance/get"), true);
  assert.equal(isAdministratorRoutePath("/api/server/appearance/update"), true);
  assert.equal(serverSettingsRoutes.getAppearance.path, "/api/server/appearance/get");
  assert.equal(serverSettingsRoutes.updateAppearance.path, "/api/server/appearance/update");
  assert.equal(serverSettingsRoutes.getAppearance.authorization, "administrator");
  assert.equal(serverSettingsRoutes.updateAppearance.authorization, "administrator");
  // The palette itself rides bootstrap, which the sign-in page reads without a token.
  assert.equal(isAdministratorRoutePath("/api/system/bootstrap"), false);
  assert.equal(isAdministratorRoutePath("/api/search/jobs/create"), false);
  assert.equal(isAdministratorRoutePath("/api/search/suggestions"), false);
  assert.equal(isAdministratorRoutePath("/api/search/ws"), false);
  assert.equal(knowledgeRoutes.validate.path, "/api/knowledge/objects/validate");
  assert.equal(knowledgeRoutes.validate.maximumResponseBytes, 8 << 20);
  assert.equal(knowledgeRoutes.preview.path, "/api/knowledge/objects/preview");
  assert.equal(knowledgeRoutes.preview.maximumResponseBytes, 8 << 20);
  for (const route of Object.values(lookupRoutes)) {
    assert.equal(route.maximumResponseBytes, 9 << 20);
  }
  assert.equal(searchRoutes.inspect.maximumResponseBytes, 8 << 20);
});

/* == Parity with the Go server: the allowlist is exactly what the server gates == */

/** Reads `internal/server/*.go` (never the tests) as a map of file name to source. */
function goServerSources(): Map<string, string> {
  const directory = join(process.cwd(), "internal", "server");
  assert.ok(existsSync(join(directory, "handler.go")), `run from the repository root; ${directory} has no handler.go`);
  const sources = new Map<string, string>();
  for (const name of readdirSync(directory)) {
    if (name.endsWith(".go") && !name.endsWith("_test.go")) sources.set(name, readFileSync(join(directory, name), "utf8"));
  }
  return sources;
}

/**
 * Every package-level string constant, with `apiPathPrefix + xRoute` sums
 * folded: the route tables name paths both as literals and as constants.
 */
function goStringConstants(sources: Map<string, string>): GoConstants {
  const literals = new Map<string, string>();
  const sums = new Map<string, [string, string]>();
  for (const source of sources.values()) {
    for (const [, name, value] of source.matchAll(/^\s*(\w+)\s*=\s*"([^"\n]*)"\s*$/gmu)) literals.set(name, value);
    for (const [, name, left, right] of source.matchAll(/^\s*(\w+)\s*=\s*(\w+)\s*\+\s*(\w+)\s*$/gmu)) sums.set(name, [left, right]);
  }
  // Resolved on demand: only the names a route table mentions are followed,
  // so a numeric sum elsewhere in the package is never read as a string.
  const resolve: GoConstants = (name) => {
    const literal = literals.get(name);
    if (literal !== undefined) return literal;
    const sum = sums.get(name);
    assert.ok(sum, `no Go string constant named ${name}`);
    return resolve(sum[0]) + resolve(sum[1]);
  };
  return resolve;
}

/** Resolves a package-level Go string constant by name. */
type GoConstants = (name: string) => string;

/** A Go string literal or a constant, as one element of a `[]string{…}` or a map index. */
function goPath(token: string, constants: GoConstants): string {
  const literal = /^"([^"]*)"$/u.exec(token);
  if (literal !== null) return literal[1];
  return constants(token);
}

/**
 * The paths `handler.go` puts into `administratorRoutes`: every element of a
 * `for _, path := range []string{…}` whose body writes the map, and every
 * direct `administratorRoutes[x] = struct{}{}`.
 */
function goAdministratorRoutes(sources: Map<string, string>, constants: GoConstants): Set<string> {
  const handler = sources.get("handler.go") ?? "";
  const start = handler.indexOf("administratorRoutes := make(");
  const end = handler.indexOf("api.administratorRoutes = administratorRoutes", start);
  assert.ok(start >= 0 && end > start, "handler.go no longer builds administratorRoutes the way this test reads it");
  const region = handler.slice(start, end);
  const routes = new Set<string>();
  for (const [, list, body] of region.matchAll(/for _, path := range \[\]string\{([^}]*)\}\s*\{([^}]*)\}/gu)) {
    if (!body.includes("administratorRoutes[path]")) continue;
    for (const token of list.split(",").map((part) => part.trim()).filter((part) => part !== "")) {
      routes.add(goPath(token, constants));
    }
  }
  for (const [, token] of region.matchAll(/administratorRoutes\[([^\]]+)\] = struct\{\}\{\}/gu)) {
    if (token.trim() !== "path") routes.add(goPath(token.trim(), constants));
  }
  return routes;
}

/**
 * The paths the knowledge attempt boundary authenticates on its own: the
 * `case` constants of `knowledgeAttemptFallbackAction`, plus the preview path
 * `protectKnowledgeManagementRoutes` compares against directly.
 */
function goKnowledgeBoundaryRoutes(sources: Map<string, string>, constants: GoConstants): Set<string> {
  const boundary = sources.get("knowledge_attempt_boundary.go") ?? "";
  const start = boundary.indexOf("func knowledgeAttemptFallbackAction(");
  const end = boundary.indexOf("\n}\n", start);
  assert.ok(start >= 0 && end > start, "knowledge_attempt_boundary.go no longer has knowledgeAttemptFallbackAction");
  const routes = new Set<string>();
  for (const [, cases] of boundary.slice(start, end).matchAll(/case ([\w,\s]+):/gu)) {
    for (const token of cases.split(",").map((part) => part.trim()).filter((part) => part !== "")) {
      routes.add(goPath(token, constants));
    }
  }
  for (const [, token] of boundary.matchAll(/request\.URL\.Path == (\w+)/gu)) routes.add(goPath(token, constants));
  return routes;
}

/** The literals inside `ADMINISTRATOR_ROUTE_PATHS`, read from this module's source. */
function typescriptAllowlist(): string[] {
  const source = readFileSync(join(process.cwd(), "lib", "api", "administrator-session.ts"), "utf8");
  const start = source.indexOf("ADMINISTRATOR_ROUTE_PATHS");
  const end = source.indexOf("]);", start);
  assert.ok(start >= 0 && end > start);
  return [...source.slice(start, end).matchAll(/"(\/api\/[^"]+)"/gu)].map(([, literal]) => literal);
}

test("the allowlist is exactly the set of paths the Go server authenticates with a bearer", () => {
  const sources = goServerSources();
  const constants = goStringConstants(sources);
  const administratorRoutes = goAdministratorRoutes(sources, constants);
  const knowledgeGated = goKnowledgeBoundaryRoutes(sources, constants);
  const allowlist = typescriptAllowlist();

  // Sanity on the reading itself, so a regex that silently matched nothing
  // cannot pass the parity check by comparing two empty sets.
  assert.ok(administratorRoutes.size >= 40, `only ${administratorRoutes.size} Go administrator routes were read`);
  assert.ok(knowledgeGated.size >= 10, `only ${knowledgeGated.size} knowledge boundary routes were read`);
  assert.ok(administratorRoutes.has("/api/server/appearance/get"));
  assert.ok(administratorRoutes.has("/api/server/appearance/update"));
  assert.ok(administratorRoutes.has("/api/hec/operations/get"), "a constant-named path was not resolved");
  assert.ok(knowledgeGated.has("/api/knowledge/objects/preview"));
  for (const route of knowledgeGated) assert.ok(!administratorRoutes.has(route), `${route} is gated twice`);

  // No duplicates, and every literal is something `isAdministratorRoutePath` admits.
  assert.equal(new Set(allowlist).size, allowlist.length, "the allowlist repeats a path");
  for (const route of allowlist) assert.equal(isAdministratorRoutePath(route), true, route);

  const serverGated = [...administratorRoutes, ...knowledgeGated].toSorted();
  assert.deepEqual(allowlist.toSorted(), serverGated);
  for (const route of serverGated) assert.equal(isAdministratorRoutePath(route), true, route);
  // Paths the server serves without a bearer never get one.
  for (const route of ["/api/system/bootstrap", "/api/search/validate", "/api/dashboards/list", "/api/search/history/list"]) {
    assert.equal(administratorRoutes.has(route) || knowledgeGated.has(route), false, route);
    assert.equal(isAdministratorRoutePath(route), false, route);
  }
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

  assert.equal(requestPath, "/api/ingestion-tokens/state/set");
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
    "/api/indexes/list",
    ListIndexesRequest,
    ListIndexesResponse,
  );
  const searchRoute = defineProtobufRoute(
    "/api/search/validate",
    ValidateSearchRequest,
    ValidateSearchResponse,
  );
  const knowledgeRoute = defineProtobufRoute(
    "/api/knowledge/objects/list",
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

test("protected protobuf routes discard injected authorization with or without a memory token", async () => {
  const requests: Headers[] = [];
  const transport = new ProtobufTransport({
    headers: {
      aUtHoRiZaTiOn: "Bearer injected-default",
      "X-Default": "present",
    },
    fetch: async (_input, init) => {
      requests.push(new Headers(init?.headers));
      return new Response(
        ListIndexesResponse.encode(ListIndexesResponse.fromPartial({})).finish(),
        { status: 200, headers: { "Content-Type": PROTOBUF_CONTENT_TYPE } },
      );
    },
  });
  const protectedRoute = defineProtobufRoute(
    "/api/indexes/list",
    ListIndexesRequest,
    ListIndexesResponse,
  );

  await transport.post(protectedRoute, ListIndexesRequest.fromPartial({}), {
    headers: {
      AUTHORIZATION: "Bearer injected-request",
      "X-Request": "present",
    },
  });
  setAdministratorBearerToken(administratorToken);
  await transport.post(protectedRoute, ListIndexesRequest.fromPartial({}), {
    headers: { authorization: "Bearer still-injected" },
  });

  assert.equal(requests[0]?.get("Authorization"), null);
  assert.equal(requests[0]?.get("X-Default"), "present");
  assert.equal(requests[0]?.get("X-Request"), "present");
  assert.equal(requests[1]?.get("Authorization"), `Bearer ${administratorToken}`);
});

test("transport rejects forged route authorization before protected-path traffic", async () => {
  let fetches = 0;
  const transport = new ProtobufTransport({
    headers: { Authorization: "Bearer injected" },
    fetch: async () => {
      fetches += 1;
      return new Response(null, { status: 500 });
    },
  });
  const protectedRoute = defineProtobufRoute(
    "/api/indexes/list",
    ListIndexesRequest,
    ListIndexesResponse,
  );
  const forgedRoute = { ...protectedRoute, authorization: "none" as const };

  await assert.rejects(
    transport.post(forgedRoute, ListIndexesRequest.fromPartial({})),
    TypeError,
  );
  assert.equal(fetches, 0);
});

test("unprotected protobuf routes preserve caller authorization headers", async () => {
  let observed: Headers | undefined;
  const transport = new ProtobufTransport({
    headers: { Authorization: "Bearer caller-default" },
    fetch: async (_input, init) => {
      observed = new Headers(init?.headers);
      return new Response(
        ValidateSearchResponse.encode(ValidateSearchResponse.fromPartial({ valid: true })).finish(),
        { status: 200, headers: { "Content-Type": PROTOBUF_CONTENT_TYPE } },
      );
    },
  });
  const searchRoute = defineProtobufRoute(
    "/api/search/validate",
    ValidateSearchRequest,
    ValidateSearchResponse,
  );

  await transport.post(searchRoute, ValidateSearchRequest.fromPartial({}), {
    headers: { Authorization: "Bearer caller-request" },
  });

  assert.equal(observed?.get("Authorization"), "Bearer caller-request");
});

test("all shipped knowledge mutations and Preview receive administrator bearer authority", async () => {
  const requests: Array<{ path: string; authorization: string | null }> = [];
  const responseByPath = new Map<string, Uint8Array>([
    [knowledgeRoutes.create.path, CreateKnowledgeObjectResponse.encode(
      CreateKnowledgeObjectResponse.fromPartial({}),
    ).finish()],
    [knowledgeRoutes.validate.path, ValidateKnowledgeObjectResponse.encode(
      ValidateKnowledgeObjectResponse.fromPartial({}),
    ).finish()],
    [knowledgeRoutes.preview.path, PreviewKnowledgeObjectResponse.encode(
      PreviewKnowledgeObjectResponse.fromPartial({}),
    ).finish()],
    [knowledgeRoutes.update.path, UpdateKnowledgeObjectResponse.encode(
      UpdateKnowledgeObjectResponse.fromPartial({}),
    ).finish()],
    [knowledgeRoutes.setState.path, SetKnowledgeObjectStateResponse.encode(
      SetKnowledgeObjectStateResponse.fromPartial({}),
    ).finish()],
    [knowledgeRoutes.delete.path, DeleteKnowledgeObjectResponse.encode(
      DeleteKnowledgeObjectResponse.fromPartial({}),
    ).finish()],
    [knowledgeRoutes.prepareQuarantine.path, PrepareKnowledgeObjectQuarantineResponse.encode(
      PrepareKnowledgeObjectQuarantineResponse.fromPartial({}),
    ).finish()],
    [knowledgeRoutes.quarantine.path, QuarantineKnowledgeObjectResponse.encode(
      QuarantineKnowledgeObjectResponse.fromPartial({}),
    ).finish()],
  ]);
  const transport = new ProtobufTransport({
    fetch: async (input, init) => {
      const path = new URL(String(input), "https://example.test").pathname;
      requests.push({
        path,
        authorization: new Headers(init?.headers).get("Authorization"),
      });
      const body = responseByPath.get(path);
      assert.ok(body, `unexpected route ${path}`);
      return new Response(Uint8Array.from(body).buffer, {
        status: 200,
        headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
      });
    },
  });

  setAdministratorBearerToken(administratorToken);
  await transport.post(knowledgeRoutes.create, CreateKnowledgeObjectRequest.fromPartial({}));
  await transport.post(knowledgeRoutes.validate, ValidateKnowledgeObjectRequest.fromPartial({}));
  await transport.post(knowledgeRoutes.preview, PreviewKnowledgeObjectRequest.fromPartial({}));
  await transport.post(knowledgeRoutes.update, UpdateKnowledgeObjectRequest.fromPartial({}));
  await transport.post(knowledgeRoutes.setState, SetKnowledgeObjectStateRequest.fromPartial({}));
  await transport.post(knowledgeRoutes.delete, DeleteKnowledgeObjectRequest.fromPartial({}));
  await transport.post(
    knowledgeRoutes.prepareQuarantine,
    PrepareKnowledgeObjectQuarantineRequest.fromPartial({}),
  );
  await transport.post(
    knowledgeRoutes.quarantine,
    QuarantineKnowledgeObjectRequest.fromPartial({}),
  );

  assert.deepEqual(requests.map(({ path }) => path), [
    "/api/knowledge/objects/create",
    "/api/knowledge/objects/validate",
    "/api/knowledge/objects/preview",
    "/api/knowledge/objects/update",
    "/api/knowledge/objects/set-state",
    "/api/knowledge/objects/delete",
    "/api/knowledge/objects/quarantine/prepare",
    "/api/knowledge/objects/quarantine",
  ]);
  assert.ok(requests.every(({ authorization }) =>
    authorization === `Bearer ${administratorToken}`));
  assert.equal(isAdministratorRoutePath("/api/knowledge/objects/preview"), true);
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
