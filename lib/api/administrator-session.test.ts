import assert from "node:assert/strict";
import test from "node:test";

import {
  clearAdministratorBearerToken,
  getAdministratorBearerToken,
  isAdministratorRoutePath,
  isValidAdministratorBearerToken,
  setAdministratorBearerToken,
} from "./administrator-session";
import { PROTOBUF_CONTENT_TYPE, ProtobufTransport, defineProtobufRoute } from "./protobuf-transport";
import { ListIndexesRequest, ListIndexesResponse } from "@/gen/ts/open_splunk/v1/index_api";
import { ValidateSearchRequest, ValidateSearchResponse } from "@/gen/ts/open_splunk/v1/search_api";

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
  assert.equal(isAdministratorRoutePath("/api/v1/search/jobs/inspect"), true);
  assert.equal(isAdministratorRoutePath("/api/v1/search/jobs/create"), false);
  assert.equal(isAdministratorRoutePath("/api/v1/search/suggestions"), false);
  assert.equal(isAdministratorRoutePath("/api/v1/search/ws"), false);
});

test("transport attaches the memory-only token only to protected protobuf calls", async () => {
  const requests: Array<{ url: string; headers: Headers }> = [];
  const fetchImplementation: typeof fetch = async (input, init) => {
    const url = String(input);
    requests.push({ url, headers: new Headers(init?.headers) });
    const body = url.endsWith("/indexes/list")
      ? ListIndexesResponse.encode(ListIndexesResponse.fromPartial({})).finish()
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

  setAdministratorBearerToken(administratorToken);
  await transport.post(protectedRoute, ListIndexesRequest.fromPartial({}), {
    headers: { Authorization: "Bearer also-replaced" },
  });
  await transport.post(searchRoute, ValidateSearchRequest.fromPartial({}));

  assert.equal(requests[0]?.headers.get("Authorization"), `Bearer ${administratorToken}`);
  assert.equal(requests[0]?.headers.get("X-Test"), "present");
  assert.equal(requests[1]?.headers.get("Authorization"), null);
});
