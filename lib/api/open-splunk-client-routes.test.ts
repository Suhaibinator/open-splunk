import assert from "node:assert/strict";
import test from "node:test";

import { GetSystemBootstrapResponse } from "@/gen/ts/open_splunk/system_api";

import { isAdministratorRoutePath } from "./administrator-session";
import { createOpenSplunkApiClient, OpenSplunkApiClient } from "./open-splunk-client";
import {
  PROTOBUF_CONTENT_TYPE,
  type ProtobufRequestOptions,
  type ProtobufTransport,
} from "./protobuf-transport";
import {
  alertRoutes,
  appRoutes,
  auditEventRoutes,
  collectorRoutes,
  dashboardRoutes,
  exportRoutes,
  hecOperationsRoutes,
  historyRoutes,
  indexRoutes,
  ingestionTokenRoutes,
  savedSearchRoutes,
  scheduleRoutes,
  searchAttemptAuditRoutes,
  searchRoutes,
  serverSettingsRoutes,
  systemRoutes,
} from "./routes";

const EXPECTED_METHOD_COUNT = 78;

/**
 * The binder's failure mode is a transposition — a method wired to a sibling
 * route — so every namespace is compared against the route table by object
 * identity rather than by shape.
 */
const expectedRoutes: Readonly<Record<string, Readonly<Record<string, object>>>> = {
  system: { bootstrap: systemRoutes.bootstrap },
  serverSettings: serverSettingsRoutes,
  apps: appRoutes,
  collectors: collectorRoutes,
  auditEvents: auditEventRoutes,
  searchAttemptAudit: searchAttemptAuditRoutes,
  indexes: indexRoutes,
  ingestionTokens: ingestionTokenRoutes,
  hec: { getOperationalSnapshot: hecOperationsRoutes.get },
  search: searchRoutes,
  savedSearches: savedSearchRoutes,
  schedules: scheduleRoutes,
  alerts: alertRoutes,
  dashboards: dashboardRoutes,
  history: historyRoutes,
  exports: exportRoutes,
};

interface RecordedCall {
  route: { path: string; authorization: string };
  request: unknown;
  options: ProtobufRequestOptions | undefined;
}

function recordingClient() {
  const calls: RecordedCall[] = [];
  const transport = {
    post(route: RecordedCall["route"], request: unknown, options?: ProtobufRequestOptions) {
      calls.push({ route, request, options });
      return Promise.resolve({ echoedPath: route.path });
    },
  } as unknown as ProtobufTransport;
  return { calls, client: new OpenSplunkApiClient(transport), transport };
}

type BoundMethod = (request: unknown, options?: ProtobufRequestOptions) => Promise<unknown>;

function namespaceEntries(client: OpenSplunkApiClient): [string, Record<string, BoundMethod>][] {
  return Object.entries(client)
    .filter(([name]) => name !== "transport") as [string, Record<string, BoundMethod>][];
}

test("every bound method forwards its own route object, request, and options", async () => {
  const { calls, client, transport } = recordingClient();
  assert.equal(client.transport, transport);
  const namespaces = namespaceEntries(client);
  assert.deepEqual(
    namespaces.map(([name]) => name).toSorted(),
    Object.keys(expectedRoutes).toSorted(),
  );

  const expectations: { label: string; route: object; request: object; options: object }[] = [];
  const pending: Promise<unknown>[] = [];
  for (const [namespace, methods] of namespaces) {
    const routes = expectedRoutes[namespace]!;
    assert.deepEqual(Object.keys(methods).toSorted(), Object.keys(routes).toSorted(), namespace);
    for (const [method, invoke] of Object.entries(methods)) {
      const label = `${namespace}.${method}`;
      const request = { marker: label };
      const options = { timeoutMs: 1_234 };
      assert.equal(typeof invoke, "function", label);
      pending.push(invoke(request, options));
      expectations.push({ label, route: routes[method]!, request, options });
    }
  }

  const results = await Promise.all(pending);
  assert.equal(calls.length, EXPECTED_METHOD_COUNT);
  assert.equal(expectations.length, EXPECTED_METHOD_COUNT);
  for (const [index, expectation] of expectations.entries()) {
    const call = calls[index]!;
    assert.equal(call.route, expectation.route, `${expectation.label} is bound to the wrong route`);
    assert.equal(call.request, expectation.request, expectation.label);
    assert.equal(call.options, expectation.options, expectation.label);
    assert.deepEqual(
      results[index],
      { echoedPath: (expectation.route as { path: string }).path },
      expectation.label,
    );
  }
  assert.equal(
    new Set(calls.map((call) => call.route)).size,
    EXPECTED_METHOD_COUNT,
    "no route object may be reachable through two client methods",
  );
});

test("every bound route carries the authorization its registered path implies", () => {
  const { calls, client } = recordingClient();
  for (const [, methods] of namespaceEntries(client)) {
    for (const invoke of Object.values(methods)) void invoke({});
  }
  assert.equal(calls.length, EXPECTED_METHOD_COUNT);
  for (const { route } of calls) {
    assert.equal(
      route.authorization,
      isAdministratorRoutePath(route.path) ? "administrator" : "none",
      route.path,
    );
    assert.match(route.path, /^\/api\//);
    assert.doesNotMatch(route.path, /^\/api\/v\d/u);
  }
});

test("an omitted options argument reaches the transport as undefined", async () => {
  const { calls, client } = recordingClient();
  await client.search.cancel({ searchJobId: "job" });
  assert.equal(calls.length, 1);
  assert.equal(calls[0]?.route, searchRoutes.cancel);
  assert.equal(calls[0]?.options, undefined);
});

test("the factory binds routes onto a real transport and preserves the route path", async () => {
  const seen: string[] = [];
  const body = GetSystemBootstrapResponse.encode(
    GetSystemBootstrapResponse.fromPartial({}),
  ).finish();
  const fetchStub: typeof globalThis.fetch = (input) => {
    seen.push(typeof input === "string" ? input : String(input));
    return Promise.resolve(new Response(body as BodyInit, {
      status: 200,
      headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
    }));
  };
  const client = createOpenSplunkApiClient({
    baseUrl: "https://example.test/base//",
    fetch: fetchStub,
  });
  await client.system.bootstrap({});
  assert.deepEqual(seen, ["https://example.test/base/api/system/bootstrap"]);
});
