import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";

import { BinaryWriter } from "@bufbuild/protobuf/wire";

import * as openSplunkV1 from "@/gen/ts/index.open_splunk.v1";
import { AppSelector } from "@/gen/ts/open_splunk/v1/app";
import { GetAppRequest } from "@/gen/ts/open_splunk/v1/app_api";
import { GetSystemBootstrapResponse } from "@/gen/ts/open_splunk/v1/system_api";
import { openSplunkRoutes } from "@/lib/api/routes";

interface ProtobufRouteContractRecord {
  path: string;
  requestType: string;
  responseType: string;
  requestKnownWire: string;
  requestFutureWire: string;
  responseKnownWire: string;
  responseFutureWire: string;
}

interface ProtobufRouteContractFixture {
  version: number;
  futureFieldNumber: number;
  routes: ProtobufRouteContractRecord[];
}

interface RuntimeMessageCodec {
  encode(message: unknown, writer?: BinaryWriter): BinaryWriter;
  decode(input: Uint8Array): unknown;
}

const routeFixture = JSON.parse(
  readFileSync(
    path.join(process.cwd(), "testdata", "protobuf-http-route-contracts.json"),
    "utf8",
  ),
) as ProtobufRouteContractFixture;
const futureFieldTag = (routeFixture.futureFieldNumber << 3) | 2;

function registeredRoutePaths(value: unknown): string[] {
  if (value === null || typeof value !== "object") {
    return [];
  }

  if ("path" in value && typeof value.path === "string") {
    return [value.path];
  }

  return Object.values(value).flatMap(registeredRoutePaths);
}

function runtimeMessageCodec(typeName: string): RuntimeMessageCodec {
  // oxlint-disable-next-line import/namespace -- the shared route manifest names generated codecs dynamically.
  const candidate = openSplunkV1[typeName as keyof typeof openSplunkV1] as unknown;
  assert.ok(candidate !== null && typeof candidate === "object", `${typeName} codec is missing`);
  const codec = candidate as Partial<RuntimeMessageCodec>;
  assert.equal(typeof codec.encode, "function", `${typeName}.encode is missing`);
  assert.equal(typeof codec.decode, "function", `${typeName}.decode is missing`);
  return codec as RuntimeMessageCodec;
}

function assertRuntimeWireContract(
  typeName: string,
  pathName: string,
  direction: "request" | "response",
  knownBase64: string,
  futureBase64: string,
): void {
  const codec = runtimeMessageCodec(typeName);
  const known = Buffer.from(knownBase64, "base64");
  const future = Buffer.from(futureBase64, "base64");
  assert.ok(known.length > 0, `${typeName} known fixture is empty`);

  const decodedKnown = codec.decode(known);
  assert.deepEqual(
    codec.decode(codec.encode(decodedKnown).finish()),
    decodedKnown,
    `${typeName} did not preserve its known fields through a TypeScript round trip`,
  );

  const decodedFuture = codec.decode(future);
  assert.deepEqual(
    decodedFuture,
    decodedKnown,
    `${typeName} changed its known fields while discarding its future field`,
  );

  const producedFuture = new BinaryWriter()
    .raw(known)
    .uint32(futureFieldTag)
    .string(`future:${pathName}:${direction}`)
    .finish();
  assert.deepEqual(
    Buffer.from(producedFuture),
    future,
    `${typeName} future fixture is not TypeScript-reproducible`,
  );
}

test("every protobuf HTTP route round-trips generated TypeScript messages across version skew", () => {
  assert.equal(routeFixture.version, 1);
  assert.equal(routeFixture.routes.length, 51);
  assert.equal(new Set(routeFixture.routes.map((route) => route.path)).size, 51);

  for (const route of routeFixture.routes) {
    assertRuntimeWireContract(
      route.requestType,
      route.path,
      "request",
      route.requestKnownWire,
      route.requestFutureWire,
    );
    assertRuntimeWireContract(
      route.responseType,
      route.path,
      "response",
      route.responseKnownWire,
      route.responseFutureWire,
    );
  }
});

test("the browser client registers every protobuf HTTP route exposed by the backend", () => {
  const backendPaths = routeFixture.routes.map((route) => route.path).toSorted();
  const browserPaths = registeredRoutePaths(openSplunkRoutes).toSorted();

  assert.deepEqual(browserPaths, backendPaths);
});

test("generated protobuf request decoders ignore future fields recursively", () => {
  const selector = AppSelector.encode({
    selector: { $case: "appId", value: "app-current" },
  });
  selector.uint32(futureFieldTag).string("future-selector-field");

  const request = new BinaryWriter();
  request.uint32(10).bytes(selector.finish());
  request.uint32(futureFieldTag).string("future-request-field");

  assert.deepEqual(GetAppRequest.decode(request.finish()), {
    selector: {
      selector: { $case: "appId", value: "app-current" },
    },
  });
});

test("generated protobuf response decoders retain known fields from future servers", () => {
  const response = GetSystemBootstrapResponse.encode(
    GetSystemBootstrapResponse.fromPartial({
      apiVersion: "v1",
      splCompatibilityVersion: "open-splunk-v0.1",
    }),
  );
  response.uint32(futureFieldTag).string("future-response-field");

  const decoded = GetSystemBootstrapResponse.decode(response.finish());
  assert.equal(decoded.apiVersion, "v1");
  assert.equal(decoded.splCompatibilityVersion, "open-splunk-v0.1");
});
