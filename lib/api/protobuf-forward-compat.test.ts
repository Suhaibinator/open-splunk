import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import path from "node:path";
import test from "node:test";

import { BinaryWriter } from "@bufbuild/protobuf/wire";

import * as openSplunkV1 from "@/gen/ts/index.open_splunk.v1";
import { AppSelector } from "@/gen/ts/open_splunk/v1/app";
import { GetAppRequest } from "@/gen/ts/open_splunk/v1/app_api";
import {
  FieldExtractionDefinition,
  KnowledgeObjectType,
  KnowledgeOverwriteBehavior,
  KnowledgeSearchStage,
  KnowledgeSnapshot,
  KnowledgeSnapshotRef,
  KnowledgeSnapshotSummary,
} from "@/gen/ts/open_splunk/v1/knowledge";
import {
  CreateKnowledgeObjectResponse,
  DeleteKnowledgeObjectResponse,
  KnowledgeMutationOutcomeRecord,
  SetKnowledgeObjectStateResponse,
  UpdateKnowledgeObjectResponse,
} from "@/gen/ts/open_splunk/v1/knowledge_api";
import { SearchJob } from "@/gen/ts/open_splunk/v1/search";
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

interface FieldExtractionWireContract {
  name: "regex" | "json";
  inputField: string;
  overwriteBehavior: number;
  regex?: {
    pattern: string;
    outputFields: string[];
  };
  json?: {
    path: string;
    outputField: string;
  };
  wireHex: string;
}

interface FieldExtractionWireFixture {
  version: number;
  cases: FieldExtractionWireContract[];
}

interface SnapshotWireRecord {
  byteLength: number;
  sha256: string;
}

interface SnapshotFinalWireRecord extends SnapshotWireRecord {
  wireBase64: string;
}

interface KnowledgeSnapshotWireFixture {
  version: number;
  digestDomain: string;
  canonicalSnapshotBytes: number;
  b0: SnapshotWireRecord;
  b1: SnapshotWireRecord;
  snapshotSha256: string;
  final: SnapshotFinalWireRecord;
}

type KnowledgeSnapshotSummaryWireRecord = SnapshotFinalWireRecord;

interface KnowledgeSnapshotReferenceContract {
  snapshotSha256: string;
  tenantCatalogRevision: string;
  tenantCatalogStateToken: string;
  objectCount: number;
  compilerCompatibilityVersion: string;
}

interface KnowledgeSnapshotAuthorizedObjectContract {
  knowledgeObjectId: string;
  version: string;
  name: string;
}

interface KnowledgeSnapshotObjectSummaryContract {
  resolutionOrdinal: number;
  objectType: number;
  stage: number;
  authorizedObject?: KnowledgeSnapshotAuthorizedObjectContract;
  redacted?: boolean;
}

interface KnowledgeSnapshotSummaryWireCase {
  name: "absent" | "enabled-empty" | "authorized-and-redacted";
  ref: KnowledgeSnapshotReferenceContract | null;
  objects?: KnowledgeSnapshotObjectSummaryContract[];
  objectsTruncated?: boolean;
  refWire: KnowledgeSnapshotSummaryWireRecord | null;
  summaryWire: KnowledgeSnapshotSummaryWireRecord | null;
  searchJobWire: KnowledgeSnapshotSummaryWireRecord;
}

interface KnowledgeSnapshotSummaryWireFixture {
  version: number;
  cases: KnowledgeSnapshotSummaryWireCase[];
}

const routeFixture = JSON.parse(
  readFileSync(
    path.join(process.cwd(), "testdata", "protobuf-http-route-contracts.json"),
    "utf8",
  ),
) as ProtobufRouteContractFixture;
const fieldExtractionFixture = JSON.parse(
  readFileSync(
    path.join(process.cwd(), "testdata", "knowledge-field-extraction-wire.json"),
    "utf8",
  ),
) as FieldExtractionWireFixture;
const knowledgeSnapshotWireFixture = JSON.parse(
  readFileSync(
    path.join(process.cwd(), "testdata", "knowledge-snapshot-wire.json"),
    "utf8",
  ),
) as KnowledgeSnapshotWireFixture;
const knowledgeSnapshotSummaryWireFixture = JSON.parse(
  readFileSync(
    path.join(process.cwd(), "testdata", "knowledge-snapshot-summary-wire.json"),
    "utf8",
  ),
) as KnowledgeSnapshotSummaryWireFixture;
const futureFieldTag = (routeFixture.futureFieldNumber << 3) | 2;

function assertWireHash(name: string, wire: Uint8Array, contract: SnapshotWireRecord): void {
  assert.equal(wire.length, contract.byteLength, `${name} byte length changed`);
  assert.equal(
    createHash("sha256").update(wire).digest("hex"),
    contract.sha256,
    `${name} SHA-256 changed`,
  );
}

function assertKnowledgeSnapshotSummaryWire(
  name: string,
  wire: Uint8Array,
  contract: KnowledgeSnapshotSummaryWireRecord,
): void {
  assertWireHash(name, wire, contract);
  assert.deepEqual(
    wire,
    Uint8Array.from(Buffer.from(contract.wireBase64, "base64")),
    `${name} exact wire changed`,
  );
}

function knowledgeSnapshotSummaryFromContract(
  contract: KnowledgeSnapshotSummaryWireCase,
): ReturnType<typeof KnowledgeSnapshotSummary.fromPartial> | undefined {
  if (contract.ref === null) {
    return undefined;
  }

  const objects = (contract.objects ?? []).map((object, index) => {
    assert.notEqual(
      object.authorizedObject === undefined,
      object.redacted === undefined,
      `${contract.name} object ${index} must contain exactly one disclosure variant`,
    );
    const disclosure = object.authorizedObject !== undefined
      ? {
        $case: "authorizedObject" as const,
        value: {
          knowledgeObjectId: object.authorizedObject.knowledgeObjectId,
          version: BigInt(object.authorizedObject.version),
          name: object.authorizedObject.name,
        },
      }
      : {
        $case: "redacted" as const,
        value: object.redacted!,
      };
    return {
      resolutionOrdinal: object.resolutionOrdinal,
      objectType: object.objectType as KnowledgeObjectType,
      stage: object.stage as KnowledgeSearchStage,
      disclosure,
    };
  });

  return KnowledgeSnapshotSummary.fromPartial({
    ref: KnowledgeSnapshotRef.fromPartial({
      snapshotSha256: Uint8Array.from(Buffer.from(contract.ref.snapshotSha256, "hex")),
      tenantCatalogRevision: BigInt(contract.ref.tenantCatalogRevision),
      tenantCatalogStateToken: Uint8Array.from(
        Buffer.from(contract.ref.tenantCatalogStateToken, "hex"),
      ),
      objectCount: contract.ref.objectCount,
      compilerCompatibilityVersion: contract.ref.compilerCompatibilityVersion,
    }),
    objects,
    objectsTruncated: contract.objectsTruncated ?? false,
  });
}

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

test("generated field extraction definitions match shared Go wire goldens", () => {
  assert.equal(fieldExtractionFixture.version, 1);
  assert.equal(fieldExtractionFixture.cases.length, 2);
  assert.deepEqual(
    fieldExtractionFixture.cases.map((contract) => contract.name).toSorted(),
    ["json", "regex"],
  );

  for (const contract of fieldExtractionFixture.cases) {
    assert.notEqual(contract.regex === undefined, contract.json === undefined);
    const overwriteBehavior = contract.overwriteBehavior as KnowledgeOverwriteBehavior;
    const message = contract.regex !== undefined
      ? FieldExtractionDefinition.fromPartial({
        inputField: contract.inputField,
        overwriteBehavior,
        extraction: { $case: "regex", value: contract.regex },
      })
      : FieldExtractionDefinition.fromPartial({
        inputField: contract.inputField,
        overwriteBehavior,
        extraction: { $case: "json", value: contract.json },
      });
    const expected = Uint8Array.from(Buffer.from(contract.wireHex, "hex"));
    assert.ok(expected.length > 0, `${contract.name} golden wire is empty`);

    const first = FieldExtractionDefinition.encode(message).finish();
    const second = FieldExtractionDefinition.encode(message).finish();
    assert.deepEqual(first, second, `${contract.name} TypeScript wire changed between runs`);
    assert.deepEqual(
      first,
      expected,
      `${contract.name} TypeScript wire differs from the shared Go/TypeScript golden`,
    );
    assert.deepEqual(FieldExtractionDefinition.decode(first), message);
  }
});

test("generated knowledge snapshots match the shared Go deterministic wire and digest golden", () => {
  const fixture = knowledgeSnapshotWireFixture;
  assert.equal(fixture.version, 1);
  assert.equal(fixture.digestDomain, "open-splunk-knowledge-snapshot-v0.1\0");

  const expectedFinal = Uint8Array.from(Buffer.from(fixture.final.wireBase64, "base64"));
  assertWireHash("final snapshot", expectedFinal, fixture.final);
  const snapshot = KnowledgeSnapshot.decode(expectedFinal);
  assert.equal(
    Buffer.from(snapshot.snapshotSha256).toString("hex"),
    fixture.snapshotSha256,
  );
  assert.deepEqual(
    KnowledgeSnapshot.encode(snapshot).finish(),
    expectedFinal,
    "TypeScript field ordering differs from Go deterministic final wire",
  );

  snapshot.snapshotSha256 = new Uint8Array(0);
  const b1 = KnowledgeSnapshot.encode(snapshot).finish();
  assertWireHash("B1 digest input", b1, fixture.b1);

  const charges = snapshot.budgetCharges;
  assert.ok(charges !== undefined, "snapshot fixture omitted budget charges");
  assert.equal(charges.canonicalSnapshotBytes, BigInt(fixture.canonicalSnapshotBytes));
  charges.canonicalSnapshotBytes = 0n;
  const b0 = KnowledgeSnapshot.encode(snapshot).finish();
  assertWireHash("B0 canonical charge input", b0, fixture.b0);
  assert.equal(b0.length, fixture.canonicalSnapshotBytes);

  const framedLength = Buffer.alloc(8);
  framedLength.writeBigUInt64BE(BigInt(b1.length));
  const digest = createHash("sha256")
    .update(fixture.digestDomain, "utf8")
    .update(framedLength)
    .update(b1)
    .digest();
  assert.equal(digest.toString("hex"), fixture.snapshotSha256);

  charges.canonicalSnapshotBytes = BigInt(fixture.canonicalSnapshotBytes);
  snapshot.snapshotSha256 = Uint8Array.from(digest);
  const reproducedFinal = KnowledgeSnapshot.encode(snapshot).finish();
  assert.deepEqual(
    reproducedFinal,
    expectedFinal,
    "TypeScript B0/B1 framing did not reproduce Go deterministic final wire",
  );
});

test("generated knowledge snapshot references and summaries match the shared Go wire golden", () => {
  const fixture = knowledgeSnapshotSummaryWireFixture;
  assert.equal(fixture.version, 1);
  assert.equal(fixture.cases.length, 3);
  assert.deepEqual(
    fixture.cases.map((contract) => contract.name),
    ["absent", "enabled-empty", "authorized-and-redacted"],
  );

  const maximumSafeInteger = BigInt(Number.MAX_SAFE_INTEGER);
  let absentWire: Uint8Array | undefined;
  let enabledEmptyWire: Uint8Array | undefined;
  for (const contract of fixture.cases) {
    const summary = knowledgeSnapshotSummaryFromContract(contract);
    if (summary === undefined) {
      assert.equal(contract.name, "absent");
      assert.equal(contract.refWire, null);
      assert.equal(contract.summaryWire, null);
      assert.deepEqual(contract.objects ?? [], []);
      assert.equal(contract.objectsTruncated ?? false, false);
    } else {
      assert.notEqual(contract.refWire, null);
      assert.notEqual(contract.summaryWire, null);
      assert.notEqual(summary.ref, undefined);

      const refWire = KnowledgeSnapshotRef.encode(summary.ref!).finish();
      assertKnowledgeSnapshotSummaryWire(
        `${contract.name} reference`,
        refWire,
        contract.refWire!,
      );
      assert.deepEqual(KnowledgeSnapshotRef.decode(refWire), summary.ref);
      assert.deepEqual(KnowledgeSnapshotRef.encode(summary.ref!).finish(), refWire);

      const summaryWire = KnowledgeSnapshotSummary.encode(summary).finish();
      assertKnowledgeSnapshotSummaryWire(
        `${contract.name} summary`,
        summaryWire,
        contract.summaryWire!,
      );
      assert.deepEqual(KnowledgeSnapshotSummary.decode(summaryWire), summary);
      assert.deepEqual(KnowledgeSnapshotSummary.encode(summary).finish(), summaryWire);
    }

    const job = SearchJob.fromPartial({ knowledgeSnapshot: summary });
    const jobWire = SearchJob.encode(job).finish();
    assertKnowledgeSnapshotSummaryWire(
      `${contract.name} SearchJob attachment`,
      jobWire,
      contract.searchJobWire,
    );
    assert.deepEqual(SearchJob.decode(jobWire), job);
    assert.equal(SearchJob.decode(jobWire).knowledgeSnapshot !== undefined, summary !== undefined);

    switch (contract.name) {
      case "absent":
        absentWire = jobWire;
        break;
      case "enabled-empty":
        enabledEmptyWire = jobWire;
        assert.ok(summary !== undefined);
        assert.ok(summary.ref !== undefined);
        assert.equal(summary.ref.tenantCatalogRevision > maximumSafeInteger, true);
        assert.equal(summary.ref.objectCount, 0);
        assert.deepEqual(summary.objects, []);
        assert.equal(summary.objectsTruncated, false);
        break;
      case "authorized-and-redacted": {
        assert.ok(summary !== undefined);
        assert.ok(summary.ref !== undefined);
        assert.equal(summary.ref.tenantCatalogRevision > maximumSafeInteger, true);
        assert.equal(summary.ref.objectCount, 2);
        assert.equal(summary.objects.length, 2);
        assert.equal(summary.objectsTruncated, false);
        const [authorized, redacted] = summary.objects;
        assert.equal(authorized.disclosure?.$case, "authorizedObject");
        if (authorized.disclosure?.$case !== "authorizedObject") {
          assert.fail("authorized object disclosure is missing");
        }
        assert.equal(authorized.disclosure.value.knowledgeObjectId, "ko-visible");
        assert.equal(authorized.disclosure.value.name, "visible_field");
        assert.equal(authorized.disclosure.value.version > maximumSafeInteger, true);
        assert.deepEqual(redacted.disclosure, { $case: "redacted", value: true });
        break;
      }
    }
  }

  assert.notEqual(absentWire, undefined);
  assert.notEqual(enabledEmptyWire, undefined);
  assert.equal(absentWire!.length, 0);
  assert.notEqual(enabledEmptyWire!.length, 0);
  assert.notDeepEqual(
    enabledEmptyWire,
    absentWire,
    "enabled empty knowledge authority collapsed into disabled/absent knowledge authority",
  );
});

test("generated knowledge mutation responses encode paired revision state deterministically", () => {
  const stateToken = Uint8Array.from({ length: 32 }, (_, index) => index);
  const sharedWire = Uint8Array.from([0x10, 0x07, 0x1a, 0x20, ...stateToken]);
  const deleteWire = Uint8Array.from([0x18, 0x07, 0x22, 0x20, ...stateToken]);
  const cases = [
    {
      name: "create",
      encode: () => CreateKnowledgeObjectResponse.encode(
        CreateKnowledgeObjectResponse.fromPartial({
          tenantCatalogRevision: 7n,
          tenantCatalogStateToken: stateToken,
        }),
      ).finish(),
      expected: sharedWire,
    },
    {
      name: "update",
      encode: () => UpdateKnowledgeObjectResponse.encode(
        UpdateKnowledgeObjectResponse.fromPartial({
          tenantCatalogRevision: 7n,
          tenantCatalogStateToken: stateToken,
        }),
      ).finish(),
      expected: sharedWire,
    },
    {
      name: "set-state",
      encode: () => SetKnowledgeObjectStateResponse.encode(
        SetKnowledgeObjectStateResponse.fromPartial({
          tenantCatalogRevision: 7n,
          tenantCatalogStateToken: stateToken,
        }),
      ).finish(),
      expected: sharedWire,
    },
    {
      name: "delete",
      encode: () => DeleteKnowledgeObjectResponse.encode(
        DeleteKnowledgeObjectResponse.fromPartial({
          tenantCatalogRevision: 7n,
          tenantCatalogStateToken: stateToken,
        }),
      ).finish(),
      expected: deleteWire,
    },
  ];

  for (const contract of cases) {
    const first = contract.encode();
    const second = contract.encode();
    assert.deepEqual(first, second, `${contract.name} response encoding changed between runs`);
    assert.deepEqual(first, contract.expected, `${contract.name} response wire fields changed`);
  }
});

test("generated knowledge mutation outcome authority pins canonical wire", () => {
  const message = KnowledgeMutationOutcomeRecord.fromPartial({
    route: "objects.update",
    mutationKind: "scope_change",
    object: {
      knowledgeObjectId: "ko-1",
      version: 7n,
      definitionSha256: Uint8Array.of(1, 2),
    },
    tenantCatalogRevision: 9n,
    tenantCatalogStateToken: Uint8Array.of(0xaa, 0xbb),
    auditAuthority: { $case: "successfulAuditSequence", value: 11n },
    occurredAtUnixMicro: 13n,
    retentionAnchorUnixMicro: 17n,
    retainUntilUnixMicro: 19n,
  });
  const expected = Uint8Array.from([
    0x0a, 0x0e, ...Buffer.from("objects.update"),
    0x12, 0x0c, ...Buffer.from("scope_change"),
    0x1a, 0x0c, 0x0a, 0x04, ...Buffer.from("ko-1"),
    0x10, 0x07, 0x1a, 0x02, 0x01, 0x02,
    0x20, 0x09, 0x2a, 0x02, 0xaa, 0xbb,
    0x40, 0x0d, 0x48, 0x11, 0x50, 0x13, 0x30, 0x0b,
  ]);
  const first = KnowledgeMutationOutcomeRecord.encode(message).finish();
  const second = KnowledgeMutationOutcomeRecord.encode(message).finish();
  assert.deepEqual(first, second);
  assert.deepEqual(first, expected);
});
