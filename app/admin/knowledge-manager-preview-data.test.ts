import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";
import { createElement } from "react";

import { SharingScope } from "@/gen/ts/open_splunk/common";
import {
  KnowledgeObjectDefinition,
  KnowledgeObjectType,
  KnowledgeOverwriteBehavior,
} from "@/gen/ts/open_splunk/knowledge";
import {
  PreviewKnowledgeObjectRequest,
  PreviewKnowledgeObjectResponse,
  type PreviewKnowledgeObjectRequest as PreviewKnowledgeObjectRequestMessage,
} from "@/gen/ts/open_splunk/knowledge_api";
import {
  ResultRow,
  ResultSchema,
  ResultSetKind,
} from "@/gen/ts/open_splunk/result";
import {
  MissingValue,
  TypedValue,
  ValueType,
} from "@/gen/ts/open_splunk/value";
import {
  PROTOBUF_CONTENT_TYPE,
  ProtobufResponseTooLargeError,
} from "@/lib/api/protobuf-transport";

import {
  KNOWLEDGE_PREVIEW_DEFAULT_MAXIMUM_ROWS,
  KNOWLEDGE_PREVIEW_MAXIMUM_RESPONSE_BYTES,
  adaptKnowledgePreviewResponse,
  createKnowledgePreviewClient,
  knowledgePreviewValueText,
  knowledgePreviewRequest,
} from "./knowledge-manager-preview-data";
import {
  KnowledgePreviewComparison,
  KnowledgeManagerPreview,
  KNOWLEDGE_PREVIEW_RENDERED_CELL_CAP,
  knowledgePreviewUpdateMask,
  knowledgePreviewRowWindow,
  knowledgePreviewRequestIsCurrent,
} from "./knowledge-manager-preview";

const retainedJobID = "retained-preview-job";

function previewDefinition(): KnowledgeObjectDefinition {
  return KnowledgeObjectDefinition.fromPartial({
    appId: "app_preview",
    name: "preview_alias",
    sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
    body: {
      $case: "fieldAlias",
      value: {
        sourceField: "status",
        destinationField: "preview_status",
        overwriteBehavior:
          KnowledgeOverwriteBehavior.KNOWLEDGE_OVERWRITE_BEHAVIOR_PRESERVE_EXISTING,
      },
    },
  });
}

function previewRequest(maximumRows?: number): PreviewKnowledgeObjectRequestMessage {
  return PreviewKnowledgeObjectRequest.fromPartial({
    retainedSearchJobId: retainedJobID,
    definition: previewDefinition(),
    maximumRows,
  });
}

async function definitionDigest(definition: KnowledgeObjectDefinition): Promise<Uint8Array> {
  const bytes = KnowledgeObjectDefinition.encode(definition).finish();
  return new Uint8Array(await globalThis.crypto.subtle.digest(
    "SHA-256",
    Uint8Array.from(bytes).buffer,
  ));
}

function previewSchema(): ResultSchema {
  return ResultSchema.fromPartial({
    schemaId: retainedJobID,
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [{
      fieldName: "preview_status",
      displayName: "preview_status",
      valueType: ValueType.VALUE_TYPE_STRING,
    }],
  });
}

function previewRow(index: number, value = `value-${index}`): ResultRow {
  return ResultRow.fromPartial({
    rowId: `${retainedJobID}:${index}`,
    ordinal: BigInt(index),
    cells: [TypedValue.fromPartial({
      kind: { $case: "stringValue", value },
    })],
  });
}

function widePreviewSchema(prefix: string, columns: number): ResultSchema {
  return ResultSchema.fromPartial({
    schemaId: retainedJobID,
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: Array.from({ length: columns }, (_, index) => ({
      fieldName: `${prefix}_${index}`,
      displayName: `${prefix}_${index}`,
      valueType: ValueType.VALUE_TYPE_STRING,
    })),
  });
}

function widePreviewRow(index: number, columns: number, prefix: string): ResultRow {
  return ResultRow.fromPartial({
    rowId: `${retainedJobID}:${index}`,
    ordinal: BigInt(index),
    cells: Array.from({ length: columns }, (_, column) => TypedValue.fromPartial({
      kind: { $case: "stringValue", value: `${prefix}-${index}-${column}` },
    })),
  });
}

function invalidPreviewResponse(): PreviewKnowledgeObjectResponse {
  return PreviewKnowledgeObjectResponse.fromPartial({
    validation: {
      valid: false,
      objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
      fieldViolations: [{
        fieldPath: "name",
        code: "INVALID_NAME",
        message: "Name is invalid",
      }],
    },
    tenantCatalogRevision: 0n,
  });
}

async function validPreviewResponse(
  beforeRows: ResultRow[] = [previewRow(0, "before")],
  afterRows: ResultRow[] = [previewRow(0, "after")],
): Promise<PreviewKnowledgeObjectResponse> {
  const definition = previewDefinition();
  return PreviewKnowledgeObjectResponse.fromPartial({
    validation: {
      valid: true,
      objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
      normalizedDefinition: definition,
      definitionSha256: await definitionDigest(definition),
      resources: {
        normalizedDefinitionBytes: BigInt(
          KnowledgeObjectDefinition.encode(definition).finish().byteLength,
        ),
      },
    },
    beforeSchema: previewSchema(),
    afterSchema: previewSchema(),
    beforeRows,
    afterRows,
    tenantCatalogRevision: 7n,
  });
}

async function widePreviewResponse(rows: number, columns: number): Promise<PreviewKnowledgeObjectResponse> {
  const response = await validPreviewResponse([], []);
  response.beforeSchema = widePreviewSchema("before", columns);
  response.afterSchema = widePreviewSchema("after", columns);
  response.beforeRows = Array.from(
    { length: rows },
    (_, index) => widePreviewRow(index, columns, "before"),
  );
  response.afterRows = Array.from(
    { length: rows },
    (_, index) => widePreviewRow(index, columns, "after"),
  );
  return response;
}

test("Preview request freezes absent, zero, one, one-thousand, and overflow row semantics", () => {
  assert.equal(knowledgePreviewRequest(previewRequest()).maximumRows, undefined);
  assert.equal(
    knowledgePreviewRequest(previewRequest(1)).maximumRows,
    1,
  );
  assert.equal(
    knowledgePreviewRequest(previewRequest(1_000)).maximumRows,
    1_000,
  );
  assert.throws(() => knowledgePreviewRequest(previewRequest(0)), TypeError);
  assert.throws(() => knowledgePreviewRequest(previewRequest(1_001)), TypeError);
  assert.equal(KNOWLEDGE_PREVIEW_DEFAULT_MAXIMUM_ROWS, 100);
});

test("Preview update mask is canonical and accepted by the request boundary", () => {
  const definition = previewDefinition();
  const updateMask = knowledgePreviewUpdateMask(definition);
  assert.deepEqual(updateMask, [
    "app_id",
    "description",
    "field_alias",
    "name",
    "selector",
    "sharing_scope",
  ]);
  assert.deepEqual(
    knowledgePreviewRequest(PreviewKnowledgeObjectRequest.fromPartial({
      retainedSearchJobId: retainedJobID,
      definition,
      knowledgeObjectId: "ko-preview",
      expectedVersion: 3n,
      updateMask: updateMask ?? undefined,
      maximumRows: 7,
    })).updateMask,
    updateMask,
  );
});

test("Preview response accepts exact paired rows and detaches before asynchronous validation", async () => {
  const response = await validPreviewResponse();
  const pending = adaptKnowledgePreviewResponse(response, previewRequest());
  response.beforeRows[0]!.cells[0]!.kind = {
    $case: "stringValue",
    value: "caller-mutated-secret",
  };
  response.beforeSchema!.columns[0]!.fieldName = "caller-mutated-field";
  const receipt = await pending;
  assert.equal(receipt.validation.valid, true);
  assert.equal(receipt.before?.schema.schemaId, retainedJobID);
  assert.equal(receipt.after?.schema.schemaId, retainedJobID);
  assert.equal(receipt.before?.rows[0]?.rowId, `${retainedJobID}:0`);
  assert.equal(receipt.before?.rows[0]?.cells[0]?.kind?.$case, "stringValue");
  assert.equal(receipt.before?.rows[0]?.cells[0]?.kind?.value, "before");
  assert.notEqual(receipt.before?.schema.columns[0]?.fieldName, "caller-mutated-field");
});

test("Preview response preserves explicit missing cells separately from null", async () => {
  const response = await validPreviewResponse();
  for (const schema of [response.beforeSchema, response.afterSchema]) {
    schema!.columns[0]!.valueType = ValueType.VALUE_TYPE_MISSING;
    schema!.columns[0]!.nullable = true;
  }
  for (const row of [...response.beforeRows, ...response.afterRows]) {
    row.cells[0]!.kind = {
      $case: "missingValue",
      value: MissingValue.MISSING_VALUE_MISSING,
    };
  }

  const receipt = await adaptKnowledgePreviewResponse(response, previewRequest());
  const missing = receipt.before?.rows[0]?.cells[0];
  assert.equal(missing?.kind?.$case, "missingValue");
  assert.equal(knowledgePreviewValueText(missing!), "");
});

test("Preview response fails closed on unpaired, mismatched, over-row, and invalid-output bodies", async () => {
  const wrongJob = await validPreviewResponse();
  wrongJob.afterSchema!.schemaId = "different-job";

  const wrongType = await validPreviewResponse();
  wrongType.afterRows[0]!.cells[0]!.kind = { $case: "boolValue", value: true };

  const overRows = await validPreviewResponse(
    [previewRow(0), previewRow(1)],
    [previewRow(0)],
  );

  const invalidWithOutput = invalidPreviewResponse();
  invalidWithOutput.beforeSchema = previewSchema();

  await assert.rejects(
    adaptKnowledgePreviewResponse(wrongJob, previewRequest()),
    TypeError,
  );
  await assert.rejects(
    adaptKnowledgePreviewResponse(wrongType, previewRequest()),
    TypeError,
  );
  await assert.rejects(
    adaptKnowledgePreviewResponse(overRows, previewRequest(1)),
    TypeError,
  );
  await assert.rejects(
    adaptKnowledgePreviewResponse(invalidWithOutput, previewRequest()),
    TypeError,
  );
});

test("Preview transport rejects unknown response authority and enforces the response cap", async () => {
  let response = invalidPreviewResponse();
  const ordinary = PreviewKnowledgeObjectResponse.encode(response).finish();
  const unknown = new Uint8Array(ordinary.byteLength + 3);
  unknown.set(ordinary);
  unknown.set([0xa0, 0x06, 0x01], ordinary.byteLength);
  let payload = unknown;
  const client = createKnowledgePreviewClient({
    baseUrl: "https://example.test",
    fetch: async () => new Response(payload, {
      status: 200,
      headers: { "Content-Type": PROTOBUF_CONTENT_TYPE },
    }),
  });
  await assert.rejects(client.preview(previewRequest()), TypeError);

  payload = new Uint8Array(KNOWLEDGE_PREVIEW_MAXIMUM_RESPONSE_BYTES + 1);
  await assert.rejects(
    client.preview(previewRequest()),
    ProtobufResponseTooLargeError,
  );

  response = PreviewKnowledgeObjectResponse.decode(ordinary);
  assert.equal(response.validation?.valid, false);
});

test("Preview client construction is traffic-free and forwards cancellation", async () => {
  let fetches = 0;
  let observedSignal: AbortSignal | undefined;
  const client = createKnowledgePreviewClient({
    baseUrl: "https://example.test",
    fetch: async (_input, init) => {
      fetches += 1;
      observedSignal = init?.signal ?? undefined;
      return await new Promise<Response>((_resolve, reject) => {
        const abort = () => reject(new DOMException("Aborted", "AbortError"));
        if (observedSignal?.aborted) abort();
        else observedSignal?.addEventListener("abort", abort, { once: true });
      });
    },
  });
  assert.equal(fetches, 0);
  const controller = new AbortController();
  const pending = client.preview(previewRequest(), { signal: controller.signal });
  controller.abort();
  await assert.rejects(
    pending,
    (error: unknown) => error instanceof DOMException && error.name === "AbortError",
  );
  assert.equal(fetches, 1);
  assert.equal(observedSignal?.aborted, true);
});

test("Preview generation guard rejects aborted, replaced, and stale completions", () => {
  const current = new AbortController();
  const replacement = new AbortController();
  const active = { controller: current, generation: 9 };
  assert.equal(knowledgePreviewRequestIsCurrent(active, current, 9), true);
  assert.equal(knowledgePreviewRequestIsCurrent(active, replacement, 9), false);
  assert.equal(knowledgePreviewRequestIsCurrent(active, current, 8), false);
  assert.equal(knowledgePreviewRequestIsCurrent(null, current, 9), false);
  current.abort();
  assert.equal(knowledgePreviewRequestIsCurrent(active, current, 9), false);
});

test("Preview comparison renders the exact sealed schemas and bounded rows side by side", async () => {
  const receipt = await adaptKnowledgePreviewResponse(
    await validPreviewResponse(
      [previewRow(0, "before-value")],
      [previewRow(0, "after-value")],
    ),
    previewRequest(1),
  );
  const markup = renderToStaticMarkup(createElement(KnowledgePreviewComparison, { receipt }));
  assert.match(markup, /aria-label="Before projection"/);
  assert.match(markup, /aria-label="After projection"/);
  assert.equal((markup.match(/Schema retained-preview-job/g) ?? []).length, 2);
  assert.equal((markup.match(/data-row-id="retained-preview-job:0"/g) ?? []).length, 2);
  assert.match(markup, /before-value/);
  assert.match(markup, /after-value/);
  assert.doesNotMatch(markup, /value-1/);
});

test("Preview row window shares one strict combined rendered-cell budget", () => {
  const exact = knowledgePreviewRowWindow(1_024, 1_024, 8, 8, 0);
  assert.deepEqual(exact, {
    page: 0,
    pageCount: 2,
    rowsPerPage: 7,
    start: 0,
    end: 7,
    totalRows: 8,
  });
  assert.equal((1_024 + 1_024) * (1 + exact.rowsPerPage), KNOWLEDGE_PREVIEW_RENDERED_CELL_CAP);
  assert.ok(
    (1_024 + 1_024) * (2 + exact.rowsPerPage)
      > KNOWLEDGE_PREVIEW_RENDERED_CELL_CAP,
  );

  const finalPage = knowledgePreviewRowWindow(1_024, 1_024, 8, 8, 99);
  assert.equal(finalPage.page, 1);
  assert.equal(finalPage.start, 7);
  assert.equal(finalPage.end, 8);

  const asymmetric = knowledgePreviewRowWindow(1_024, 1, 1_000, 1_000, 0);
  assert.equal(asymmetric.rowsPerPage, 14);
  assert.ok(1_025 * (1 + asymmetric.rowsPerPage) <= KNOWLEDGE_PREVIEW_RENDERED_CELL_CAP);
  assert.ok(1_025 * (2 + asymmetric.rowsPerPage) > KNOWLEDGE_PREVIEW_RENDERED_CELL_CAP);
});

test("Preview comparison maps only the shared first-page cell window", async () => {
  const receipt = await adaptKnowledgePreviewResponse(
    await widePreviewResponse(8, 1_024),
    previewRequest(8),
  );
  const markup = renderToStaticMarkup(createElement(KnowledgePreviewComparison, { receipt }));
  assert.equal(
    (markup.match(/<(?:th|td)\b/g) ?? []).length,
    KNOWLEDGE_PREVIEW_RENDERED_CELL_CAP,
  );
  assert.match(markup, /<output[^>]*aria-label="Knowledge Preview row status"/);
  assert.match(markup, /Rows 1–7 of 8 · page 1 of 2/);
  assert.match(markup, /aria-label="Previous Knowledge Preview rows"[^>]*disabled/);
  assert.match(markup, /aria-label="Next Knowledge Preview rows"/);
  assert.equal((markup.match(/data-row-id="retained-preview-job:6"/g) ?? []).length, 2);
  assert.doesNotMatch(markup, /data-row-id="retained-preview-job:7"/);
});

test("Preview adapter rejects a malformed cell beyond the first rendered page", async () => {
  const response = await widePreviewResponse(8, 1_024);
  response.afterRows[7]!.cells[1_023]!.kind = { $case: "boolValue", value: true };
  await assert.rejects(
    adaptKnowledgePreviewResponse(response, previewRequest(8)),
    TypeError,
  );
});

test("Preview UI is hidden-network idle until an administrator submits a retained job", () => {
  let calls = 0;
  const markup = renderToStaticMarkup(createElement(KnowledgeManagerPreview, {
    client: {
      async preview() {
        calls += 1;
        throw new Error("server rendering must not issue Preview traffic");
      },
    },
    currentKnowledgeObject: {
      knowledgeObjectId: "ko-preview",
      tenantId: "tenant-preview",
      appId: "app_preview",
      ownerId: "administrator",
      objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
      name: "preview_alias",
      version: 3n,
      sharingScope: SharingScope.SHARING_SCOPE_PRIVATE,
      state: 2,
      definition: previewDefinition(),
      definitionSha256: new Uint8Array(32),
      createdAt: new Date("2026-08-10T12:00:00Z"),
      updatedAt: new Date("2026-08-10T12:00:00Z"),
      disabledAt: undefined,
      quarantinedAt: undefined,
      deletedAt: undefined,
      quarantineReason: undefined,
    },
  }));
  assert.equal(calls, 0);
  assert.match(markup, /Preview on retained search/);
  assert.match(markup, /No Preview request has been sent/);
  assert.match(markup, /Maximum rows per side/);
  assert.doesNotMatch(markup, /Before projection|After projection/);
});
