import assert from "node:assert/strict";
import test from "node:test";

import {
  InspectSearchJobResponse,
  SearchInspectionOutputKind,
  type SearchInspectionLogicalStage,
  type SearchInspectionOutputProvenance,
} from "@/gen/ts/open_splunk/search_inspection_api";
import {
  KnowledgeObjectType,
  KnowledgeSearchStage,
  type KnowledgeProvenance,
  type KnowledgeSnapshotObjectSummary,
} from "@/gen/ts/open_splunk/knowledge";

import { adaptSearchJobInspection } from "./server-inspection";

const jobId = "inspection-job";
const extractionType = KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION;
const extractionStage = KnowledgeSearchStage.KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION;
const aliasType = KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS;
const aliasStage = KnowledgeSearchStage.KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS;
const calculatedType = KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD;
const calculatedStage = KnowledgeSearchStage.KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD;
const maximumCatalogRevision = 9_223_372_036_854_775_806n;

function sourceRange() {
  return {
    start: { byteOffset: 0n, line: 1, column: 1 },
    end: { byteOffset: 1n, line: 1, column: 2 },
  };
}

function baseResponse(): InspectSearchJobResponse {
  return InspectSearchJobResponse.fromPartial({
    searchJobId: jobId,
    logicalPlan: {
      stages: [{
        stageIndex: 0,
        operator: "Scan",
        // This is valid UTF-8 byte order but not JavaScript UTF-16 order.
        inputFields: ["path\\.field", "\ue000", "\u{10000}"],
        outputFields: ["message"],
        sourceRange: sourceRange(),
        operatorProvenance: [],
        outputProvenance: [],
      }],
      referencedFields: ["path\\.field", "\ue000", "\u{10000}"],
      output: {
        kind: SearchInspectionOutputKind.SEARCH_INSPECTION_OUTPUT_KIND_STATIC,
        fields: ["message"],
        maxDynamicFields: 0,
      },
    },
    physicalPlan: {
      nodeTypes: ["ReadFromMergeTree"],
      reads: [{
        columns: ["message"],
        indexes: [{
          type: "PrimaryKey",
          name: "",
          keys: ["_time"],
          initialParts: 2n,
          selectedParts: 1n,
          initialGranules: 4n,
          selectedGranules: 2n,
        }],
      }],
    },
    generatedSql: "SELECT message FROM events",
    explainText: "ReadFromMergeTree",
    diagnosticQueryId: "diagnostic-query",
  });
}

function redacted(
  ordinal: number,
  objectType: KnowledgeObjectType,
  stage: KnowledgeSearchStage,
): KnowledgeProvenance {
  return {
    source: {
      $case: "redactedObject",
      value: { redactedObjectOrdinal: ordinal, objectType, stage },
    },
  };
}

function redactedSummary(
  ordinal: number,
  objectType: KnowledgeObjectType,
  stage: KnowledgeSearchStage,
): KnowledgeSnapshotObjectSummary {
  return {
    resolutionOrdinal: ordinal,
    objectType,
    stage,
    disclosure: { $case: "redacted", value: true },
  };
}

function addKnowledge(
  response: InspectSearchJobResponse,
  count: number,
  options: {
    duplicateOutput?: boolean;
    operator?: "CopyFieldAlias" | "ParallelExtend";
  } = {},
): InspectSearchJobResponse {
  const operator = options.operator ?? "ParallelExtend";
  const objectType = operator === "CopyFieldAlias" ? aliasType : calculatedType;
  const stage = operator === "CopyFieldAlias" ? aliasStage : calculatedStage;
  const operatorProvenance = Array.from(
    { length: count },
    (_, ordinal) => redacted(ordinal, objectType, stage),
  );
  const outputProvenance = operatorProvenance.map((provenance, ordinal) => ({
    outputField: options.duplicateOutput ? "shared" : `field-${ordinal.toString().padStart(3, "0")}`,
    provenance,
  }));
  const outputFields = options.duplicateOutput
    ? ["shared"]
    : outputProvenance.map((item) => item.outputField);
  response.logicalPlan!.stages.push({
    stageIndex: 1,
    operator,
    inputFields: ["source"],
    outputFields,
    sourceRange: undefined,
    operatorProvenance,
    outputProvenance,
  });
  response.knowledgeSnapshot = {
    ref: {
      snapshotSha256: new Uint8Array(32).fill(0x42),
      tenantCatalogRevision: maximumCatalogRevision,
      tenantCatalogStateToken: new Uint8Array(32).fill(0x24),
      objectCount: count,
      lookupAssetCount: 0,
    },
    objects: Array.from(
      { length: Math.min(count, 64) },
      (_, ordinal) => redactedSummary(ordinal, objectType, stage),
    ),
    objectsTruncated: count > 64,
    lookupAssets: [],
  };
  return response;
}

function addExtraction(response: InspectSearchJobResponse): InspectSearchJobResponse {
  const provenance = redacted(0, extractionType, extractionStage);
  response.logicalPlan!.stages.push({
    stageIndex: 1,
    operator: "ConditionalExtract",
    inputFields: ["_raw"],
    outputFields: ["alpha", "beta"],
    sourceRange: undefined,
    operatorProvenance: [provenance],
    outputProvenance: [
      { outputField: "alpha", provenance },
      { outputField: "beta", provenance },
    ],
  });
  response.knowledgeSnapshot = {
    ref: {
      snapshotSha256: new Uint8Array(32).fill(0xaa),
      tenantCatalogRevision: 1n,
      tenantCatalogStateToken: new Uint8Array(32).fill(0xbb),
      objectCount: 1,
      lookupAssetCount: 0,
    },
    objects: [redactedSummary(0, extractionType, extractionStage)],
    objectsTruncated: false,
    lookupAssets: [],
  };
  return response;
}

function addJsonExtraction(response: InspectSearchJobResponse): InspectSearchJobResponse {
  const provenance = redacted(0, extractionType, extractionStage);
  response.logicalPlan!.stages.push({
    stageIndex: 1,
    operator: "ConditionalExtractJSON",
    inputFields: ["_raw"],
    outputFields: ["json_value"],
    sourceRange: undefined,
    operatorProvenance: [provenance],
    outputProvenance: [{ outputField: "json_value", provenance }],
  });
  response.knowledgeSnapshot = {
    ref: {
      snapshotSha256: new Uint8Array(32).fill(0xcc),
      tenantCatalogRevision: 2n,
      tenantCatalogStateToken: new Uint8Array(32).fill(0xdd),
      objectCount: 1,
      lookupAssetCount: 0,
    },
    objects: [redactedSummary(0, extractionType, extractionStage)],
    objectsTruncated: false,
    lookupAssets: [],
  };
  return response;
}

function addLookup(
  response: InspectSearchJobResponse,
  automatic = false,
): InspectSearchJobResponse {
  response.logicalPlan!.stages.push({
    stageIndex: 1,
    operator: automatic ? "AutomaticLookupGroup" : "Lookup",
    inputFields: ["service_key"],
    outputFields: ["service_owner"],
    sourceRange: automatic ? undefined : sourceRange(),
    operatorProvenance: [],
    outputProvenance: [],
  });
  response.knowledgeSnapshot = {
    ref: {
      snapshotSha256: new Uint8Array(32).fill(0xee),
      tenantCatalogRevision: 3n,
      tenantCatalogStateToken: new Uint8Array(32).fill(0xff),
      objectCount: 0,
      lookupAssetCount: 1,
    },
    objects: [],
    objectsTruncated: false,
    lookupAssets: [],
  };
  return response;
}

function assertInvalid(response: InspectSearchJobResponse, exposeKnowledge = true): void {
  assert.throws(
    () => adaptSearchJobInspection(response, jobId, exposeKnowledge),
    /search job inspection response is invalid/i,
  );
}

test("inspection adaptation binds the exact job and detaches the complete legacy response", () => {
  const response = baseResponse();
  const view = adaptSearchJobInspection(response, jobId, true);
  assert.equal(view.knowledge?.state, "absent");
  assert.deepEqual(view.logicalPlan.referencedFields, ["path\\.field", "\ue000", "\u{10000}"]);
  assert.deepEqual(view.physicalPlan.nodeTypes, ["ReadFromMergeTree"]);

  response.logicalPlan!.stages[0]!.outputFields[0] = "changed";
  response.logicalPlan!.referencedFields[0] = "changed";
  response.physicalPlan!.nodeTypes[0] = "changed";
  response.physicalPlan!.reads[0]!.indexes[0]!.keys[0] = "changed";
  assert.equal(view.logicalPlan.stages[0]!.outputFields[0], "message");
  assert.equal(view.logicalPlan.referencedFields[0], "path\\.field");
  assert.equal(view.physicalPlan.nodeTypes[0], "ReadFromMergeTree");
  assert.equal(view.physicalPlan.reads[0]!.indexes[0]!.keys[0], "_time");

  const foreign = baseResponse();
  foreign.searchJobId = "foreign-secret-job";
  Object.defineProperty(foreign, "logicalPlan", {
    get() { throw new Error("response traversed before identity rejection"); },
  });
  assertInvalid(foreign);
});

test("logical fields mirror canonical SPL path spelling and segment bounds", () => {
  const valid = baseResponse();
  const canonicalFields = ["labels.kubernetes\\.io/app", "路径.字段"];
  valid.logicalPlan!.stages[0]!.inputFields = [...canonicalFields];
  valid.logicalPlan!.stages[0]!.outputFields = [...canonicalFields];
  valid.logicalPlan!.referencedFields = [...canonicalFields];
  valid.logicalPlan!.output!.fields = [...canonicalFields];
  const view = adaptSearchJobInspection(valid, jobId, true);
  assert.deepEqual(view.logicalPlan.stages[0]!.outputFields, canonicalFields);

  const invalidFields = [
    "__OS_private",
    "wild*card",
    "empty..segment",
    "unsupported\\qescape",
    "incomplete\\",
    Array.from({ length: 18 }, () => "segment").join("."),
    "é".repeat(129),
  ];
  for (const field of invalidFields) {
    const response = baseResponse();
    response.logicalPlan!.stages[0]!.outputFields = [field];
    assertInvalid(response);
  }

  const malformedProvenance = addKnowledge(baseResponse(), 1);
  malformedProvenance.logicalPlan!.stages[1]!.outputProvenance[0]!.outputField =
    "unsupported\\qescape";
  assertInvalid(malformedProvenance);
});

test("capability-off adaptation strips forged Knowledge without traversing it", () => {
  const response = baseResponse();
  const stage = response.logicalPlan!.stages[0]!;
  Object.defineProperty(stage, "operatorProvenance", {
    get() { throw new Error("operator provenance was traversed"); },
  });
  Object.defineProperty(stage, "outputProvenance", {
    get() { throw new Error("output provenance was traversed"); },
  });
  Object.defineProperty(response, "knowledgeSnapshot", {
    get() { throw new Error("knowledge snapshot was traversed"); },
  });
  const view = adaptSearchJobInspection(response, jobId, false);
  assert.equal(view.knowledge, undefined);
  assert.deepEqual(view.logicalPlan.stages[0]!.operatorProvenance, []);
  assert.deepEqual(view.logicalPlan.stages[0]!.outputProvenance, []);
});

test("absent and enabled-empty Knowledge authority stay distinct", () => {
  const absent = adaptSearchJobInspection(baseResponse(), jobId, true);
  assert.deepEqual(absent.knowledge, { state: "absent" });

  const response = baseResponse();
  response.knowledgeSnapshot = {
    ref: {
      snapshotSha256: new Uint8Array(32).fill(0x12),
      tenantCatalogRevision: maximumCatalogRevision,
      tenantCatalogStateToken: new Uint8Array(32).fill(0x34),
      objectCount: 0,
      lookupAssetCount: 0,
    },
    objects: [],
    objectsTruncated: false,
    lookupAssets: [],
  };
  const enabled = adaptSearchJobInspection(response, jobId, true);
  assert.equal(enabled.knowledge?.state, "enabled");
  if (enabled.knowledge?.state !== "enabled") assert.fail("enabled Knowledge was omitted");
  assert.equal(enabled.knowledge.objectCount, 0);
  assert.equal(enabled.knowledge.lookupAssetCount, 0);
  assert.equal(enabled.knowledge.tenantCatalogRevision, maximumCatalogRevision);
  assert.equal(enabled.knowledge.digestSha256, "12".repeat(32));
  assert.deepEqual(enabled.knowledge.objects, []);
  assert.equal("tenantCatalogStateToken" in enabled.knowledge, false);
});

test("Knowledge summary boundaries 1, 64, 65, and 256 preserve only the exact prefix", () => {
  for (const count of [1, 64, 65, 256]) {
    const view = adaptSearchJobInspection(addKnowledge(baseResponse(), count), jobId, true);
    assert.equal(view.knowledge?.state, "enabled");
    if (view.knowledge?.state !== "enabled") assert.fail(`count ${count} was not enabled`);
    assert.equal(view.knowledge.objectCount, count);
    assert.equal(view.knowledge.objects.length, Math.min(count, 64));
    assert.equal(view.knowledge.objectsTruncated, count > 64);
    assert.deepEqual(
      view.knowledge.objects.map((object) => object.ordinal),
      Array.from({ length: Math.min(count, 64) }, (_, ordinal) => ordinal),
    );
  }
});

test("explicit and automatic lookup stages retain only logical fields", () => {
  for (const automatic of [false, true]) {
    const view = adaptSearchJobInspection(addLookup(baseResponse(), automatic), jobId, true);
    assert.equal(
      view.logicalPlan.stages[1]!.operator,
      automatic ? "AutomaticLookupGroup" : "Lookup",
    );
    assert.deepEqual(view.logicalPlan.stages[1]!.inputFields, ["service_key"]);
    assert.deepEqual(view.logicalPlan.stages[1]!.outputFields, ["service_owner"]);
    assert.equal(view.logicalPlan.stages[1]!.sourceRange === undefined, automatic);
    assert.deepEqual(view.logicalPlan.stages[1]!.operatorProvenance, []);
    assert.deepEqual(view.logicalPlan.stages[1]!.outputProvenance, []);
    if (view.knowledge?.state !== "enabled") assert.fail("lookup Knowledge was omitted");
    assert.equal(view.knowledge.lookupAssetCount, 1);
  }
});

test("all authored operators remain inspectable", () => {
  for (const operator of [
    "RegexFilter",
    "Reverse",
    "Strcat",
    "FillNull",
    "RowTotal",
    "OrderedDelta",
    "MakeMultivalue",
    "ExpandMultivalue",
  ]) {
    const response = baseResponse();
    response.logicalPlan!.stages.push({
      stageIndex: 1,
      operator,
      inputFields: [],
      outputFields: [],
      sourceRange: sourceRange(),
      operatorProvenance: [],
      outputProvenance: [],
    });
    const view = adaptSearchJobInspection(response, jobId, true);
    assert.equal(view.logicalPlan.stages[1]!.operator, operator);
  }
});

test("automatic lookup stage is unique at the generated prefix boundary", () => {
  const afterAuthored = addLookup(baseResponse(), true);
  afterAuthored.logicalPlan!.stages.splice(1, 0, {
    stageIndex: 1,
    operator: "Filter",
    inputFields: ["status"],
    outputFields: [],
    sourceRange: sourceRange(),
    operatorProvenance: [],
    outputProvenance: [],
  });
  afterAuthored.logicalPlan!.stages[2]!.stageIndex = 2;
  assertInvalid(afterAuthored);

  const duplicated = addLookup(baseResponse(), true);
  duplicated.logicalPlan!.stages.push({
    ...duplicated.logicalPlan!.stages[1]!,
    stageIndex: 2,
  });
  assertInvalid(duplicated);
});

test("lookup summary count is exact, bounded, and identity-redacted", () => {
  const overflow = addLookup(baseResponse());
  overflow.knowledgeSnapshot!.ref!.lookupAssetCount = 17;
  assertInvalid(overflow);

  const disclosed = addLookup(baseResponse());
  disclosed.knowledgeSnapshot!.lookupAssets.push({} as never);
  assertInvalid(disclosed);
});

test("ConditionalExtract preserves one-to-many output provenance", () => {
  const view = adaptSearchJobInspection(addExtraction(baseResponse()), jobId, true);
  const stage = view.logicalPlan.stages[1]!;
  assert.deepEqual(stage.operatorProvenance, [{ ordinal: 0, kind: "Field extraction" }]);
  assert.deepEqual(stage.outputProvenance, [{
    outputField: "alpha",
    provenance: { ordinal: 0, kind: "Field extraction" },
  }, {
    outputField: "beta",
    provenance: { ordinal: 0, kind: "Field extraction" },
  }]);
});

test("ConditionalExtractJSON requires the exact one-object one-output shape", () => {
  const valid = adaptSearchJobInspection(addJsonExtraction(baseResponse()), jobId, true);
  assert.deepEqual(valid.logicalPlan.stages[1]!.outputProvenance, [{
    outputField: "json_value",
    provenance: { ordinal: 0, kind: "Field extraction" },
  }]);

  const extraOutput = addJsonExtraction(baseResponse());
  extraOutput.logicalPlan!.stages[1]!.outputFields.push("second");
  extraOutput.logicalPlan!.stages[1]!.outputProvenance.push({
    outputField: "second",
    provenance: extraOutput.logicalPlan!.stages[1]!.operatorProvenance[0],
  });
  assertInvalid(extraOutput);
});

test("duplicate output occurrences remain distinct by redacted ordinal", () => {
  const view = adaptSearchJobInspection(addKnowledge(baseResponse(), 2, {
    duplicateOutput: true,
    operator: "CopyFieldAlias",
  }), jobId, true);
  const stage = view.logicalPlan.stages[1]!;
  assert.deepEqual(stage.outputFields, ["shared"]);
  assert.deepEqual(stage.outputProvenance.map((item) => ({
    field: item.outputField,
    ordinal: item.provenance.ordinal,
  })), [{ field: "shared", ordinal: 0 }, { field: "shared", ordinal: 1 }]);
});

test("all non-redacted disclosure variants fail closed", () => {
  const mutations: Array<(response: InspectSearchJobResponse) => void> = [
    (response) => { response.knowledgeSnapshot!.objects[0]!.disclosure = undefined; },
    (response) => {
      response.knowledgeSnapshot!.objects[0]!.disclosure = { $case: "redacted", value: false };
    },
    (response) => {
      response.knowledgeSnapshot!.objects[0]!.disclosure = {
        $case: "authorizedObject",
        value: { knowledgeObjectId: "secret-id", version: 1n, name: "secret-name" },
      };
    },
    (response) => { response.logicalPlan!.stages[1]!.operatorProvenance[0]!.source = undefined; },
    (response) => {
      response.logicalPlan!.stages[1]!.operatorProvenance[0]!.source = {
        $case: "authored",
        value: { sourceRange: sourceRange() },
      };
    },
    (response) => {
      response.logicalPlan!.stages[1]!.operatorProvenance[0]!.source = {
        $case: "authorizedObject",
        value: {
          knowledgeObjectId: "secret-id",
          knowledgeObjectVersion: 1n,
          objectType: calculatedType,
          objectName: "secret-name",
          definitionLocation: "secret-definition-location",
          stage: calculatedStage,
        },
      };
    },
  ];
  for (const mutate of mutations) {
    const response = addKnowledge(baseResponse(), 1);
    mutate(response);
    assertInvalid(response);
  }
});

test("ordinal, type, stage, operator, and output bindings fail closed", () => {
  const mutations: Array<(response: InspectSearchJobResponse) => void> = [
    (response) => {
      const source = response.logicalPlan!.stages[1]!.operatorProvenance[0]!.source;
      if (source?.$case === "redactedObject") source.value.redactedObjectOrdinal = 1;
    },
    (response) => {
      const source = response.logicalPlan!.stages[1]!.operatorProvenance[0]!.source;
      if (source?.$case === "redactedObject") source.value.objectType = aliasType;
    },
    (response) => { response.knowledgeSnapshot!.objects[0]!.stage = aliasStage; },
    (response) => {
      response.knowledgeSnapshot!.objects[0]!.objectType = aliasType;
      response.knowledgeSnapshot!.objects[0]!.stage = aliasStage;
    },
    (response) => { response.knowledgeSnapshot!.objects[0]!.resolutionOrdinal = 1; },
    (response) => { response.logicalPlan!.stages[1]!.operator = "CopyFieldAlias"; },
    (response) => { response.logicalPlan!.stages[1]!.outputFields = ["other"]; },
    (response) => {
      response.logicalPlan!.stages[1]!.outputProvenance[0]!.outputField = "z-field";
    },
    (response) => {
      const provenance = response.logicalPlan!.stages[1]!.outputProvenance[0]!.provenance;
      if (provenance?.source?.$case === "redactedObject") {
        provenance.source.value.redactedObjectOrdinal = 9;
      }
    },
  ];
  for (const mutate of mutations) {
    const response = addKnowledge(baseResponse(), 1);
    mutate(response);
    assertInvalid(response);
  }
});

test("Knowledge operator rank and fused-stage nonrepeatability are canonical", () => {
  const repeatedAlias = addKnowledge(baseResponse(), 1, { operator: "CopyFieldAlias" });
  const firstAlias = repeatedAlias.logicalPlan!.stages[1]!;
  const secondProvenance = redacted(1, aliasType, aliasStage);
  repeatedAlias.logicalPlan!.stages.push({
    stageIndex: 2,
    operator: "CopyFieldAlias",
    inputFields: ["source_two"],
    outputFields: ["target_two"],
    sourceRange: undefined,
    operatorProvenance: [secondProvenance],
    outputProvenance: [{ outputField: "target_two", provenance: secondProvenance }],
  });
  repeatedAlias.knowledgeSnapshot!.ref!.objectCount = 2;
  repeatedAlias.knowledgeSnapshot!.objects.push(redactedSummary(1, aliasType, aliasStage));
  assertInvalid(repeatedAlias);
  assert.equal(firstAlias.operator, "CopyFieldAlias");

  const rankRegression = addKnowledge(baseResponse(), 1);
  const aliasProvenance = redacted(1, aliasType, aliasStage);
  rankRegression.logicalPlan!.stages.push({
    stageIndex: 2,
    operator: "CopyFieldAlias",
    inputFields: ["source_two"],
    outputFields: ["target_two"],
    sourceRange: undefined,
    operatorProvenance: [aliasProvenance],
    outputProvenance: [{ outputField: "target_two", provenance: aliasProvenance }],
  });
  rankRegression.knowledgeSnapshot!.ref!.objectCount = 2;
  rankRegression.knowledgeSnapshot!.objects.push(redactedSummary(1, aliasType, aliasStage));
  assertInvalid(rankRegression, false);
});

test("summary truncation is exact at the retained-prefix boundary", () => {
  const truncatedTooEarly = addKnowledge(baseResponse(), 64);
  truncatedTooEarly.knowledgeSnapshot!.objectsTruncated = true;
  assertInvalid(truncatedTooEarly);

  const missingTruncation = addKnowledge(baseResponse(), 65);
  missingTruncation.knowledgeSnapshot!.objectsTruncated = false;
  assertInvalid(missingTruncation);
});

test("raw repeated bounds reject before traversing malformed entries", () => {
  const stageOverflow = baseResponse();
  const stages = Array.from({ length: 514 }, () => null);
  Object.defineProperty(stages, 0, {
    get() { throw new Error("stage traversal occurred"); },
  });
  stageOverflow.logicalPlan!.stages = stages as unknown as SearchInspectionLogicalStage[];
  assertInvalid(stageOverflow);

  const fieldOverflow = baseResponse();
  const fields = Array.from({ length: 1_025 }, () => "field");
  Object.defineProperty(fields, 0, {
    get() { throw new Error("field traversal occurred"); },
  });
  fieldOverflow.logicalPlan!.stages[0]!.inputFields = fields;
  assertInvalid(fieldOverflow);

  const operatorOverflow = addKnowledge(baseResponse(), 1);
  const operators = Array.from({ length: 257 }, () => null);
  Object.defineProperty(operators, 0, {
    get() { throw new Error("operator provenance traversal occurred"); },
  });
  operatorOverflow.logicalPlan!.stages[1]!.operatorProvenance =
    operators as unknown as KnowledgeProvenance[];
  assertInvalid(operatorOverflow);

  const outputOverflow = addKnowledge(baseResponse(), 1);
  const outputs = Array.from({ length: 513 }, () => null);
  Object.defineProperty(outputs, 0, {
    get() { throw new Error("output provenance traversal occurred"); },
  });
  outputOverflow.logicalPlan!.stages[1]!.outputProvenance =
    outputs as unknown as SearchInspectionOutputProvenance[];
  assertInvalid(outputOverflow);

  const summaryOverflow = addKnowledge(baseResponse(), 65);
  const summaryObjects = Array.from({ length: 65 }, () => null);
  Object.defineProperty(summaryObjects, 0, {
    get() { throw new Error("summary traversal occurred"); },
  });
  summaryOverflow.knowledgeSnapshot!.objects =
    summaryObjects as unknown as KnowledgeSnapshotObjectSummary[];
  assertInvalid(summaryOverflow);
});

test("cumulative field occurrences and logical strings use global byte budgets", () => {
  const occurrenceOverflow = baseResponse();
  const boundedFields = Array.from(
    { length: 1_024 },
    (_, index) => `field-${index.toString().padStart(4, "0")}`,
  );
  occurrenceOverflow.logicalPlan!.stages = Array.from({ length: 9 }, (_, stageIndex) => ({
    stageIndex,
    operator: stageIndex === 0 ? "Scan" : "Filter",
    inputFields: [...boundedFields],
    outputFields: [...boundedFields],
    sourceRange: sourceRange(),
    operatorProvenance: [],
    outputProvenance: [],
  }));
  assertInvalid(occurrenceOverflow);

  const stringOverflow = baseResponse();
  stringOverflow.logicalPlan!.stages[0]!.inputFields = Array.from(
    { length: 128 },
    (_, index) => `${index.toString().padStart(3, "0")}-${"x".repeat(8_690)}`,
  );
  assertInvalid(stringOverflow);
});

test("reference commitments, revision, and count fail closed", () => {
  const mutations: Array<(response: InspectSearchJobResponse) => void> = [
    (response) => { response.knowledgeSnapshot!.ref!.snapshotSha256 = new Uint8Array(31); },
    (response) => { response.knowledgeSnapshot!.ref!.tenantCatalogStateToken = new Uint8Array(31); },
    (response) => { response.knowledgeSnapshot!.ref!.tenantCatalogRevision = maximumCatalogRevision + 1n; },
    (response) => { response.knowledgeSnapshot!.ref!.objectCount = 257; },
  ];
  for (const mutate of mutations) {
    const response = addKnowledge(baseResponse(), 1);
    mutate(response);
    assertInvalid(response);
  }
});

test("feature-off still validates non-nested logical structure without reading provenance", () => {
  const missingRange = baseResponse();
  missingRange.logicalPlan!.stages[0]!.sourceRange = undefined;
  assertInvalid(missingRange, false);

  const tooManyAuthored = baseResponse();
  tooManyAuthored.logicalPlan!.stages = Array.from({ length: 257 }, (_, stageIndex) => ({
    stageIndex,
    operator: stageIndex === 0 ? "Scan" : "Filter",
    inputFields: [],
    outputFields: [],
    sourceRange: sourceRange(),
    operatorProvenance: [],
    outputProvenance: [],
  }));
  assertInvalid(tooManyAuthored, false);

  const interleaved = addKnowledge(baseResponse(), 1);
  interleaved.logicalPlan!.stages.splice(1, 0, {
    stageIndex: 1,
    operator: "Filter",
    inputFields: [],
    outputFields: [],
    sourceRange: sourceRange(),
    operatorProvenance: [],
    outputProvenance: [],
  });
  interleaved.logicalPlan!.stages[2]!.stageIndex = 2;
  assertInvalid(interleaved, false);

  const generated = addKnowledge(baseResponse(), 1);
  Object.defineProperty(generated.logicalPlan!.stages[1]!, "operatorProvenance", {
    get() { throw new Error("generated operator provenance was traversed"); },
  });
  Object.defineProperty(generated.logicalPlan!.stages[1]!, "outputProvenance", {
    get() { throw new Error("generated output provenance was traversed"); },
  });
  Object.defineProperty(generated, "knowledgeSnapshot", {
    get() { throw new Error("generated snapshot was traversed"); },
  });
  const hidden = adaptSearchJobInspection(generated, jobId, false);
  assert.equal(hidden.knowledge, undefined);
  assert.deepEqual(hidden.logicalPlan.stages[1]!.operatorProvenance, []);

  const tooManyGenerated = baseResponse();
  tooManyGenerated.logicalPlan!.stages.push(...Array.from(
    { length: 257 },
    (_, generatedIndex): SearchInspectionLogicalStage => ({
      stageIndex: generatedIndex + 1,
      operator: "ConditionalExtract",
      inputFields: ["_raw"],
      outputFields: [`generated-${generatedIndex.toString().padStart(3, "0")}`],
      sourceRange: undefined,
      operatorProvenance: [],
      outputProvenance: [],
    }),
  ));
  Object.defineProperty(tooManyGenerated.logicalPlan!.stages[1]!, "operatorProvenance", {
    get() { throw new Error("generated operator provenance was traversed before its stage bound"); },
  });
  Object.defineProperty(tooManyGenerated, "knowledgeSnapshot", {
    get() { throw new Error("generated snapshot was traversed before its stage bound"); },
  });
  assertInvalid(tooManyGenerated, false);

  const emptyGeneratedOutput = addKnowledge(baseResponse(), 1);
  emptyGeneratedOutput.logicalPlan!.stages[1]!.outputFields = [];
  Object.defineProperty(emptyGeneratedOutput.logicalPlan!.stages[1]!, "operatorProvenance", {
    get() { throw new Error("empty-stage provenance was traversed"); },
  });
  Object.defineProperty(emptyGeneratedOutput, "knowledgeSnapshot", {
    get() { throw new Error("empty-stage snapshot was traversed"); },
  });
  assertInvalid(emptyGeneratedOutput, false);

  const wideJsonOutput = addJsonExtraction(baseResponse());
  wideJsonOutput.logicalPlan!.stages[1]!.outputFields = ["first", "second"];
  Object.defineProperty(wideJsonOutput.logicalPlan!.stages[1]!, "outputProvenance", {
    get() { throw new Error("wide-JSON provenance was traversed"); },
  });
  Object.defineProperty(wideJsonOutput, "knowledgeSnapshot", {
    get() { throw new Error("wide-JSON snapshot was traversed"); },
  });
  assertInvalid(wideJsonOutput, false);
});

test("malicious display primitives remain inert data and no Knowledge identity is retained", () => {
  const malicious = "<img src=x onerror=globalThis.__inspectionExecuted=true>";
  const response = addExtraction(baseResponse());
  response.logicalPlan!.stages[1]!.outputFields = [malicious];
  response.logicalPlan!.stages[1]!.outputProvenance = [{
    outputField: malicious,
    provenance: response.logicalPlan!.stages[1]!.operatorProvenance[0],
  }];
  const view = adaptSearchJobInspection(response, jobId, true);
  assert.equal(view.logicalPlan.stages[1]!.outputFields[0], malicious);
  assert.equal(view.knowledge?.state, "enabled");
  if (view.knowledge?.state !== "enabled") assert.fail("Knowledge was not enabled");
  const keys = Object.keys(view.knowledge).toSorted();
  assert.deepEqual(keys, [
    "digestSha256",
    "lookupAssetCount",
    "objectCount",
    "objects",
    "objectsTruncated",
    "state",
    "tenantCatalogRevision",
  ]);
});

test("nonempty Knowledge adaptation is detached from every decoded container", () => {
  const response = addExtraction(baseResponse());
  const view = adaptSearchJobInspection(response, jobId, true);
  const snapshotView = view.knowledge;
  if (snapshotView?.state !== "enabled") assert.fail("Knowledge was not enabled");
  const digest = snapshotView.digestSha256;
  const summaryObject = { ...snapshotView.objects[0]! };
  const operatorObject = { ...view.logicalPlan.stages[1]!.operatorProvenance[0]! };
  const outputObject = {
    ...view.logicalPlan.stages[1]!.outputProvenance[0]!,
    provenance: { ...view.logicalPlan.stages[1]!.outputProvenance[0]!.provenance },
  };

  response.knowledgeSnapshot!.ref!.snapshotSha256.fill(0);
  response.knowledgeSnapshot!.ref!.tenantCatalogStateToken.fill(0);
  response.knowledgeSnapshot!.objects[0]!.disclosure = undefined;
  const operatorSource = response.logicalPlan!.stages[1]!.operatorProvenance[0]!.source;
  if (operatorSource?.$case === "redactedObject") {
    operatorSource.value.redactedObjectOrdinal = 99;
  }
  response.logicalPlan!.stages[1]!.outputProvenance[0]!.outputField = "changed";

  assert.equal(snapshotView.digestSha256, digest);
  assert.deepEqual(snapshotView.objects[0], summaryObject);
  assert.deepEqual(view.logicalPlan.stages[1]!.operatorProvenance[0], operatorObject);
  assert.deepEqual(view.logicalPlan.stages[1]!.outputProvenance[0], outputObject);
});

test("output-shape, physical, SQL, and EXPLAIN representative bounds fail closed", () => {
  const invalidResponses: InspectSearchJobResponse[] = [];

  const staticDynamic = baseResponse();
  staticDynamic.logicalPlan!.output!.maxDynamicFields = 1;
  invalidResponses.push(staticDynamic);

  const dynamicZero = baseResponse();
  dynamicZero.logicalPlan!.output!.kind =
    SearchInspectionOutputKind.SEARCH_INSPECTION_OUTPUT_KIND_DYNAMIC;
  dynamicZero.logicalPlan!.output!.maxDynamicFields = 0;
  invalidResponses.push(dynamicZero);

  const outputOverflow = baseResponse();
  const outputFields = Array.from({ length: 4_097 }, () => "field");
  Object.defineProperty(outputFields, 0, {
    get() { throw new Error("output shape was traversed"); },
  });
  outputOverflow.logicalPlan!.output!.fields = outputFields;
  invalidResponses.push(outputOverflow);

  const selectedOverflow = baseResponse();
  selectedOverflow.physicalPlan!.reads[0]!.indexes[0]!.selectedParts = 3n;
  invalidResponses.push(selectedOverflow);

  const physicalNodesOverflow = baseResponse();
  const nodeTypes = Array.from({ length: 4_097 }, () => "ReadNothing");
  Object.defineProperty(nodeTypes, 0, {
    get() { throw new Error("physical nodes were traversed"); },
  });
  physicalNodesOverflow.physicalPlan!.nodeTypes = nodeTypes;
  invalidResponses.push(physicalNodesOverflow);

  const sqlControl = baseResponse();
  sqlControl.generatedSql = "SELECT\u0001value";
  invalidResponses.push(sqlControl);

  const sqlOverflow = baseResponse();
  sqlOverflow.generatedSql = "S".repeat((256 << 10) + 1);
  invalidResponses.push(sqlOverflow);

  const explainLineOverflow = baseResponse();
  explainLineOverflow.explainText = "x".repeat((32 << 10) + 1);
  invalidResponses.push(explainLineOverflow);

  const explainCountOverflow = baseResponse();
  explainCountOverflow.explainText = Array.from({ length: 4_097 }, () => "x").join("\n");
  invalidResponses.push(explainCountOverflow);

  const explainEmptyLine = baseResponse();
  explainEmptyLine.explainText = "first\n\nthird";
  invalidResponses.push(explainEmptyLine);

  for (const response of invalidResponses) assertInvalid(response);
});
