import {
  SearchInspectionOutputKind,
  type InspectSearchJobResponse,
  type SearchInspectionLogicalStage,
  type SearchInspectionPhysicalIndex,
} from "@/gen/ts/open_splunk/v1/search_inspection_api";
import {
  KnowledgeObjectType,
  KnowledgeSearchStage,
  type KnowledgeSnapshotSummary,
} from "@/gen/ts/open_splunk/v1/knowledge";

const UTF8 = new TextEncoder();
const CONTROL_CHARACTER = /\p{Cc}/u;

const MAXIMUM_UINT32 = 0xffff_ffff;
const MAXIMUM_UINT64 = (1n << 64n) - 1n;
const MAXIMUM_CATALOG_REVISION = (1n << 63n) - 2n;
const MAXIMUM_SOURCE_BYTES = 16 << 10;
const MAXIMUM_AUTHORED_PLAN_STAGES = 256;
const MAXIMUM_KNOWLEDGE_OBJECTS = 256;
const MAXIMUM_PLAN_STAGES = MAXIMUM_AUTHORED_PLAN_STAGES + MAXIMUM_KNOWLEDGE_OBJECTS;
const MAXIMUM_STAGE_FIELDS = 1_024;
const MAXIMUM_FINAL_OUTPUT_FIELDS = 4_096;
const MAXIMUM_FIELD_OCCURRENCES = 16_384;
const MAXIMUM_LOGICAL_STRING_BYTES = 1 << 20;
const MAXIMUM_FIELD_NAME_BYTES = 8_720;
const MAXIMUM_FIELD_PATH_SEGMENTS = 17;
const MAXIMUM_FIELD_PATH_SEGMENT_BYTES = 256;
const MAXIMUM_OPERATOR_BYTES = 32;
const MAXIMUM_DYNAMIC_FIELDS = 1_024;
const MAXIMUM_OPERATOR_PROVENANCE = 256;
const MAXIMUM_OUTPUT_PROVENANCE = 512;
const MAXIMUM_SUMMARY_OBJECTS = 64;
const MAXIMUM_COMPILER_BYTES = 128;
const MAXIMUM_GENERATED_SQL_BYTES = 256 << 10;
const MAXIMUM_EXPLAIN_BYTES = 1 << 20;
const MAXIMUM_EXPLAIN_LINES = 4_096;
const MAXIMUM_EXPLAIN_LINE_BYTES = 16 << 10;
const MAXIMUM_DIAGNOSTIC_QUERY_ID_BYTES = 128;
const MAXIMUM_PHYSICAL_NODES = 4_096;
const MAXIMUM_PHYSICAL_READS = 256;
const MAXIMUM_PHYSICAL_HEADERS = 4_096;
const MAXIMUM_PHYSICAL_INDEXES = 4_096;
const MAXIMUM_PHYSICAL_INDEX_KEYS = 64;
const MAXIMUM_PHYSICAL_STRING_BYTES = 16 << 10;
const MAXIMUM_PHYSICAL_TOTAL_STRING_BYTES = 1 << 20;

const AUTHORED_OPERATORS = new Set([
  "Scan",
  "Filter",
  "Project",
  "Extend",
  "TimeBucket",
  "NumericBucket",
  "Extract",
  "ExtractJSON",
  "Rename",
  "Aggregate",
  "EventAggregate",
  "StreamAggregate",
  "Timechart",
  "Chart",
  "Window",
  "Sort",
  "Deduplicate",
  "Limit",
]);

const PHYSICAL_NODE_TYPES = new Set([
  "Aggregating",
  "CreatingSet",
  "CreatingSets",
  "Expression",
  "Filter",
  "Join",
  "JoinLazyColumnsStep",
  "LazilyReadFromMergeTree",
  "Limit",
  "MaterializingCTE",
  "MaterializingCTEs",
  "ReadFromMemoryStorage",
  "ReadFromMergeTree",
  "ReadFromSystemNumbers",
  "ReadNothing",
  "Sorting",
  "Union",
  "Window",
]);

const PHYSICAL_INDEX_TYPES = new Set(["MinMax", "Partition", "PrimaryKey", "Skip"]);
const SKIP_INDEX_NAMES = new Set([
  "",
  "idx_event_id",
  "idx_trace_id",
  "idx_span_id",
  "idx_field_names",
  "idx_raw_text",
  "idx_visibility_seq",
]);

const CANONICAL_SPL_FIELDS = new Set([
  "event_id",
  "index",
  "_time",
  "_indextime",
  "host",
  "source",
  "sourcetype",
  "service",
  "severity",
  "level",
  "message",
  "_raw",
  "trace_id",
  "span_id",
  "collector_id",
  "batch_id",
]);

export type ServerInspectionKnowledgeKind =
  | "Field extraction"
  | "Field alias"
  | "Calculated field";

interface KnowledgeContract {
  objectType: KnowledgeObjectType;
  stage: KnowledgeSearchStage;
  kind: ServerInspectionKnowledgeKind;
  rank: number;
  shape: "one-to-many" | "one-to-one" | "fused-one-to-one";
  repeatable: boolean;
}

const KNOWLEDGE_OPERATOR_CONTRACTS = new Map<string, KnowledgeContract>([
  ["ConditionalExtract", {
    objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
    stage: KnowledgeSearchStage.KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
    kind: "Field extraction",
    rank: 1,
    shape: "one-to-many",
    repeatable: true,
  }],
  ["ConditionalExtractJSON", {
    objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_EXTRACTION,
    stage: KnowledgeSearchStage.KNOWLEDGE_SEARCH_STAGE_FIELD_EXTRACTION,
    kind: "Field extraction",
    rank: 1,
    shape: "one-to-one",
    repeatable: true,
  }],
  ["CopyFieldAlias", {
    objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_FIELD_ALIAS,
    stage: KnowledgeSearchStage.KNOWLEDGE_SEARCH_STAGE_FIELD_ALIAS,
    kind: "Field alias",
    rank: 2,
    shape: "fused-one-to-one",
    repeatable: false,
  }],
  ["ParallelExtend", {
    objectType: KnowledgeObjectType.KNOWLEDGE_OBJECT_TYPE_CALCULATED_FIELD,
    stage: KnowledgeSearchStage.KNOWLEDGE_SEARCH_STAGE_CALCULATED_FIELD,
    kind: "Calculated field",
    rank: 3,
    shape: "fused-one-to-one",
    repeatable: false,
  }],
]);

export interface ServerInspectionSourcePosition {
  byteOffset: bigint;
  line: number;
  column: number;
}

export interface ServerInspectionSourceRange {
  start: ServerInspectionSourcePosition;
  end: ServerInspectionSourcePosition;
}

export interface ServerInspectionRedactedProvenance {
  ordinal: number;
  kind: ServerInspectionKnowledgeKind;
}

export interface ServerInspectionOutputProvenance {
  outputField: string;
  provenance: ServerInspectionRedactedProvenance;
}

export interface ServerInspectionLogicalStageView {
  stageIndex: number;
  operator: string;
  inputFields: string[];
  outputFields: string[];
  sourceRange?: ServerInspectionSourceRange;
  operatorProvenance: ServerInspectionRedactedProvenance[];
  outputProvenance: ServerInspectionOutputProvenance[];
}

export interface ServerInspectionLogicalPlanView {
  stages: ServerInspectionLogicalStageView[];
  referencedFields: string[];
  output: {
    kind: "open" | "static" | "dynamic";
    fields: string[];
    maxDynamicFields: number;
  };
}

export interface ServerInspectionPhysicalPlanView {
  nodeTypes: string[];
  reads: Array<{
    columns: string[];
    indexes: Array<{
      type: string;
      name: string;
      keys: string[];
      initialParts: bigint;
      selectedParts: bigint;
      initialGranules: bigint;
      selectedGranules: bigint;
    }>;
  }>;
}

export type ServerInspectionKnowledgeView =
  | { state: "absent" }
  | {
    state: "enabled";
    digestSha256: string;
    tenantCatalogRevision: bigint;
    objectCount: number;
    compilerCompatibilityVersion: string;
    objects: ServerInspectionRedactedProvenance[];
    objectsTruncated: boolean;
  };

export interface ServerSearchJobInspection {
  logicalPlan: ServerInspectionLogicalPlanView;
  physicalPlan: ServerInspectionPhysicalPlanView;
  generatedSql: string;
  explainText: string;
  diagnosticQueryId: string;
  /** Undefined means the capability was not advertised and no Knowledge field was parsed. */
  knowledge?: ServerInspectionKnowledgeView;
}

export type ServerSearchJobInspectionState =
  | { status: "idle" }
  | { status: "loading" }
  | { status: "available"; response: ServerSearchJobInspection }
  | { status: "error"; error: string };

interface LogicalBudget {
  fieldOccurrences: number;
  stringBytes: number;
  operatorProvenance: number;
  outputProvenance: number;
}

interface AdaptedLogicalPlan {
  view: ServerInspectionLogicalPlanView;
  knowledgeObjects: ServerInspectionRedactedProvenance[];
}

function invalidInspection(): never {
  throw new TypeError("The search job inspection response is invalid.");
}

function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null;
}

interface BoundedUtf8 {
  bytes: Uint8Array;
  value: string;
}

function boundedUtf8Value(
  value: unknown,
  maximumBytes: number,
  allowEmpty = false,
): BoundedUtf8 {
  if (
    typeof value !== "string"
    || (!allowEmpty && value.length === 0)
    || value.length > maximumBytes
    || !value.isWellFormed()
  ) {
    return invalidInspection();
  }
  const bytes = UTF8.encode(value);
  if (bytes.byteLength > maximumBytes) return invalidInspection();
  return { bytes, value: value.slice(0) };
}

function boundedUtf8(value: unknown, maximumBytes: number, allowEmpty = false): string {
  return boundedUtf8Value(value, maximumBytes, allowEmpty).value;
}

function safeLogicalString(
  value: unknown,
  maximumBytes: number,
  budget: LogicalBudget,
): string {
  const copied = boundedUtf8Value(value, maximumBytes);
  if (CONTROL_CHARACTER.test(copied.value)) return invalidInspection();
  if (copied.bytes.byteLength > MAXIMUM_LOGICAL_STRING_BYTES - budget.stringBytes) {
    return invalidInspection();
  }
  budget.stringBytes += copied.bytes.byteLength;
  return copied.value;
}

function safePhysicalString(
  value: unknown,
  budget: { stringBytes: number },
  allowEmpty = false,
): string {
  const copied = boundedUtf8Value(value, MAXIMUM_PHYSICAL_STRING_BYTES, allowEmpty);
  if (CONTROL_CHARACTER.test(copied.value)) return invalidInspection();
  if (copied.bytes.byteLength > MAXIMUM_PHYSICAL_TOTAL_STRING_BYTES - budget.stringBytes) {
    return invalidInspection();
  }
  budget.stringBytes += copied.bytes.byteLength;
  return copied.value;
}

function uint32(value: unknown): number {
  if (typeof value !== "number" || !Number.isInteger(value) || value < 0 || value > MAXIMUM_UINT32) {
    return invalidInspection();
  }
  return value;
}

function uint64(value: unknown): bigint {
  if (typeof value !== "bigint" || value < 0n || value > MAXIMUM_UINT64) {
    return invalidInspection();
  }
  return value;
}

function booleanValue(value: unknown): boolean {
  if (typeof value !== "boolean") return invalidInspection();
  return value;
}

function asciiWhitespace(codePoint: number): boolean {
  return codePoint === 0x20 || (codePoint >= 0x09 && codePoint <= 0x0d);
}

function hasAsciiEdgeWhitespace(value: string): boolean {
  return asciiWhitespace(value.codePointAt(0)!)
    || asciiWhitespace(value.codePointAt(value.length - 1)!);
}

function sqlForbiddenControl(value: string): boolean {
  for (const character of value) {
    const codePoint = character.codePointAt(0)!;
    if (
      (codePoint <= 0x1f && codePoint !== 0x09 && codePoint !== 0x0a && codePoint !== 0x0d)
      || (codePoint >= 0x7f && codePoint <= 0x9f)
    ) {
      return true;
    }
  }
  return false;
}

function goWhitespace(codePoint: number): boolean {
  return (codePoint >= 0x09 && codePoint <= 0x0d)
    || codePoint === 0x20
    || codePoint === 0x85
    || codePoint === 0xa0
    || codePoint === 0x1680
    || (codePoint >= 0x2000 && codePoint <= 0x200a)
    || codePoint === 0x2028
    || codePoint === 0x2029
    || codePoint === 0x202f
    || codePoint === 0x205f
    || codePoint === 0x3000;
}

function hasNonGoWhitespace(value: string): boolean {
  for (const character of value) {
    if (!goWhitespace(character.codePointAt(0)!)) return true;
  }
  return false;
}

function requireArray(value: unknown, maximum: number): unknown[] {
  if (!Array.isArray(value) || value.length > maximum) return invalidInspection();
  return value;
}

function compareUtf8(left: string, right: string): number {
  // UTF-8 preserves scalar-value order, and boundedUtf8Value already rejects
  // unpaired surrogates, so this matches Go's byte order without allocations.
  let leftIndex = 0;
  let rightIndex = 0;
  while (leftIndex < left.length && rightIndex < right.length) {
    const leftCodePoint = left.codePointAt(leftIndex)!;
    const rightCodePoint = right.codePointAt(rightIndex)!;
    if (leftCodePoint !== rightCodePoint) return leftCodePoint - rightCodePoint;
    leftIndex += leftCodePoint > 0xffff ? 2 : 1;
    rightIndex += rightCodePoint > 0xffff ? 2 : 1;
  }
  return (leftIndex < left.length ? 1 : 0) - (rightIndex < right.length ? 1 : 0);
}

function utf8CharacterBytes(character: string): number {
  const codePoint = character.codePointAt(0)!;
  return codePoint <= 0x7f ? 1 : codePoint <= 0x7ff ? 2 : codePoint <= 0xffff ? 3 : 4;
}

function validInspectionField(field: string): boolean {
  if (CANONICAL_SPL_FIELDS.has(field)) return true;
  if (field.slice(0, 5).toLowerCase() === "__os_" || field.includes("*")) return false;
  let segments = 1;
  let segmentBytes = 0;
  let escaped = false;
  for (const character of field) {
    if (escaped) {
      if (character !== "." && character !== "\\") return false;
      segmentBytes += 1;
      escaped = false;
    } else if (character === "\\") {
      escaped = true;
    } else if (character === ".") {
      if (segmentBytes === 0 || segments === MAXIMUM_FIELD_PATH_SEGMENTS) return false;
      segments += 1;
      segmentBytes = 0;
    } else {
      segmentBytes += utf8CharacterBytes(character);
    }
    if (segmentBytes > MAXIMUM_FIELD_PATH_SEGMENT_BYTES) return false;
  }
  return !escaped && segmentBytes > 0;
}

function safeInspectionField(value: unknown, budget: LogicalBudget): string {
  const field = safeLogicalString(value, MAXIMUM_FIELD_NAME_BYTES, budget);
  return validInspectionField(field) ? field : invalidInspection();
}

function adaptFields(
  value: unknown,
  maximum: number,
  budget: LogicalBudget,
  strictlySorted: boolean,
): string[] {
  const raw = requireArray(value, maximum);
  if (raw.length > MAXIMUM_FIELD_OCCURRENCES - budget.fieldOccurrences) {
    return invalidInspection();
  }
  budget.fieldOccurrences += raw.length;
  const fields = raw.map((field) => safeInspectionField(field, budget));
  if (strictlySorted) {
    for (let index = 1; index < fields.length; index += 1) {
      if (compareUtf8(fields[index - 1]!, fields[index]!) >= 0) return invalidInspection();
    }
  }
  return fields;
}

function adaptSourcePosition(value: unknown): ServerInspectionSourcePosition {
  if (!isRecord(value)) return invalidInspection();
  const byteOffset = uint64(value.byteOffset);
  const line = uint32(value.line);
  const column = uint32(value.column);
  if (byteOffset > BigInt(MAXIMUM_SOURCE_BYTES) || line === 0 || column === 0) {
    return invalidInspection();
  }
  const maximumCoordinate = byteOffset + 1n;
  if (
    BigInt(line) > maximumCoordinate
    || BigInt(column) > maximumCoordinate
    || BigInt(line) + BigInt(column) > byteOffset + 2n
  ) {
    return invalidInspection();
  }
  return { byteOffset, line, column };
}

function adaptSourceRange(value: unknown): ServerInspectionSourceRange {
  if (!isRecord(value)) return invalidInspection();
  const start = adaptSourcePosition(value.start);
  const end = adaptSourcePosition(value.end);
  if (
    end.byteOffset < start.byteOffset
    || end.line < start.line
    || (end.line === start.line && end.column < start.column)
  ) {
    return invalidInspection();
  }
  const byteDistance = end.byteOffset - start.byteOffset;
  if (byteDistance === 0n) {
    if (start.line !== end.line || start.column !== end.column) return invalidInspection();
    return { start, end };
  }
  const lineDistance = BigInt(end.line - start.line);
  if (lineDistance > byteDistance) return invalidInspection();
  if (lineDistance === 0n) {
    const columnDistance = BigInt(end.column - start.column);
    if (columnDistance === 0n || columnDistance > byteDistance) return invalidInspection();
  } else if (BigInt(end.column) > byteDistance - lineDistance + 1n) {
    return invalidInspection();
  }
  return { start, end };
}

function redactedProvenance(
  value: unknown,
  contract: KnowledgeContract,
): ServerInspectionRedactedProvenance {
  if (!isRecord(value)) return invalidInspection();
  const source = value.source;
  if (!isRecord(source) || source.$case !== "redactedObject" || !isRecord(source.value)) {
    return invalidInspection();
  }
  const ordinal = uint32(source.value.redactedObjectOrdinal);
  if (
    ordinal >= MAXIMUM_KNOWLEDGE_OBJECTS
    || source.value.objectType !== contract.objectType
    || source.value.stage !== contract.stage
  ) {
    return invalidInspection();
  }
  return { ordinal, kind: contract.kind };
}

function compareOutputProvenance(
  left: ServerInspectionOutputProvenance,
  right: ServerInspectionOutputProvenance,
): number {
  const fieldComparison = compareUtf8(left.outputField, right.outputField);
  return fieldComparison === 0
    ? left.provenance.ordinal - right.provenance.ordinal
    : fieldComparison;
}

function adaptKnowledgeStage(
  stage: SearchInspectionLogicalStage,
  contract: KnowledgeContract,
  outputFields: string[],
  budget: LogicalBudget,
): {
  operatorProvenance: ServerInspectionRedactedProvenance[];
  outputProvenance: ServerInspectionOutputProvenance[];
} {
  const rawOperators = requireArray(stage.operatorProvenance, MAXIMUM_OPERATOR_PROVENANCE);
  const rawOutputs = requireArray(stage.outputProvenance, MAXIMUM_OUTPUT_PROVENANCE);
  if (
    rawOperators.length > MAXIMUM_OPERATOR_PROVENANCE - budget.operatorProvenance
    || rawOutputs.length > MAXIMUM_OUTPUT_PROVENANCE - budget.outputProvenance
  ) {
    return invalidInspection();
  }
  budget.operatorProvenance += rawOperators.length;
  budget.outputProvenance += rawOutputs.length;
  if (contract.shape === "one-to-many" && rawOperators.length !== 1) return invalidInspection();
  if (
    contract.shape === "one-to-one"
    && (rawOperators.length !== 1 || rawOutputs.length !== 1 || outputFields.length !== 1)
  ) {
    return invalidInspection();
  }
  if (contract.shape === "fused-one-to-one" && rawOperators.length !== rawOutputs.length) {
    return invalidInspection();
  }
  if (rawOperators.length === 0 || rawOutputs.length === 0) return invalidInspection();

  const operatorProvenance = rawOperators.map((item) => redactedProvenance(item, contract));
  for (let index = 1; index < operatorProvenance.length; index += 1) {
    if (operatorProvenance[index - 1]!.ordinal >= operatorProvenance[index]!.ordinal) {
      return invalidInspection();
    }
  }
  const byOrdinal = new Map(operatorProvenance.map((item) => [item.ordinal, item]));
  const usedOrdinals = new Set<number>();
  const outputProvenance = rawOutputs.map((item) => {
    if (!isRecord(item)) return invalidInspection();
    const outputField = safeInspectionField(item.outputField, budget);
    const provenance = redactedProvenance(item.provenance, contract);
    const operatorObject = byOrdinal.get(provenance.ordinal);
    if (operatorObject === undefined) return invalidInspection();
    usedOrdinals.add(provenance.ordinal);
    return { outputField, provenance: { ...operatorObject } };
  });
  for (let index = 1; index < outputProvenance.length; index += 1) {
    if (compareOutputProvenance(outputProvenance[index - 1]!, outputProvenance[index]!) >= 0) {
      return invalidInspection();
    }
  }
  if (usedOrdinals.size !== operatorProvenance.length) return invalidInspection();
  const distinctOutputs = outputProvenance.filter((item, index) => (
    index === 0 || item.outputField !== outputProvenance[index - 1]!.outputField
  )).map((item) => item.outputField);
  if (
    distinctOutputs.length !== outputFields.length
    || distinctOutputs.some((field, index) => field !== outputFields[index])
  ) {
    return invalidInspection();
  }
  return { operatorProvenance, outputProvenance };
}

function adaptLogicalPlan(value: unknown, exposeKnowledge: boolean): AdaptedLogicalPlan {
  if (!isRecord(value)) return invalidInspection();
  const rawStages = requireArray(value.stages, MAXIMUM_PLAN_STAGES);
  if (rawStages.length === 0) return invalidInspection();
  const budget: LogicalBudget = {
    fieldOccurrences: 0,
    stringBytes: 0,
    operatorProvenance: 0,
    outputProvenance: 0,
  };
  const knowledgeObjects: ServerInspectionRedactedProvenance[] = [];
  let knowledgePrefixEnded = false;
  let previousKnowledgeRank = 0;
  let authoredStages = 0;
  let generatedStages = 0;
  const stages = rawStages.map((rawStage, index): ServerInspectionLogicalStageView => {
    if (!isRecord(rawStage)) return invalidInspection();
    const stage = rawStage as unknown as SearchInspectionLogicalStage;
    const stageIndex = uint32(stage.stageIndex);
    const operator = safeLogicalString(stage.operator, MAXIMUM_OPERATOR_BYTES, budget);
    const contract = KNOWLEDGE_OPERATOR_CONTRACTS.get(operator);
    if (stageIndex !== index || (index === 0) !== (operator === "Scan")) return invalidInspection();
    if (contract === undefined && !AUTHORED_OPERATORS.has(operator)) return invalidInspection();
    const inputFields = adaptFields(stage.inputFields, MAXIMUM_STAGE_FIELDS, budget, true);
    const outputFields = adaptFields(stage.outputFields, MAXIMUM_STAGE_FIELDS, budget, true);

    if (contract === undefined) {
      authoredStages += 1;
      if (
        authoredStages > MAXIMUM_AUTHORED_PLAN_STAGES
        || stage.sourceRange === undefined
      ) {
        return invalidInspection();
      }
      if (index > 0) knowledgePrefixEnded = true;
      const sourceRange = adaptSourceRange(stage.sourceRange);
      if (exposeKnowledge && (
        requireArray(stage.operatorProvenance, MAXIMUM_OPERATOR_PROVENANCE).length !== 0
        || requireArray(stage.outputProvenance, MAXIMUM_OUTPUT_PROVENANCE).length !== 0
      )) {
        return invalidInspection();
      }
      return {
        stageIndex,
        operator,
        inputFields,
        outputFields,
        sourceRange,
        operatorProvenance: [],
        outputProvenance: [],
      };
    }
    if (
      index === 0
      || knowledgePrefixEnded
      || contract.rank < previousKnowledgeRank
      || (contract.rank === previousKnowledgeRank && !contract.repeatable)
      || stage.sourceRange !== undefined
    ) {
      return invalidInspection();
    }
    generatedStages += 1;
    if (
      generatedStages > MAXIMUM_KNOWLEDGE_OBJECTS
      || outputFields.length === 0
      || (contract.shape === "one-to-one" && outputFields.length !== 1)
    ) {
      return invalidInspection();
    }
    previousKnowledgeRank = contract.rank;
    if (!exposeKnowledge) {
      return {
        stageIndex,
        operator,
        inputFields,
        outputFields,
        operatorProvenance: [],
        outputProvenance: [],
      };
    }
    const provenance = adaptKnowledgeStage(stage, contract, outputFields, budget);
    knowledgeObjects.push(...provenance.operatorProvenance.map((item) => ({
      ordinal: item.ordinal,
      kind: item.kind,
    })));
    return {
      stageIndex,
      operator,
      inputFields,
      outputFields,
      operatorProvenance: provenance.operatorProvenance,
      outputProvenance: provenance.outputProvenance,
    };
  });
  for (let index = 0; index < knowledgeObjects.length; index += 1) {
    if (knowledgeObjects[index]!.ordinal !== index) return invalidInspection();
  }
  const referencedFields = adaptFields(
    value.referencedFields,
    MAXIMUM_STAGE_FIELDS,
    budget,
    true,
  );
  if (!isRecord(value.output)) return invalidInspection();
  const outputKind = value.output.kind;
  const maxDynamicFields = uint32(value.output.maxDynamicFields);
  let kind: "open" | "static" | "dynamic";
  let maximumFields: number;
  if (outputKind === SearchInspectionOutputKind.SEARCH_INSPECTION_OUTPUT_KIND_OPEN) {
    kind = "open";
    maximumFields = 0;
  } else if (outputKind === SearchInspectionOutputKind.SEARCH_INSPECTION_OUTPUT_KIND_STATIC) {
    kind = "static";
    maximumFields = MAXIMUM_FINAL_OUTPUT_FIELDS;
  } else if (outputKind === SearchInspectionOutputKind.SEARCH_INSPECTION_OUTPUT_KIND_DYNAMIC) {
    kind = "dynamic";
    maximumFields = MAXIMUM_STAGE_FIELDS;
  } else {
    return invalidInspection();
  }
  const fields = adaptFields(value.output.fields, maximumFields, budget, false);
  if (
    (kind === "open" && (fields.length !== 0 || maxDynamicFields !== 0))
    || (kind === "static" && (fields.length === 0 || maxDynamicFields !== 0))
    || (kind === "dynamic" && (
      fields.length === 0 || maxDynamicFields === 0 || maxDynamicFields > MAXIMUM_DYNAMIC_FIELDS
    ))
    || new Set(fields).size !== fields.length
  ) {
    return invalidInspection();
  }
  return {
    view: {
      stages,
      referencedFields,
      output: { kind, fields, maxDynamicFields },
    },
    knowledgeObjects,
  };
}

function adaptPhysicalIndex(
  value: unknown,
  budget: { stringBytes: number },
): ServerInspectionPhysicalPlanView["reads"][number]["indexes"][number] {
  if (!isRecord(value)) return invalidInspection();
  const index = value as unknown as SearchInspectionPhysicalIndex;
  const keys = requireArray(index.keys, MAXIMUM_PHYSICAL_INDEX_KEYS)
    .map((key) => safePhysicalString(key, budget));
  const type = safePhysicalString(index.type, budget);
  const name = safePhysicalString(index.name, budget, true);
  if (
    !PHYSICAL_INDEX_TYPES.has(type)
    || (type === "Skip" ? !SKIP_INDEX_NAMES.has(name) : name !== "")
  ) {
    return invalidInspection();
  }
  const initialParts = uint64(index.initialParts);
  const selectedParts = uint64(index.selectedParts);
  const initialGranules = uint64(index.initialGranules);
  const selectedGranules = uint64(index.selectedGranules);
  if (selectedParts > initialParts || selectedGranules > initialGranules) {
    return invalidInspection();
  }
  return {
    type,
    name,
    keys,
    initialParts,
    selectedParts,
    initialGranules,
    selectedGranules,
  };
}

function adaptPhysicalPlan(value: unknown): ServerInspectionPhysicalPlanView {
  if (!isRecord(value)) return invalidInspection();
  const rawNodes = requireArray(value.nodeTypes, MAXIMUM_PHYSICAL_NODES);
  const rawReads = requireArray(value.reads, MAXIMUM_PHYSICAL_READS);
  const budget = { stringBytes: 0 };
  const nodeTypes = rawNodes.map((node) => {
    const type = safePhysicalString(node, budget);
    if (!PHYSICAL_NODE_TYPES.has(type)) return invalidInspection();
    return type;
  });
  let headers = 0;
  let indexes = 0;
  const reads = rawReads.map((rawRead) => {
    if (!isRecord(rawRead)) return invalidInspection();
    const rawColumns = requireArray(rawRead.columns, MAXIMUM_PHYSICAL_HEADERS);
    const rawIndexes = requireArray(rawRead.indexes, MAXIMUM_PHYSICAL_INDEXES);
    if (
      rawColumns.length > MAXIMUM_PHYSICAL_HEADERS - headers
      || rawIndexes.length > MAXIMUM_PHYSICAL_INDEXES - indexes
    ) {
      return invalidInspection();
    }
    headers += rawColumns.length;
    indexes += rawIndexes.length;
    return {
      columns: rawColumns.map((column) => safePhysicalString(column, budget)),
      indexes: rawIndexes.map((index) => adaptPhysicalIndex(index, budget)),
    };
  });
  return { nodeTypes, reads };
}

function canonicalCompiler(value: unknown): string {
  const compiler = boundedUtf8(value, MAXIMUM_COMPILER_BYTES);
  if (hasAsciiEdgeWhitespace(compiler) || CONTROL_CHARACTER.test(compiler)) {
    return invalidInspection();
  }
  return compiler;
}

function digestHex(value: unknown): string {
  if (!(value instanceof Uint8Array) || value.byteLength !== 32) return invalidInspection();
  return Array.from(value, (byte) => byte.toString(16).padStart(2, "0")).join("");
}

function validateStateToken(value: unknown): void {
  if (!(value instanceof Uint8Array) || value.byteLength !== 32) return invalidInspection();
}

function summaryContract(
  objectType: unknown,
  stage: unknown,
): KnowledgeContract {
  for (const contract of KNOWLEDGE_OPERATOR_CONTRACTS.values()) {
    if (contract.objectType === objectType && contract.stage === stage) return contract;
  }
  return invalidInspection();
}

function adaptKnowledgeSummary(
  value: KnowledgeSnapshotSummary | undefined,
  planObjects: ServerInspectionRedactedProvenance[],
): ServerInspectionKnowledgeView {
  if (value === undefined) {
    if (planObjects.length !== 0) return invalidInspection();
    return { state: "absent" };
  }
  if (!isRecord(value) || !isRecord(value.ref)) return invalidInspection();
  const rawObjects = requireArray(value.objects, MAXIMUM_SUMMARY_OBJECTS);
  const objectCount = uint32(value.ref.objectCount);
  if (objectCount > MAXIMUM_KNOWLEDGE_OBJECTS || planObjects.length !== objectCount) {
    return invalidInspection();
  }
  const wantedPrefix = Math.min(objectCount, MAXIMUM_SUMMARY_OBJECTS);
  const objectsTruncated = booleanValue(value.objectsTruncated);
  if (rawObjects.length !== wantedPrefix || objectsTruncated !== (objectCount > wantedPrefix)) {
    return invalidInspection();
  }
  const tenantCatalogRevision = uint64(value.ref.tenantCatalogRevision);
  if (tenantCatalogRevision > MAXIMUM_CATALOG_REVISION) return invalidInspection();
  validateStateToken(value.ref.tenantCatalogStateToken);
  const digestSha256 = digestHex(value.ref.snapshotSha256);
  const compilerCompatibilityVersion = canonicalCompiler(
    value.ref.compilerCompatibilityVersion,
  );
  let previousRank = 0;
  const objects = rawObjects.map((raw, index): ServerInspectionRedactedProvenance => {
    if (!isRecord(raw)) return invalidInspection();
    const ordinal = uint32(raw.resolutionOrdinal);
    if (ordinal !== index) return invalidInspection();
    const contract = summaryContract(raw.objectType, raw.stage);
    const disclosure = raw.disclosure;
    if (
      !isRecord(disclosure)
      || disclosure.$case !== "redacted"
      || disclosure.value !== true
    ) {
      return invalidInspection();
    }
    const planObject = planObjects[index];
    if (planObject === undefined || planObject.ordinal !== ordinal || planObject.kind !== contract.kind) {
      return invalidInspection();
    }
    if (contract.rank < previousRank) return invalidInspection();
    previousRank = contract.rank;
    return { ordinal, kind: contract.kind };
  });
  return {
    state: "enabled",
    digestSha256,
    tenantCatalogRevision,
    objectCount,
    compilerCompatibilityVersion,
    objects,
    objectsTruncated,
  };
}

function validGeneratedSql(value: unknown): string {
  const sql = boundedUtf8(value, MAXIMUM_GENERATED_SQL_BYTES);
  if (sqlForbiddenControl(sql) || !hasNonGoWhitespace(sql)) return invalidInspection();
  return sql;
}

function validExplainText(value: unknown): string {
  const text = boundedUtf8Value(value, MAXIMUM_EXPLAIN_BYTES);
  let lineBytes = 0;
  let lineCount = 1;
  for (const byte of text.bytes) {
    if (byte === 0x0a) {
      if (lineBytes === 0 || lineCount >= MAXIMUM_EXPLAIN_LINES) {
        return invalidInspection();
      }
      lineBytes = 0;
      lineCount += 1;
    } else {
      lineBytes += 1;
      if (lineBytes > MAXIMUM_EXPLAIN_LINE_BYTES) return invalidInspection();
    }
  }
  return lineBytes === 0 ? invalidInspection() : text.value;
}

/**
 * Validates and detaches the complete administrator inspection response.
 * Knowledge extension fields are not read when their capability is absent.
 */
export function adaptSearchJobInspection(
  response: InspectSearchJobResponse,
  expectedSearchJobId: string,
  exposeKnowledge: boolean,
): ServerSearchJobInspection {
  if (
    !isRecord(response)
    || typeof expectedSearchJobId !== "string"
    || expectedSearchJobId.length === 0
    || response.searchJobId !== expectedSearchJobId
  ) {
    return invalidInspection();
  }
  const logical = adaptLogicalPlan(response.logicalPlan, exposeKnowledge);
  const result: ServerSearchJobInspection = {
    logicalPlan: logical.view,
    physicalPlan: adaptPhysicalPlan(response.physicalPlan),
    generatedSql: validGeneratedSql(response.generatedSql),
    explainText: validExplainText(response.explainText),
    diagnosticQueryId: boundedUtf8(
      response.diagnosticQueryId,
      MAXIMUM_DIAGNOSTIC_QUERY_ID_BYTES,
    ),
  };
  if (exposeKnowledge) {
    result.knowledge = adaptKnowledgeSummary(response.knowledgeSnapshot, logical.knowledgeObjects);
  }
  return result;
}
