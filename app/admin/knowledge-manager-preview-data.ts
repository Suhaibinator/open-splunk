import type { KnowledgeObject } from "@/gen/ts/open_splunk/v1/knowledge";
import {
  KnowledgeValidationIntent,
  PreviewKnowledgeObjectRequest,
  PreviewKnowledgeObjectResponse,
  type KnowledgeValidationResult,
  type PreviewKnowledgeObjectRequest as PreviewKnowledgeObjectRequestMessage,
  type PreviewKnowledgeObjectResponse as PreviewKnowledgeObjectResponseMessage,
} from "@/gen/ts/open_splunk/v1/knowledge_api";
import {
  ColumnSemanticType,
  ResultSetKind,
  type ResultColumn,
  type ResultRow,
  type ResultSchema,
} from "@/gen/ts/open_splunk/v1/result";
import {
  NullValue,
  ValueType,
  type TypedValue,
} from "@/gen/ts/open_splunk/v1/value";
import {
  ProtobufTransport,
  type ProtobufRequestOptions,
  type ProtobufTransportOptions,
} from "@/lib/api/protobuf-transport";
import {
  MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES,
  knowledgeRoutes,
} from "@/lib/api/routes";

import {
  adaptKnowledgeValidationResponse,
  knowledgeValidateRequest,
} from "./knowledge-manager-data";

export const KNOWLEDGE_PREVIEW_DEFAULT_MAXIMUM_ROWS = 100;
export const KNOWLEDGE_PREVIEW_MAXIMUM_ROWS = 1_000;
export const KNOWLEDGE_PREVIEW_MAXIMUM_COLUMNS = 1_024;
export const KNOWLEDGE_PREVIEW_MAXIMUM_REQUEST_BYTES = (4 << 20) + (64 << 10);
export const KNOWLEDGE_PREVIEW_MAXIMUM_RESPONSE_BYTES =
  MAXIMUM_KNOWLEDGE_MANAGEMENT_RESPONSE_BYTES;

const MAXIMUM_JOB_ID_BYTES = 256;
const MAXIMUM_SIGNED_REVISION = 9_223_372_036_854_775_807n;
const MAXIMUM_DURATION_SECONDS = 315_576_000_000n;

export interface KnowledgePreviewRequestOptions extends ProtobufRequestOptions {
  readonly currentKnowledgeObject?: KnowledgeObject;
}

export interface KnowledgePreviewProjection {
  readonly schema: ResultSchema;
  readonly rows: readonly ResultRow[];
}

export interface KnowledgePreviewReceipt {
  readonly validation: KnowledgeValidationResult;
  readonly before: KnowledgePreviewProjection | null;
  readonly after: KnowledgePreviewProjection | null;
  readonly truncated: boolean;
  readonly tenantCatalogRevision: bigint;
}

export interface KnowledgePreviewClient {
  preview(
    request: PreviewKnowledgeObjectRequestMessage,
    options?: KnowledgePreviewRequestOptions,
  ): Promise<KnowledgePreviewReceipt>;
}

export function createKnowledgePreviewClient(
  options: ProtobufTransportOptions = {},
): KnowledgePreviewClient {
  const transport = new ProtobufTransport(options);
  return {
    preview: async (submitted, requestOptions) => {
      const request = knowledgePreviewRequest(submitted);
      const { currentKnowledgeObject, ...transportOptions } = requestOptions ?? {};
      const response = await transport.post(
        knowledgeRoutes.preview,
        request,
        transportOptions,
      );
      return adaptKnowledgePreviewResponse(
        response,
        request,
        currentKnowledgeObject,
      );
    },
  };
}

export function knowledgePreviewRequest(
  submitted: PreviewKnowledgeObjectRequestMessage,
): PreviewKnowledgeObjectRequestMessage {
  if (
    !validIdentity(submitted.retainedSearchJobId, MAXIMUM_JOB_ID_BYTES)
    || submitted.definition === undefined
    || submitted.maximumRows === 0
    || submitted.maximumRows !== undefined
      && (!Number.isInteger(submitted.maximumRows)
        || submitted.maximumRows < 1
        || submitted.maximumRows > KNOWLEDGE_PREVIEW_MAXIMUM_ROWS)
  ) {
    throw new TypeError("Knowledge Preview request is outside the browser contract.");
  }
  const candidate = knowledgeValidateRequest({
    definition: submitted.definition,
    knowledgeObjectId: submitted.knowledgeObjectId,
    expectedVersion: submitted.expectedVersion,
    updateMask: submitted.updateMask,
    intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
  });
  const request = PreviewKnowledgeObjectRequest.fromPartial({
    retainedSearchJobId: submitted.retainedSearchJobId,
    definition: candidate.definition,
    knowledgeObjectId: candidate.knowledgeObjectId,
    expectedVersion: candidate.expectedVersion,
    updateMask: candidate.updateMask,
    maximumRows: submitted.maximumRows,
  });
  const wire = PreviewKnowledgeObjectRequest.encode(request).finish();
  if (wire.byteLength === 0 || wire.byteLength > KNOWLEDGE_PREVIEW_MAXIMUM_REQUEST_BYTES) {
    throw new TypeError("Knowledge Preview request exceeds its browser bound.");
  }
  return PreviewKnowledgeObjectRequest.decode(wire);
}

export async function adaptKnowledgePreviewResponse(
  response: PreviewKnowledgeObjectResponseMessage,
  submitted: PreviewKnowledgeObjectRequestMessage,
  currentKnowledgeObject?: KnowledgeObject,
): Promise<KnowledgePreviewReceipt> {
  const request = knowledgePreviewRequest(submitted);
  const wire = PreviewKnowledgeObjectResponse.encode(response).finish();
  if (wire.byteLength === 0 || wire.byteLength > KNOWLEDGE_PREVIEW_MAXIMUM_RESPONSE_BYTES) {
    throw new TypeError("Knowledge Preview response exceeds its browser bound.");
  }
  const detached = PreviewKnowledgeObjectResponse.decode(wire);
  if (
    detached.validation === undefined
    || detached.tenantCatalogRevision < 0n
    || detached.tenantCatalogRevision > MAXIMUM_SIGNED_REVISION
  ) {
    throw new TypeError("Knowledge Preview response is outside the browser contract.");
  }
  const validation = await adaptKnowledgeValidationResponse(
    {
      result: detached.validation,
      tenantCatalogRevision: detached.tenantCatalogRevision,
    },
    {
      definition: request.definition,
      knowledgeObjectId: request.knowledgeObjectId,
      expectedVersion: request.expectedVersion,
      updateMask: request.updateMask,
      intent: KnowledgeValidationIntent.KNOWLEDGE_VALIDATION_INTENT_ACTIVE_PUBLICATION,
    },
    currentKnowledgeObject,
  );
  if (!validation.result.valid) {
    if (
      detached.beforeSchema !== undefined
      || detached.afterSchema !== undefined
      || detached.beforeRows.length !== 0
      || detached.afterRows.length !== 0
      || detached.truncated
    ) {
      throw new TypeError("Invalid Knowledge Preview returned executable output.");
    }
    return {
      validation: validation.result,
      before: null,
      after: null,
      truncated: false,
      tenantCatalogRevision: validation.tenantCatalogRevision,
    };
  }
  const maximumRows = request.maximumRows ?? KNOWLEDGE_PREVIEW_DEFAULT_MAXIMUM_ROWS;
  if (
    detached.beforeSchema === undefined
    || detached.afterSchema === undefined
    || !validPreviewSchema(detached.beforeSchema, request.retainedSearchJobId)
    || !validPreviewSchema(detached.afterSchema, request.retainedSearchJobId)
    || detached.beforeSchema.resultKind !== detached.afterSchema.resultKind
    || !validPreviewRows(
      detached.beforeRows,
      detached.beforeSchema,
      request.retainedSearchJobId,
      maximumRows,
    )
    || !validPreviewRows(
      detached.afterRows,
      detached.afterSchema,
      request.retainedSearchJobId,
      maximumRows,
    )
  ) {
    throw new TypeError("Knowledge Preview rows or schema are malformed.");
  }
  return {
    validation: validation.result,
    before: {
      schema: detached.beforeSchema,
      rows: detached.beforeRows,
    },
    after: {
      schema: detached.afterSchema,
      rows: detached.afterRows,
    },
    truncated: detached.truncated,
    tenantCatalogRevision: validation.tenantCatalogRevision,
  };
}

function validPreviewSchema(schema: ResultSchema, jobID: string): boolean {
  if (
    schema.schemaId !== jobID
    || schema.revision !== 1n
    || schema.resultKind < ResultSetKind.RESULT_SET_KIND_EVENTS
    || schema.resultKind > ResultSetKind.RESULT_SET_KIND_TIME_SERIES
    || schema.columns.length === 0
    || schema.columns.length > KNOWLEDGE_PREVIEW_MAXIMUM_COLUMNS
  ) return false;
  const names = new Set<string>();
  for (const column of schema.columns) {
    if (
      !validColumn(column)
      || names.has(column.fieldName)
    ) return false;
    names.add(column.fieldName);
  }
  return true;
}

function validColumn(column: ResultColumn): boolean {
  return validText(column.fieldName)
    && column.displayName === column.fieldName
    && column.valueType >= ValueType.VALUE_TYPE_NULL
    && column.valueType <= ValueType.VALUE_TYPE_MIXED
    && column.semanticType >= ColumnSemanticType.COLUMN_SEMANTIC_TYPE_UNSPECIFIED
    && column.semanticType <= ColumnSemanticType.COLUMN_SEMANTIC_TYPE_DIMENSION
    && !column.hiddenByDefault;
}

function validPreviewRows(
  rows: readonly ResultRow[],
  schema: ResultSchema,
  jobID: string,
  maximumRows: number,
): boolean {
  if (rows.length > maximumRows) return false;
  for (let index = 0; index < rows.length; index += 1) {
    const row = rows[index];
    const ordinal = BigInt(index);
    if (
      row === undefined
      || row.ordinal !== ordinal
      || row.rowId !== `${jobID}:${ordinal.toString()}`
      || row.cells.length !== schema.columns.length
    ) return false;
    for (let cellIndex = 0; cellIndex < row.cells.length; cellIndex += 1) {
      const cell = row.cells[cellIndex];
      const column = schema.columns[cellIndex];
      if (
        cell === undefined
        || column === undefined
        || !validPreviewCell(cell, column)
      ) return false;
    }
  }
  return true;
}

function validPreviewCell(value: TypedValue, column: ResultColumn): boolean {
  const kind = previewValueType(value, 0);
  if (kind === null) return false;
  if (column.valueType === ValueType.VALUE_TYPE_MIXED) return true;
  if (kind === ValueType.VALUE_TYPE_NULL) {
    return column.nullable || column.valueType === ValueType.VALUE_TYPE_NULL;
  }
  return kind === column.valueType;
}

function previewValueType(value: TypedValue, depth: number): ValueType | null {
  if (depth > 32 || value.kind === undefined) return null;
  switch (value.kind.$case) {
    case "nullValue":
      return value.kind.value === NullValue.NULL_VALUE_NULL
        ? ValueType.VALUE_TYPE_NULL
        : null;
    case "stringValue":
      return validUTF16(value.kind.value) ? ValueType.VALUE_TYPE_STRING : null;
    case "sint64Value":
      return ValueType.VALUE_TYPE_SINT64;
    case "uint64Value":
      return value.kind.value >= 0n ? ValueType.VALUE_TYPE_UINT64 : null;
    case "doubleValue":
      return ValueType.VALUE_TYPE_DOUBLE;
    case "boolValue":
      return ValueType.VALUE_TYPE_BOOL;
    case "bytesValue":
      return ValueType.VALUE_TYPE_BYTES;
    case "timestampValue":
      return Number.isFinite(value.kind.value.getTime())
        ? ValueType.VALUE_TYPE_TIMESTAMP
        : null;
    case "durationValue": {
      const { seconds, nanos } = value.kind.value;
      const signsAgree = seconds === 0n || nanos === 0
        || seconds > 0n && nanos > 0
        || seconds < 0n && nanos < 0;
      return seconds >= -MAXIMUM_DURATION_SECONDS
        && seconds <= MAXIMUM_DURATION_SECONDS
        && Number.isInteger(nanos)
        && nanos >= -999_999_999
        && nanos <= 999_999_999
        && signsAgree
        ? ValueType.VALUE_TYPE_DURATION
        : null;
    }
    case "decimalValue":
      return validDecimal(value.kind.value.value)
        ? ValueType.VALUE_TYPE_DECIMAL
        : null;
    case "listValue":
      for (const child of value.kind.value.values) {
        if (previewValueType(child, depth + 1) === null) return null;
      }
      return ValueType.VALUE_TYPE_LIST;
    case "objectValue": {
      const names = new Set<string>();
      for (const field of value.kind.value.fields) {
        if (
          !validText(field.name)
          || names.has(field.name)
          || field.value === undefined
          || previewValueType(field.value, depth + 1) === null
        ) return null;
        names.add(field.name);
      }
      return ValueType.VALUE_TYPE_OBJECT;
    }
    case "missingValue":
      return null;
  }
}

export function knowledgePreviewValueText(value: TypedValue): string {
  if (previewValueType(value, 0) === null || value.kind === undefined) {
    throw new TypeError("Knowledge Preview value is malformed.");
  }
  switch (value.kind.$case) {
    case "nullValue": return "null";
    case "stringValue": return value.kind.value;
    case "sint64Value":
    case "uint64Value": return value.kind.value.toString();
    case "doubleValue": return String(value.kind.value);
    case "boolValue": return value.kind.value ? "true" : "false";
    case "bytesValue": return Array.from(
      value.kind.value,
      (octet) => octet.toString(16).padStart(2, "0"),
    ).join("");
    case "timestampValue": return value.kind.value.toISOString();
    case "durationValue": return `${value.kind.value.seconds.toString()}s ${value.kind.value.nanos}ns`;
    case "decimalValue": return value.kind.value.value;
    case "listValue": return `[${value.kind.value.values
      .map((child) => JSON.stringify(knowledgePreviewValueText(child)))
      .join(",")}]`;
    case "objectValue": return `{${value.kind.value.fields
      .map((field) => `${JSON.stringify(field.name)}:${JSON.stringify(
        knowledgePreviewValueText(field.value!),
      )}`)
      .join(",")}}`;
    case "missingValue": throw new TypeError("Knowledge Preview value is missing.");
  }
}

function validDecimal(value: string): boolean {
  return /^[+-]?[0-9]+(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?$/.test(value);
}

function validIdentity(value: string, maximumBytes: number): boolean {
  return validText(value)
    && utf8ByteLength(value, maximumBytes) <= maximumBytes
    && value.replace(/^[\t-\r ]+|[\t-\r ]+$/g, "") === value;
}

function validText(value: string): boolean {
  if (value.length === 0 || !validUTF16(value)) return false;
  for (const character of value) {
    const codePoint = character.codePointAt(0) ?? 0;
    if (codePoint <= 0x1f || codePoint >= 0x7f && codePoint <= 0x9f) return false;
  }
  return true;
}

function validUTF16(value: string): boolean {
  return utf8ByteLength(value) !== Number.MAX_SAFE_INTEGER;
}

function utf8ByteLength(value: string, stopAfter = Number.MAX_SAFE_INTEGER): number {
  let bytes = 0;
  for (let index = 0; index < value.length; index += 1) {
    const code = value.charCodeAt(index);
    if (code <= 0x7f) bytes += 1;
    else if (code <= 0x7ff) bytes += 2;
    else if (code >= 0xd800 && code <= 0xdbff) {
      const low = value.charCodeAt(index + 1);
      if (low < 0xdc00 || low > 0xdfff) return Number.MAX_SAFE_INTEGER;
      bytes += 4;
      index += 1;
    } else if (code >= 0xdc00 && code <= 0xdfff) {
      return Number.MAX_SAFE_INTEGER;
    } else bytes += 3;
    if (bytes > stopAfter) return bytes;
  }
  return bytes;
}
