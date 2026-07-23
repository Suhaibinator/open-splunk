import {
  ResultRow as ResultRowCodec,
  ResultSetKind,
  type ResultRow,
  type ResultSchema,
} from "../../gen/ts/open_splunk/v1/result";
import {
  PreviewUpdateMode,
  type ResultPreview,
} from "../../gen/ts/open_splunk/v1/search_ws";
import {
  MissingValue,
  NullValue,
  ValueType,
  type TypedValue,
} from "../../gen/ts/open_splunk/v1/value";

export interface LivePreviewSnapshot {
  schemaId: string;
  revision: bigint;
  rows: ResultRow[];
  truncated: boolean;
}

export type LivePreviewApplyResult =
  | {
      status: "applied";
      snapshot: LivePreviewSnapshot;
    }
  | {
      status: "ignored";
      snapshot: LivePreviewSnapshot;
      reason: "duplicate" | "stale";
    }
  | {
      status: "invalid";
      message: string;
    };

const VALID_RESULT_KINDS = new Set<ResultSetKind>([
  ResultSetKind.RESULT_SET_KIND_EVENTS,
  ResultSetKind.RESULT_SET_KIND_STATISTICS,
  ResultSetKind.RESULT_SET_KIND_TIME_SERIES,
]);

const VALID_VALUE_TYPES = new Set<ValueType>([
  ValueType.VALUE_TYPE_NULL,
  ValueType.VALUE_TYPE_STRING,
  ValueType.VALUE_TYPE_SINT64,
  ValueType.VALUE_TYPE_UINT64,
  ValueType.VALUE_TYPE_DOUBLE,
  ValueType.VALUE_TYPE_BOOL,
  ValueType.VALUE_TYPE_BYTES,
  ValueType.VALUE_TYPE_TIMESTAMP,
  ValueType.VALUE_TYPE_DURATION,
  ValueType.VALUE_TYPE_LIST,
  ValueType.VALUE_TYPE_OBJECT,
  ValueType.VALUE_TYPE_DECIMAL,
  ValueType.VALUE_TYPE_MIXED,
  ValueType.VALUE_TYPE_MISSING,
]);

const MAXIMUM_NESTED_VALUE_NODES = 20_000;
const MAXIMUM_UINT64 = (1n << 64n) - 1n;
const MAXIMUM_PREVIEW_ROW_LIMIT = 0xffff_ffff;
const MINIMUM_PROTOBUF_TIMESTAMP_MS = -62_135_596_800_000;
const MAXIMUM_PROTOBUF_TIMESTAMP_MS = 253_402_300_799_999;
const MAXIMUM_PROTOBUF_DURATION_SECONDS = 315_576_000_000n;

function canonicalDecimal(value: string): boolean {
  if (value === "0") return true;
  const unsigned = value.startsWith("-") ? value.slice(1) : value;
  if (unsigned.length === 0 || unsigned === "0") return false;
  const exponentIndex = unsigned.indexOf("e");

  if (exponentIndex >= 0) {
    const coefficient = unsigned.slice(0, exponentIndex);
    const exponent = unsigned.slice(exponentIndex + 1);
    if (!/^[1-9](?:\.\d*[1-9])?$/.test(coefficient)) return false;
    if (!/^-?[1-9]\d*$/.test(exponent)) return false;
    if (exponent.length > 3) return true;
    const exponentValue = Number(exponent);
    return exponentValue < -6 || exponentValue >= 21;
  }

  if (!/^(?:0|[1-9]\d*)(?:\.\d*[1-9])?$/.test(unsigned)) return false;
  const [integer, fraction = ""] = unsigned.split(".");
  const digits = `${integer}${fraction}`;
  const firstNonzero = digits.search(/[1-9]/);
  if (firstNonzero < 0) return false;
  const scientificExponent = integer.length - firstNonzero - 1;
  return scientificExponent >= -6 && scientificExponent < 21;
}

function typedValueType(value: TypedValue | undefined): ValueType | null {
  switch (value?.kind?.$case) {
    case "nullValue": return ValueType.VALUE_TYPE_NULL;
    case "stringValue": return ValueType.VALUE_TYPE_STRING;
    case "sint64Value": return ValueType.VALUE_TYPE_SINT64;
    case "uint64Value": return ValueType.VALUE_TYPE_UINT64;
    case "doubleValue": return ValueType.VALUE_TYPE_DOUBLE;
    case "boolValue": return ValueType.VALUE_TYPE_BOOL;
    case "bytesValue": return ValueType.VALUE_TYPE_BYTES;
    case "timestampValue": return ValueType.VALUE_TYPE_TIMESTAMP;
    case "durationValue": return ValueType.VALUE_TYPE_DURATION;
    case "listValue": return ValueType.VALUE_TYPE_LIST;
    case "objectValue": return ValueType.VALUE_TYPE_OBJECT;
    case "decimalValue": return ValueType.VALUE_TYPE_DECIMAL;
    case "missingValue": return ValueType.VALUE_TYPE_MISSING;
    default: return null;
  }
}

function validateTypedValue(root: TypedValue | undefined, path: string): string | null {
  const pending: Array<{ value: TypedValue | undefined; path: string }> = [{ value: root, path }];
  let visited = 0;

  while (pending.length > 0) {
    const current = pending.pop();
    if (current === undefined) break;
    visited += 1;
    if (visited > MAXIMUM_NESTED_VALUE_NODES) {
      return `${path} exceeds the supported nested value size.`;
    }

    const kind = current.value?.kind;
    if (kind === undefined) return `${current.path} does not contain a typed value.`;

    switch (kind.$case) {
      case "nullValue":
        if (kind.value !== NullValue.NULL_VALUE_NULL) {
          return `${current.path} contains an invalid null marker.`;
        }
        break;
      case "missingValue":
        if (kind.value !== MissingValue.MISSING_VALUE_MISSING) {
          return `${current.path} contains an invalid missing-value marker.`;
        }
        break;
      case "stringValue":
        break;
      case "sint64Value":
        if (BigInt.asIntN(64, kind.value) !== kind.value) {
          return `${current.path} contains an out-of-range signed integer.`;
        }
        break;
      case "uint64Value":
        if (BigInt.asUintN(64, kind.value) !== kind.value) {
          return `${current.path} contains an out-of-range unsigned integer.`;
        }
        break;
      case "doubleValue":
        if (!Number.isFinite(kind.value)) {
          return `${current.path} contains a non-finite number.`;
        }
        break;
      case "boolValue":
      case "bytesValue":
        break;
      case "timestampValue":
        if (
          Number.isNaN(kind.value.valueOf())
          || kind.value.valueOf() < MINIMUM_PROTOBUF_TIMESTAMP_MS
          || kind.value.valueOf() > MAXIMUM_PROTOBUF_TIMESTAMP_MS
        ) {
          return `${current.path} contains an invalid timestamp.`;
        }
        break;
      case "durationValue":
        if (
          kind.value.seconds < -MAXIMUM_PROTOBUF_DURATION_SECONDS
          || kind.value.seconds > MAXIMUM_PROTOBUF_DURATION_SECONDS
          || !Number.isInteger(kind.value.nanos)
          || kind.value.nanos < -999_999_999
          || kind.value.nanos > 999_999_999
          || (kind.value.seconds > 0n && kind.value.nanos < 0)
          || (kind.value.seconds < 0n && kind.value.nanos > 0)
        ) {
          return `${current.path} contains an invalid duration.`;
        }
        break;
      case "decimalValue":
        if (!canonicalDecimal(kind.value.value)) {
          return `${current.path} contains an invalid decimal.`;
        }
        break;
      case "listValue":
        for (let index = kind.value.values.length - 1; index >= 0; index -= 1) {
          pending.push({
            value: kind.value.values[index],
            path: `${current.path}[${index}]`,
          });
        }
        break;
      case "objectValue": {
        const names = new Set<string>();
        for (let index = kind.value.fields.length - 1; index >= 0; index -= 1) {
          const field = kind.value.fields[index];
          if (field === undefined || field.name.length === 0) {
            return `${current.path} contains an unnamed object field.`;
          }
          if (names.has(field.name)) {
            return `${current.path} contains duplicate object field “${field.name}”.`;
          }
          names.add(field.name);
          pending.push({
            value: field.value,
            path: `${current.path}.${field.name}`,
          });
        }
        break;
      }
    }
  }

  return null;
}

export function validateLivePreviewSchema(schema: ResultSchema): string | null {
  if (schema.schemaId.trim().length === 0) return "The preview schema is missing its identifier.";
  if (schema.revision <= 0n || schema.revision > MAXIMUM_UINT64) {
    return "The preview schema has an invalid revision.";
  }
  if (!VALID_RESULT_KINDS.has(schema.resultKind)) {
    return "The preview schema has an unsupported result kind.";
  }
  if (schema.columns.length === 0) return "The preview schema does not contain any columns.";

  const fieldNames = new Set<string>();
  for (const column of schema.columns) {
    if (column.fieldName.length === 0) {
      return "The preview schema contains an unnamed column.";
    }
    if (fieldNames.has(column.fieldName)) {
      return `The preview schema repeats column “${column.fieldName}”.`;
    }
    if (!VALID_VALUE_TYPES.has(column.valueType)) {
      return `The preview schema contains an unsupported type for “${column.fieldName}”.`;
    }
    fieldNames.add(column.fieldName);
  }
  return null;
}

function validateRows(
  schema: ResultSchema,
  rows: readonly ResultRow[],
  rowLimit: number,
): string | null {
  if (
    !Number.isInteger(rowLimit)
    || rowLimit <= 0
    || rowLimit > MAXIMUM_PREVIEW_ROW_LIMIT
  ) {
    return "The negotiated preview row limit is invalid.";
  }
  if (rows.length > rowLimit) {
    return `The server returned ${rows.length} preview rows above the negotiated limit of ${rowLimit}.`;
  }

  const rowIds = new Set<string>();
  const ordinals = new Set<bigint>();
  let previousOrdinal: bigint | null = null;
  for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
    const row = rows[rowIndex];
    if (row === undefined || row.rowId.trim().length === 0) {
      return `Preview row ${rowIndex + 1} is missing its stable identifier.`;
    }
    if (row.ordinal < 0n || row.ordinal > MAXIMUM_UINT64) {
      return `Preview row ${rowIndex + 1} has an invalid ordinal.`;
    }
    if (rowIds.has(row.rowId) || ordinals.has(row.ordinal)) {
      return `Preview row ${rowIndex + 1} repeats a row identifier or ordinal.`;
    }
    if (previousOrdinal !== null && row.ordinal <= previousOrdinal) {
      return "Preview rows are not in increasing server order.";
    }
    if (row.cells.length !== schema.columns.length) {
      return `Preview row ${rowIndex + 1} does not match the advertised schema.`;
    }
    for (let cellIndex = 0; cellIndex < row.cells.length; cellIndex += 1) {
      const valueError = validateTypedValue(
        row.cells[cellIndex],
        `Preview row ${rowIndex + 1}, column ${cellIndex + 1}`,
      );
      if (valueError !== null) return valueError;
      const column = schema.columns[cellIndex];
      const actualType = typedValueType(row.cells[cellIndex]);
      if (column === undefined || actualType === null) {
        return `Preview row ${rowIndex + 1}, column ${cellIndex + 1} has no matching schema type.`;
      }
      if (actualType === ValueType.VALUE_TYPE_NULL) {
        if (!column.nullable && column.valueType !== ValueType.VALUE_TYPE_NULL) {
          return `Preview row ${rowIndex + 1}, column ${cellIndex + 1} is null in a non-nullable column.`;
        }
      } else if (actualType === ValueType.VALUE_TYPE_MISSING) {
        if (
          !column.nullable
          && column.valueType !== ValueType.VALUE_TYPE_MISSING
          && column.valueType !== ValueType.VALUE_TYPE_MIXED
        ) {
          return `Preview row ${rowIndex + 1}, column ${cellIndex + 1} is missing in a non-nullable column.`;
        }
      } else if (
        column.valueType !== ValueType.VALUE_TYPE_MIXED
        && column.valueType !== actualType
      ) {
        return `Preview row ${rowIndex + 1}, column ${cellIndex + 1} does not match its schema type.`;
      }
    }
    rowIds.add(row.rowId);
    ordinals.add(row.ordinal);
    previousOrdinal = row.ordinal;
  }
  return null;
}

function equalBytes(left: Uint8Array, right: Uint8Array): boolean {
  if (left.length !== right.length) return false;
  return left.every((value, index) => value === right[index]);
}

function equalRows(left: readonly ResultRow[], right: readonly ResultRow[]): boolean {
  if (left.length !== right.length) return false;
  for (let index = 0; index < left.length; index += 1) {
    const leftRow = left[index];
    const rightRow = right[index];
    if (
      leftRow === undefined
      || rightRow === undefined
      || !equalBytes(
        ResultRowCodec.encode(leftRow).finish(),
        ResultRowCodec.encode(rightRow).finish(),
      )
    ) {
      return false;
    }
  }
  return true;
}

export function applyLiveResultPreview(
  current: LivePreviewSnapshot | null,
  schema: ResultSchema,
  preview: ResultPreview,
  rowLimit: number,
): LivePreviewApplyResult {
  const schemaError = validateLivePreviewSchema(schema);
  if (schemaError !== null) return { status: "invalid", message: schemaError };
  if (preview.schemaId.trim().length === 0 || preview.schemaId !== schema.schemaId) {
    return { status: "invalid", message: "The preview references an unknown result schema." };
  }
  if (preview.previewRevision <= 0n || preview.previewRevision > MAXIMUM_UINT64) {
    return { status: "invalid", message: "The preview has an invalid revision." };
  }

  const rowError = validateRows(schema, preview.rows, rowLimit);
  if (rowError !== null) return { status: "invalid", message: rowError };

  if (current !== null && preview.previewRevision < current.revision) {
    return { status: "ignored", snapshot: current, reason: "stale" };
  }

  if (preview.updateMode === PreviewUpdateMode.PREVIEW_UPDATE_MODE_RESET) {
    const next: LivePreviewSnapshot = {
      schemaId: preview.schemaId,
      revision: preview.previewRevision,
      rows: [...preview.rows],
      truncated: preview.truncated,
    };
    if (current !== null && preview.previewRevision === current.revision) {
      const sharedRowCount = Math.min(current.rows.length, next.rows.length);
      if (
        current.schemaId !== next.schemaId
        || !equalRows(
          current.rows.slice(0, sharedRowCount),
          next.rows.slice(0, sharedRowCount),
        )
      ) {
        return {
          status: "invalid",
          message: "The server changed preview contents without advancing the preview revision.",
        };
      }
      if (
        current.truncated === next.truncated
        && current.rows.length === next.rows.length
      ) {
        return { status: "ignored", snapshot: current, reason: "duplicate" };
      }
    }
    return { status: "applied", snapshot: next };
  }

  if (preview.updateMode === PreviewUpdateMode.PREVIEW_UPDATE_MODE_APPEND) {
    if (current === null || current.schemaId !== preview.schemaId) {
      return {
        status: "invalid",
        message: "The server sent an incremental preview without a matching reset snapshot.",
      };
    }
    if (preview.previewRevision === current.revision) {
      return {
        status: "invalid",
        message: "The server sent an incremental preview without advancing its revision.",
      };
    }
    const rowIds = new Set(current.rows.map((row) => row.rowId));
    const ordinals = new Set(current.rows.map((row) => row.ordinal));
    const firstAppend = preview.rows[0];
    const lastCurrent = current.rows.at(-1);
    if (
      firstAppend !== undefined
      && lastCurrent !== undefined
      && firstAppend.ordinal <= lastCurrent.ordinal
    ) {
      return {
        status: "invalid",
        message: "The incremental preview does not continue the current server row order.",
      };
    }
    if (preview.rows.some((row) => rowIds.has(row.rowId) || ordinals.has(row.ordinal))) {
      return {
        status: "invalid",
        message: "The incremental preview repeats a row that is already visible.",
      };
    }
    const rows = [...current.rows, ...preview.rows];
    if (rows.length > rowLimit) {
      return {
        status: "invalid",
        message: "The incremental preview exceeds the negotiated row limit.",
      };
    }
    return {
      status: "applied",
      snapshot: {
        schemaId: preview.schemaId,
        revision: preview.previewRevision,
        rows,
        // APPEND cannot restore rows omitted by an earlier bounded snapshot.
        // Keep the incomplete marker monotonic until a RESET replaces it.
        truncated: current.truncated || preview.truncated,
      },
    };
  }

  return {
    status: "invalid",
    message: "The server sent a preview with an unsupported update mode.",
  };
}
