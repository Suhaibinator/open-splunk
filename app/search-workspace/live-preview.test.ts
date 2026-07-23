import assert from "node:assert/strict";
import test from "node:test";

import {
  ColumnSemanticType,
  ResultSetKind,
  type ResultColumn,
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
import {
  applyLiveResultPreview,
  validateLivePreviewSchema,
  type LivePreviewSnapshot,
} from "./live-preview";

const MAXIMUM_UINT64 = (1n << 64n) - 1n;
const MAXIMUM_PROTOBUF_DURATION_SECONDS = 315_576_000_000n;
const MINIMUM_PROTOBUF_TIMESTAMP_MS = -62_135_596_800_000;
const MAXIMUM_PROTOBUF_TIMESTAMP_MS = 253_402_300_799_999;

function column(overrides: Partial<ResultColumn> = {}): ResultColumn {
  return {
    fieldName: "_raw",
    displayName: "Raw event",
    valueType: ValueType.VALUE_TYPE_STRING,
    semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW,
    nullable: false,
    multivalue: false,
    hiddenByDefault: false,
    ...overrides,
  };
}

function schema(overrides: Partial<ResultSchema> = {}): ResultSchema {
  return {
    schemaId: "schema-1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [column()],
    ...overrides,
  };
}

function row(rowId: string, ordinal: bigint, value = rowId): ResultRow {
  return {
    rowId,
    ordinal,
    cells: [{ kind: { $case: "stringValue", value } }],
  };
}

function typedRow(
  cells: TypedValue[],
  overrides: Partial<ResultRow> = {},
): ResultRow {
  return {
    rowId: "typed-row",
    ordinal: 0n,
    cells,
    ...overrides,
  };
}

function preview(overrides: Partial<ResultPreview> = {}): ResultPreview {
  return {
    searchJobId: "job-1",
    schemaId: "schema-1",
    previewRevision: 1n,
    updateMode: PreviewUpdateMode.PREVIEW_UPDATE_MODE_RESET,
    rows: [row("row-1", 0n)],
    truncated: false,
    ...overrides,
  };
}

function appliedSnapshot(
  current: LivePreviewSnapshot | null,
  next: ResultPreview,
  rowLimit = 10,
): LivePreviewSnapshot {
  const result = applyLiveResultPreview(current, schema(), next, rowLimit);
  assert.equal(result.status, "applied");
  if (result.status !== "applied") throw new Error("preview was not applied");
  return result.snapshot;
}

function applyTypedPreview(
  nextSchema: ResultSchema,
  cells: TypedValue[],
  overrides: Partial<ResultPreview> = {},
): ReturnType<typeof applyLiveResultPreview> {
  return applyLiveResultPreview(
    null,
    nextSchema,
    preview({
      schemaId: nextSchema.schemaId,
      rows: [typedRow(cells)],
      ...overrides,
    }),
    10,
  );
}

test("validates schema identity, revision, columns, and supported types", () => {
  assert.equal(validateLivePreviewSchema(schema()), null);
  assert.match(validateLivePreviewSchema(schema({ schemaId: "" })) ?? "", /identifier/);
  assert.match(validateLivePreviewSchema(schema({ revision: 0n })) ?? "", /revision/);
  assert.match(validateLivePreviewSchema(schema({ columns: [] })) ?? "", /columns/);
  assert.match(validateLivePreviewSchema(schema({
    columns: [schema().columns[0], schema().columns[0]],
  })) ?? "", /repeats column/);
});

test("schema and object field names preserve whitespace but reject empty names", () => {
  assert.equal(validateLivePreviewSchema(schema({
    columns: [column({ fieldName: "   " })],
  })), null);
  assert.match(validateLivePreviewSchema(schema({
    columns: [column({ fieldName: "" })],
  })) ?? "", /unnamed column/);

  const objectSchema = schema({
    columns: [column({ valueType: ValueType.VALUE_TYPE_OBJECT })],
  });
  const whitespaceObjectField = applyTypedPreview(objectSchema, [{
    kind: {
      $case: "objectValue",
      value: {
        fields: [{
          name: " ",
          value: { kind: { $case: "stringValue", value: "preserved" } },
        }],
      },
    },
  }]);
  assert.equal(whitespaceObjectField.status, "applied");

  const emptyObjectField = applyTypedPreview(objectSchema, [{
    kind: {
      $case: "objectValue",
      value: {
        fields: [{
          name: "",
          value: { kind: { $case: "stringValue", value: "rejected" } },
        }],
      },
    },
  }]);
  assert.equal(emptyObjectField.status, "invalid");
  if (emptyObjectField.status === "invalid") {
    assert.match(emptyObjectField.message, /unnamed object field/);
  }
});

test("RESET applies once, ignores an identical duplicate, and rejects mutation at one revision", () => {
  const reset = preview();
  const snapshot = appliedSnapshot(null, reset);

  const duplicate = applyLiveResultPreview(snapshot, schema(), reset, 10);
  assert.equal(duplicate.status, "ignored");
  if (duplicate.status === "ignored") assert.equal(duplicate.reason, "duplicate");

  const mutation = applyLiveResultPreview(snapshot, schema(), preview({
    rows: [row("row-1", 0n, "changed")],
  }), 10);
  assert.equal(mutation.status, "invalid");
  if (mutation.status === "invalid") assert.match(mutation.message, /without advancing/);
});

test("equal-revision RESET accepts compatible prefixes and an empty tombstone", () => {
  const initial = appliedSnapshot(null, preview({
    previewRevision: 7n,
    rows: [row("row-1", 0n), row("row-2", 1n)],
  }));

  const grownResult = applyLiveResultPreview(initial, schema(), preview({
    previewRevision: 7n,
    rows: [row("row-1", 0n), row("row-2", 1n), row("row-3", 2n)],
  }), 10);
  assert.equal(grownResult.status, "applied");
  if (grownResult.status !== "applied") throw new Error("compatible growth was not applied");
  assert.deepEqual(grownResult.snapshot.rows.map((item) => item.rowId), [
    "row-1",
    "row-2",
    "row-3",
  ]);

  const shrunkResult = applyLiveResultPreview(grownResult.snapshot, schema(), preview({
    previewRevision: 7n,
    rows: [row("row-1", 0n)],
  }), 10);
  assert.equal(shrunkResult.status, "applied");
  if (shrunkResult.status !== "applied") throw new Error("compatible shrink was not applied");
  assert.deepEqual(shrunkResult.snapshot.rows.map((item) => item.rowId), ["row-1"]);

  const tombstoneResult = applyLiveResultPreview(shrunkResult.snapshot, schema(), preview({
    previewRevision: 7n,
    rows: [],
    truncated: false,
  }), 10);
  assert.equal(tombstoneResult.status, "applied");
  if (tombstoneResult.status === "applied") {
    assert.deepEqual(tombstoneResult.snapshot.rows, []);
    assert.equal(tombstoneResult.snapshot.revision, 7n);
  }
});

test("equal-revision RESET rejects a changed row in its shared prefix", () => {
  const current = appliedSnapshot(null, preview({
    previewRevision: 11n,
    rows: [row("row-1", 0n), row("row-2", 1n)],
  }));
  const changedSharedRow = applyLiveResultPreview(current, schema(), preview({
    previewRevision: 11n,
    rows: [row("row-1", 0n), row("row-2", 1n, "mutated")],
  }), 10);

  assert.equal(changedSharedRow.status, "invalid");
  if (changedSharedRow.status === "invalid") {
    assert.match(changedSharedRow.message, /without advancing/);
  }
});

test("ignores stale RESET revisions without replacing the current snapshot", () => {
  const current = appliedSnapshot(null, preview({ previewRevision: 3n }));
  const result = applyLiveResultPreview(current, schema(), preview({ previewRevision: 2n }), 10);

  assert.equal(result.status, "ignored");
  if (result.status === "ignored") {
    assert.equal(result.reason, "stale");
    assert.equal(result.snapshot, current);
  }
});

test("APPEND requires continuity, adds ordered rows, and keeps truncation monotonic", () => {
  const appendWithoutReset = applyLiveResultPreview(null, schema(), preview({
    previewRevision: 2n,
    updateMode: PreviewUpdateMode.PREVIEW_UPDATE_MODE_APPEND,
  }), 10);
  assert.equal(appendWithoutReset.status, "invalid");

  const current = appliedSnapshot(null, preview({ truncated: true }));
  const appended = appliedSnapshot(current, preview({
    previewRevision: 2n,
    updateMode: PreviewUpdateMode.PREVIEW_UPDATE_MODE_APPEND,
    rows: [row("row-2", 1n)],
    truncated: false,
  }));
  assert.deepEqual(appended.rows.map((item) => item.rowId), ["row-1", "row-2"]);
  assert.equal(appended.truncated, true);

  const repeated = applyLiveResultPreview(appended, schema(), preview({
    previewRevision: 3n,
    updateMode: PreviewUpdateMode.PREVIEW_UPDATE_MODE_APPEND,
    rows: [row("row-2", 2n)],
  }), 10);
  assert.equal(repeated.status, "invalid");

  const unordered = applyLiveResultPreview(appended, schema(), preview({
    previewRevision: 3n,
    updateMode: PreviewUpdateMode.PREVIEW_UPDATE_MODE_APPEND,
    rows: [row("row-3", 1n)],
  }), 10);
  assert.equal(unordered.status, "invalid");
});

test("RESET may replace a truncated snapshot with a complete newer snapshot", () => {
  const current = appliedSnapshot(null, preview({ truncated: true }));
  const replacement = appliedSnapshot(current, preview({
    previewRevision: 2n,
    rows: [row("replacement", 0n)],
    truncated: false,
  }));

  assert.equal(replacement.truncated, false);
  assert.equal(replacement.rows[0]?.rowId, "replacement");
});

test("rejects malformed rows before they enter display state", () => {
  const cases: Array<{ name: string; value: ResultPreview; rowLimit?: number }> = [
    {
      name: "row limit",
      value: preview({ rows: [row("row-1", 0n), row("row-2", 1n)] }),
      rowLimit: 1,
    },
    { name: "missing row id", value: preview({ rows: [row("", 0n)] }) },
    { name: "negative ordinal", value: preview({ rows: [row("row-1", -1n)] }) },
    {
      name: "duplicate ordinal",
      value: preview({ rows: [row("row-1", 0n), row("row-2", 0n)] }),
    },
    {
      name: "cell count",
      value: preview({ rows: [{ rowId: "row-1", ordinal: 0n, cells: [] }] }),
    },
    {
      name: "missing typed value",
      value: preview({ rows: [{ rowId: "row-1", ordinal: 0n, cells: [{ kind: undefined }] }] }),
    },
    {
      name: "non-finite double",
      value: preview({
        rows: [{
          rowId: "row-1",
          ordinal: 0n,
          cells: [{ kind: { $case: "doubleValue", value: Number.POSITIVE_INFINITY } }],
        }],
      }),
    },
  ];

  for (const item of cases) {
    const result = applyLiveResultPreview(null, schema(), item.value, item.rowLimit ?? 10);
    assert.equal(result.status, "invalid", item.name);
  }
});

test("enforces schema types, UINT64 bounds, and nullable MIXED values", () => {
  const typedSchema = schema({
    columns: [
      column({ fieldName: "unsigned", valueType: ValueType.VALUE_TYPE_UINT64 }),
      column({
        fieldName: "anything",
        valueType: ValueType.VALUE_TYPE_MIXED,
        nullable: true,
      }),
    ],
  });

  const accepted = [
    { kind: { $case: "uint64Value" as const, value: MAXIMUM_UINT64 } },
    { kind: { $case: "nullValue" as const, value: NullValue.NULL_VALUE_NULL } },
  ];
  assert.equal(applyTypedPreview(typedSchema, accepted).status, "applied");

  const nullableMixedMissing = [
    { kind: { $case: "uint64Value" as const, value: 0n } },
    {
      kind: {
        $case: "missingValue" as const,
        value: MissingValue.MISSING_VALUE_MISSING,
      },
    },
  ];
  assert.equal(applyTypedPreview(typedSchema, nullableMixedMissing).status, "applied");

  const typeMismatch = applyTypedPreview(typedSchema, [
    { kind: { $case: "sint64Value", value: 1n } },
    { kind: { $case: "stringValue", value: "mixed accepts this" } },
  ]);
  assert.equal(typeMismatch.status, "invalid");
  if (typeMismatch.status === "invalid") {
    assert.match(typeMismatch.message, /does not match its schema type/);
  }

  const nonNullableNull = applyTypedPreview(
    schema({ columns: [column({ valueType: ValueType.VALUE_TYPE_UINT64 })] }),
    [{ kind: { $case: "nullValue", value: NullValue.NULL_VALUE_NULL } }],
  );
  assert.equal(nonNullableNull.status, "invalid");
  if (nonNullableNull.status === "invalid") {
    assert.match(nonNullableNull.message, /null in a non-nullable column/);
  }

  const uint64Overflow = applyTypedPreview(
    schema({ columns: [column({ valueType: ValueType.VALUE_TYPE_UINT64 })] }),
    [{ kind: { $case: "uint64Value", value: MAXIMUM_UINT64 + 1n } }],
  );
  assert.equal(uint64Overflow.status, "invalid");
  if (uint64Overflow.status === "invalid") {
    assert.match(uint64Overflow.message, /out-of-range unsigned integer/);
  }
});

test("validates protobuf duration range, nanos, and sign invariants", () => {
  const durationSchema = schema({
    columns: [column({ valueType: ValueType.VALUE_TYPE_DURATION })],
  });
  const duration = (seconds: bigint, nanos: number) => applyTypedPreview(
    durationSchema,
    [{
      kind: {
        $case: "durationValue",
        value: { seconds, nanos },
      },
    }],
  );

  for (const [seconds, nanos] of [
    [0n, 0],
    [0n, 999_999_999],
    [0n, -999_999_999],
    [MAXIMUM_PROTOBUF_DURATION_SECONDS, 999_999_999],
    [-MAXIMUM_PROTOBUF_DURATION_SECONDS, -999_999_999],
  ] as const) {
    assert.equal(duration(seconds, nanos).status, "applied", `${seconds}:${nanos}`);
  }

  for (const [seconds, nanos] of [
    [MAXIMUM_PROTOBUF_DURATION_SECONDS + 1n, 0],
    [-MAXIMUM_PROTOBUF_DURATION_SECONDS - 1n, 0],
    [1n, -1],
    [-1n, 1],
    [0n, 1_000_000_000],
    [0n, -1_000_000_000],
    [0n, 0.5],
  ] as const) {
    const result = duration(seconds, nanos);
    assert.equal(result.status, "invalid", `${seconds}:${nanos}`);
    if (result.status === "invalid") assert.match(result.message, /invalid duration/);
  }
});

test("accepts only canonical decimal spellings", () => {
  const decimalSchema = schema({
    columns: [column({ valueType: ValueType.VALUE_TYPE_DECIMAL })],
  });
  const decimal = (value: string) => applyTypedPreview(decimalSchema, [{
    kind: { $case: "decimalValue", value: { value } },
  }]);

  for (const value of [
    "0",
    "1",
    "-1",
    "12.34",
    "-0.000001",
    "999999999999999999999",
    "1e21",
    "1e-7",
    "-9.9e42",
  ]) {
    assert.equal(decimal(value).status, "applied", value);
  }

  for (const value of [
    "",
    "-0",
    "+1",
    "01",
    ".1",
    "1.",
    "1.0",
    "1E21",
    "1e0",
    "1e+21",
    "1e021",
    "1.20e21",
    "10e21",
    "1e20",
    "1e-6",
    "0.0000001",
    "1000000000000000000000",
  ]) {
    const result = decimal(value);
    assert.equal(result.status, "invalid", value);
    if (result.status === "invalid") assert.match(result.message, /invalid decimal/);
  }
});

test("validates protobuf timestamp limits", () => {
  const timestampSchema = schema({
    columns: [column({ valueType: ValueType.VALUE_TYPE_TIMESTAMP })],
  });
  const timestamp = (milliseconds: number) => applyTypedPreview(timestampSchema, [{
    kind: { $case: "timestampValue", value: new Date(milliseconds) },
  }]);

  assert.equal(timestamp(MINIMUM_PROTOBUF_TIMESTAMP_MS).status, "applied");
  assert.equal(timestamp(MAXIMUM_PROTOBUF_TIMESTAMP_MS).status, "applied");
  assert.equal(timestamp(MINIMUM_PROTOBUF_TIMESTAMP_MS - 1).status, "invalid");
  assert.equal(timestamp(MAXIMUM_PROTOBUF_TIMESTAMP_MS + 1).status, "invalid");

  const invalidDate = applyTypedPreview(timestampSchema, [{
    kind: { $case: "timestampValue", value: new Date(Number.NaN) },
  }]);
  assert.equal(invalidDate.status, "invalid");
  if (invalidDate.status === "invalid") {
    assert.match(invalidDate.message, /invalid timestamp/);
  }
});

test("enforces uint64 bounds for schema, preview revision, and row ordinal", () => {
  const maximumRevisionSchema = schema({ revision: MAXIMUM_UINT64 });
  assert.equal(validateLivePreviewSchema(maximumRevisionSchema), null);
  assert.match(validateLivePreviewSchema(schema({
    revision: MAXIMUM_UINT64 + 1n,
  })) ?? "", /invalid revision/);

  assert.equal(applyLiveResultPreview(
    null,
    maximumRevisionSchema,
    preview({ previewRevision: MAXIMUM_UINT64 }),
    10,
  ).status, "applied");
  assert.equal(applyLiveResultPreview(
    null,
    maximumRevisionSchema,
    preview({ previewRevision: MAXIMUM_UINT64 + 1n }),
    10,
  ).status, "invalid");

  assert.equal(applyLiveResultPreview(
    null,
    schema(),
    preview({ rows: [row("maximum-ordinal", MAXIMUM_UINT64)] }),
    10,
  ).status, "applied");
  assert.equal(applyLiveResultPreview(
    null,
    schema(),
    preview({ rows: [row("overflow-ordinal", MAXIMUM_UINT64 + 1n)] }),
    10,
  ).status, "invalid");
});
