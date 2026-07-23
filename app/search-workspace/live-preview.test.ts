import assert from "node:assert/strict";
import test from "node:test";

import {
  ColumnSemanticType,
  ResultSetKind,
  type ResultRow,
  type ResultSchema,
} from "../../gen/ts/open_splunk/v1/result";
import {
  PreviewUpdateMode,
  type ResultPreview,
} from "../../gen/ts/open_splunk/v1/search_ws";
import { ValueType } from "../../gen/ts/open_splunk/v1/value";
import {
  applyLiveResultPreview,
  validateLivePreviewSchema,
  type LivePreviewSnapshot,
} from "./live-preview";

function schema(overrides: Partial<ResultSchema> = {}): ResultSchema {
  return {
    schemaId: "schema-1",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_EVENTS,
    columns: [{
      fieldName: "_raw",
      displayName: "Raw event",
      valueType: ValueType.VALUE_TYPE_STRING,
      semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_RAW,
      nullable: false,
      multivalue: false,
      hiddenByDefault: false,
    }],
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

test("validates schema identity, revision, columns, and supported types", () => {
  assert.equal(validateLivePreviewSchema(schema()), null);
  assert.match(validateLivePreviewSchema(schema({ schemaId: "" })) ?? "", /identifier/);
  assert.match(validateLivePreviewSchema(schema({ revision: 0n })) ?? "", /revision/);
  assert.match(validateLivePreviewSchema(schema({ columns: [] })) ?? "", /columns/);
  assert.match(validateLivePreviewSchema(schema({
    columns: [schema().columns[0], schema().columns[0]],
  })) ?? "", /repeats column/);
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
