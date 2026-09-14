import assert from "node:assert/strict";
import test from "node:test";

import { ResultRow, ResultSchema, VisualizationSpec, VisualizationStackMode, VisualizationType } from "@/gen/ts/open_splunk/result";
import type { TypedValue } from "@/gen/ts/open_splunk/value";
import { dashboardCellText, dashboardDurationFromInput, projectDashboardVisualization } from "./dashboard-visualization-model";

const schema = ResultSchema.fromPartial({
  schemaId: "adversarial-chart",
  revision: 1n,
  columns: [{ fieldName: "x" }, { fieldName: "y" }, { fieldName: "group" }],
});

function row(y: TypedValue, x: TypedValue = { kind: { $case: "sint64Value", value: 1n } }) {
  return ResultRow.fromPartial({
    rowId: "row-1",
    cells: [x, y, { kind: { $case: "stringValue", value: "group-a" } }],
  });
}

function spec(type: VisualizationType) {
  return VisualizationSpec.fromPartial({ type, xField: "x", yFields: ["y"] });
}

test("a negative decimal pie value remains negative when its plotting coordinate underflows", () => {
  const result = projectDashboardVisualization(spec(VisualizationType.VISUALIZATION_TYPE_PIE), schema, [
    row({ kind: { $case: "decimalValue", value: { value: "-1e-400" } } }),
  ]);
  assert.equal(result.kind, "error");
  if (result.kind === "error") assert.match(result.issues.join(" "), /negative/iu);
  const positive = projectDashboardVisualization(spec(VisualizationType.VISUALIZATION_TYPE_PIE), schema, [
    row({ kind: { $case: "decimalValue", value: { value: "1e-400" } } }),
  ]);
  assert.equal(positive.kind, "pie");
});

test("a single-value chart rejects malformed scalar representations", () => {
  const valid = projectDashboardVisualization(spec(VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE), schema, [
    row({ kind: { $case: "sint64Value", value: 3n } }),
  ]);
  assert.equal(valid.kind, "single");
  for (const value of [
    { kind: { $case: "doubleValue", value: Number.POSITIVE_INFINITY } },
    { kind: { $case: "timestampValue", value: new Date(Number.NaN) } },
    { kind: { $case: "decimalValue", value: { value: "not-a-decimal" } } },
  ] satisfies TypedValue[]) {
    const result = projectDashboardVisualization(spec(VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE), schema, [row(value)]);
    assert.equal(result.kind, "error", value.kind?.$case);
  }
});

test("distinct scalar byte values retain distinguishable exact labels", () => {
  const first = dashboardCellText({ kind: { $case: "bytesValue", value: new Uint8Array([0, 255]) } });
  const second = dashboardCellText({ kind: { $case: "bytesValue", value: new Uint8Array([255, 0]) } });
  assert.notEqual(first, second);
});

test("time bucket input rejects durations outside the protobuf duration range", () => {
  assert.equal(dashboardDurationFromInput("315576000001"), null);
  assert.equal(dashboardDurationFromInput("999999999999999999999999999999999999999999"), null);
  assert.equal(dashboardDurationFromInput("0"), null);
  assert.equal(dashboardDurationFromInput("0.0000000001"), null);
  assert.deepEqual(dashboardDurationFromInput("315576000000"), { seconds: 315576000000n, nanos: 0 });
  assert.deepEqual(dashboardDurationFromInput("0.000000001"), { seconds: 0n, nanos: 1 });
});

test("mixed-sign duration components do not silently normalize a malformed imported setting", () => {
  const configured = spec(VisualizationType.VISUALIZATION_TYPE_LINE);
  configured.timeBucketWidth = { seconds: 1n, nanos: -1 };
  const result = projectDashboardVisualization(configured, schema, [row({ kind: { $case: "sint64Value", value: 3n } })]);
  assert.equal(result.kind, "error");
});

test("non-Cartesian and scatter charts reject stacking instead of silently ignoring it", () => {
  for (const type of [VisualizationType.VISUALIZATION_TYPE_PIE, VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE, VisualizationType.VISUALIZATION_TYPE_SCATTER]) {
    const configured = spec(type);
    configured.stackMode = VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED;
    const result = projectDashboardVisualization(configured, schema, [row({ kind: { $case: "sint64Value", value: 3n } })]);
    assert.equal(result.kind, "error", String(type));
  }
});

test("interleaved groups retain typed identities and detect spacing within each group", () => {
  const configured = spec(VisualizationType.VISUALIZATION_TYPE_LINE);
  configured.seriesField = "group";
  configured.timeBucketWidth = { seconds: 2n, nanos: 0 };
  const rows = [0n, 1n, 4n, 5n].map((x, index) => {
    const source = row({ kind: { $case: "sint64Value", value: 7n } }, { kind: { $case: "sint64Value", value: x } });
    source.rowId = `row-${index}`;
    source.ordinal = BigInt(index);
    source.cells[2] = index % 2 === 0
      ? { kind: { $case: "sint64Value", value: 1n } }
      : { kind: { $case: "stringValue", value: "1" } };
    return source;
  });
  const result = projectDashboardVisualization(configured, schema, rows);
  assert.equal(result.kind, "cartesian");
  if (result.kind !== "cartesian") return;
  assert.deepEqual(result.rows.map((entry) => entry.source), rows);
  assert.deepEqual(result.rows.map((entry) => entry.x.coordinate), [0, 1, 4, 5]);
  assert.deepEqual(result.rows.map((entry) => entry.gapBefore), [false, false, true, true]);
  assert.notEqual(result.rows[0].group?.identity, result.rows[1].group?.identity);
  assert.equal(result.rows[0].group?.identity, result.rows[2].group?.identity);
});

test("Cartesian numeric X coordinates disclose precision loss without changing exact labels", () => {
  const result = projectDashboardVisualization(spec(VisualizationType.VISUALIZATION_TYPE_LINE), schema, [
    row({ kind: { $case: "sint64Value", value: 7n } }, { kind: { $case: "uint64Value", value: 9007199254740993n } }),
  ]);
  assert.equal(result.kind, "cartesian");
  if (result.kind !== "cartesian") return;
  assert.equal(result.approximate, true);
  assert.equal(result.rows[0].x.approximate, true);
  assert.equal(result.rows[0].x.exact, "9007199254740993");
  assert.equal(result.rows[0].x.coordinate, 9007199254740992);
});

test("decimal bucket spacing does not invent gaps from binary subtraction rounding", () => {
  const configured = spec(VisualizationType.VISUALIZATION_TYPE_LINE);
  configured.timeBucketWidth = { seconds: 0n, nanos: 100_000_000 };
  const rows = ["0.1", "0.2", "0.3", "0.4"].map((x, index) => {
    const source = row({ kind: { $case: "sint64Value", value: 7n } }, { kind: { $case: "decimalValue", value: { value: x } } });
    source.rowId = `row-${index}`;
    return source;
  });
  const result = projectDashboardVisualization(configured, schema, rows);
  assert.equal(result.kind, "cartesian");
  if (result.kind !== "cartesian") return;
  assert.deepEqual(result.rows.map((entry) => entry.gapBefore), [false, false, false, false]);
});
