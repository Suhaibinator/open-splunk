import assert from "node:assert/strict";
import test from "node:test";

import {
  ResultSetKind,
  VisualizationStackMode,
  VisualizationType,
  type ResultRow,
  type ResultSchema,
  type VisualizationSpec,
} from "@/gen/ts/open_splunk/result";
import { ValueType } from "@/gen/ts/open_splunk/value";

import {
  dashboardDurationFromInput,
  dashboardDurationInput,
  projectDashboardVisualization,
} from "./dashboard-visualization-model";

const schema: ResultSchema = {
  schemaId: "dashboard-v1",
  revision: 1n,
  resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
  columns: [
    { fieldName: "service", displayName: "Service", valueType: ValueType.VALUE_TYPE_STRING, semanticType: 0, nullable: false, multivalue: false, hiddenByDefault: false, flatMultivalueDelimiter: undefined, statsSparkline: false },
    { fieldName: "errors", displayName: "Errors", valueType: ValueType.VALUE_TYPE_UINT64, semanticType: 0, nullable: true, multivalue: false, hiddenByDefault: false, flatMultivalueDelimiter: undefined, statsSparkline: false },
  ],
};

const rows: ResultRow[] = [
  { rowId: "api", ordinal: 1n, cells: [{ kind: { $case: "stringValue", value: "API" } }, { kind: { $case: "uint64Value", value: 7n } }], timeBucket: undefined },
  { rowId: "web", ordinal: 2n, cells: [{ kind: { $case: "stringValue", value: "Web" } }, { kind: { $case: "nullValue", value: 1 } }], timeBucket: undefined },
];

const lineSpec: VisualizationSpec = {
  type: VisualizationType.VISUALIZATION_TYPE_LINE,
  title: "Errors by service",
  xField: "service",
  yFields: ["errors"],
  seriesField: undefined,
  stackMode: VisualizationStackMode.VISUALIZATION_STACK_MODE_NONE,
  showLegend: true,
  showDataLabels: true,
  timeBucketWidth: undefined,
};

test("a persisted line specification projects typed rows directly and keeps null gaps", () => {
  const model = projectDashboardVisualization(lineSpec, schema, rows);
  assert.equal(model.kind, "cartesian");
  if (model.kind !== "cartesian") return;
  assert.equal(model.style, "line");
  assert.deepEqual(model.series.map((series) => series.label), ["Errors"]);
  assert.deepEqual(model.rows.map((row) => ({ x: row.x.label, y: row.values[0]?.coordinate ?? null })), [
    { x: "API", y: 7 },
    { x: "Web", y: null },
  ]);
});

test("an absent legacy specification remains a table while explicit unspecified is an error", () => {
  assert.equal(projectDashboardVisualization(undefined, schema, rows).kind, "table");
  const invalid = projectDashboardVisualization({
    ...lineSpec,
    type: VisualizationType.VISUALIZATION_TYPE_UNSPECIFIED,
  }, schema, rows);
  assert.equal(invalid.kind, "error");
  if (invalid.kind === "error") assert.match(invalid.issues.join(" "), /unspecified or unsupported/u);
});

test("all Cartesian types preserve row, series, and group order", () => {
  const groupedSchema: ResultSchema = {
    ...schema,
    columns: [
      ...schema.columns,
      { fieldName: "warnings", displayName: "Warnings", valueType: ValueType.VALUE_TYPE_SINT64, semanticType: 0, nullable: false, multivalue: false, hiddenByDefault: false, flatMultivalueDelimiter: undefined, statsSparkline: false },
      { fieldName: "region", displayName: "Region", valueType: ValueType.VALUE_TYPE_STRING, semanticType: 0, nullable: false, multivalue: false, hiddenByDefault: false, flatMultivalueDelimiter: undefined, statsSparkline: false },
    ],
  };
  const groupedRows: ResultRow[] = [
    { rowId: "west", ordinal: 9n, cells: [{ kind: { $case: "stringValue", value: "B" } }, { kind: { $case: "uint64Value", value: 2n } }, { kind: { $case: "sint64Value", value: -1n } }, { kind: { $case: "stringValue", value: "west" } }], timeBucket: undefined },
    { rowId: "east", ordinal: 3n, cells: [{ kind: { $case: "stringValue", value: "A" } }, { kind: { $case: "uint64Value", value: 4n } }, { kind: { $case: "sint64Value", value: 3n } }, { kind: { $case: "stringValue", value: "east" } }], timeBucket: undefined },
  ];
  for (const type of [
    VisualizationType.VISUALIZATION_TYPE_LINE,
    VisualizationType.VISUALIZATION_TYPE_AREA,
    VisualizationType.VISUALIZATION_TYPE_COLUMN,
    VisualizationType.VISUALIZATION_TYPE_BAR,
  ]) {
    const model = projectDashboardVisualization({
      ...lineSpec,
      seriesField: "region",
      type,
      yFields: ["warnings", "errors"],
    }, groupedSchema, groupedRows);
    assert.equal(model.kind, "cartesian");
    if (model.kind !== "cartesian") continue;
    assert.deepEqual(model.series.map((series) => series.fieldName), ["warnings", "errors"]);
    assert.deepEqual(model.rows.map((row) => row.id), ["west", "east"]);
    assert.deepEqual(model.rows.map((row) => row.group?.exact), ["west", "east"]);
    assert.deepEqual(model.rows.map((row) => row.values.map((value) => value.coordinate)), [[-1, 2], [3, 4]]);
  }
});

test("scatter requires typed numeric coordinates and retains exact approximate integers", () => {
  const scatterSchema: ResultSchema = {
    ...schema,
    columns: [
      { ...schema.columns[0], fieldName: "x", displayName: "X", valueType: ValueType.VALUE_TYPE_UINT64 },
      schema.columns[1],
    ],
  };
  const exactRows: ResultRow[] = [{
    rowId: "large",
    ordinal: 1n,
    cells: [
      { kind: { $case: "uint64Value", value: 9_007_199_254_740_993n } },
      { kind: { $case: "uint64Value", value: 18_014_398_509_481_987n } },
    ],
    timeBucket: undefined,
  }];
  const model = projectDashboardVisualization({
    ...lineSpec,
    type: VisualizationType.VISUALIZATION_TYPE_SCATTER,
    xField: "x",
  }, scatterSchema, exactRows);
  assert.equal(model.kind, "cartesian");
  if (model.kind === "cartesian") {
    assert.equal(model.approximate, true);
    assert.equal(model.rows[0].x.exact, "9007199254740993");
    assert.equal(model.rows[0].values[0].exact, "18014398509481987");
  }
  const stringRows: ResultRow[] = [{
    ...exactRows[0],
    cells: [{ kind: { $case: "stringValue", value: "12" } }, exactRows[0].cells[1]],
  }];
  const invalid = projectDashboardVisualization({
    ...lineSpec,
    type: VisualizationType.VISUALIZATION_TYPE_SCATTER,
    xField: "x",
  }, scatterSchema, stringRows);
  assert.equal(invalid.kind, "error");
  if (invalid.kind === "error") {
    assert.match(invalid.issues.join(" "), /non-numeric X field/u);
    assert.equal(invalid.rows, stringRows);
  }
});

test("unknown, ambiguous, and repeated configured fields keep every row inspectable", () => {
  const duplicateSchema = { ...schema, columns: [...schema.columns, { ...schema.columns[1] }] };
  const models = [
    projectDashboardVisualization({ ...lineSpec, xField: "unknown" }, schema, rows),
    projectDashboardVisualization({ ...lineSpec, yFields: ["errors", "errors"] }, schema, rows),
    projectDashboardVisualization(lineSpec, duplicateSchema, rows),
  ];
  for (const model of models) {
    assert.equal(model.kind, "error");
    if (model.kind === "error") assert.equal(model.rows, rows);
  }
});

test("pie maps one nonnegative value per source row and reports an all-zero surface", () => {
  const zeroRows: ResultRow[] = [{
    ...rows[0],
    cells: [rows[0].cells[0], { kind: { $case: "uint64Value", value: 0n } }],
  }];
  const pie = projectDashboardVisualization({
    ...lineSpec,
    type: VisualizationType.VISUALIZATION_TYPE_PIE,
  }, schema, zeroRows);
  assert.equal(pie.kind, "pie");
  if (pie.kind === "pie") {
    assert.equal(pie.zeroTotal, true);
    assert.equal(pie.slices.length, 1);
  }
  const negativeRows: ResultRow[] = [{
    ...rows[0],
    cells: [rows[0].cells[0], { kind: { $case: "sint64Value", value: -1n } }],
  }];
  assert.equal(projectDashboardVisualization({
    ...lineSpec,
    type: VisualizationType.VISUALIZATION_TYPE_PIE,
  }, schema, negativeRows).kind, "error");
});

test("single value requires exactly one row and one non-null scalar", () => {
  const spec = {
    ...lineSpec,
    type: VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE,
    xField: undefined,
  };
  assert.equal(projectDashboardVisualization(spec, schema, rows).kind, "error");
  const single = projectDashboardVisualization(spec, schema, [rows[0]]);
  assert.equal(single.kind, "single");
  if (single.kind === "single") assert.equal(single.value.exact, "7");
  assert.equal(projectDashboardVisualization(spec, schema, [rows[1]]).kind, "error");
});

test("time bucket duration roundtrips nanoseconds and only marks source spacing gaps", () => {
  const duration = dashboardDurationFromInput("1.000000001");
  assert.deepEqual(duration, { seconds: 1n, nanos: 1 });
  assert.equal(dashboardDurationInput(duration ?? undefined), "1.000000001");
  assert.equal(dashboardDurationFromInput("0"), null);
  assert.equal(dashboardDurationFromInput("-1"), null);
  assert.equal(dashboardDurationFromInput("1.0000000001"), null);

  const numericSchema = {
    ...schema,
    columns: [{ ...schema.columns[0], valueType: ValueType.VALUE_TYPE_DOUBLE }, schema.columns[1]],
  };
  const spacedRows: ResultRow[] = [0, 1, 4].map((x, index) => ({
    rowId: `row-${index}`,
    ordinal: BigInt(index),
    cells: [
      { kind: { $case: "doubleValue", value: x } },
      { kind: { $case: "uint64Value", value: BigInt(index) } },
    ],
    timeBucket: undefined,
  }));
  const model = projectDashboardVisualization({
    ...lineSpec,
    timeBucketWidth: { seconds: 1n, nanos: 0 },
  }, numericSchema, spacedRows);
  assert.equal(model.kind, "cartesian");
  if (model.kind === "cartesian") {
    assert.deepEqual(model.rows.map((row) => row.gapBefore), [false, false, true]);
    assert.deepEqual(model.rows.map((row) => row.x.exact), ["0", "1", "4"]);
  }
});
