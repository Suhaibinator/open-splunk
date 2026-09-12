import assert from "node:assert/strict";
import test from "node:test";

import { renderToStaticMarkup } from "react-dom/server";

import {
  ResultRow,
  ResultSchema,
  VisualizationSpec,
  VisualizationStackMode,
  VisualizationType,
} from "@/gen/ts/open_splunk/result";
import { ValueType } from "@/gen/ts/open_splunk/value";

import {
  DashboardVisualization,
  DashboardVisualizationEditor,
} from "./dashboard-visualization";

const schema = ResultSchema.fromPartial({
  schemaId: "render-schema",
  revision: 1n,
  columns: [
    { fieldName: "x", displayName: "Position", valueType: ValueType.VALUE_TYPE_SINT64 },
    { fieldName: "latency", displayName: "Latency", valueType: ValueType.VALUE_TYPE_SINT64, nullable: true },
    { fieldName: "throughput", displayName: "Throughput", valueType: ValueType.VALUE_TYPE_SINT64 },
    { fieldName: "region", displayName: "Region", valueType: ValueType.VALUE_TYPE_STRING },
  ],
});

function row(index: number, latency = 9_007_199_254_740_993n): ResultRow {
  return ResultRow.fromPartial({
    rowId: `row-${index}`,
    ordinal: BigInt(index),
    cells: [
      { kind: { $case: "sint64Value", value: BigInt(index) } },
      { kind: { $case: "sint64Value", value: latency } },
      { kind: { $case: "sint64Value", value: 4n } },
      { kind: { $case: "stringValue", value: index % 2 === 0 ? "west" : "east" } },
    ],
  });
}

function spec(type: VisualizationType): VisualizationSpec {
  return VisualizationSpec.fromPartial({
    showDataLabels: true,
    showLegend: true,
    stackMode: VisualizationStackMode.VISUALIZATION_STACK_MODE_NONE,
    title: `${type} exact values`,
    type,
    xField: "x",
    yFields: ["latency"],
  });
}

test("the editor exposes every persisted visualization field and ordered measures", () => {
  const value = VisualizationSpec.fromPartial({
    ...spec(VisualizationType.VISUALIZATION_TYPE_AREA),
    seriesField: "region",
    stackMode: VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED,
    timeBucketWidth: { seconds: 1n, nanos: 1 },
    yFields: ["latency", "throughput"],
  });
  const markup = renderToStaticMarkup(<DashboardVisualizationEditor
    disabled={false}
    fields={schema.columns.map((column) => ({ fieldName: column.fieldName, label: column.displayName }))}
    onChange={() => undefined}
    value={value}
  />);
  assert.match(markup, /aria-label="Visualization"/u);
  assert.match(markup, /value="1\.000000001"/u);
  assert.match(markup, /aria-label="Move Latency down"/u);
  assert.match(markup, /aria-label="Move Throughput up"/u);
  assert.match(markup, /Show legend/u);
  assert.match(markup, /Show data labels/u);
  for (const label of ["Table", "Line", "Area", "Column", "Bar", "Pie", "Single value", "Scatter"]) {
    assert.match(markup, new RegExp(`>${label}<`, "u"));
  }
  assert.doesNotMatch(markup, /<select\b/u);
});

test("all eight persisted visualization types render their distinct presentation", () => {
  for (const type of [
    VisualizationType.VISUALIZATION_TYPE_TABLE,
    VisualizationType.VISUALIZATION_TYPE_LINE,
    VisualizationType.VISUALIZATION_TYPE_AREA,
    VisualizationType.VISUALIZATION_TYPE_COLUMN,
    VisualizationType.VISUALIZATION_TYPE_BAR,
    VisualizationType.VISUALIZATION_TYPE_PIE,
    VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE,
    VisualizationType.VISUALIZATION_TYPE_SCATTER,
  ]) {
    const candidate = spec(type);
    if (type === VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE) candidate.xField = undefined;
    const markup = renderToStaticMarkup(<DashboardVisualization panelTitle="Latency panel" rows={[row(1)]} schema={schema} spec={candidate} />);
    assert.match(markup, /aria-label="Latency panel visualization"/u);
    assert.match(markup, /9007199254740993/u);
    if (type === VisualizationType.VISUALIZATION_TYPE_TABLE) assert.match(markup, /<table/u);
    else if (type === VisualizationType.VISUALIZATION_TYPE_SINGLE_VALUE) assert.match(markup, /operations-dashboard-single-value/u);
    else assert.match(markup, /role="img"/u);
  }
});

test("chart inspection has one tab stop and exact labels disclose approximate coordinates", () => {
  const markup = renderToStaticMarkup(<DashboardVisualization
    panelTitle="Latency panel"
    rows={[row(1), row(2, 9_007_199_254_740_995n)]}
    schema={schema}
    spec={spec(VisualizationType.VISUALIZATION_TYPE_LINE)}
  />);
  assert.equal((markup.match(/tabindex="0"/gu) ?? []).length, 1);
  assert.equal((markup.match(/tabindex="-1"/gu) ?? []).length, 1);
  assert.match(markup, /Position.*Latency/su);
  assert.match(markup, /positions are approximate; displayed values are exact/u);
  assert.match(markup, /aria-label="1; inspect chart values"/u);
  assert.match(markup, /aria-label="Chart values for 1"/u);
});

test("column rows retain distinct category slots when numeric X labels repeat", () => {
  const first = row(1);
  const second = row(2);
  second.cells[0] = { kind: { $case: "sint64Value", value: 1n } };
  const markup = renderToStaticMarkup(<DashboardVisualization
    panelTitle="Repeated categories"
    rows={[first, second]}
    schema={schema}
    spec={spec(VisualizationType.VISUALIZATION_TYPE_COLUMN)}
  />);
  const positions = [...markup.matchAll(/operations-dashboard-chart-column[^>]+\sx="([^"]+)"/gu)].map((match) => match[1]);
  assert.equal(positions.length, 2);
  assert.notEqual(positions[0], positions[1]);
});

test("incompatible typed rows render an alert and exact inspection table without a chart", () => {
  const invalid = row(1);
  invalid.cells[1] = { kind: { $case: "stringValue", value: "not-a-number" } };
  const markup = renderToStaticMarkup(<DashboardVisualization
    panelTitle="Broken panel"
    rows={[invalid]}
    schema={schema}
    spec={spec(VisualizationType.VISUALIZATION_TYPE_SCATTER)}
  />);
  assert.match(markup, /role="alert"/u);
  assert.match(markup, /Visualization compatibility/u);
  assert.match(markup, /Inspect rows/u);
  assert.match(markup, /Rows that could not be plotted/u);
  assert.match(markup, /not-a-number/u);
  assert.doesNotMatch(markup, /role="img"/u);
});

test("legacy tables expose cursor controls and charts disclose capped coverage", () => {
  const table = renderToStaticMarkup(<DashboardVisualization
    onNextPage={() => undefined}
    onPreviousPage={() => undefined}
    pageNumber={2}
    panelTitle="Legacy"
    rows={[row(1)]}
    schema={schema}
    spec={undefined}
  />);
  assert.match(table, /Previous results page/u);
  assert.match(table, /Next results page/u);
  assert.match(table, /Page 2/u);
  const chart = renderToStaticMarkup(<DashboardVisualization
    capped
    complete={false}
    panelTitle="Capped"
    rows={[row(1)]}
    schema={schema}
    spec={spec(VisualizationType.VISUALIZATION_TYPE_LINE)}
  />);
  assert.match(chart, /first 10,000 rows/u);
  assert.match(chart, /More rows are available/u);
});
