import assert from "node:assert/strict";
import test from "node:test";
import { renderToStaticMarkup } from "react-dom/server";

import { ResultRow, ResultSchema, VisualizationSpec, VisualizationStackMode, VisualizationType } from "@/gen/ts/open_splunk/result";
import { DashboardVisualization } from "./dashboard-visualization";

const schema = ResultSchema.fromPartial({
  schemaId: "chart-geometry", revision: 1n,
  columns: [{ fieldName: "category" }, { fieldName: "first" }, { fieldName: "second" }],
});
const rows = [ResultRow.fromPartial({
  rowId: "row-1", cells: [
    { kind: { $case: "stringValue", value: "Service" } },
    { kind: { $case: "sint64Value", value: 2n } },
    { kind: { $case: "sint64Value", value: 3n } },
  ],
})];

function markup(type: VisualizationType, stackMode = VisualizationStackMode.VISUALIZATION_STACK_MODE_NONE) {
  return renderToStaticMarkup(<DashboardVisualization
    panelTitle="Geometry"
    schema={schema}
    rows={rows}
    spec={VisualizationSpec.fromPartial({ type, stackMode, xField: "category", yFields: ["first", "second"] })}
  />);
}

function elements(source: string, tag: string, className: string): Array<Record<string, string>> {
  return [...source.matchAll(new RegExp(`<${tag}\\b([^>]*)>`, "gu"))].map((match) =>
    Object.fromEntries([...match[1].matchAll(/([\w-]+)="([^"]*)"/gu)].map((attribute) => [attribute[1], attribute[2]])),
  ).filter((attributes) => attributes.class?.split(" ").includes(className));
}

test("unstacked measures occupy separate column positions", () => {
  const columns = elements(markup(VisualizationType.VISUALIZATION_TYPE_COLUMN), "rect", "operations-dashboard-chart-column");
  assert.equal(columns.length, 2);
  const ranges = columns.map((column) => ({ left: Number(column.x), right: Number(column.x) + Number(column.width) }));
  assert.ok(ranges[0].right <= ranges[1].left || ranges[1].right <= ranges[0].left, "independent measures must not overlap");
});

test("stacked horizontal measures share a row and continue from the previous segment", () => {
  const bars = elements(markup(VisualizationType.VISUALIZATION_TYPE_BAR, VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED), "rect", "operations-dashboard-chart-bar");
  assert.equal(bars.length, 2);
  assert.equal(bars[0].y, bars[1].y);
  assert.equal(bars[0].height, bars[1].height);
  assert.ok(Math.abs(Number(bars[1].x) - Number(bars[0].x) - Number(bars[0].width)) < 0.001);
});

test("a one-row line retains visible measure markers", () => {
  const points = elements(markup(VisualizationType.VISUALIZATION_TYPE_LINE), "circle", "operations-dashboard-chart-point");
  assert.equal(points.length, 2);
});

test("an exact numeric data label is never prefixed with approximation notation", () => {
  const largeRows = [ResultRow.fromPartial({ ...rows[0], cells: [rows[0].cells[0], { kind: { $case: "uint64Value", value: 9007199254740993n } }, rows[0].cells[2]] })];
  const source = renderToStaticMarkup(<DashboardVisualization panelTitle="Exact" schema={schema} rows={largeRows} spec={VisualizationSpec.fromPartial({
    type: VisualizationType.VISUALIZATION_TYPE_COLUMN, xField: "category", yFields: ["first"], showDataLabels: true,
  })} />);
  assert.match(source, />9007199254740993<\/text>/u);
  assert.doesNotMatch(source, /≈9007199254740993/u);
  assert.match(source, /positions are approximate/u);
});

test("stack overflow discloses approximate positions while retaining exact source labels", () => {
  const hugeRows = [ResultRow.fromPartial({ ...rows[0], cells: [rows[0].cells[0], { kind: { $case: "doubleValue", value: 1e308 } }, { kind: { $case: "doubleValue", value: 1e308 } }] })];
  const source = renderToStaticMarkup(<DashboardVisualization panelTitle="Overflow" schema={schema} rows={hugeRows} spec={VisualizationSpec.fromPartial({
    type: VisualizationType.VISUALIZATION_TYPE_COLUMN, xField: "category", yFields: ["first", "second"], stackMode: VisualizationStackMode.VISUALIZATION_STACK_MODE_STACKED,
  })} />);
  assert.match(source, /positions are approximate/u);
  assert.doesNotMatch(source, /NaN|Infinity/u);
  assert.match(source, /1e\+308/u);
});
