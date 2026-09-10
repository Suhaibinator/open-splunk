import assert from "node:assert/strict";
import test from "node:test";

import type { ComponentProps } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import type { TimelinePoint } from "@/lib/demo/search-data";
import {
  ColumnSemanticType,
  ResultSetKind,
  type ResultRow,
  type ResultSchema,
} from "@/gen/ts/open_splunk/result";
import { NullValue, ValueType } from "@/gen/ts/open_splunk/value";
import { adaptSearchResults, type WorkspaceStatistic } from "@/lib/search/backend-data";

import {
  VisualizationPanel,
  categoricalChartModel,
  categoricalStackWindow,
} from "./visualization-panel";

const timechartPoints: TimelinePoint[] = [
  { id: "first", label: "00:00", count: 5, series: { east: 2, west: 3 } },
  { id: "second", label: "01:00", count: 5, series: { east: 4, west: 1 } },
];

const categoricalRows: WorkspaceStatistic[] = [{
  id: "api",
  level: "api",
  count: 2,
  percent: "100%",
  avgDuration: 0,
  series: [
    { key: "success", label: "Success", value: 2 },
    { key: "failure", label: "Failure", value: 3 },
  ],
}];

const baseProps = {
  chartStyle: "column",
  chartTitle: "Results",
  isPreview: false,
  isTimechartResult: false,
  legendPosition: "bottom",
  showDataLabels: false,
  stackMode: "none",
  statisticsDimension: "service",
  statisticsRows: [] as WorkspaceStatistic[],
  timechartCoverage: null,
  timelinePoints: [] as TimelinePoint[],
  onApplyPivot: () => undefined,
  onChartStyleChange: () => undefined,
  onChartTitleChange: () => undefined,
  onLegendPositionChange: () => undefined,
  onShowDataLabelsChange: () => undefined,
  onStackModeChange: () => undefined,
  onVisualizationEdited: () => undefined,
  previewTruncated: false,
} satisfies ComponentProps<typeof VisualizationPanel>;

function renderPanel(
  overrides: Partial<ComponentProps<typeof VisualizationPanel>>,
): string {
  return renderToStaticMarkup(<VisualizationPanel {...baseProps} {...overrides} />);
}

test("split timecharts expose Area and an accessible stacking selector", () => {
  const markup = renderPanel({
    chartStyle: "area",
    isTimechartResult: true,
    stackMode: "stacked100",
    timelinePoints: timechartPoints,
  });

  assert.match(markup, /data-chart-style="area"/u);
  assert.match(markup, /data-stack-mode="stacked100"/u);
  assert.match(markup, /> Area<\/button>/u);
  assert.match(markup, /<span>Stacking<\/span><div class="select">/u);
  assert.match(markup, /role="combobox"[^>]*><span class="select__value">100%/u);
  assert.match(markup, /role="option" aria-selected="true"[^>]*>100%/u);
});

test("single-series timecharts force unsupported stacking to none", () => {
  const markup = renderPanel({
    chartStyle: "area",
    isTimechartResult: true,
    stackMode: "stacked",
    timelinePoints: timechartPoints.map(({ count, id, label }) => ({ count, id, label })),
  });

  assert.match(markup, /data-chart-style="area"/u);
  assert.match(markup, /data-stack-mode="none"/u);
  assert.doesNotMatch(markup, /<span>Stacking<\/span>/u);
});

test("wide timecharts bound rendered series and expose paging controls", () => {
  const series = Object.fromEntries(Array.from({ length: 30 }, (_, index) => [`series-${index + 1}`, index + 1]));
  const markup = renderPanel({
    chartStyle: "line",
    isTimechartResult: true,
    timelinePoints: [{ id: "wide", label: "00:00", count: 465, series }],
  });

  assert.equal((markup.match(/class="time-series-chart__line time-series-chart__series"/gu) ?? []).length, 24);
  assert.match(markup, /Showing 1–24 of 30/u);
  assert.match(markup, />Previous series<\/button>/u);
  assert.match(markup, />Next series<\/button>/u);
  assert.doesNotMatch(markup, /data-series-name="series-25"/u);
});

test("adapts and renders timecharts wider than 64 columns", () => {
  const seriesCount = 65;
  const schema: ResultSchema = {
    schemaId: "wide-timechart",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_TIME_SERIES,
    columns: [
      {
        fieldName: "_time",
        displayName: "_time",
        valueType: ValueType.VALUE_TYPE_TIMESTAMP,
        semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
        nullable: false,
        multivalue: false,
        hiddenByDefault: false,
        statsSparkline: false,
      },
      ...Array.from({ length: seriesCount }, (_, index) => ({
        fieldName: `series-${index + 1}`,
        displayName: `series-${index + 1}`,
        valueType: ValueType.VALUE_TYPE_UINT64,
        semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
        nullable: false,
        multivalue: false,
        hiddenByDefault: false,
        statsSparkline: false,
      })),
    ],
  };
  const row: ResultRow = {
    rowId: "wide-row",
    ordinal: 0n,
    cells: [
      { kind: { $case: "timestampValue", value: new Date("2026-09-10T00:00:00Z") } },
      ...Array.from({ length: seriesCount }, () => ({
        kind: { $case: "uint64Value" as const, value: 1n },
      })),
    ],
    timeBucket: {
      earliest: "2026-09-10T00:00:00Z",
      latest: "2026-09-10T00:00:00.000000001Z",
    },
  };
  const adapted = adaptSearchResults(schema, [row]);
  const markup = renderPanel({
    chartStyle: "line",
    isTimechartResult: true,
    timelinePoints: adapted.timeline,
  });

  assert.equal(Object.keys(adapted.timeline[0]?.series ?? {}).length, seriesCount);
  assert.equal((markup.match(/class="time-series-chart__line time-series-chart__series"/gu) ?? []).length, 24);
  assert.match(markup, /Showing 1–24 of 65/u);
});

test("adapted null timechart buckets remain visible gaps in the chart", () => {
  const schema: ResultSchema = {
    schemaId: "nullable-timechart",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_TIME_SERIES,
    columns: [
      {
        fieldName: "_time",
        displayName: "_time",
        valueType: ValueType.VALUE_TYPE_TIMESTAMP,
        semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_EVENT_TIME,
        nullable: false,
        multivalue: false,
        hiddenByDefault: false,
        statsSparkline: false,
      },
      {
        fieldName: "avg(metric)",
        displayName: "avg(metric)",
        valueType: ValueType.VALUE_TYPE_DOUBLE,
        semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
        nullable: true,
        multivalue: false,
        hiddenByDefault: false,
        statsSparkline: false,
      },
    ],
  };
  const values = [
    { kind: { $case: "doubleValue" as const, value: 1 } },
    { kind: { $case: "nullValue" as const, value: NullValue.NULL_VALUE_NULL } },
    { kind: { $case: "doubleValue" as const, value: 2 } },
  ];
  const rows: ResultRow[] = values.map((value, index) => ({
    rowId: `bucket-${index}`,
    ordinal: BigInt(index),
    cells: [
      { kind: { $case: "timestampValue", value: new Date(`2026-09-10T0${index}:00:00Z`) } },
      value,
    ],
    timeBucket: {
      earliest: `2026-09-10T0${index}:00:00Z`,
      latest: `2026-09-10T0${index + 1}:00:00Z`,
    },
  }));
  const adapted = adaptSearchResults(schema, rows);
  const markup = renderPanel({
    chartStyle: "line",
    isTimechartResult: true,
    timelinePoints: adapted.timeline,
  });

  assert.equal(adapted.timeline.length, 3);
  assert.deepEqual(adapted.timeline[1]?.series, { "avg(metric)": null });
  assert.equal((markup.match(/class="time-series-chart__line time-series-chart__series"/gu) ?? []).length, 2);
  assert.match(markup, /avg\(metric\)/u);
});

test("default column timecharts preserve exact sparse bucket positions and inspection bounds", () => {
  const origin = 1_789_027_750_123_456_789n;
  const timelinePoints: TimelinePoint[] = [
    {
      id: "first",
      label: "first",
      count: 2,
      earliest: "2026-09-10T00:00:00.000000001Z",
      latest: "2026-09-10T00:00:00.250000001Z",
      timeCoordinateNanoseconds: origin,
      timeLatestCoordinateNanoseconds: origin + 250_000_000n,
    },
    {
      id: "last",
      label: "last",
      count: 3,
      earliest: "2026-09-10T00:00:00.750000001Z",
      latest: "2026-09-10T00:00:01.000000001Z",
      timeCoordinateNanoseconds: origin + 750_000_000n,
      timeLatestCoordinateNanoseconds: origin + 1_000_000_000n,
    },
  ];
  const markup = renderPanel({
    chartStyle: "column",
    isTimechartResult: true,
    timelinePoints,
  });

  assert.match(markup, /data-chart-style="column"/u);
  assert.equal((markup.match(/class="time-series-chart__columns time-series-chart__series"/gu) ?? []).length, 1);
  assert.match(markup, /d="M0\.00,[\d.]+V[\d.]+H250\.00V[\d.]+ZM750\.00,[\d.]+V[\d.]+H1000\.00V[\d.]+Z"/u);
  assert.doesNotMatch(markup, /<button[^>]+exact bucket/u);
});

test("horizontal categorical series render cumulative stacked geometry", () => {
  const markup = renderPanel({
    chartStyle: "horizontal",
    stackMode: "stacked",
    statisticsRows: categoricalRows,
  });

  assert.match(markup, /visualization-horizontal-bars is-stacked/u);
  assert.match(markup, /data-chart-end="2" data-chart-raw="2" data-chart-start="0"/u);
  assert.match(markup, /data-chart-end="5" data-chart-raw="3" data-chart-start="2"/u);
  assert.match(markup, /<span>Stacking<\/span><div class="select">/u);
});

test("vertical categorical series use the same stacked baselines", () => {
  const markup = renderPanel({
    chartStyle: "column",
    stackMode: "stacked",
    statisticsRows: categoricalRows,
  });

  assert.match(markup, /visualization-vertical-bars is-stacked/u);
  assert.match(markup, /data-chart-end="5" data-chart-raw="3" data-chart-start="2"/u);
});

test("wide categorical charts render a bounded series window with indexed row access", () => {
  const rowCount = 12;
  const seriesCount = 1_024;
  const schema: ResultSchema = {
    schemaId: "wide-categorical",
    revision: 1n,
    resultKind: ResultSetKind.RESULT_SET_KIND_STATISTICS,
    columns: [
      ...Array.from({ length: seriesCount }, (_value, index) => ({
        fieldName: `series-${index + 1}`,
        displayName: `series-${index + 1}`,
        valueType: ValueType.VALUE_TYPE_UINT64,
        semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_METRIC,
        nullable: false,
        multivalue: false,
        hiddenByDefault: false,
        statsSparkline: false,
      })),
      {
        fieldName: "category",
        displayName: "category",
        valueType: ValueType.VALUE_TYPE_STRING,
        semanticType: ColumnSemanticType.COLUMN_SEMANTIC_TYPE_DIMENSION,
        nullable: false,
        multivalue: false,
        hiddenByDefault: false,
        statsSparkline: false,
      },
    ],
  };
  const rows: ResultRow[] = Array.from({ length: rowCount }, (_value, rowIndex) => ({
    rowId: `row-${rowIndex}`,
    ordinal: BigInt(rowIndex),
    cells: [
      ...Array.from({ length: seriesCount }, (_item, seriesIndex) => ({
        kind: { $case: "uint64Value" as const, value: BigInt(rowIndex + seriesIndex + 1) },
      })),
      { kind: { $case: "stringValue" as const, value: `row-${rowIndex}` } },
    ],
    timeBucket: undefined,
  }));
  const adapted = adaptSearchResults(schema, rows);
  let seriesEntryReads = 0;
  const statisticsRows = adapted.statistics.map((row): WorkspaceStatistic => ({
    ...row,
    series: new Proxy(row.series ?? [], {
        get(target, property, receiver) {
          if (/^\d+$/u.test(String(property))) seriesEntryReads += 1;
          return Reflect.get(target, property, receiver);
        },
      }),
  }));
  const markup = renderPanel({ chartStyle: "column", statisticsRows });

  assert.equal(adapted.statistics[0]?.series?.length, seriesCount);
  assert.equal(seriesEntryReads, rowCount * seriesCount);
  assert.equal((markup.match(/class="visualization-vertical-bar"/gu) ?? []).length, rowCount * 24);
  assert.match(markup, /Showing 1–24 of 1,024/u);
  assert.match(markup, />Previous series<\/button>/u);
  assert.match(markup, />Next series<\/button>/u);
  assert.doesNotMatch(markup, />series-25<\/span>/u);
  assert.ok(markup.length < 300_000, `wide categorical markup was ${markup.length} bytes`);

  const model = categoricalChartModel(statisticsRows);
  const secondWindow = categoricalStackWindow(model, 24, 48, "none");
  assert.equal(secondWindow.rows[0]?.length, 24);
  assert.equal(secondWindow.rows[0]?.[0]?.raw, 25);
  assert.deepEqual(secondWindow.domain, model.domains.none);
});

test("stacked categorical windows retain baselines from hidden preceding series", () => {
  const series = Array.from({ length: 30 }, (_value, index) => ({
    key: `series-${index + 1}`,
    label: `series-${index + 1}`,
    value: 1,
  }));
  const model = categoricalChartModel([{
    id: "wide",
    level: "wide",
    count: 30,
    percent: "100%",
    avgDuration: 0,
    series,
  }]);
  const window = categoricalStackWindow(model, 24, 30, "stacked100");

  assert.deepEqual(window.domain, [0, 100]);
  assert.equal(window.rows[0]?.length, 6);
  assert.equal(window.rows[0]?.[0]?.start, 80);
  assert.ok(Math.abs((window.rows[0]?.[0]?.end ?? 0) - (250 / 3)) < Number.EPSILON * 100);
});

test("legacy categorical results do not offer or apply stacking", () => {
  const markup = renderPanel({
    stackMode: "stacked100",
    statisticsRows: [{
      level: "INFO",
      count: 4,
      percent: "100%",
      avgDuration: 1,
    }],
  });

  assert.match(markup, /data-stack-mode="none"/u);
  assert.doesNotMatch(markup, /is-stacked/u);
  assert.doesNotMatch(markup, /<span>Stacking<\/span>/u);
});
