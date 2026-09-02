import assert from "node:assert/strict";
import test from "node:test";

import type { ComponentProps } from "react";
import { renderToStaticMarkup } from "react-dom/server";

import type { TimelinePoint } from "@/lib/demo/search-data";
import type { WorkspaceStatistic } from "@/lib/search/backend-data";

import { VisualizationPanel } from "./visualization-panel";

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
  assert.match(markup, /<span>Stacking<\/span><select>/u);
  assert.match(markup, /<option value="stacked100" selected="">100%<\/option>/u);
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

test("horizontal categorical series render cumulative stacked geometry", () => {
  const markup = renderPanel({
    chartStyle: "horizontal",
    stackMode: "stacked",
    statisticsRows: categoricalRows,
  });

  assert.match(markup, /visualization-horizontal-bars is-stacked/u);
  assert.match(markup, /data-chart-end="2" data-chart-raw="2" data-chart-start="0"/u);
  assert.match(markup, /data-chart-end="5" data-chart-raw="3" data-chart-start="2"/u);
  assert.match(markup, /<span>Stacking<\/span><select>/u);
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
